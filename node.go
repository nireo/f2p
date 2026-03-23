package f2p

import (
	"bytes"
	"fmt"
	"io"
	"sync"
)

type Node struct {
	Info      NodeInfo
	rt        *RoutingTable
	store     map[Key][]byte
	providers map[Key]map[Key]NodeInfo
	chunks    map[Key][]byte
	manifests map[Key]ContentManifest
	transport Transport
	mu        sync.RWMutex
}

func NewNode(info NodeInfo, transport Transport) *Node {
	if transport == nil {
		transport = NewRPCTransport(0)
	}

	node := &Node{
		Info:      info,
		rt:        &RoutingTable{LocalID: info.ID},
		store:     make(map[Key][]byte),
		providers: make(map[Key]map[Key]NodeInfo),
		chunks:    make(map[Key][]byte),
		manifests: make(map[Key]ContentManifest),
		transport: transport,
	}
	node.rt.SetPingFunc(node.pingContact)
	return node
}

func (n *Node) JoinNetwork(bootstrap NodeInfo) error {
	if err := n.ensureState(); err != nil {
		return err
	}

	if bootstrap.ID == n.rt.LocalID {
		return nil
	}

	transport := n.transportClient()
	if transport == nil {
		return errTransportNotInitialized
	}

	var pingReply PingReply
	if err := transport.Ping(bootstrap, PingArgs{Sender: n.localInfo()}, &pingReply); err != nil {
		return err
	}

	if pingReply.Receiver.ID != (Key{}) {
		n.rt.UpdateContact(pingReply.Receiver)
	} else {
		n.rt.UpdateContact(bootstrap)
	}

	_ = n.LookupNode(n.rt.LocalID)
	return nil
}

func (n *Node) StoreValue(key Key, value []byte) error {
	if err := n.ensureState(); err != nil {
		return err
	}

	n.setValue(key, value)
	closest := n.LookupNode(key)
	if len(closest) == 0 {
		return nil
	}

	var firstErr error
	storedRemotely := false
	for _, result := range n.storeBatch(closest, key, value) {
		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
			}
			continue
		}

		storedRemotely = true
		n.rt.UpdateContact(result.contact)
	}

	if storedRemotely || firstErr == nil {
		return nil
	}

	return firstErr
}

func (n *Node) AnnounceProvider(key Key) error {
	if err := n.ensureState(); err != nil {
		return err
	}

	provider := n.localInfo()
	n.addProvider(key, provider)

	closest := n.LookupNode(key)
	if len(closest) == 0 {
		return nil
	}

	var firstErr error
	announcedRemotely := false
	for _, result := range n.addProviderBatch(closest, key, provider) {
		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
			}
			continue
		}

		announcedRemotely = true
		n.rt.UpdateContact(result.contact)
	}

	if announcedRemotely || firstErr == nil {
		return nil
	}

	return firstErr
}

func (n *Node) Ping(args PingArgs, reply *PingReply) error {
	if err := n.ensureState(); err != nil {
		return err
	}

	n.rt.UpdateContact(args.Sender)
	reply.Receiver = n.localInfo()
	return nil
}

func (n *Node) Store(args StoreArgs, reply *StoreReply) error {
	if err := n.ensureState(); err != nil {
		return err
	}

	n.rt.UpdateContact(args.Sender)
	n.setValue(args.Key, args.Value)
	return nil
}

func (n *Node) FindNode(args FindNodeArgs, reply *FindNodeReply) error {
	if err := n.ensureState(); err != nil {
		return err
	}

	n.rt.UpdateContact(args.Sender)
	reply.Nodes = n.rt.FindClosest(args.Target, parameterK)
	return nil
}

func (n *Node) FindValue(args FindValueArgs, reply *FindValueReply) error {
	if err := n.ensureState(); err != nil {
		return err
	}

	n.rt.UpdateContact(args.Sender)
	if value, ok := n.getValue(args.Key); ok {
		reply.Value = value
		reply.Found = true
		reply.Nodes = nil
		return nil
	}

	reply.Found = false
	reply.Value = nil
	reply.Nodes = n.rt.FindClosest(args.Key, parameterK)
	return nil
}

func (n *Node) AddProvider(args AddProviderArgs, reply *AddProviderReply) error {
	if err := n.ensureState(); err != nil {
		return err
	}

	n.rt.UpdateContact(args.Sender)
	provider := args.Provider
	if provider.ID == (Key{}) {
		provider = args.Sender
	}
	n.addProvider(args.Key, provider)
	return nil
}

func (n *Node) FindProviders(args FindProvidersArgs, reply *FindProvidersReply) error {
	if err := n.ensureState(); err != nil {
		return err
	}

	n.rt.UpdateContact(args.Sender)
	reply.Providers = n.getProviders(args.Key)
	if len(reply.Providers) > 0 {
		reply.Nodes = nil
		return nil
	}

	reply.Nodes = n.rt.FindClosest(args.Key, parameterK)
	return nil
}

func (n *Node) FetchChunk(args FetchChunkArgs, reply *FetchChunkReply) error {
	if err := n.ensureState(); err != nil {
		return err
	}

	n.rt.UpdateContact(args.Sender)
	chunk, ok := n.getChunk(args.Key)
	if !ok {
		return errChunkNotFound
	}

	reply.Data = chunk
	return nil
}

func (n *Node) ensureState() error {
	if n == nil {
		return errNodeNotInitialized
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.rt == nil {
		n.rt = &RoutingTable{}
	}
	if n.rt.LocalID == (Key{}) {
		n.rt.LocalID = n.Info.ID
	}
	if n.Info.ID == (Key{}) {
		n.Info.ID = n.rt.LocalID
	}
	if n.rt.LocalID == (Key{}) {
		return errNodeNotInitialized
	}
	if n.rt.pingFunc() == nil {
		n.rt.SetPingFunc(n.pingContact)
	}
	if n.store == nil {
		n.store = make(map[Key][]byte)
	}
	if n.providers == nil {
		n.providers = make(map[Key]map[Key]NodeInfo)
	}
	if n.chunks == nil {
		n.chunks = make(map[Key][]byte)
	}
	if n.manifests == nil {
		n.manifests = make(map[Key]ContentManifest)
	}
	if n.transport == nil {
		n.transport = NewRPCTransport(0)
	}

	return nil
}

func (n *Node) localInfo() NodeInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()

	info := n.Info
	if info.ID == (Key{}) && n.rt != nil {
		info.ID = n.rt.LocalID
	}
	return info
}

func (n *Node) getValue(key Key) ([]byte, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	value, ok := n.store[key]
	if !ok {
		return nil, false
	}

	return cloneBytes(value), true
}

func (n *Node) setValue(key Key, value []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.store == nil {
		n.store = make(map[Key][]byte)
	}
	n.store[key] = cloneBytes(value)
}

func (n *Node) getProviders(key Key) []NodeInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()

	providerSet := n.providers[key]
	if len(providerSet) == 0 {
		return nil
	}

	providers := make([]NodeInfo, 0, len(providerSet))
	for _, provider := range providerSet {
		providers = append(providers, provider)
	}
	sortProviders(providers)
	return providers
}

func (n *Node) addProvider(key Key, provider NodeInfo) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.providers == nil {
		n.providers = make(map[Key]map[Key]NodeInfo)
	}
	if n.providers[key] == nil {
		n.providers[key] = make(map[Key]NodeInfo)
	}
	n.providers[key][provider.ID] = provider
}

func (n *Node) addProviders(key Key, providers []NodeInfo) {
	for _, provider := range providers {
		n.addProvider(key, provider)
	}
}

func (n *Node) getChunk(key Key) ([]byte, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	chunk, ok := n.chunks[key]
	if !ok {
		return nil, false
	}

	return cloneBytes(chunk), true
}

func (n *Node) setChunk(key Key, data []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.chunks == nil {
		n.chunks = make(map[Key][]byte)
	}
	n.chunks[key] = cloneBytes(data)
}

func (n *Node) getManifest(key Key) (ContentManifest, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	manifest, ok := n.manifests[key]
	if !ok {
		return ContentManifest{}, false
	}

	return cloneManifest(manifest), true
}

func (n *Node) setManifest(manifest ContentManifest) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.manifests == nil {
		n.manifests = make(map[Key]ContentManifest)
	}
	n.manifests[manifest.Key] = cloneManifest(manifest)
}

func (n *Node) transportClient() Transport {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.transport
}

func (n *Node) LookupNode(target Key) []NodeInfo {
	if err := n.ensureState(); err != nil {
		return nil
	}

	return n.iterativeLookup(target, func(batch []NodeInfo) []lookupResponse {
		return n.findNodeBatch(batch, target)
	}, nil)
}

func (n *Node) LookupValue(key Key) ([]byte, bool, error) {
	if err := n.ensureState(); err != nil {
		return nil, false, err
	}

	if value, ok := n.getValue(key); ok {
		return value, true, nil
	}

	var (
		found    bool
		value    []byte
		firstErr error
	)

	n.iterativeLookup(key, func(batch []NodeInfo) []lookupResponse {
		return n.findValueBatch(batch, key)
	}, func(results []lookupResponse) bool {
		for _, result := range results {
			if result.err != nil {
				if firstErr == nil {
					firstErr = result.err
				}
				continue
			}
			if !result.found {
				continue
			}

			value = cloneBytes(result.value)
			found = true
			return true
		}

		return false
	})

	if found {
		n.setValue(key, value)
		return cloneBytes(value), true, nil
	}

	return nil, false, firstErr
}

func (n *Node) LookupProviders(key Key) ([]NodeInfo, error) {
	if err := n.ensureState(); err != nil {
		return nil, err
	}

	if providers := n.getProviders(key); len(providers) > 0 {
		return providers, nil
	}

	var (
		providers []NodeInfo
		firstErr  error
	)

	n.iterativeLookup(key, func(batch []NodeInfo) []lookupResponse {
		return n.findProvidersBatch(batch, key)
	}, func(results []lookupResponse) bool {
		for _, result := range results {
			if result.err != nil {
				if firstErr == nil {
					firstErr = result.err
				}
				continue
			}
			if len(result.providers) == 0 {
				continue
			}

			providers = mergeProviders(providers, result.providers)
		}

		return len(providers) > 0
	})

	if len(providers) > 0 {
		n.addProviders(key, providers)
		return providers, nil
	}

	return nil, firstErr
}

func (n *Node) PublishContent(name string, reader io.Reader, chunkSize int) (ContentManifest, error) {
	if err := n.ensureState(); err != nil {
		return ContentManifest{}, err
	}

	bundle, err := BuildContentBundle(name, reader, chunkSize)
	if err != nil {
		return ContentManifest{}, err
	}

	provider := n.localInfo()
	for _, chunk := range bundle.Chunks {
		n.setChunk(chunk.Ref.Key, chunk.Data)
		n.addProvider(chunk.Ref.Key, provider)
	}
	n.setManifest(bundle.Manifest)
	n.addProvider(bundle.Manifest.Key, provider)

	manifestData, err := bundle.Manifest.MarshalBinary()
	if err != nil {
		return ContentManifest{}, err
	}
	if err := n.StoreValue(bundle.Manifest.Key, manifestData); err != nil {
		return ContentManifest{}, err
	}

	var firstErr error
	if err := n.AnnounceProvider(bundle.Manifest.Key); err != nil && firstErr == nil {
		firstErr = err
	}
	for _, chunk := range bundle.Chunks {
		if err := n.AnnounceProvider(chunk.Ref.Key); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return bundle.Manifest, firstErr
}

func (n *Node) PublishContentBytes(name string, data []byte, chunkSize int) (ContentManifest, error) {
	return n.PublishContent(name, bytes.NewReader(data), chunkSize)
}

func (n *Node) LookupManifest(key Key) (ContentManifest, bool, error) {
	if err := n.ensureState(); err != nil {
		return ContentManifest{}, false, err
	}

	if manifest, ok := n.getManifest(key); ok {
		return manifest, true, nil
	}

	raw, found, err := n.LookupValue(key)
	if err != nil {
		return ContentManifest{}, false, err
	}
	if !found {
		return ContentManifest{}, false, nil
	}

	manifest, err := ParseContentManifest(raw)
	if err != nil {
		return ContentManifest{}, false, err
	}
	if manifest.Key != key {
		return ContentManifest{}, false, fmt.Errorf("%w: manifest lookup key mismatch", errInvalidManifest)
	}

	n.setManifest(manifest)
	return manifest, true, nil
}

func (n *Node) DownloadContent(key Key) (DownloadedContent, error) {
	if err := n.ensureState(); err != nil {
		return DownloadedContent{}, err
	}

	manifest, found, err := n.LookupManifest(key)
	if err != nil {
		return DownloadedContent{}, err
	}
	if !found {
		return DownloadedContent{}, errManifestNotFound
	}

	data := make([]byte, 0)
	for _, chunk := range manifest.Chunks {
		chunkData, err := n.loadChunk(chunk)
		if err != nil {
			return DownloadedContent{}, err
		}
		data = append(data, chunkData...)
	}
	if int64(len(data)) != manifest.Size {
		return DownloadedContent{}, fmt.Errorf("%w: downloaded size mismatch", errInvalidManifest)
	}

	n.setManifest(manifest)
	return DownloadedContent{Manifest: manifest, Data: data}, nil
}

func (n *Node) iterativeLookup(target Key, request func([]NodeInfo) []lookupResponse, handleBatch func([]lookupResponse) bool) []NodeInfo {
	closest := n.rt.FindClosest(target, parameterK)
	queried := make(map[Key]struct{}, len(closest))

	for {
		batch := nextLookupBatch(closest, queried, alpha)
		if len(batch) == 0 {
			return closest
		}

		results := request(batch)
		for _, result := range results {
			if result.err != nil {
				continue
			}

			n.rt.UpdateContact(result.contact)
			for _, node := range result.nodes {
				n.rt.UpdateContact(node)
			}
			closest = mergeClosest(target, n.rt.LocalID, closest, result.nodes, parameterK)
		}

		if handleBatch != nil && handleBatch(results) {
			return closest
		}
	}
}

func (n *Node) findNodeBatch(batch []NodeInfo, target Key) []lookupResponse {
	local := n.localInfo()
	transport := n.transportClient()
	if transport == nil {
		return parallelContacts(batch, func(contact NodeInfo) lookupResponse {
			return lookupResponse{contact: contact, err: errTransportNotInitialized}
		})
	}

	return parallelContacts(batch, func(contact NodeInfo) lookupResponse {
		var reply FindNodeReply
		err := transport.FindNode(contact, FindNodeArgs{
			Sender: local,
			Target: target,
		}, &reply)
		return lookupResponse{contact: contact, nodes: reply.Nodes, err: err}
	})
}

func (n *Node) storeBatch(batch []NodeInfo, key Key, value []byte) []operationResponse {
	local := n.localInfo()
	transport := n.transportClient()
	if transport == nil {
		return parallelContacts(batch, func(contact NodeInfo) operationResponse {
			return operationResponse{contact: contact, err: errTransportNotInitialized}
		})
	}

	return parallelContacts(batch, func(contact NodeInfo) operationResponse {
		var reply StoreReply
		err := transport.Store(contact, StoreArgs{
			Sender: local,
			Key:    key,
			Value:  cloneBytes(value),
		}, &reply)
		return operationResponse{contact: contact, err: err}
	})
}

func (n *Node) findValueBatch(batch []NodeInfo, key Key) []lookupResponse {
	local := n.localInfo()
	transport := n.transportClient()
	if transport == nil {
		return parallelContacts(batch, func(contact NodeInfo) lookupResponse {
			return lookupResponse{contact: contact, err: errTransportNotInitialized}
		})
	}

	return parallelContacts(batch, func(contact NodeInfo) lookupResponse {
		var reply FindValueReply
		err := transport.FindValue(contact, FindValueArgs{
			Sender: local,
			Key:    key,
		}, &reply)
		return lookupResponse{
			contact: contact,
			value:   reply.Value,
			found:   reply.Found,
			nodes:   reply.Nodes,
			err:     err,
		}
	})
}

func (n *Node) addProviderBatch(batch []NodeInfo, key Key, provider NodeInfo) []operationResponse {
	local := n.localInfo()
	transport := n.transportClient()
	if transport == nil {
		return parallelContacts(batch, func(contact NodeInfo) operationResponse {
			return operationResponse{contact: contact, err: errTransportNotInitialized}
		})
	}

	return parallelContacts(batch, func(contact NodeInfo) operationResponse {
		var reply AddProviderReply
		err := transport.AddProvider(contact, AddProviderArgs{
			Sender:   local,
			Key:      key,
			Provider: provider,
		}, &reply)
		return operationResponse{contact: contact, err: err}
	})
}

func (n *Node) findProvidersBatch(batch []NodeInfo, key Key) []lookupResponse {
	local := n.localInfo()
	transport := n.transportClient()
	if transport == nil {
		return parallelContacts(batch, func(contact NodeInfo) lookupResponse {
			return lookupResponse{contact: contact, err: errTransportNotInitialized}
		})
	}

	return parallelContacts(batch, func(contact NodeInfo) lookupResponse {
		var reply FindProvidersReply
		err := transport.FindProviders(contact, FindProvidersArgs{
			Sender: local,
			Key:    key,
		}, &reply)
		return lookupResponse{
			contact:   contact,
			providers: reply.Providers,
			nodes:     reply.Nodes,
			err:       err,
		}
	})
}

func (n *Node) loadChunk(ref ChunkRef) ([]byte, error) {
	if chunk, ok := n.getChunk(ref.Key); ok {
		if err := validateChunk(ref, chunk); err == nil {
			return chunk, nil
		}
	}

	providers, err := n.LookupProviders(ref.Key)
	if err != nil {
		return nil, err
	}
	transport := n.transportClient()
	if transport == nil {
		return nil, errTransportNotInitialized
	}

	sender := n.localInfo()
	var firstErr error
	for _, provider := range providers {
		if provider.ID == sender.ID {
			if chunk, ok := n.getChunk(ref.Key); ok {
				if err := validateChunk(ref, chunk); err == nil {
					return chunk, nil
				} else if firstErr == nil {
					firstErr = err
				}
			}
			continue
		}

		var reply FetchChunkReply
		err := transport.FetchChunk(provider, FetchChunkArgs{Sender: sender, Key: ref.Key}, &reply)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		n.rt.UpdateContact(provider)
		if err := validateChunk(ref, reply.Data); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		n.setChunk(ref.Key, reply.Data)
		return cloneBytes(reply.Data), nil
	}

	if firstErr != nil {
		return nil, firstErr
	}

	return nil, errChunkNotFound
}

func (n *Node) pingContact(contact NodeInfo) bool {
	transport := n.transportClient()
	if transport == nil {
		return false
	}

	var reply PingReply
	err := transport.Ping(contact, PingArgs{Sender: n.localInfo()}, &reply)
	if err != nil {
		return false
	}

	if reply.Receiver.ID != (Key{}) {
		contact = reply.Receiver
	}
	if n.rt != nil {
		n.rt.UpdateContact(contact)
	}
	return true
}

type lookupResponse struct {
	contact   NodeInfo
	nodes     []NodeInfo
	value     []byte
	found     bool
	providers []NodeInfo
	err       error
}

type operationResponse struct {
	contact NodeInfo
	err     error
}

func parallelContacts[T any](contacts []NodeInfo, fn func(NodeInfo) T) []T {
	results := make([]T, len(contacts))

	var wg sync.WaitGroup
	for i, contact := range contacts {
		wg.Add(1)
		go func(i int, contact NodeInfo) {
			defer wg.Done()
			results[i] = fn(contact)
		}(i, contact)
	}
	wg.Wait()

	return results
}

func nextLookupBatch(closest []NodeInfo, queried map[Key]struct{}, limit int) []NodeInfo {
	batch := make([]NodeInfo, 0, limit)
	for _, contact := range closest {
		if _, ok := queried[contact.ID]; ok {
			continue
		}

		queried[contact.ID] = struct{}{}
		batch = append(batch, contact)
		if len(batch) == limit {
			break
		}
	}

	return batch
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}

	cloned := make([]byte, len(value))
	copy(cloned, value)
	return cloned
}
