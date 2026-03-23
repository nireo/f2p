package f2p

import (
	"bytes"
	"errors"
	"net"
	"sort"
	"sync"
)

var errNodeNotInitialized = errors.New("node is missing routing information")

const (
	parameterK = 20
	alpha      = 3
)

type NodeID [20]byte

func (id NodeID) Distance(other NodeID) (dist NodeID) {
	for i := range len(id) {
		dist[i] = id[i] ^ other[i]
	}

	return dist
}

type KBucket struct {
	Contacts []NodeInfo
	mu       sync.RWMutex
}

func (b *KBucket) AddOrUpdate(contact NodeInfo) (evicted NodeInfo, needsPing bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, c := range b.Contacts {
		if c.ID == contact.ID {
			copy(b.Contacts[i:], b.Contacts[i+1:])
			b.Contacts[len(b.Contacts)-1] = contact
			return NodeInfo{}, false
		}
	}

	// if not full no need to evict, just add the new contact
	if len(b.Contacts) < parameterK {
		b.Contacts = append(b.Contacts, contact)
		return NodeInfo{}, false
	}

	return b.Contacts[0], true
}

func (b *KBucket) Remove(id NodeID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, c := range b.Contacts {
		if c.ID == id {
			copy(b.Contacts[i:], b.Contacts[i+1:])
			b.Contacts[len(b.Contacts)-1] = NodeInfo{}
			b.Contacts = b.Contacts[:len(b.Contacts)-1]
			return true
		}
	}

	return false
}

func (b *KBucket) Replace(id NodeID, contact NodeInfo) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, c := range b.Contacts {
		if c.ID == contact.ID {
			copy(b.Contacts[i:], b.Contacts[i+1:])
			b.Contacts[len(b.Contacts)-1] = contact
			return true
		}
	}

	for i, c := range b.Contacts {
		if c.ID != id {
			continue
		}

		copy(b.Contacts[i:], b.Contacts[i+1:])
		b.Contacts[len(b.Contacts)-1] = contact
		return true
	}

	if len(b.Contacts) < parameterK {
		b.Contacts = append(b.Contacts, contact)
		return true
	}

	return false
}

// IsFull reports whether the bucket already holds parameterK contacts.
func (b *KBucket) IsFull() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.Contacts) >= parameterK
}

type NodeInfo struct {
	ID   NodeID
	IP   net.IPAddr
	Port uint16
}

type Transport interface {
	Ping(contact NodeInfo, args PingArgs, reply *PingReply) error
	Store(contact NodeInfo, args StoreArgs, reply *StoreReply) error
	FindNode(contact NodeInfo, args FindNodeArgs, reply *FindNodeReply) error
	FindValue(contact NodeInfo, args FindValueArgs, reply *FindValueReply) error
	AddProvider(contact NodeInfo, args AddProviderArgs, reply *AddProviderReply) error
	FindProviders(contact NodeInfo, args FindProvidersArgs, reply *FindProvidersReply) error
}

type RoutingTable struct {
	LocalID NodeID
	Buckets [160]KBucket
	mu      sync.RWMutex
	ping    func(NodeInfo) bool
}

// GetBucketIndex should return the index of the first differing bit between
// LocalID and target. Return -1 when target is the local node itself.
func (rt *RoutingTable) GetBucketIndex(target NodeID) int {
	for i := range len(rt.LocalID) {
		xor := rt.LocalID[i] ^ target[i]
		if xor != 0 {
			for j := range 8 {
				if (xor & (1 << (7 - j))) != 0 {
					return i*8 + j
				}
			}
		}
	}

	return -1
}

// UpdateContact should place a contact into the correct bucket and apply the
// Kademlia rules for refreshing, splitting, or evicting entries.
func (rt *RoutingTable) UpdateContact(contact NodeInfo) {
	if rt == nil {
		return
	}

	bucketIndex := rt.GetBucketIndex(contact.ID)
	if bucketIndex < 0 {
		return
	}

	evicted, needsPing := rt.Buckets[bucketIndex].AddOrUpdate(contact)
	if !needsPing {
		return
	}

	ping := rt.pingFunc()
	if ping == nil {
		return
	}

	bucket := &rt.Buckets[bucketIndex]
	if ping(evicted) {
		bucket.AddOrUpdate(evicted)
		return
	}

	bucket.Replace(evicted.ID, contact)
}

func (rt *RoutingTable) SetPingFunc(ping func(NodeInfo) bool) {
	if rt == nil {
		return
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.ping = ping
}

func (rt *RoutingTable) pingFunc() func(NodeInfo) bool {
	if rt == nil {
		return nil
	}

	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.ping
}

// FindClosest should return up to limit contacts ordered by XOR distance to
// target, usually starting from the target bucket and expanding outward.
func (rt *RoutingTable) FindClosest(target NodeID, limit int) []NodeInfo {
	if rt == nil || limit <= 0 {
		return nil
	}

	bucketOrder := make([]int, 0, len(rt.Buckets))
	targetBucket := rt.GetBucketIndex(target)
	if targetBucket < 0 {
		for i := range len(rt.Buckets) {
			bucketOrder = append(bucketOrder, i)
		}
	} else {
		bucketOrder = append(bucketOrder, targetBucket)
		for step := 1; step < len(rt.Buckets); step++ {
			left := targetBucket - step
			right := targetBucket + step
			if left >= 0 {
				bucketOrder = append(bucketOrder, left)
			}
			if right < len(rt.Buckets) {
				bucketOrder = append(bucketOrder, right)
			}
		}
	}

	contacts := make([]NodeInfo, 0, limit)
	seen := make(map[NodeID]struct{}, limit)
	for _, bucketIndex := range bucketOrder {
		bucket := &rt.Buckets[bucketIndex]
		bucket.mu.RLock()
		for _, contact := range bucket.Contacts {
			if contact.ID == rt.LocalID {
				continue
			}
			if _, ok := seen[contact.ID]; ok {
				continue
			}
			seen[contact.ID] = struct{}{}
			contacts = append(contacts, contact)
		}
		bucket.mu.RUnlock()
	}

	sortContactsByDistance(target, contacts)
	if len(contacts) > limit {
		contacts = contacts[:limit]
	}

	return contacts
}

// Node represents a single Kademlia participant and its local DHT state.
type Node struct {
	Info      NodeInfo
	rt        *RoutingTable
	store     map[NodeID][]byte
	providers map[NodeID]map[NodeID]NodeInfo
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
		store:     make(map[NodeID][]byte),
		providers: make(map[NodeID]map[NodeID]NodeInfo),
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

	var pingReply PingReply
	if err := transport.Ping(bootstrap, PingArgs{Sender: n.localInfo()}, &pingReply); err != nil {
		return err
	}

	if pingReply.Receiver.ID != (NodeID{}) {
		n.rt.UpdateContact(pingReply.Receiver)
	} else {
		n.rt.UpdateContact(bootstrap)
	}

	_ = n.LookupNode(n.rt.LocalID)
	return nil
}

func (n *Node) LookupNode(target NodeID) []NodeInfo {
	if err := n.ensureState(); err != nil {
		return nil
	}

	closest := n.rt.FindClosest(target, parameterK)
	queried := make(map[NodeID]struct{}, len(closest))

	for {
		batch := make([]NodeInfo, 0, alpha)
		for _, contact := range closest {
			if _, ok := queried[contact.ID]; ok {
				continue
			}
			queried[contact.ID] = struct{}{}
			batch = append(batch, contact)
			if len(batch) == alpha {
				break
			}
		}

		if len(batch) == 0 {
			break
		}

		for _, result := range n.findNodeBatch(batch, target) {
			if result.err != nil {
				continue
			}

			n.rt.UpdateContact(result.contact)
			for _, node := range result.nodes {
				n.rt.UpdateContact(node)
			}
			closest = mergeClosest(target, n.rt.LocalID, closest, result.nodes, parameterK)
		}
	}

	return closest
}

func (n *Node) StoreValue(key NodeID, value []byte) error {
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

func (n *Node) LookupValue(key NodeID) ([]byte, bool, error) {
	if err := n.ensureState(); err != nil {
		return nil, false, err
	}

	if value, ok := n.getValue(key); ok {
		return value, true, nil
	}

	closest := n.rt.FindClosest(key, parameterK)
	queried := make(map[NodeID]struct{}, len(closest))
	var firstErr error

	for {
		batch := make([]NodeInfo, 0, alpha)
		for _, contact := range closest {
			if _, ok := queried[contact.ID]; ok {
				continue
			}
			queried[contact.ID] = struct{}{}
			batch = append(batch, contact)
			if len(batch) == alpha {
				break
			}
		}

		if len(batch) == 0 {
			break
		}

		for _, result := range n.findValueBatch(batch, key) {
			if result.err != nil {
				if firstErr == nil {
					firstErr = result.err
				}
				continue
			}

			n.rt.UpdateContact(result.contact)
			if result.found {
				n.setValue(key, result.value)
				return cloneBytes(result.value), true, nil
			}

			for _, node := range result.nodes {
				n.rt.UpdateContact(node)
			}
			closest = mergeClosest(key, n.rt.LocalID, closest, result.nodes, parameterK)
		}
	}

	return nil, false, firstErr
}

func (n *Node) AnnounceProvider(key NodeID) error {
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

func (n *Node) LookupProviders(key NodeID) ([]NodeInfo, error) {
	if err := n.ensureState(); err != nil {
		return nil, err
	}

	if providers := n.getProviders(key); len(providers) > 0 {
		return providers, nil
	}

	closest := n.rt.FindClosest(key, parameterK)
	queried := make(map[NodeID]struct{}, len(closest))
	var firstErr error

	for {
		batch := make([]NodeInfo, 0, alpha)
		for _, contact := range closest {
			if _, ok := queried[contact.ID]; ok {
				continue
			}
			queried[contact.ID] = struct{}{}
			batch = append(batch, contact)
			if len(batch) == alpha {
				break
			}
		}

		if len(batch) == 0 {
			break
		}

		providers := make([]NodeInfo, 0, parameterK)
		for _, result := range n.findProvidersBatch(batch, key) {
			if result.err != nil {
				if firstErr == nil {
					firstErr = result.err
				}
				continue
			}

			n.rt.UpdateContact(result.contact)
			if len(result.providers) > 0 {
				providers = mergeProviders(providers, result.providers)
				continue
			}

			for _, node := range result.nodes {
				n.rt.UpdateContact(node)
			}
			closest = mergeClosest(key, n.rt.LocalID, closest, result.nodes, parameterK)
		}

		if len(providers) > 0 {
			n.addProviders(key, providers)
			return providers, nil
		}
	}

	return nil, firstErr
}

type PingArgs struct {
	Sender NodeInfo
}

type PingReply struct {
	Receiver NodeInfo
}

type StoreArgs struct {
	Sender NodeInfo
	Key    NodeID
	Value  []byte
}

type StoreReply struct{}

type FindNodeArgs struct {
	Sender NodeInfo
	Target NodeID
}

type FindNodeReply struct {
	Nodes []NodeInfo
}

type FindValueArgs struct {
	Sender NodeInfo
	Key    NodeID
}

type FindValueReply struct {
	Value []byte
	Found bool
	Nodes []NodeInfo
}

type AddProviderArgs struct {
	Sender   NodeInfo
	Key      NodeID
	Provider NodeInfo
}

type AddProviderReply struct{}

type FindProvidersArgs struct {
	Sender NodeInfo
	Key    NodeID
}

type FindProvidersReply struct {
	Providers []NodeInfo
	Nodes     []NodeInfo
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
	if provider.ID == (NodeID{}) {
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

func (n *Node) ensureState() error {
	if n == nil {
		return errNodeNotInitialized
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.rt == nil {
		n.rt = &RoutingTable{LocalID: n.Info.ID}
	}
	if n.rt.pingFunc() == nil {
		n.rt.SetPingFunc(n.pingContact)
	}
	if n.Info.ID == (NodeID{}) {
		n.Info.ID = n.rt.LocalID
	}
	if n.rt.LocalID == (NodeID{}) {
		n.rt.LocalID = n.Info.ID
	}
	if n.rt == nil {
		return errNodeNotInitialized
	}
	if n.store == nil {
		n.store = make(map[NodeID][]byte)
	}
	if n.providers == nil {
		n.providers = make(map[NodeID]map[NodeID]NodeInfo)
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
	if info.ID == (NodeID{}) && n.rt != nil {
		info.ID = n.rt.LocalID
	}
	return info
}

func (n *Node) getValue(key NodeID) ([]byte, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	value, ok := n.store[key]
	if !ok {
		return nil, false
	}

	return cloneBytes(value), true
}

func (n *Node) setValue(key NodeID, value []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.store == nil {
		n.store = make(map[NodeID][]byte)
	}
	n.store[key] = cloneBytes(value)
}

func (n *Node) getProviders(key NodeID) []NodeInfo {
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

func (n *Node) addProvider(key NodeID, provider NodeInfo) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.providers == nil {
		n.providers = make(map[NodeID]map[NodeID]NodeInfo)
	}
	if n.providers[key] == nil {
		n.providers[key] = make(map[NodeID]NodeInfo)
	}
	n.providers[key][provider.ID] = provider
}

func (n *Node) addProviders(key NodeID, providers []NodeInfo) {
	for _, provider := range providers {
		n.addProvider(key, provider)
	}
}

func (n *Node) transportClient() Transport {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.transport
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

	if reply.Receiver.ID != (NodeID{}) {
		contact = reply.Receiver
	}
	n.rt.Buckets[n.rt.GetBucketIndex(contact.ID)].AddOrUpdate(contact)
	return true
}

// cloneBytes creates a copy of the given byte slice to avoid sharing mutable state between nodes.
func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}

	cloned := make([]byte, len(value))
	copy(cloned, value)
	return cloned
}

func mergeClosest(target NodeID, localID NodeID, current []NodeInfo, extra []NodeInfo, limit int) []NodeInfo {
	if limit <= 0 {
		return nil
	}

	mergedByID := make(map[NodeID]NodeInfo, len(current)+len(extra))
	for _, contact := range current {
		if contact.ID == localID {
			continue
		}
		mergedByID[contact.ID] = contact
	}
	for _, contact := range extra {
		if contact.ID == localID {
			continue
		}
		mergedByID[contact.ID] = contact
	}

	merged := make([]NodeInfo, 0, len(mergedByID))
	for _, contact := range mergedByID {
		merged = append(merged, contact)
	}

	sortContactsByDistance(target, merged)
	if len(merged) > limit {
		merged = merged[:limit]
	}

	return merged
}

func sortContactsByDistance(target NodeID, contacts []NodeInfo) {
	sort.SliceStable(contacts, func(i, j int) bool {
		left := target.Distance(contacts[i].ID)
		right := target.Distance(contacts[j].ID)

		cmp := bytes.Compare(left[:], right[:])
		if cmp != 0 {
			return cmp < 0
		}

		return bytes.Compare(contacts[i].ID[:], contacts[j].ID[:]) < 0
	})
}

func sortProviders(providers []NodeInfo) {
	sort.SliceStable(providers, func(i, j int) bool {
		return bytes.Compare(providers[i].ID[:], providers[j].ID[:]) < 0
	})
}

func mergeProviders(current []NodeInfo, extra []NodeInfo) []NodeInfo {
	mergedByID := make(map[NodeID]NodeInfo, len(current)+len(extra))
	for _, provider := range current {
		mergedByID[provider.ID] = provider
	}
	for _, provider := range extra {
		mergedByID[provider.ID] = provider
	}

	merged := make([]NodeInfo, 0, len(mergedByID))
	for _, provider := range mergedByID {
		merged = append(merged, provider)
	}
	sortProviders(merged)
	return merged
}

type findNodeResult struct {
	contact NodeInfo
	nodes   []NodeInfo
	err     error
}

func (n *Node) findNodeBatch(batch []NodeInfo, target NodeID) []findNodeResult {
	results := make([]findNodeResult, len(batch))
	local := n.localInfo()
	transport := n.transportClient()

	var wg sync.WaitGroup
	for i, contact := range batch {
		wg.Add(1)
		go func(i int, contact NodeInfo) {
			defer wg.Done()

			var reply FindNodeReply
			err := transport.FindNode(contact, FindNodeArgs{
				Sender: local,
				Target: target,
			}, &reply)
			results[i] = findNodeResult{contact: contact, nodes: reply.Nodes, err: err}
		}(i, contact)
	}
	wg.Wait()

	return results
}

type storeResult struct {
	contact NodeInfo
	err     error
}

func (n *Node) storeBatch(batch []NodeInfo, key NodeID, value []byte) []storeResult {
	results := make([]storeResult, len(batch))
	local := n.localInfo()
	transport := n.transportClient()

	var wg sync.WaitGroup
	for i, contact := range batch {
		wg.Add(1)
		go func(i int, contact NodeInfo) {
			defer wg.Done()

			var reply StoreReply
			err := transport.Store(contact, StoreArgs{
				Sender: local,
				Key:    key,
				Value:  cloneBytes(value),
			}, &reply)
			results[i] = storeResult{contact: contact, err: err}
		}(i, contact)
	}
	wg.Wait()

	return results
}

type findValueResult struct {
	contact NodeInfo
	value   []byte
	found   bool
	nodes   []NodeInfo
	err     error
}

func (n *Node) findValueBatch(batch []NodeInfo, key NodeID) []findValueResult {
	results := make([]findValueResult, len(batch))
	local := n.localInfo()
	transport := n.transportClient()

	var wg sync.WaitGroup
	for i, contact := range batch {
		wg.Add(1)
		go func(i int, contact NodeInfo) {
			defer wg.Done()

			var reply FindValueReply
			err := transport.FindValue(contact, FindValueArgs{
				Sender: local,
				Key:    key,
			}, &reply)
			results[i] = findValueResult{
				contact: contact,
				value:   reply.Value,
				found:   reply.Found,
				nodes:   reply.Nodes,
				err:     err,
			}
		}(i, contact)
	}
	wg.Wait()

	return results
}

type addProviderResult struct {
	contact NodeInfo
	err     error
}

func (n *Node) addProviderBatch(batch []NodeInfo, key NodeID, provider NodeInfo) []addProviderResult {
	results := make([]addProviderResult, len(batch))
	local := n.localInfo()
	transport := n.transportClient()

	var wg sync.WaitGroup
	for i, contact := range batch {
		wg.Add(1)
		go func(i int, contact NodeInfo) {
			defer wg.Done()

			var reply AddProviderReply
			err := transport.AddProvider(contact, AddProviderArgs{
				Sender:   local,
				Key:      key,
				Provider: provider,
			}, &reply)
			results[i] = addProviderResult{contact: contact, err: err}
		}(i, contact)
	}
	wg.Wait()

	return results
}

type findProvidersResult struct {
	contact   NodeInfo
	providers []NodeInfo
	nodes     []NodeInfo
	err       error
}

func (n *Node) findProvidersBatch(batch []NodeInfo, key NodeID) []findProvidersResult {
	results := make([]findProvidersResult, len(batch))
	local := n.localInfo()
	transport := n.transportClient()

	var wg sync.WaitGroup
	for i, contact := range batch {
		wg.Add(1)
		go func(i int, contact NodeInfo) {
			defer wg.Done()

			var reply FindProvidersReply
			err := transport.FindProviders(contact, FindProvidersArgs{
				Sender: local,
				Key:    key,
			}, &reply)
			results[i] = findProvidersResult{
				contact:   contact,
				providers: reply.Providers,
				nodes:     reply.Nodes,
				err:       err,
			}
		}(i, contact)
	}
	wg.Wait()

	return results
}
