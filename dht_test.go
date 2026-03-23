package f2p

import (
	"bytes"
	"errors"
	"net"
	"net/rpc"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestRoutingTableFindClosestOrdersByDistance(t *testing.T) {
	localID := testNodeID(0x00)
	rt := &RoutingTable{LocalID: localID}

	contacts := []NodeInfo{
		{ID: testNodeID(0x70)},
		{ID: testNodeID(0x33)},
		{ID: testNodeID(0x31)},
		{ID: testNodeID(0x20)},
		{ID: localID},
	}
	for _, contact := range contacts {
		rt.UpdateContact(contact)
	}

	got := rt.FindClosest(testNodeID(0x30), 3)
	if len(got) != 3 {
		t.Fatalf("expected 3 contacts, got %d", len(got))
	}

	want := []NodeID{testNodeID(0x31), testNodeID(0x33), testNodeID(0x20)}
	for i, id := range want {
		if got[i].ID != id {
			t.Fatalf("closest[%d] = %x, want %x", i, got[i].ID, id)
		}
	}
}

func TestRoutingTableUpdateContactReplacesDeadOldest(t *testing.T) {
	localID := testNodeID(0x00)
	rt := &RoutingTable{LocalID: localID}

	contacts := sameBucketContacts(0x80, parameterK)
	for _, contact := range contacts {
		rt.UpdateContact(contact)
	}

	var pinged []NodeID
	rt.SetPingFunc(func(contact NodeInfo) bool {
		pinged = append(pinged, contact.ID)
		return false
	})

	newContact := NodeInfo{ID: testNodeID(0xf0)}
	rt.UpdateContact(newContact)

	if len(pinged) != 1 || pinged[0] != contacts[0].ID {
		t.Fatalf("pinged %x, want oldest %x", pinged, contacts[0].ID)
	}

	bucket := bucketSnapshot(&rt.Buckets[rt.GetBucketIndex(newContact.ID)])
	if containsContact(bucket, contacts[0].ID) {
		t.Fatalf("oldest contact was not evicted")
	}
	if !containsContact(bucket, newContact.ID) {
		t.Fatalf("new contact was not inserted after dead-oldest ping")
	}
}

func TestRoutingTableUpdateContactKeepsLiveOldest(t *testing.T) {
	localID := testNodeID(0x00)
	rt := &RoutingTable{LocalID: localID}

	contacts := sameBucketContacts(0x80, parameterK)
	for _, contact := range contacts {
		rt.UpdateContact(contact)
	}

	var pinged []NodeID
	rt.SetPingFunc(func(contact NodeInfo) bool {
		pinged = append(pinged, contact.ID)
		return true
	})

	newContact := NodeInfo{ID: testNodeID(0xf0)}
	rt.UpdateContact(newContact)

	if len(pinged) != 1 || pinged[0] != contacts[0].ID {
		t.Fatalf("pinged %x, want oldest %x", pinged, contacts[0].ID)
	}

	bucket := bucketSnapshot(&rt.Buckets[rt.GetBucketIndex(newContact.ID)])
	if containsContact(bucket, newContact.ID) {
		t.Fatalf("new contact should be discarded when oldest responds")
	}
	if len(bucket) != parameterK {
		t.Fatalf("bucket length = %d, want %d", len(bucket), parameterK)
	}
	if bucket[len(bucket)-1].ID != contacts[0].ID {
		t.Fatalf("oldest contact was not refreshed to the tail")
	}
}

func TestJoinNetworkDiscoversBootstrapPeers(t *testing.T) {
	bootstrap, stopBootstrap := newRPCNode(t, testNodeID(0x10))
	defer stopBootstrap()

	peer, stopPeer := newRPCNode(t, testNodeID(0x20))
	defer stopPeer()

	joiner, stopJoiner := newRPCNode(t, testNodeID(0x30))
	defer stopJoiner()

	bootstrap.rt.UpdateContact(peer.Info)
	peer.rt.UpdateContact(bootstrap.Info)

	if err := joiner.JoinNetwork(bootstrap.Info); err != nil {
		t.Fatalf("JoinNetwork returned error: %v", err)
	}

	known := joiner.rt.FindClosest(joiner.Info.ID, parameterK)
	if !containsContact(known, bootstrap.Info.ID) {
		t.Fatalf("joiner routing table missing bootstrap node")
	}
	if !containsContact(known, peer.Info.ID) {
		t.Fatalf("joiner routing table missing discovered peer")
	}
	if !containsContact(bootstrap.rt.FindClosest(joiner.Info.ID, parameterK), joiner.Info.ID) {
		t.Fatalf("bootstrap did not learn about joiner during ping")
	}
}

func TestJoinNetworkWithMockTransport(t *testing.T) {
	bootstrap := NodeInfo{ID: testNodeID(0x10)}
	discovered := NodeInfo{ID: testNodeID(0x20)}
	transport := &mockTransport{
		ping: func(contact NodeInfo, args PingArgs, reply *PingReply) error {
			if contact.ID != bootstrap.ID {
				t.Fatalf("pinged %x, want bootstrap %x", contact.ID, bootstrap.ID)
			}
			reply.Receiver = bootstrap
			return nil
		},
		findNode: func(contact NodeInfo, args FindNodeArgs, reply *FindNodeReply) error {
			switch contact.ID {
			case bootstrap.ID:
				reply.Nodes = []NodeInfo{discovered}
			case discovered.ID:
				reply.Nodes = nil
			default:
				t.Fatalf("FindNode contacted unexpected peer %x", contact.ID)
			}
			return nil
		},
	}

	joiner := &Node{
		Info: NodeInfo{ID: testNodeID(0x30)},
	}
	joiner = NewNode(joiner.Info, transport)

	if err := joiner.JoinNetwork(bootstrap); err != nil {
		t.Fatalf("JoinNetwork returned error: %v", err)
	}

	known := joiner.rt.FindClosest(joiner.Info.ID, parameterK)
	if !containsContact(known, bootstrap.ID) {
		t.Fatalf("joiner routing table missing bootstrap node")
	}
	if !containsContact(known, discovered.ID) {
		t.Fatalf("joiner routing table missing mock-discovered node")
	}
	if transport.pingCalls != 1 {
		t.Fatalf("ping calls = %d, want 1", transport.pingCalls)
	}
	if transport.findNodeCalls != 2 {
		t.Fatalf("findNode calls = %d, want 2", transport.findNodeCalls)
	}
}

func TestStoreValueAndLookupValueAcrossNetwork(t *testing.T) {
	bootstrap, stopBootstrap := newRPCNode(t, testNodeID(0x10))
	defer stopBootstrap()

	peer, stopPeer := newRPCNode(t, testNodeID(0x20))
	defer stopPeer()

	writer, stopWriter := newRPCNode(t, testNodeID(0x30))
	defer stopWriter()

	reader, stopReader := newRPCNode(t, testNodeID(0x40))
	defer stopReader()

	bootstrap.rt.UpdateContact(peer.Info)
	peer.rt.UpdateContact(bootstrap.Info)

	if err := writer.JoinNetwork(bootstrap.Info); err != nil {
		t.Fatalf("writer JoinNetwork returned error: %v", err)
	}

	key := testNodeID(0x22)
	value := []byte("hello kad")
	if err := writer.StoreValue(key, value); err != nil {
		t.Fatalf("StoreValue returned error: %v", err)
	}

	if stored, ok := bootstrap.getValue(key); !ok || !bytes.Equal(stored, value) {
		t.Fatalf("bootstrap missing stored value: ok=%v value=%q", ok, stored)
	}
	if stored, ok := peer.getValue(key); !ok || !bytes.Equal(stored, value) {
		t.Fatalf("peer missing stored value: ok=%v value=%q", ok, stored)
	}

	if err := reader.JoinNetwork(bootstrap.Info); err != nil {
		t.Fatalf("reader JoinNetwork returned error: %v", err)
	}

	got, found, err := reader.LookupValue(key)
	if err != nil {
		t.Fatalf("LookupValue returned error: %v", err)
	}
	if !found {
		t.Fatalf("LookupValue did not find stored value")
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("LookupValue = %q, want %q", got, value)
	}
	if cached, ok := reader.getValue(key); !ok || !bytes.Equal(cached, value) {
		t.Fatalf("reader did not cache looked up value: ok=%v value=%q", ok, cached)
	}
}

func TestAnnounceProviderAndLookupProvidersAcrossNetwork(t *testing.T) {
	bootstrap, stopBootstrap := newRPCNode(t, testNodeID(0x10))
	defer stopBootstrap()

	peer, stopPeer := newRPCNode(t, testNodeID(0x20))
	defer stopPeer()

	provider, stopProvider := newRPCNode(t, testNodeID(0x30))
	defer stopProvider()

	reader, stopReader := newRPCNode(t, testNodeID(0x40))
	defer stopReader()

	bootstrap.rt.UpdateContact(peer.Info)
	peer.rt.UpdateContact(bootstrap.Info)

	if err := provider.JoinNetwork(bootstrap.Info); err != nil {
		t.Fatalf("provider JoinNetwork returned error: %v", err)
	}

	contentID := testNodeID(0x24)
	if err := provider.AnnounceProvider(contentID); err != nil {
		t.Fatalf("AnnounceProvider returned error: %v", err)
	}

	if err := reader.JoinNetwork(bootstrap.Info); err != nil {
		t.Fatalf("reader JoinNetwork returned error: %v", err)
	}

	providers, err := reader.LookupProviders(contentID)
	if err != nil {
		t.Fatalf("LookupProviders returned error: %v", err)
	}
	if !containsContact(providers, provider.Info.ID) {
		t.Fatalf("LookupProviders missing provider %x", provider.Info.ID)
	}

	if cached := reader.getProviders(contentID); !containsContact(cached, provider.Info.ID) {
		t.Fatalf("reader did not cache discovered providers")
	}
	if stored := bootstrap.getProviders(contentID); !containsContact(stored, provider.Info.ID) && !containsContact(peer.getProviders(contentID), provider.Info.ID) {
		t.Fatalf("network did not retain provider record on closest peers")
	}
}

func TestPingContactTimesOut(t *testing.T) {
	contact, stop := newSlowPingServer(t, testNodeID(0x55), 250*time.Millisecond)
	defer stop()

	node := NewNode(NodeInfo{ID: testNodeID(0x44)}, NewRPCTransport(50*time.Millisecond))

	started := time.Now()
	if ok := node.pingContact(contact); ok {
		t.Fatalf("pingContact unexpectedly succeeded")
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("pingContact took too long to time out: %v", elapsed)
	}
}

func newRPCNode(t *testing.T, id NodeID) (*Node, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	host, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		listener.Close()
		t.Fatalf("split host port: %v", err)
	}

	port, err := strconv.Atoi(portText)
	if err != nil {
		listener.Close()
		t.Fatalf("parse port: %v", err)
	}

	node := NewNode(NodeInfo{
		ID:   id,
		IP:   net.IPAddr{IP: net.ParseIP(host)},
		Port: uint16(port),
	}, NewRPCTransport(0))

	server := rpc.NewServer()
	if err := server.RegisterName("Node", node); err != nil {
		listener.Close()
		t.Fatalf("register rpc server: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				return
			}
			go server.ServeConn(conn)
		}
	}()

	stop := func() {
		listener.Close()
		<-done
	}

	return node, stop
}

func newSlowPingServer(t *testing.T, id NodeID, delay time.Duration) (NodeInfo, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	host, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		listener.Close()
		t.Fatalf("split host port: %v", err)
	}

	port, err := strconv.Atoi(portText)
	if err != nil {
		listener.Close()
		t.Fatalf("parse port: %v", err)
	}

	server := rpc.NewServer()
	if err := server.RegisterName("Node", &slowPingNode{
		receiver: NodeInfo{ID: id, IP: net.IPAddr{IP: net.ParseIP(host)}, Port: uint16(port)},
		delay:    delay,
	}); err != nil {
		listener.Close()
		t.Fatalf("register rpc server: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				return
			}
			go server.ServeConn(conn)
		}
	}()

	stop := func() {
		listener.Close()
		<-done
	}

	return NodeInfo{ID: id, IP: net.IPAddr{IP: net.ParseIP(host)}, Port: uint16(port)}, stop
}

func containsContact(contacts []NodeInfo, id NodeID) bool {
	for _, contact := range contacts {
		if contact.ID == id {
			return true
		}
	}

	return false
}

func testNodeID(b byte) NodeID {
	var id NodeID
	id[0] = b
	return id
}

func sameBucketContacts(start byte, count int) []NodeInfo {
	contacts := make([]NodeInfo, count)
	for i := range count {
		contacts[i] = NodeInfo{ID: testNodeID(start + byte(i))}
	}
	return contacts
}

func bucketSnapshot(bucket *KBucket) []NodeInfo {
	bucket.mu.RLock()
	defer bucket.mu.RUnlock()

	contacts := make([]NodeInfo, len(bucket.Contacts))
	copy(contacts, bucket.Contacts)
	return contacts
}

type slowPingNode struct {
	receiver NodeInfo
	delay    time.Duration
}

func (n *slowPingNode) Ping(args PingArgs, reply *PingReply) error {
	time.Sleep(n.delay)
	reply.Receiver = n.receiver
	return nil
}

type mockTransport struct {
	mu            sync.Mutex
	pingCalls     int
	findNodeCalls int
	ping          func(contact NodeInfo, args PingArgs, reply *PingReply) error
	store         func(contact NodeInfo, args StoreArgs, reply *StoreReply) error
	findNode      func(contact NodeInfo, args FindNodeArgs, reply *FindNodeReply) error
	findValue     func(contact NodeInfo, args FindValueArgs, reply *FindValueReply) error
	addProvider   func(contact NodeInfo, args AddProviderArgs, reply *AddProviderReply) error
	findProviders func(contact NodeInfo, args FindProvidersArgs, reply *FindProvidersReply) error
}

func (m *mockTransport) Ping(contact NodeInfo, args PingArgs, reply *PingReply) error {
	m.mu.Lock()
	m.pingCalls++
	m.mu.Unlock()
	if m.ping == nil {
		return errors.New("unexpected Ping call")
	}
	return m.ping(contact, args, reply)
}

func (m *mockTransport) Store(contact NodeInfo, args StoreArgs, reply *StoreReply) error {
	if m.store == nil {
		return errors.New("unexpected Store call")
	}
	return m.store(contact, args, reply)
}

func (m *mockTransport) FindNode(contact NodeInfo, args FindNodeArgs, reply *FindNodeReply) error {
	m.mu.Lock()
	m.findNodeCalls++
	m.mu.Unlock()
	if m.findNode == nil {
		return errors.New("unexpected FindNode call")
	}
	return m.findNode(contact, args, reply)
}

func (m *mockTransport) FindValue(contact NodeInfo, args FindValueArgs, reply *FindValueReply) error {
	if m.findValue == nil {
		return errors.New("unexpected FindValue call")
	}
	return m.findValue(contact, args, reply)
}

func (m *mockTransport) AddProvider(contact NodeInfo, args AddProviderArgs, reply *AddProviderReply) error {
	if m.addProvider == nil {
		return errors.New("unexpected AddProvider call")
	}
	return m.addProvider(contact, args, reply)
}

func (m *mockTransport) FindProviders(contact NodeInfo, args FindProvidersArgs, reply *FindProvidersReply) error {
	if m.findProviders == nil {
		return errors.New("unexpected FindProviders call")
	}
	return m.findProviders(contact, args, reply)
}
