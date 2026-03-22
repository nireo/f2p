package f2p

import (
	"net"
	"sync"
)

const parameterK = 20

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

type RoutingTable struct {
	LocalID NodeID
	Buckets [160]KBucket
	mu      sync.RWMutex
}

// GetBucketIndex should return the index of the first differing bit between
// LocalID and target. Return -1 when target is the local node itself.
func (rt *RoutingTable) GetBucketIndex(target NodeID) int {
	for i := range len(rt.LocalID) {
		xor := rt.LocalID[i] ^ target[i]
		if xor != 0 {
			for j := 0; j < 8; j++ {
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
	panic("TODO: update the routing table with a contact")
}

// FindClosest should return up to limit contacts ordered by XOR distance to
// target, usually starting from the target bucket and expanding outward.
func (rt *RoutingTable) FindClosest(target NodeID, limit int) []NodeInfo {
	panic("TODO: collect the closest known contacts")
}

// Node represents a single Kademlia participant and its local DHT state.
type Node struct {
	rt *RoutingTable
}

// JoinNetwork should bootstrap this node by contacting a known peer and
// populating the routing table with the closest nodes it can discover.
func (n *Node) JoinNetwork(bootstrap NodeInfo) error {
	panic("TODO: join the network through a bootstrap node")
}

// LookupNode should perform the iterative Kademlia node lookup and return the
// closest contacts it finds for target.
func (n *Node) LookupNode(target NodeID) []NodeInfo {
	panic("TODO: implement iterative FIND_NODE lookup")
}

// StoreValue should locate the k closest peers for key and ask them to store
// value so the data is replicated in the DHT.
func (n *Node) StoreValue(key NodeID, value []byte) error {
	panic("TODO: store a value on the closest peers")
}

// LookupValue should try to read the value for key from the network and fall
// back to iterative node lookup when peers do not have the data yet.
func (n *Node) LookupValue(key NodeID) ([]byte, bool, error) {
	panic("TODO: implement iterative FIND_VALUE lookup")
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

// Ping should confirm liveness and usually refresh the sender in the routing
// table because they just proved they are reachable.
func (n *Node) Ping(args PingArgs, reply *PingReply) error {
	panic("TODO: respond to a ping and refresh the routing table")
}

// Store should save a value locally for the requested key and refresh the
// sender in the routing table.
func (n *Node) Store(args StoreArgs, reply *StoreReply) error {
	panic("TODO: store a value received from another node")
}

// FindNode should return the closest contacts this node knows about for the
// requested target and refresh the sender in the routing table.
func (n *Node) FindNode(args FindNodeArgs, reply *FindNodeReply) error {
	panic("TODO: return closest contacts for a target ID")
}

// FindValue should return the value if present, otherwise it should return the
// closest contacts so the caller can continue the lookup.
func (n *Node) FindValue(args FindValueArgs, reply *FindValueReply) error {
	panic("TODO: return a value or the next closest contacts")
}
