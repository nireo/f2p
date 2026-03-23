package f2p

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
)

type Key [20]byte

func (key Key) Distance(other Key) (dist Key) {
	for i := 0; i < len(key); i++ {
		dist[i] = key[i] ^ other[i]
	}

	return dist
}

func (key Key) String() string {
	return hex.EncodeToString(key[:])
}

func (key Key) MarshalText() ([]byte, error) {
	return []byte(key.String()), nil
}

func (key *Key) UnmarshalText(text []byte) error {
	var zero Key
	expectedLen := hex.EncodedLen(len(zero))
	if len(text) != expectedLen {
		return fmt.Errorf("invalid key length: got %d want %d", len(text), expectedLen)
	}

	decoded := make([]byte, len(zero))
	if _, err := hex.Decode(decoded, text); err != nil {
		return err
	}

	copy(key[:], decoded)
	return nil
}

func HashBytes(data []byte) Key {
	sum := sha256.Sum256(data)
	var key Key
	copy(key[:], sum[:len(key)])
	return key
}

type NodeInfo struct {
	ID   Key
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
	FetchChunk(contact NodeInfo, args FetchChunkArgs, reply *FetchChunkReply) error
}

type PingArgs struct {
	Sender NodeInfo
}

type PingReply struct {
	Receiver NodeInfo
}

type StoreArgs struct {
	Sender NodeInfo
	Key    Key
	Value  []byte
}

type StoreReply struct{}

type FindNodeArgs struct {
	Sender NodeInfo
	Target Key
}

type FindNodeReply struct {
	Nodes []NodeInfo
}

type FindValueArgs struct {
	Sender NodeInfo
	Key    Key
}

type FindValueReply struct {
	Value []byte
	Found bool
	Nodes []NodeInfo
}

type AddProviderArgs struct {
	Sender   NodeInfo
	Key      Key
	Provider NodeInfo
}

type AddProviderReply struct{}

type FindProvidersArgs struct {
	Sender NodeInfo
	Key    Key
}

type FindProvidersReply struct {
	Providers []NodeInfo
	Nodes     []NodeInfo
}

type FetchChunkArgs struct {
	Sender NodeInfo
	Key    Key
}

type FetchChunkReply struct {
	Data []byte
}
