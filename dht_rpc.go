package f2p

import (
	"errors"
	"net"
	"net/rpc"
	"strconv"
	"time"
)

const defaultRPCTimeout = 2 * time.Second

type RPCTransport struct {
	timeout time.Duration
}

func NewRPCTransport(timeout time.Duration) *RPCTransport {
	if timeout <= 0 {
		timeout = defaultRPCTimeout
	}

	return &RPCTransport{timeout: timeout}
}

func (t *RPCTransport) Ping(contact NodeInfo, args PingArgs, reply *PingReply) error {
	return t.call(contact, "Node.Ping", args, reply)
}

func (t *RPCTransport) Store(contact NodeInfo, args StoreArgs, reply *StoreReply) error {
	return t.call(contact, "Node.Store", args, reply)
}

func (t *RPCTransport) FindNode(contact NodeInfo, args FindNodeArgs, reply *FindNodeReply) error {
	return t.call(contact, "Node.FindNode", args, reply)
}

func (t *RPCTransport) FindValue(contact NodeInfo, args FindValueArgs, reply *FindValueReply) error {
	return t.call(contact, "Node.FindValue", args, reply)
}

func (t *RPCTransport) AddProvider(contact NodeInfo, args AddProviderArgs, reply *AddProviderReply) error {
	return t.call(contact, "Node.AddProvider", args, reply)
}

func (t *RPCTransport) FindProviders(contact NodeInfo, args FindProvidersArgs, reply *FindProvidersReply) error {
	return t.call(contact, "Node.FindProviders", args, reply)
}

func (t *RPCTransport) FetchChunk(contact NodeInfo, args FetchChunkArgs, reply *FetchChunkReply) error {
	return t.call(contact, "Node.FetchChunk", args, reply)
}

func (t *RPCTransport) call(contact NodeInfo, method string, args any, reply any) error {
	address, err := contactAddress(contact)
	if err != nil {
		return err
	}

	conn, err := net.DialTimeout("tcp", address, t.timeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(t.timeout)); err != nil {
		return err
	}

	client := rpc.NewClient(conn)
	defer client.Close()

	return client.Call(method, args, reply)
}

func contactAddress(contact NodeInfo) (string, error) {
	if len(contact.IP.IP) == 0 || contact.Port == 0 {
		return "", errors.New("contact has no reachable address")
	}

	return net.JoinHostPort(contact.IP.String(), strconv.Itoa(int(contact.Port))), nil
}
