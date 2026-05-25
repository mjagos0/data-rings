package dht

import (
	"context"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/mjagos0/datarings/store"
)

const DefaultBootstrapMultiaddr = "/ip4/127.0.0.1/tcp/7000"

type NodeAddr struct {
	ID	NodeID
	Addr	string
}

type DHT interface {
	Put(ctx context.Context, key cid.Cid, data []byte) error
	Get(ctx context.Context, key cid.Cid) ([]byte, error)
	Has(ctx context.Context, key cid.Cid) (bool, error)
	Remove(ctx context.Context, key cid.Cid) error

	FetchDAG(ctx context.Context, root cid.Cid) error

	FindSuccessor(ctx context.Context, id NodeID) (NodeAddr, error)

	Join(ctx context.Context, bootstrap NodeAddr) error

	Leave(ctx context.Context) error

	LocalNode() NodeAddr
}

type Transport interface {
	FindSuccessor(ctx context.Context, target NodeAddr, id NodeID) (NodeAddr, error)

	ClosestPrecedingNode(ctx context.Context, target NodeAddr, id NodeID) (predecessor NodeAddr, successor NodeAddr, err error)
	GetPredecessor(ctx context.Context, target NodeAddr) (*NodeAddr, error)
	GetSuccessorList(ctx context.Context, target NodeAddr) ([]NodeAddr, error)
	Notify(ctx context.Context, target NodeAddr, caller NodeAddr) error

	PutBlock(ctx context.Context, target NodeAddr, key cid.Cid, data []byte, rootCID cid.Cid, rootExpiry int64) error
	PushBlocks(ctx context.Context, target NodeAddr, keys []cid.Cid, data [][]byte, blockRoots [][]store.NetworkRootEntry) error

	ReconcileBlocks(ctx context.Context, target NodeAddr, keys []cid.Cid) ([]cid.Cid, error)
	FetchBlock(ctx context.Context, target NodeAddr, key cid.Cid) ([]byte, error)
	HasBlock(ctx context.Context, target NodeAddr, key cid.Cid) (bool, error)
	RemoveBlock(ctx context.Context, target NodeAddr, key cid.Cid) error

	DeleteCID(ctx context.Context, target NodeAddr, key cid.Cid, propagate bool) error

	TransferKeys(ctx context.Context, target NodeAddr, from, to NodeID) ([]cid.Cid, [][]byte, [][]store.NetworkRootEntry, error)

	NotifyLeave(ctx context.Context, target NodeAddr, self NodeAddr, successor NodeAddr, predecessor NodeAddr) error

	Ping(ctx context.Context, target NodeAddr) error
}

type RecordTransport interface {
	Transport

	PutRecord(ctx context.Context, target NodeAddr, key NodeID, data []byte, callerID NodeID) error

	GetRecord(ctx context.Context, target NodeAddr, key NodeID) ([]byte, error)

	PushRecords(ctx context.Context, target NodeAddr, keys []NodeID, data [][]byte) error

	TransferRecords(ctx context.Context, target NodeAddr, from, to NodeID) ([]NodeID, [][]byte, error)
}

type Config struct {
	ListenAddr	string

	AdvertiseAddr	string

	Replication	int

	SuccessorListSize	int

	StabilizeInterval	time.Duration
	FixFingersInterval	time.Duration
	CheckPredInterval	time.Duration

	PeerRecordTTL		time.Duration
	GroupRecordTTL		time.Duration
	ProviderRecordTTL	time.Duration

	RecordPurgePeriod	time.Duration

	PeerRepublishInterval		time.Duration
	ProviderRepublishInterval	time.Duration
}

func (c *Config) stabilizeInterval() time.Duration {
	if c.StabilizeInterval > 0 {
		return c.StabilizeInterval
	}
	return 500 * time.Millisecond
}

func (c *Config) fixFingersInterval() time.Duration {
	if c.FixFingersInterval > 0 {
		return c.FixFingersInterval
	}
	return 300 * time.Millisecond
}

func (c *Config) checkPredInterval() time.Duration {
	if c.CheckPredInterval > 0 {
		return c.CheckPredInterval
	}
	return time.Second
}

func (c *Config) replication() int {
	if c.Replication > 0 {
		return c.Replication
	}
	return 3
}

func (c *Config) successorListSize() int {
	if c.SuccessorListSize > 0 {
		return c.SuccessorListSize
	}

	r := c.replication()
	if r < 5 {
		r = 5
	}
	return r
}

func (c *Config) peerRecordTTL() time.Duration {
	if c.PeerRecordTTL > 0 {
		return c.PeerRecordTTL
	}
	return 20 * time.Minute
}

func (c *Config) groupRecordTTL() time.Duration {
	if c.GroupRecordTTL > 0 {
		return c.GroupRecordTTL
	}
	return 20 * time.Minute
}

func (c *Config) providerRecordTTL() time.Duration {
	if c.ProviderRecordTTL > 0 {
		return c.ProviderRecordTTL
	}
	return 70 * time.Minute
}

func (c *Config) recordPurgePeriod() time.Duration {
	if c.RecordPurgePeriod < 0 {
		return 0
	}
	if c.RecordPurgePeriod > 0 {
		return c.RecordPurgePeriod
	}
	return 5 * time.Minute
}

func (c *Config) peerRepublishInterval() time.Duration {
	if c.PeerRepublishInterval > 0 {
		return c.PeerRepublishInterval
	}
	return 15 * time.Minute
}

func (c *Config) providerRepublishInterval() time.Duration {
	if c.ProviderRepublishInterval > 0 {
		return c.ProviderRepublishInterval
	}
	return 60 * time.Minute
}
