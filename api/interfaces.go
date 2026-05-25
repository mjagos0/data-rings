package api

import (
	"context"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/mjagos0/datarings/store"
)

type StorageReporter interface {
	StorageStatus() *store.StorageStatus
}

type GarbageCollector interface {
	GC(ctx context.Context) (store.GCResult, error)
}

type RootAdder interface {
	Add(root store.Root) (store.Root, error)
	List() []store.Root
	Remove(id string) error
	Rename(id, newName string) error
	GetByCID(c cid.Cid) ([]store.Root, error)
	GetByName(name string) ([]store.Root, error)
}

type DHTNode interface {
	JoinPeer(ctx context.Context, peerMultiaddr string) error

	FetchDAG(ctx context.Context, c cid.Cid) error
}

type PrivateRingInfo struct {
	GroupID		string	`json:"group_id"`
	Name		string	`json:"name,omitempty"`
	ListenAddr	string	`json:"listen_addr"`
}

type PrivateDrings interface {
	ListRings() []PrivateRingInfo

	PushDAG(ctx context.Context, groupRef, cidStr string, ttl time.Duration) error

	FetchDAG(ctx context.Context, groupRef, cidStr string) error

	JoinRing(ctx context.Context, keyHex, listenAddr, name string, storageMaxBytes int64) (PrivateRingInfo, error)

	LeaveRing(ctx context.Context, groupRef string) error

	DeleteCID(ctx context.Context, groupRef, cidStr string) error

	SetRingQuota(groupRef string, max int64) error

	RingQuota(groupRef string) (int64, int64, error)
}

type NodeStater interface {
	StateJSON() []byte
}

type NodeStabilizer interface {
	StabilizeFull()
}

type PrivateRingStabilizer interface {
	StabilizeRing(groupRef string) error
}

type PrivateRingBackgroundControl interface {
	PauseRingBackground(groupRef string) error
	ResumeRingBackground(groupRef string) error
	RingBackgroundPaused(groupRef string) (bool, error)
}

type BackgroundStabilizer interface {
	PauseBackground()
	ResumeBackground()
	BackgroundPaused() bool
}

type RecordIntrospector interface {
	RecordKeysJSON() []byte
	HasRecord(keyHex string) bool
	DeleteRecord(keyHex string) bool
}

type BlockIntrospector interface {
	HasLocalBlock(cidStr string) (bool, error)
	DeleteLocalBlock(cidStr string) error
}

type NetworkRootLister interface {
	ListRoots() []string
	RootCount() int
}

type NetworkBlockIntrospector interface {
	HasBlockStr(cidStr string) (bool, error)
	ListBlockCIDs() ([]string, error)
}

type CIDUsageReporter interface {
	CIDUsage() map[string]int64
}

type PrivateRingStater interface {
	RingStatesJSON() []byte
}

type PublicDHT interface {
	PublishSelf(ctx context.Context) error

	LookupPeerJSON(ctx context.Context, peerIDHex string) ([]byte, error)

	LookupGroupJSON(ctx context.Context, groupIDHex string) ([]byte, error)

	PublishProvider(ctx context.Context, cidStr string) error

	FindProvidersJSON(ctx context.Context, cidStr string) ([]byte, error)

	FetchDAGFromProvidersStr(ctx context.Context, cidStr string) error
}
