package dht

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/ipfs/go-cid"

	"github.com/mjagos0/datarings/store"
)

type recordingTransport struct {
	Transport
	mu		sync.Mutex
	pushCalls	[]int
	pushTarget	[]NodeID
	pushKeys	[]cid.Cid
}

func (r *recordingTransport) PushBlocks(ctx context.Context, target NodeAddr, keys []cid.Cid, data [][]byte, blockRoots [][]store.NetworkRootEntry) error {
	r.mu.Lock()
	r.pushCalls = append(r.pushCalls, len(keys))
	r.pushTarget = append(r.pushTarget, target.ID)
	r.pushKeys = append(r.pushKeys, keys...)
	r.mu.Unlock()
	return r.Transport.PushBlocks(ctx, target, keys, data, blockRoots)
}

func TestPushBlocksChunked_BoundsBatchSize(t *testing.T) {
	ring := NewTestRing()
	ring.Replication = 3
	sender := ring.AddNode(0)
	target1 := ring.AddNode(64)
	target2 := ring.AddNode(128)
	ring.StabilizeRounds(10)

	const totalBlocks = defaultPushBlocksChunkSize*2 + defaultPushBlocksChunkSize/2
	ctx := context.Background()
	keys := make([]cid.Cid, 0, totalBlocks)
	for i := 0; i < totalBlocks; i++ {
		k, data := testBlock(fmt.Sprintf("chunked-block-%d", i))
		if err := sender.blocks.Put(ctx, k, data); err != nil {
			t.Fatalf("seed put: %v", err)
		}
		keys = append(keys, k)
	}

	rec := &recordingTransport{Transport: sender.transport}
	sender.transport = rec

	targets := []NodeAddr{
		{ID: target1.id, Addr: target1.addr},
		{ID: target2.id, Addr: target2.addr},
	}
	sender.pushBlocksChunked(ctx, targets, keys, "test")

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.pushCalls) == 0 {
		t.Fatal("no PushBlocks calls recorded")
	}
	for i, n := range rec.pushCalls {
		if n > defaultPushBlocksChunkSize {
			t.Errorf("call %d pushed %d keys, exceeds chunk size %d",
				i, n, defaultPushBlocksChunkSize)
		}
		if n == 0 {
			t.Errorf("call %d pushed 0 keys", i)
		}
	}

	for _, tgt := range []*Node{target1, target2} {
		for _, k := range keys {
			has, err := tgt.blocks.Has(ctx, k)
			if err != nil {
				t.Fatalf("target %d Has(%s): %v", testNodeIDVal(tgt.id), k, err)
			}
			if !has {
				t.Errorf("target %d missing block %s", testNodeIDVal(tgt.id), k)
			}
		}
	}

	expectedPerTarget := (totalBlocks + defaultPushBlocksChunkSize - 1) / defaultPushBlocksChunkSize
	expectedTotal := expectedPerTarget * len(targets)
	if len(rec.pushCalls) != expectedTotal {
		t.Errorf("expected %d PushBlocks calls (%d per target × %d targets), got %d",
			expectedTotal, expectedPerTarget, len(targets), len(rec.pushCalls))
	}
}

func TestPushBlocksChunked_EmptyInputs(t *testing.T) {
	ring := NewTestRing()
	ring.Replication = 2
	sender := ring.AddNode(0)
	target := ring.AddNode(128)
	ring.StabilizeRounds(5)

	rec := &recordingTransport{Transport: sender.transport}
	sender.transport = rec
	ctx := context.Background()

	sender.pushBlocksChunked(ctx, []NodeAddr{{ID: target.id, Addr: target.addr}}, nil, "test-empty-keys")

	k, _ := testBlock("lonely")
	sender.pushBlocksChunked(ctx, nil, []cid.Cid{k}, "test-empty-targets")

	if len(rec.pushCalls) != 0 {
		t.Errorf("expected 0 PushBlocks calls with empty inputs, got %d", len(rec.pushCalls))
	}
}

func TestPushBlocksChunked_SkipsMissingBlocks(t *testing.T) {
	ring := NewTestRing()
	ring.Replication = 2
	sender := ring.AddNode(0)
	target := ring.AddNode(128)
	ring.StabilizeRounds(5)

	ctx := context.Background()
	present, dataPresent := testBlock("present")
	missing, _ := testBlock("missing")
	if err := sender.blocks.Put(ctx, present, dataPresent); err != nil {
		t.Fatalf("seed put: %v", err)
	}

	keys := []cid.Cid{missing, present, missing, present, missing}

	rec := &recordingTransport{Transport: sender.transport}
	sender.transport = rec
	targets := []NodeAddr{{ID: target.id, Addr: target.addr}}
	sender.pushBlocksChunked(ctx, targets, keys, "test-missing")

	has, _ := target.blocks.Has(ctx, present)
	if !has {
		t.Errorf("target missing the one block that was actually in the sender store")
	}
	has, _ = target.blocks.Has(ctx, missing)
	if has {
		t.Errorf("target somehow has the missing block (which was never in the sender store)")
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	for _, k := range rec.pushKeys {
		if k.Equals(missing) {
			t.Errorf("PushBlocks call contained missing CID %s", k)
		}
	}
}
