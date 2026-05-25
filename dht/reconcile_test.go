package dht

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/ipfs/go-cid"

	"github.com/mjagos0/datarings/store"
)

type countingReconcileTransport struct {
	Transport
	mu			sync.Mutex
	reconcileCalls		int
	pushCalls		int
	totalKeysPushed		int
	reconcileReturns	func(keys []cid.Cid) ([]cid.Cid, error)
}

func (c *countingReconcileTransport) ReconcileBlocks(ctx context.Context, target NodeAddr, keys []cid.Cid) ([]cid.Cid, error) {
	c.mu.Lock()
	c.reconcileCalls++
	c.mu.Unlock()
	if c.reconcileReturns != nil {
		return c.reconcileReturns(keys)
	}
	return c.Transport.ReconcileBlocks(ctx, target, keys)
}

func (c *countingReconcileTransport) PushBlocks(ctx context.Context, target NodeAddr, keys []cid.Cid, data [][]byte, blockRoots [][]store.NetworkRootEntry) error {
	c.mu.Lock()
	c.pushCalls++
	c.totalKeysPushed += len(keys)
	c.mu.Unlock()
	return c.Transport.PushBlocks(ctx, target, keys, data, blockRoots)
}

func TestReconcile_SkipsPushWhenTargetAlreadyHasAll(t *testing.T) {
	ring := NewTestRing()
	ring.Replication = 2
	sender := ring.AddNode(0)
	target := ring.AddNode(128)
	ring.StabilizeRounds(5)

	ctx := context.Background()

	var keys []cid.Cid
	for i := 0; i < 20; i++ {
		k, data := testBlock(fmt.Sprintf("reconcile-present-%d", i))
		if err := sender.blocks.Put(ctx, k, data); err != nil {
			t.Fatalf("seed sender: %v", err)
		}
		if err := target.blocks.Put(ctx, k, data); err != nil {
			t.Fatalf("seed target: %v", err)
		}
		keys = append(keys, k)
	}

	counter := &countingReconcileTransport{Transport: sender.transport}
	sender.transport = counter

	targets := []NodeAddr{{ID: target.id, Addr: target.addr}}
	sender.pushBlocksChunked(ctx, targets, keys, "reconcile-skip-test")

	counter.mu.Lock()
	defer counter.mu.Unlock()
	if counter.reconcileCalls != 1 {
		t.Errorf("reconcileCalls=%d, want 1 (one manifest per target)", counter.reconcileCalls)
	}
	if counter.pushCalls != 0 {
		t.Errorf("pushCalls=%d, want 0 — target already has all blocks, push should be skipped",
			counter.pushCalls)
	}
	if counter.totalKeysPushed != 0 {
		t.Errorf("totalKeysPushed=%d, want 0", counter.totalKeysPushed)
	}
}

func TestReconcile_PushesOnlyMissingDelta(t *testing.T) {
	ring := NewTestRing()
	ring.Replication = 2
	sender := ring.AddNode(0)
	target := ring.AddNode(128)
	ring.StabilizeRounds(5)

	ctx := context.Background()

	var keys []cid.Cid
	for i := 0; i < 20; i++ {
		k, data := testBlock(fmt.Sprintf("reconcile-delta-%d", i))
		if err := sender.blocks.Put(ctx, k, data); err != nil {
			t.Fatalf("seed sender: %v", err)
		}
		if i < 12 {
			if err := target.blocks.Put(ctx, k, data); err != nil {
				t.Fatalf("seed target: %v", err)
			}
		}
		keys = append(keys, k)
	}

	counter := &countingReconcileTransport{Transport: sender.transport}
	sender.transport = counter

	targets := []NodeAddr{{ID: target.id, Addr: target.addr}}
	sender.pushBlocksChunked(ctx, targets, keys, "reconcile-delta-test")

	counter.mu.Lock()
	defer counter.mu.Unlock()

	if counter.totalKeysPushed != 8 {
		t.Errorf("totalKeysPushed=%d, want 8 (= 20 total − 12 already at target)",
			counter.totalKeysPushed)
	}
	if counter.pushCalls != 1 {
		t.Errorf("pushCalls=%d, want 1 (8 keys fit in one chunk of %d)",
			counter.pushCalls, defaultPushBlocksChunkSize)
	}

	for _, k := range keys {
		has, _ := target.blocks.Has(ctx, k)
		if !has {
			t.Errorf("target missing %s after reconcile+push", k)
		}
	}
}

func TestReconcile_FallsBackWhenRPCFails(t *testing.T) {
	ring := NewTestRing()
	ring.Replication = 2
	sender := ring.AddNode(0)
	target := ring.AddNode(128)
	ring.StabilizeRounds(5)

	ctx := context.Background()

	var keys []cid.Cid
	for i := 0; i < 10; i++ {
		k, data := testBlock(fmt.Sprintf("reconcile-err-%d", i))
		if err := sender.blocks.Put(ctx, k, data); err != nil {
			t.Fatalf("seed: %v", err)
		}
		keys = append(keys, k)
	}

	counter := &countingReconcileTransport{Transport: sender.transport}
	counter.reconcileReturns = func(_ []cid.Cid) ([]cid.Cid, error) {
		return nil, fmt.Errorf("simulated RPC failure")
	}
	sender.transport = counter

	targets := []NodeAddr{{ID: target.id, Addr: target.addr}}
	sender.pushBlocksChunked(ctx, targets, keys, "reconcile-err-test")

	counter.mu.Lock()
	defer counter.mu.Unlock()

	if counter.totalKeysPushed != len(keys) {
		t.Errorf("totalKeysPushed=%d, want %d (reconcile failed → fallback to full push)",
			counter.totalKeysPushed, len(keys))
	}

	for _, k := range keys {
		has, _ := target.blocks.Has(ctx, k)
		if !has {
			t.Errorf("target missing %s after fallback push", k)
		}
	}
}

func TestReconcileBlocks_RPCHandler(t *testing.T) {
	ring := NewTestRing()
	ring.Replication = 1
	holder := ring.AddNode(0)

	ctx := context.Background()
	present1, d1 := testBlock("rpc-present-1")
	present2, d2 := testBlock("rpc-present-2")
	missing1, _ := testBlock("rpc-missing-1")
	missing2, _ := testBlock("rpc-missing-2")

	if err := holder.blocks.Put(ctx, present1, d1); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := holder.blocks.Put(ctx, present2, d2); err != nil {
		t.Fatalf("seed: %v", err)
	}

	query := []cid.Cid{present1, missing1, present2, missing2}
	missing, err := holder.transport.ReconcileBlocks(ctx,
		NodeAddr{ID: holder.id, Addr: holder.addr},
		query)
	if err != nil {
		t.Fatalf("ReconcileBlocks: %v", err)
	}

	wantMissing := map[cid.Cid]bool{missing1: true, missing2: true}
	if len(missing) != 2 {
		t.Fatalf("missing len=%d, want 2 — got %v", len(missing), missing)
	}
	for _, c := range missing {
		if !wantMissing[c] {
			t.Errorf("unexpected CID in missing set: %s", c)
		}
	}
}
