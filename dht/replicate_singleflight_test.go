package dht

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/mjagos0/datarings/store"
)

type holdingPushTransport struct {
	Transport
	gate	chan struct{}
}

func (h *holdingPushTransport) PushBlocks(ctx context.Context, target NodeAddr, keys []cid.Cid, data [][]byte, blockRoots [][]store.NetworkRootEntry) error {
	select {
	case <-h.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	return h.Transport.PushBlocks(ctx, target, keys, data, blockRoots)
}

func TestReplicateToNewSuccessors_SingleFlight_DropsOverlappingRuns(t *testing.T) {
	ring := NewTestRing()
	ring.Replication = 2
	sender := ring.AddNode(0)
	target := ring.AddNode(128)
	ring.StabilizeRounds(5)

	ctx := context.Background()
	k, data := testBlock("sf-block")
	if err := sender.blocks.Put(ctx, k, data); err != nil {
		t.Fatalf("seed: %v", err)
	}

	waitForReplicateSlotFree(t, sender, 2*time.Second)
	baseline := sender.replicateToSuccessorsDropped.Load()

	gate := make(chan struct{})
	sender.transport = &holdingPushTransport{
		Transport:	sender.transport,
		gate:		gate,
	}

	targets := []NodeAddr{{ID: target.id, Addr: target.addr}}

	sender.triggerReplicateToNewSuccessors(ctx, nil, targets)

	if !waitUntilSlotBusy(sender, 500*time.Millisecond) {
		t.Fatal("first trigger never acquired the single-flight slot")
	}

	const extra = 2
	for i := 0; i < extra; i++ {
		sender.triggerReplicateToNewSuccessors(ctx, nil, targets)
	}
	if got := sender.replicateToSuccessorsDropped.Load() - baseline; got != int64(extra) {
		t.Errorf("dropped delta=%d, want %d — single-flight not enforced", got, extra)
	}

	close(gate)
	waitForReplicateSlotFree(t, sender, 2*time.Second)
}

func TestReplicateToNewSuccessors_SingleFlight_AllowsSequentialRuns(t *testing.T) {
	ring := NewTestRing()
	ring.Replication = 2
	sender := ring.AddNode(0)
	target := ring.AddNode(128)
	ring.StabilizeRounds(5)

	ctx := context.Background()
	var keys []cid.Cid
	for i := 0; i < 3; i++ {
		k, data := testBlock(fmt.Sprintf("sf-seq-%d", i))
		if err := sender.blocks.Put(ctx, k, data); err != nil {
			t.Fatalf("seed: %v", err)
		}
		keys = append(keys, k)
	}

	var pushCalls sync.WaitGroup
	var callCount int64
	var mu sync.Mutex
	wrapped := sender.transport
	sender.transport = &recordingPushHook{
		Transport:	wrapped,
		onPush: func() {
			mu.Lock()
			callCount++
			mu.Unlock()
		},
	}

	waitForReplicateSlotFree(t, sender, 2*time.Second)
	baseline := sender.replicateToSuccessorsDropped.Load()

	targets := []NodeAddr{{ID: target.id, Addr: target.addr}}

	for i := 0; i < 3; i++ {
		pushCalls.Add(1)
		func() {
			defer pushCalls.Done()
			sender.triggerReplicateToNewSuccessors(ctx, nil, targets)

			waitForReplicateSlotFree(t, sender, 2*time.Second)
		}()
	}
	pushCalls.Wait()

	if got := sender.replicateToSuccessorsDropped.Load() - baseline; got != 0 {
		t.Errorf("dropped delta=%d, want 0 — sequential runs should all execute", got)
	}
	mu.Lock()
	defer mu.Unlock()

	if callCount == 0 {
		t.Errorf("recorded 0 PushBlocks — no actual work observed across 3 sequential runs")
	}
	_ = keys
}

type recordingPushHook struct {
	Transport
	onPush	func()
}

func (r *recordingPushHook) PushBlocks(ctx context.Context, target NodeAddr, keys []cid.Cid, data [][]byte, blockRoots [][]store.NetworkRootEntry) error {
	if r.onPush != nil {
		r.onPush()
	}
	return r.Transport.PushBlocks(ctx, target, keys, data, blockRoots)
}

func waitForReplicateSlotFree(t *testing.T, n *Node, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		select {
		case n.replicateToSuccessorsSlot <- struct{}{}:
			<-n.replicateToSuccessorsSlot
			return
		default:
			if time.Now().After(deadline) {
				t.Fatalf("replicateToSuccessors slot never freed within %s", timeout)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}
}

func waitUntilSlotBusy(n *Node, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		select {
		case n.replicateToSuccessorsSlot <- struct{}{}:

			<-n.replicateToSuccessorsSlot
			if time.Now().After(deadline) {
				return false
			}
			time.Sleep(time.Millisecond)
		default:
			return true
		}
	}
}
