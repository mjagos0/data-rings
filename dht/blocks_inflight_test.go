package dht

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"golang.org/x/sync/semaphore"

	"github.com/mjagos0/datarings/store"
)

type delayPushTransport struct {
	Transport
	delay	time.Duration
}

func (d *delayPushTransport) PushBlocks(ctx context.Context, target NodeAddr, keys []cid.Cid, data [][]byte, blockRoots [][]store.NetworkRootEntry) error {
	if d.delay > 0 {
		select {
		case <-time.After(d.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return d.Transport.PushBlocks(ctx, target, keys, data, blockRoots)
}

func TestBlocksInFlight_BoundsConcurrentPushers(t *testing.T) {
	ring := NewTestRing()
	ring.Replication = 2
	sender := ring.AddNode(0)
	target := ring.AddNode(128)
	ring.StabilizeRounds(5)

	cap := int64(defaultPushBlocksChunkSize)
	sender.blocksInFlightSem = semaphore.NewWeighted(cap)
	sender.blocksInFlightCap = cap
	sender.blocksInFlightActive.Store(0)
	sender.blocksInFlightMax.Store(0)

	sender.transport = &delayPushTransport{
		Transport:	sender.transport,
		delay:		25 * time.Millisecond,
	}

	ctx := context.Background()
	pushers := 3
	keysPerPusher := defaultPushBlocksChunkSize
	keySets := make([][]cid.Cid, pushers)
	for p := 0; p < pushers; p++ {
		keys := make([]cid.Cid, 0, keysPerPusher)
		for i := 0; i < keysPerPusher; i++ {
			k, data := testBlock(fmt.Sprintf("inflight-p%d-b%d", p, i))
			if err := sender.blocks.Put(ctx, k, data); err != nil {
				t.Fatalf("seed: %v", err)
			}
			keys = append(keys, k)
		}
		keySets[p] = keys
	}

	targets := []NodeAddr{{ID: target.id, Addr: target.addr}}

	var wg sync.WaitGroup
	for p := 0; p < pushers; p++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sender.pushBlocksChunked(ctx, targets, keySets[idx], "inflight-test")
		}(p)
	}
	wg.Wait()

	maxActive := sender.blocksInFlightMax.Load()
	if maxActive > cap {
		t.Errorf("blocksInFlightMax=%d exceeds configured cap=%d — semaphore not enforced",
			maxActive, cap)
	}
	if maxActive == 0 {
		t.Errorf("blocksInFlightMax=0 — semaphore never observed in use (test did not exercise gate)")
	}

	if got := sender.blocksInFlightActive.Load(); got != 0 {
		t.Errorf("blocksInFlightActive=%d after drain, want 0 (slots leaked)", got)
	}

	for _, keys := range keySets {
		for _, k := range keys {
			has, _ := target.blocks.Has(ctx, k)
			if !has {
				t.Errorf("target missing %s — pushes should not be dropped by the gate", k)
			}
		}
	}
}

func TestBlocksInFlight_ReleasesOnSkippedBlock(t *testing.T) {
	ring := NewTestRing()
	ring.Replication = 2
	sender := ring.AddNode(0)
	target := ring.AddNode(128)
	ring.StabilizeRounds(5)

	ctx := context.Background()
	present, data := testBlock("present-inflight")
	missing, _ := testBlock("missing-inflight")
	if err := sender.blocks.Put(ctx, present, data); err != nil {
		t.Fatalf("seed: %v", err)
	}

	targets := []NodeAddr{{ID: target.id, Addr: target.addr}}
	keys := []cid.Cid{missing, present, missing}
	sender.pushBlocksChunked(ctx, targets, keys, "inflight-skip")

	if got := sender.blocksInFlightActive.Load(); got != 0 {
		t.Errorf("blocksInFlightActive=%d after missing-skip, want 0 (slot leaked)", got)
	}
}
