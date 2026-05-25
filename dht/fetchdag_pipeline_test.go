package dht

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/mjagos0/datarings/store"
)

func newSlowFetchRing(t *testing.T, fetchBlockDelay time.Duration, branches, leavesPerMid int) (*TestRing, *slowTransport, *Node, cid.Cid, int) {
	t.Helper()
	ring := NewTestRing()
	ring.Replication = 3
	for i := 0; i < 8; i++ {
		ring.AddNode(byte(i * 32))
	}
	ring.StabilizeRounds(12)

	publisher := ring.Nodes[0]
	ctx := context.Background()
	rootCID, allCIDs := wideDAG(t, ctx, publisher, branches, leavesPerMid)
	totalBlocks := len(allCIDs)

	if err := publisher.ShareDAG(ctx, rootCID); err != nil {
		t.Fatalf("setup ShareDAG: %v", err)
	}
	for _, n := range ring.Nodes {
		n.WaitForReplicationDrain(2 * time.Second)
	}

	st := &slowTransport{fetchBlockDelay: fetchBlockDelay}
	for _, n := range ring.Nodes {
		st.Transport = n.transport
		n.transport = st
	}

	fetcher := ring.Nodes[4]
	if err := wipeBlocksFor(t, fetcher, rootCID); err != nil {
		t.Fatalf("wipe fetcher store: %v", err)
	}
	return ring, st, fetcher, rootCID, totalBlocks
}

func wipeBlocksFor(t *testing.T, n *Node, root cid.Cid) error {
	t.Helper()
	ctx := context.Background()
	publisher := n
	_ = publisher

	keys, err := n.blocks.AllKeysChan(ctx)
	if err != nil {
		return err
	}
	for c := range keys {
		_ = n.blocks.Delete(ctx, c)
	}
	return nil
}

func TestFetchDAGPipeline_AllBlocksDownloaded(t *testing.T) {

	_, _, fetcher, rootCID, totalBlocks := newSlowFetchRing(t, 0, 4, 4)
	ctx := context.Background()

	if err := fetcher.FetchDAG(ctx, rootCID); err != nil {
		t.Fatalf("FetchDAG: %v", err)
	}

	if has, _ := fetcher.blocks.Has(ctx, rootCID); !has {
		t.Errorf("fetcher missing root after FetchDAG: %s", rootCID)
	}

	visited := make(map[cid.Cid]struct{})
	var walk func(c cid.Cid) error
	walk = func(c cid.Cid) error {
		if _, seen := visited[c]; seen {
			return nil
		}
		visited[c] = struct{}{}
		data, err := fetcher.blocks.Get(ctx, c)
		if err != nil {
			return err
		}
		children, err := store.LinksOf(c, data)
		if err != nil {
			return nil
		}
		for _, child := range children {
			if err := walk(child); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(rootCID); err != nil {
		t.Fatalf("verify walk after FetchDAG: %v", err)
	}
	if len(visited) != totalBlocks {
		t.Errorf("walked %d blocks after FetchDAG, want %d", len(visited), totalBlocks)
	}
}

func TestFetchDAGPipeline_FasterThanSerialBound(t *testing.T) {

	const (
		branches	= 8
		leavesPerMid	= 8
		fetchDelay	= 50 * time.Millisecond
	)
	_, _, fetcher, rootCID, totalBlocks := newSlowFetchRing(t, fetchDelay, branches, leavesPerMid)
	ctx := context.Background()

	start := time.Now()
	if err := fetcher.FetchDAG(ctx, rootCID); err != nil {
		t.Fatalf("FetchDAG: %v", err)
	}
	elapsed := time.Since(start)

	serialBound := time.Duration(totalBlocks) * fetchDelay

	t.Logf("FetchDAG of %d blocks took %v (serial bound %v)", totalBlocks, elapsed, serialBound)

	if elapsed >= serialBound/2 {
		t.Errorf("FetchDAG too slow: %v ≥ %v (serial/2) — pipeline likely collapsed to sequential",
			elapsed, serialBound/2)
	}
}

func TestFetchDAGPipeline_BoundedConcurrency(t *testing.T) {

	const (
		branches	= 16
		leavesPerMid	= 16
		fetchDelay	= 30 * time.Millisecond
	)
	_, st, fetcher, rootCID, _ := newSlowFetchRing(t, fetchDelay, branches, leavesPerMid)
	ctx := context.Background()

	if err := fetcher.FetchDAG(ctx, rootCID); err != nil {
		t.Fatalf("FetchDAG: %v", err)
	}

	maxObserved := st.fetchBlockMax.Load()

	const replication = 3
	upperBound := int64(defaultDAGFetchConcurrency * replication)
	t.Logf("max in-flight FetchBlock observed = %d (cap = %d, upper bound %d), total calls = %d",
		maxObserved, defaultDAGFetchConcurrency, upperBound, st.fetchBlockCalls.Load())
	if maxObserved > upperBound {
		t.Errorf("bounded concurrency violated: max in-flight=%d > upper bound=%d",
			maxObserved, upperBound)
	}

	if maxObserved < int64(defaultDAGFetchConcurrency)/2 {
		t.Errorf("max in-flight=%d is suspiciously low — pipeline appears under-utilised",
			maxObserved)
	}
}

func TestFetchDAGPipeline_FirstErrorPropagatesAndCancels(t *testing.T) {

	_, _, fetcher, rootCID, _ := newSlowFetchRing(t, 0, 8, 8)

	failAfter := int64(8)
	pt := &poisonFetchTransport{failAfter: failAfter, Transport: fetcher.transport}

	fetcher.transport = pt

	ctx := context.Background()
	err := fetcher.FetchDAG(ctx, rootCID)
	if err == nil {
		t.Fatalf("expected FetchDAG to return the injected error")
	}
	if !errors.Is(err, errPoisonedFetch) {
		t.Errorf("expected wrapped errPoisonedFetch, got %v", err)
	}

	totalCalls := pt.calls.Load()
	t.Logf("FetchDAG returned %v after %d FetchBlock calls (DAG had 65 blocks)", err, totalCalls)

	maxAllowed := failAfter + int64(defaultDAGFetchConcurrency*3)
	if totalCalls > maxAllowed {
		t.Errorf("pipeline did not short-circuit: %d FetchBlock calls (expected ≤ %d)",
			totalCalls, maxAllowed)
	}
}

type poisonFetchTransport struct {
	Transport
	calls		atomic.Int64
	failAfter	int64
}

var errPoisonedFetch = errors.New("poisoned FetchBlock")

func (p *poisonFetchTransport) FetchBlock(ctx context.Context, target NodeAddr, key cid.Cid) ([]byte, error) {
	n := p.calls.Add(1)
	if n > p.failAfter {
		return nil, errPoisonedFetch
	}
	return p.Transport.FetchBlock(ctx, target, key)
}
