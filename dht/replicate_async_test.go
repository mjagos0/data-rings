package dht

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/mjagos0/datarings/store"
)

type slowTransport struct {
	Transport
	pushDelay		time.Duration
	pushCalls		atomic.Int64
	pushActive		atomic.Int64
	putBlockDelay		time.Duration
	putBlockCalls		atomic.Int64
	putBlockActive		atomic.Int64
	putBlockMax		atomic.Int64
	fetchBlockDelay		time.Duration
	fetchBlockCalls		atomic.Int64
	fetchBlockActive	atomic.Int64
	fetchBlockMax		atomic.Int64
}

func (s *slowTransport) PushBlocks(ctx context.Context, target NodeAddr, keys []cid.Cid, data [][]byte, blockRoots [][]store.NetworkRootEntry) error {
	s.pushCalls.Add(1)
	s.pushActive.Add(1)
	defer s.pushActive.Add(-1)
	if s.pushDelay > 0 {
		select {
		case <-time.After(s.pushDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.Transport.PushBlocks(ctx, target, keys, data, blockRoots)
}

func (s *slowTransport) PutBlock(ctx context.Context, target NodeAddr, key cid.Cid, data []byte, rootCID cid.Cid, rootExpiry int64) error {
	s.putBlockCalls.Add(1)
	cur := s.putBlockActive.Add(1)
	defer s.putBlockActive.Add(-1)
	for {
		prev := s.putBlockMax.Load()
		if cur <= prev || s.putBlockMax.CompareAndSwap(prev, cur) {
			break
		}
	}
	if s.putBlockDelay > 0 {
		select {
		case <-time.After(s.putBlockDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.Transport.PutBlock(ctx, target, key, data, rootCID, rootExpiry)
}

func (s *slowTransport) FetchBlock(ctx context.Context, target NodeAddr, key cid.Cid) ([]byte, error) {
	s.fetchBlockCalls.Add(1)
	cur := s.fetchBlockActive.Add(1)
	defer s.fetchBlockActive.Add(-1)
	for {
		prev := s.fetchBlockMax.Load()
		if cur <= prev || s.fetchBlockMax.CompareAndSwap(prev, cur) {
			break
		}
	}
	if s.fetchBlockDelay > 0 {
		select {
		case <-time.After(s.fetchBlockDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.Transport.FetchBlock(ctx, target, key)
}

func newSlowReplicaRing(t *testing.T, replication int, pushDelay time.Duration) (*TestRing, *slowTransport) {
	t.Helper()
	ring := NewTestRing()
	ring.Replication = replication
	ring.AddNode(0)
	ring.AddNode(64)
	ring.AddNode(128)
	ring.AddNode(192)
	ring.StabilizeRounds(10)

	st := &slowTransport{pushDelay: pushDelay}
	for _, n := range ring.Nodes {
		st.Transport = n.transport
		n.transport = st
	}
	return ring, st
}

func totalInFlight(ring *TestRing) int {
	t := 0
	for _, n := range ring.Nodes {
		t += n.ReplicationsInFlight()
	}
	return t
}

func TestReplicationDrain_StopWaitsForInFlightPushes(t *testing.T) {
	ring, st := newSlowReplicaRing(t, 3, 200*time.Millisecond)

	ctx := context.Background()
	const writes = 16
	for i := 0; i < writes; i++ {
		key, data := testBlock(fmt.Sprintf("drain-%d", i))

		if err := ring.Nodes[i%len(ring.Nodes)].Put(ctx, key, data); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	time.Sleep(30 * time.Millisecond)
	if got := totalInFlight(ring); got == 0 {
		t.Fatalf("expected total in-flight replications > 0 after rapid Puts, got %d", got)
	}

	start := time.Now()
	for _, n := range ring.Nodes {
		n.Stop()
	}
	elapsed := time.Since(start)

	if got := totalInFlight(ring); got != 0 {
		t.Errorf("after Stop: total in-flight replications=%d, want 0 (Stop did not drain)", got)
	}

	if elapsed < 50*time.Millisecond {
		t.Errorf("Stop returned in %s — too fast, replications were probably abandoned", elapsed)
	}
	t.Logf("Stop drained in %s; pushes completed=%d", elapsed, st.pushCalls.Load())
}

func TestReplicationDrain_BoundedConcurrencyUnderLoad(t *testing.T) {
	ring, _ := newSlowReplicaRing(t, 3, 100*time.Millisecond)
	primary := ring.FindNode(0)
	defer primary.Stop()

	const concurrentPuts = defaultReplicationConcurrency * 4
	var maxInFlight int64
	var samplerStop atomic.Bool
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		for !samplerStop.Load() {
			cur := int64(primary.ReplicationsInFlight())
			for {
				prev := atomic.LoadInt64(&maxInFlight)
				if cur <= prev || atomic.CompareAndSwapInt64(&maxInFlight, prev, cur) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < concurrentPuts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key, data := testBlock(fmt.Sprintf("load-%d", i))
			_ = primary.Put(context.Background(), key, data)
		}(i)
	}
	wg.Wait()

	primary.WaitForReplicationDrain(5 * time.Second)
	samplerStop.Store(true)
	<-samplerDone

	if maxInFlight > int64(defaultReplicationConcurrency) {
		t.Errorf("bounded concurrency violated: max in-flight=%d > cap=%d",
			maxInFlight, defaultReplicationConcurrency)
	}
	if maxInFlight == 0 {
		t.Errorf("max in-flight=0 — sampler never observed a non-empty queue; test setup broken")
	}
	t.Logf("max in-flight observed=%d (cap=%d); dropped=%d",
		maxInFlight, defaultReplicationConcurrency, primary.ReplicationsDroppedTotal())
}

func TestReplicationDrain_FetchFanOutReturnsFastestReplica(t *testing.T) {

	ring, _ := newSlowReplicaRing(t, 3, 30*time.Millisecond)
	defer func() {
		for _, n := range ring.Nodes {
			n.Stop()
		}
	}()

	primary := ring.Nodes[0]
	ctx := context.Background()

	key, data := testBlock("fetch-fanout")
	if err := primary.Put(ctx, key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	for _, n := range ring.Nodes {
		n.WaitForReplicationDrain(2 * time.Second)
	}

	holders := 0
	for _, n := range ring.Nodes {
		if has, _ := n.blocks.Has(ctx, key); has {
			holders++
		}
	}
	if holders < 3 {
		t.Fatalf("setup: only %d holders, want >=3 for k=3 fan-out test", holders)
	}

	var fetcher *Node
	for _, n := range ring.Nodes {
		if has, _ := n.blocks.Has(ctx, key); !has {
			fetcher = n
			break
		}
	}
	if fetcher == nil {

		t.Skip("every node holds the block; can't exercise fan-out path")
	}

	start := time.Now()
	got, err := fetcher.Get(ctx, key)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("Get returned wrong bytes: got %d, want %d", len(got), len(data))
	}
	t.Logf("parallel-fanout fetch took %v (sequential bound: ~%v, parallel bound: ~%v)",
		elapsed, 90*time.Millisecond, 60*time.Millisecond)

	if elapsed > 80*time.Millisecond {
		t.Errorf("fetch latency %v exceeds parallel-fanout bound (80 ms) — probes look sequential",
			elapsed)
	}
}

func TestReplicationDrain_NodeGetFanOutOnLocalMiss(t *testing.T) {
	ring, _ := newSlowReplicaRing(t, 3, 30*time.Millisecond)
	defer func() {
		for _, n := range ring.Nodes {
			n.Stop()
		}
	}()

	ctx := context.Background()
	key, data := testBlock("nodeget-fanout")
	if err := ring.Nodes[0].Put(ctx, key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	for _, n := range ring.Nodes {
		n.WaitForReplicationDrain(2 * time.Second)
	}

	id := CIDToNodeID(key)
	resp, err := ring.Nodes[0].FindSuccessor(ctx, id)
	if err != nil {
		t.Fatalf("FindSuccessor: %v", err)
	}
	var responsible *Node
	for _, n := range ring.Nodes {
		if n.id.Equal(resp.ID) {
			responsible = n
			break
		}
	}
	if responsible == nil {
		t.Fatalf("could not locate responsible node %v in ring", resp.ID)
	}
	if err := responsible.blocks.Delete(ctx, key); err != nil {
		t.Fatalf("Delete from responsible: %v", err)
	}
	if has, _ := responsible.blocks.Has(ctx, key); has {
		t.Fatalf("Delete did not remove block from responsible store")
	}

	got, err := responsible.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get on responsible after local delete: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("Get returned wrong bytes: got %d, want %d", len(got), len(data))
	}
}

func TestReplicationDrain_ReplicasEventuallyHaveBlock(t *testing.T) {
	ring, _ := newSlowReplicaRing(t, 3, 30*time.Millisecond)
	primary := ring.FindNode(0)
	defer primary.Stop()

	key, data := testBlock("eventual-replica")
	ctx := context.Background()

	if err := primary.Put(ctx, key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	for _, n := range ring.Nodes {
		if !n.WaitForReplicationDrain(2 * time.Second) {
			t.Fatalf("WaitForReplicationDrain timed out on %v; in-flight=%d",
				n.id, n.ReplicationsInFlight())
		}
	}

	holders := 0
	for _, n := range ring.Nodes {
		has, err := n.blocks.Has(ctx, key)
		if err == nil && has {
			holders++
		}
	}
	if holders < 3 {
		t.Errorf("after drain: %d holders, want >=3 (k=3 replication)", holders)
	}
}
