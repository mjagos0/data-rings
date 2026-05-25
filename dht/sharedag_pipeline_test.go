package dht

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"

	"github.com/mjagos0/datarings/store"
)

func newSlowDAGRing(t *testing.T, putBlockDelay time.Duration) (*TestRing, *slowTransport) {
	t.Helper()
	ring := NewTestRing()
	ring.Replication = 3
	for i := 0; i < 8; i++ {
		ring.AddNode(byte(i * 32))
	}
	ring.StabilizeRounds(12)

	st := &slowTransport{putBlockDelay: putBlockDelay}
	for _, n := range ring.Nodes {
		st.Transport = n.transport
		n.transport = st
	}
	return ring, st
}

func linearDAG(t *testing.T, ctx context.Context, publisher *Node, count int) (cid.Cid, []cid.Cid) {
	t.Helper()
	type block struct {
		c	cid.Cid
		d	[]byte
	}
	var all []block

	mids := make([]*merkledag.ProtoNode, count)
	leafCIDs := make([]cid.Cid, count)
	leafData := make([][]byte, count)
	for i := count - 1; i >= 0; i-- {
		ld := []byte(fmt.Sprintf("leaf-%d-payload-bytes", i))
		lc := testCID(ld)
		leafCIDs[i] = lc
		leafData[i] = ld

		m := merkledag.NodeWithData([]byte(fmt.Sprintf("mid-%d", i)))
		if err := m.AddRawLink("leaf", &format.Link{Cid: lc, Size: uint64(len(ld))}); err != nil {
			t.Fatalf("AddRawLink leaf: %v", err)
		}
		if i+1 < count {
			next := mids[i+1]
			if err := m.AddRawLink("next", &format.Link{Cid: next.Cid(), Size: uint64(len(next.RawData()))}); err != nil {
				t.Fatalf("AddRawLink next: %v", err)
			}
		}
		mids[i] = m
	}

	root := merkledag.NodeWithData([]byte("root"))
	if err := root.AddRawLink("head", &format.Link{Cid: mids[0].Cid(), Size: uint64(len(mids[0].RawData()))}); err != nil {
		t.Fatalf("AddRawLink head: %v", err)
	}

	all = append(all, block{root.Cid(), root.RawData()})
	for i := 0; i < count; i++ {
		all = append(all, block{mids[i].Cid(), mids[i].RawData()})
		all = append(all, block{leafCIDs[i], leafData[i]})
	}

	for _, b := range all {
		if err := publisher.blocks.Put(ctx, b.c, b.d); err != nil {
			t.Fatalf("seed local block %s: %v", b.c, err)
		}
	}

	cids := make([]cid.Cid, 0, len(all))
	for _, b := range all {
		cids = append(cids, b.c)
	}
	return root.Cid(), cids
}

func wideDAG(t *testing.T, ctx context.Context, publisher *Node, branches, leavesPerMid int) (cid.Cid, []cid.Cid) {
	t.Helper()
	type block struct {
		c	cid.Cid
		d	[]byte
	}
	var all []block

	root := merkledag.NodeWithData([]byte("wide-root"))
	for b := 0; b < branches; b++ {
		mid := merkledag.NodeWithData([]byte(fmt.Sprintf("wide-mid-%d", b)))
		for l := 0; l < leavesPerMid; l++ {
			ld := []byte(fmt.Sprintf("wide-leaf-b%d-l%d-payload-bytes-padding", b, l))
			lc := testCID(ld)
			if err := mid.AddRawLink(fmt.Sprintf("leaf%d", l), &format.Link{Cid: lc, Size: uint64(len(ld))}); err != nil {
				t.Fatalf("AddRawLink leaf: %v", err)
			}
			all = append(all, block{lc, ld})
		}
		all = append(all, block{mid.Cid(), mid.RawData()})
		if err := root.AddRawLink(fmt.Sprintf("mid%d", b), &format.Link{Cid: mid.Cid(), Size: uint64(len(mid.RawData()))}); err != nil {
			t.Fatalf("AddRawLink mid: %v", err)
		}
	}
	all = append(all, block{root.Cid(), root.RawData()})

	for _, b := range all {
		if err := publisher.blocks.Put(ctx, b.c, b.d); err != nil {
			t.Fatalf("seed local block %s: %v", b.c, err)
		}
	}
	cids := make([]cid.Cid, 0, len(all))
	for _, b := range all {
		cids = append(cids, b.c)
	}
	return root.Cid(), cids
}

func TestShareDAGPipeline_AllBlocksDistributed(t *testing.T) {
	ring, _ := newSlowDAGRing(t, 0)
	publisher := ring.Nodes[0]
	ctx := context.Background()

	rootCID, allCIDs := linearDAG(t, ctx, publisher, 8)
	if err := publisher.ShareDAG(ctx, rootCID); err != nil {
		t.Fatalf("ShareDAG: %v", err)
	}

	for _, n := range ring.Nodes {
		n.WaitForReplicationDrain(2 * time.Second)
	}

	fetcher := ring.Nodes[3]
	for _, c := range allCIDs {
		got, err := fetcher.Get(ctx, c)
		if err != nil {
			t.Fatalf("Get %s: %v", c, err)
		}
		if len(got) == 0 {
			t.Errorf("Get %s returned empty data", c)
		}
	}
}

func TestShareDAGPipeline_FasterThanSerialBound(t *testing.T) {
	const (
		blockCount	= 32
		putDelay	= 50 * time.Millisecond
	)
	ring, _ := newSlowDAGRing(t, putDelay)
	publisher := ring.Nodes[0]
	ctx := context.Background()

	rootCID, allCIDs := linearDAG(t, ctx, publisher, blockCount)
	totalBlocks := len(allCIDs)

	start := time.Now()
	if err := publisher.ShareDAG(ctx, rootCID); err != nil {
		t.Fatalf("ShareDAG: %v", err)
	}
	elapsed := time.Since(start)

	serialBound := time.Duration(totalBlocks) * putDelay

	pipelineBound := time.Duration((totalBlocks+defaultDAGShareConcurrency-1)/defaultDAGShareConcurrency) * putDelay

	cap := pipelineBound * 2

	t.Logf("ShareDAG of %d blocks took %v (serial bound %v, pipeline bound %v, cap %v)",
		totalBlocks, elapsed, serialBound, pipelineBound, cap)

	if elapsed >= serialBound/2 {
		t.Errorf("ShareDAG too slow: %v ≥ %v (serial/2) — pipeline likely collapsed to sequential",
			elapsed, serialBound/2)
	}
	if elapsed > cap {
		t.Errorf("ShareDAG slower than expected pipeline cap: %v > %v", elapsed, cap)
	}
}

func TestShareDAGPipeline_BoundedConcurrency(t *testing.T) {
	const blockCount = 64
	const putDelay = 30 * time.Millisecond
	ring, st := newSlowDAGRing(t, putDelay)
	publisher := ring.Nodes[0]
	ctx := context.Background()

	rootCID, _ := linearDAG(t, ctx, publisher, blockCount)
	if err := publisher.ShareDAG(ctx, rootCID); err != nil {
		t.Fatalf("ShareDAG: %v", err)
	}

	maxObserved := st.putBlockMax.Load()
	t.Logf("max in-flight PutBlock observed = %d (cap = %d), total calls = %d",
		maxObserved, defaultDAGShareConcurrency, st.putBlockCalls.Load())
	if maxObserved > int64(defaultDAGShareConcurrency) {
		t.Errorf("bounded concurrency violated: max in-flight=%d > cap=%d",
			maxObserved, defaultDAGShareConcurrency)
	}

	if maxObserved < int64(defaultDAGShareConcurrency)/2 {
		t.Errorf("max in-flight=%d is suspiciously low — pipeline appears under-utilised",
			maxObserved)
	}
}

func TestShareDAGPipeline_FirstErrorPropagatesAndCancels(t *testing.T) {
	ring := NewTestRing()
	ring.Replication = 3
	for i := 0; i < 8; i++ {
		ring.AddNode(byte(i * 32))
	}
	ring.StabilizeRounds(12)

	failAfter := int64(8)
	pt := &poisonTransport{failAfter: failAfter}
	for _, n := range ring.Nodes {
		pt.Transport = n.transport
		n.transport = pt
	}

	publisher := ring.Nodes[0]
	ctx := context.Background()
	rootCID, _ := linearDAG(t, ctx, publisher, 32)

	err := publisher.ShareDAG(ctx, rootCID)
	if err == nil {
		t.Fatalf("expected ShareDAG to return the injected error")
	}
	if !errors.Is(err, errPoisoned) {
		t.Errorf("expected wrapped errPoisoned, got %v", err)
	}

	totalCalls := pt.calls.Load()
	t.Logf("ShareDAG returned %v after %d PutBlock calls (DAG had 65 blocks)",
		err, totalCalls)

	if totalCalls > failAfter+int64(defaultDAGShareConcurrency) {
		t.Errorf("pipeline did not short-circuit: %d PutBlock calls (expected ≤ %d)",
			totalCalls, failAfter+int64(defaultDAGShareConcurrency))
	}
}

type poisonTransport struct {
	Transport
	calls		atomic.Int64
	failAfter	int64
}

var errPoisoned = errors.New("poisoned PutBlock")

func (p *poisonTransport) PutBlock(ctx context.Context, target NodeAddr, key cid.Cid, data []byte, rootCID cid.Cid, rootExpiry int64) error {
	n := p.calls.Add(1)
	if n > p.failAfter {
		return errPoisoned
	}
	return p.Transport.PutBlock(ctx, target, key, data, rootCID, rootExpiry)
}

var _ = store.RingPublic
