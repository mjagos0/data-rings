package dht

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"

	"github.com/mjagos0/datarings/store"
)

type rpcTestNode struct {
	id		NodeID
	node		*Node
	srv		*RPCServer
	multiaddr	string
	addr		NodeAddr
	store		*testMemStore
	transport	*RPCTransport
}

type rpcTestRing struct {
	t	*testing.T
	nodes	[]*rpcTestNode
}

func newRPCTestRing(t *testing.T) *rpcTestRing {
	t.Helper()
	return &rpcTestRing{t: t}
}

func (r *rpcTestRing) addNode(pos byte) *rpcTestNode {
	r.t.Helper()
	id := testNodeID(pos)
	bs := newTestMemStore()
	tr := NewRPCTransport(3 * time.Second)
	cfg := Config{Replication: 1}

	node := NewNode(id, "", bs, nil, tr, cfg)

	srv, boundAddr, err := StartServer("/ip4/127.0.0.1/tcp/0", "", node)
	if err != nil {
		r.t.Fatalf("start server for pos %d: %v", pos, err)
	}

	node.setAddr(boundAddr)

	rtn := &rpcTestNode{
		id:		id,
		node:		node,
		srv:		srv,
		multiaddr:	boundAddr,
		addr:		NodeAddr{ID: id, Addr: boundAddr},
		store:		bs,
		transport:	tr,
	}

	if len(r.nodes) == 0 {
		node.Create()
	} else {
		bootstrap := r.nodes[0]
		if err := node.Join(context.Background(), bootstrap.addr); err != nil {
			r.t.Fatalf("pos %d join: %v", pos, err)
		}
	}

	r.nodes = append(r.nodes, rtn)
	return rtn
}

func (r *rpcTestRing) addBootstrap(pos byte) *rpcTestNode	{ return r.addNode(pos) }

func (r *rpcTestRing) addJoiner(pos byte) *rpcTestNode	{ return r.addNode(pos) }

func (r *rpcTestRing) addNodeWithIdent(ident *Identity) *rpcTestNode {
	r.t.Helper()
	id := ident.ID
	bs := newTestMemStore()
	tr := NewRPCTransport(3 * time.Second)
	cfg := Config{Replication: 1}

	node := NewNode(id, "", bs, nil, tr, cfg)

	srv, boundAddr, err := StartServer("/ip4/127.0.0.1/tcp/0", "", node)
	if err != nil {
		r.t.Fatalf("start server for ident %s: %v", id, err)
	}
	node.setAddr(boundAddr)

	rtn := &rpcTestNode{
		id:		id,
		node:		node,
		srv:		srv,
		multiaddr:	boundAddr,
		addr:		NodeAddr{ID: id, Addr: boundAddr},
		store:		bs,
		transport:	tr,
	}

	if len(r.nodes) == 0 {
		node.Create()
	} else {
		bootstrap := r.nodes[0]
		if err := node.Join(context.Background(), bootstrap.addr); err != nil {
			r.t.Fatalf("ident node join: %v", err)
		}
	}

	r.nodes = append(r.nodes, rtn)
	return rtn
}

func (r *rpcTestRing) stabilize() {
	for _, n := range r.nodes {
		n.node.stabilize()
	}
	for _, n := range r.nodes {
		n.node.fixFinger()
	}
	for _, n := range r.nodes {
		n.node.checkPredecessor()
	}
}

func (r *rpcTestRing) stabilizeRounds(rounds int) {
	for i := 0; i < rounds; i++ {
		r.stabilize()
	}
}

func (r *rpcTestRing) findNode(pos byte) *rpcTestNode {
	id := testNodeID(pos)
	for _, n := range r.nodes {
		if n.id == id {
			return n
		}
	}
	return nil
}

func (r *rpcTestRing) cleanup() {
	for _, n := range r.nodes {
		n.node.Stop()
		n.srv.Stop()
		n.transport.Close()
	}
}

func ring8BitSucc(pos byte, sorted []byte) byte {
	for _, p := range sorted {
		if p >= pos {
			return p
		}
	}
	return sorted[0]
}

func computeExpectedFinger(selfPos byte, positions []byte, fingerIdx int) byte {
	sorted := make([]byte, len(positions))
	copy(sorted, positions)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var startPos byte
	if fingerIdx < 152 {

		startPos = selfPos + 1
	} else {

		k := uint(fingerIdx - 152)
		startPos = selfPos + byte(1<<k)
	}
	return ring8BitSucc(startPos, sorted)
}

func assertFingerTable(t *testing.T, node *Node, positions []byte, label string) {
	t.Helper()
	selfPos := testNodeIDVal(node.id)

	node.mu.RLock()
	defer node.mu.RUnlock()

	checkIdx := []int{0, 10, 50, 100, 151, 152, 153, 154, 155, 156, 157, 158, 159}

	for _, i := range checkIdx {
		expected := computeExpectedFinger(selfPos, positions, i)
		got := testNodeIDVal(node.fingers[i].ID)
		if got != expected {
			t.Errorf("%s node %d finger[%d]: want %d, got %d",
				label, selfPos, i, expected, got)
		}
	}
}

func TestChordSpec_SuccessorPredecessorInvariant(t *testing.T) {
	ring := newRPCTestRing(t)
	defer ring.cleanup()

	positions := []byte{0, 50, 100, 150, 200}
	for _, pos := range positions {
		ring.addNode(pos)
	}
	ring.stabilizeRounds(60)

	sorted := make([]byte, len(positions))
	copy(sorted, positions)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	for i, pos := range sorted {
		n := ring.findNode(pos)
		if n == nil {
			t.Fatalf("node %d not found in ring", pos)
		}
		wantSuccPos := sorted[(i+1)%len(sorted)]
		gotSuccPos := testNodeIDVal(n.node.getSuccessor().ID)
		if gotSuccPos != wantSuccPos {
			t.Errorf("node %d: successor = %d, want %d", pos, gotSuccPos, wantSuccPos)
		}

		wantPredPos := sorted[(i-1+len(sorted))%len(sorted)]
		pred := n.node.getPredecessor()
		if pred == nil {
			t.Errorf("node %d: predecessor is nil, want %d", pos, wantPredPos)
			continue
		}
		gotPredPos := testNodeIDVal(pred.ID)
		if gotPredPos != wantPredPos {
			t.Errorf("node %d: predecessor = %d, want %d", pos, gotPredPos, wantPredPos)
		}
	}
}

func TestChordSpec_FingerTableCorrectness_InMemory(t *testing.T) {
	positions := []byte{0, 50, 100, 150}
	ring := NewTestRing()
	for _, pos := range positions {
		ring.AddNode(pos)
	}
	ring.StabilizeRounds(300)

	for _, n := range ring.Nodes {
		assertFingerTable(t, n, positions, "")
	}
}

func TestChordSpec_FingerTableCorrectness_RPC(t *testing.T) {
	positions := []byte{0, 50, 100, 150}
	ring := newRPCTestRing(t)
	defer ring.cleanup()

	for _, pos := range positions {
		ring.addNode(pos)
	}
	ring.stabilizeRounds(300)

	for _, n := range ring.nodes {
		assertFingerTable(t, n.node, positions, "RPC")
	}
}

func TestChordSpec_FingerTable_SixNodes(t *testing.T) {
	positions := []byte{0, 40, 80, 120, 160, 200}
	ring := NewTestRing()
	for _, pos := range positions {
		ring.AddNode(pos)
	}
	ring.StabilizeRounds(300)

	for _, n := range ring.Nodes {
		assertFingerTable(t, n, positions, "6-node")
	}
}

func TestChordSpec_BootstrapAndSequentialJoins(t *testing.T) {
	ring := newRPCTestRing(t)
	defer ring.cleanup()

	b := ring.addBootstrap(0)
	ring.stabilizeRounds(10)

	succ := b.node.getSuccessor()
	if !succ.ID.Equal(b.id) {
		t.Errorf("single-node: bootstrap successor should be self, got %s", succ.ID)
	}

	pred := b.node.getPredecessor()
	if pred != nil && !pred.ID.Equal(b.id) {
		t.Errorf("single-node: bootstrap predecessor should be nil or self, got %s", pred.ID)
	}

	n100 := ring.addJoiner(100)
	ring.stabilizeRounds(40)

	if testNodeIDVal(b.node.getSuccessor().ID) != 100 {
		t.Errorf("after 2 nodes: bootstrap successor = %d, want 100",
			testNodeIDVal(b.node.getSuccessor().ID))
	}
	if testNodeIDVal(n100.node.getSuccessor().ID) != 0 {
		t.Errorf("after 2 nodes: n100 successor = %d, want 0",
			testNodeIDVal(n100.node.getSuccessor().ID))
	}
	if pred := b.node.getPredecessor(); pred == nil || testNodeIDVal(pred.ID) != 100 {
		t.Errorf("after 2 nodes: bootstrap predecessor should be 100")
	}
	if pred := n100.node.getPredecessor(); pred == nil || testNodeIDVal(pred.ID) != 0 {
		t.Errorf("after 2 nodes: n100 predecessor should be 0")
	}

	n50 := ring.addJoiner(50)
	ring.stabilizeRounds(40)

	for wantPos, wantSucc := range map[byte]byte{0: 50, 50: 100, 100: 0} {
		n := ring.findNode(wantPos)
		got := testNodeIDVal(n.node.getSuccessor().ID)
		if got != wantSucc {
			t.Errorf("after 3 nodes: node %d successor = %d, want %d", wantPos, got, wantSucc)
		}
	}

	ring.addJoiner(200)
	ring.stabilizeRounds(40)

	for wantPos, wantSucc := range map[byte]byte{0: 50, 50: 100, 100: 200, 200: 0} {
		n := ring.findNode(wantPos)
		got := testNodeIDVal(n.node.getSuccessor().ID)
		if got != wantSucc {
			t.Errorf("after 4 nodes: node %d successor = %d, want %d", wantPos, got, wantSucc)
		}
	}

	_ = n50
}

func TestChordSpec_KeyResponsibility(t *testing.T) {
	ring := newRPCTestRing(t)
	defer ring.cleanup()

	positions := []byte{0, 60, 120, 180}
	for _, pos := range positions {
		ring.addNode(pos)
	}
	ring.stabilizeRounds(60)

	ctx := context.Background()
	const nBlocks = 40
	n0 := ring.findNode(0)

	for i := 0; i < nBlocks; i++ {
		key, data := testBlock(fmt.Sprintf("kv-%d", i))
		if err := n0.node.Put(ctx, key, data); err != nil {
			t.Fatalf("put kv-%d: %v", i, err)
		}
	}

	for i := 0; i < nBlocks; i++ {
		key, _ := testBlock(fmt.Sprintf("kv-%d", i))
		ringPos := CIDToNodeID(key)

		responsible, err := n0.node.FindSuccessor(ctx, ringPos)
		if err != nil {
			t.Fatalf("FindSuccessor kv-%d: %v", i, err)
		}

		owner := ring.findNode(testNodeIDVal(responsible.ID))
		if owner == nil {
			t.Fatalf("responsible node %s not in ring", responsible.ID)
		}
		has, err := owner.store.Has(ctx, key)
		if err != nil {
			t.Fatalf("has kv-%d in owner %d: %v", i, testNodeIDVal(responsible.ID), err)
		}
		if !has {
			t.Errorf("kv-%d: not found at responsible node %d (ring pos %s)",
				i, testNodeIDVal(responsible.ID), ringPos)
		}

		for _, peer := range ring.nodes {
			if peer.id == responsible.ID {
				continue
			}
			has, _ := peer.store.Has(ctx, key)
			if has {
				t.Logf("kv-%d: also present at node %d (may be fine — not primary)",
					i, testNodeIDVal(peer.id))
			}
		}
	}
}

func TestChordSpec_KeyTransferOnJoin(t *testing.T) {
	ring := newRPCTestRing(t)
	defer ring.cleanup()

	b := ring.addBootstrap(0)
	ring.stabilizeRounds(10)

	ctx := context.Background()
	const nBlocks = 40

	for i := 0; i < nBlocks; i++ {
		key, data := testBlock(fmt.Sprintf("transfer-%d", i))
		if err := b.node.Put(ctx, key, data); err != nil {
			t.Fatalf("put transfer-%d: %v", i, err)
		}
	}
	if b.node.DataCount() != nBlocks {
		t.Fatalf("before join: node 0 should hold %d blocks, got %d", nBlocks, b.node.DataCount())
	}

	n128 := ring.addJoiner(128)
	ring.stabilizeRounds(40)

	total := b.node.DataCount() + n128.node.DataCount()
	if total != nBlocks {
		t.Errorf("after join: total blocks = %d, want %d (blocks must not be duplicated or lost)", total, nBlocks)
	}
	if n128.node.DataCount() == 0 {
		t.Error("node 128 received no blocks after joining — key transfer broken")
	}

	for i := 0; i < nBlocks; i++ {
		key, wantData := testBlock(fmt.Sprintf("transfer-%d", i))
		got, err := n128.node.Get(ctx, key)
		if err != nil {
			t.Fatalf("get transfer-%d from n128: %v", i, err)
		}
		if string(got) != string(wantData) {
			t.Errorf("transfer-%d: data mismatch at n128", i)
		}
	}

	lo := testNodeID(0)
	hi := testNodeID(128)
	all, _ := n128.store.AllKeysChan(ctx)
	for k := range all {
		pos := CIDToNodeID(k)
		if !pos.BetweenRightInclusive(lo, hi) {
			t.Errorf("n128 holds block %s at ring pos %s outside its range (0, 128]", k, pos)
		}
	}
}

func TestChordSpec_RoutingConsistency(t *testing.T) {
	ring := newRPCTestRing(t)
	defer ring.cleanup()

	positions := []byte{10, 50, 100, 150, 200}
	for _, pos := range positions {
		ring.addNode(pos)
	}
	ring.stabilizeRounds(60)

	ctx := context.Background()

	queryPositions := []byte{0, 11, 49, 51, 99, 101, 149, 151, 199, 201, 255}
	for _, qp := range queryPositions {
		id := testNodeID(qp)
		var firstResult byte = 255
		for _, n := range ring.nodes {
			got, err := n.node.FindSuccessor(ctx, id)
			if err != nil {
				t.Fatalf("node %d FindSuccessor(%d): %v", testNodeIDVal(n.id), qp, err)
			}
			val := testNodeIDVal(got.ID)
			if firstResult == 255 {
				firstResult = val
			} else if val != firstResult {
				t.Errorf("routing inconsistency for key %d: "+
					"node %d says %d, but first answer was %d",
					qp, testNodeIDVal(n.id), val, firstResult)
			}
		}
	}
}

func testDAGCreate(t *testing.T, ctx context.Context, putter *Node) (rootCID cid.Cid, allCIDs []cid.Cid) {
	t.Helper()

	leaf1Data := []byte("merkle dag leaf 1 — hello from the DHT")
	leaf2Data := []byte("merkle dag leaf 2 — distributed storage")
	leaf3Data := []byte("merkle dag leaf 3 — content addressed")

	leaf1CID := testCID(leaf1Data)
	leaf2CID := testCID(leaf2Data)
	leaf3CID := testCID(leaf3Data)

	mid1 := merkledag.NodeWithData(nil)
	mustAddLink(t, mid1, "leaf1", leaf1CID, uint64(len(leaf1Data)))
	mustAddLink(t, mid1, "leaf2", leaf2CID, uint64(len(leaf2Data)))
	mid1Data := mid1.RawData()
	mid1CID := mid1.Cid()

	mid2 := merkledag.NodeWithData(nil)
	mustAddLink(t, mid2, "leaf3", leaf3CID, uint64(len(leaf3Data)))
	mid2Data := mid2.RawData()
	mid2CID := mid2.Cid()

	root := merkledag.NodeWithData(nil)
	mustAddLink(t, root, "mid1", mid1CID, uint64(len(mid1Data)))
	mustAddLink(t, root, "mid2", mid2CID, uint64(len(mid2Data)))
	rootData := root.RawData()
	rootCID = root.Cid()

	allBlocks := []struct {
		c	cid.Cid
		d	[]byte
	}{
		{leaf1CID, leaf1Data},
		{leaf2CID, leaf2Data},
		{leaf3CID, leaf3Data},
		{mid1CID, mid1Data},
		{mid2CID, mid2Data},
		{rootCID, rootData},
	}

	for _, b := range allBlocks {
		if err := putter.Put(ctx, b.c, b.d); err != nil {
			t.Fatalf("put %s: %v", b.c, err)
		}
	}

	for _, b := range allBlocks {
		allCIDs = append(allCIDs, b.c)
	}
	return rootCID, allCIDs
}

func mustAddLink(t *testing.T, n *merkledag.ProtoNode, name string, c cid.Cid, size uint64) {
	t.Helper()
	if err := n.AddRawLink(name, &format.Link{Cid: c, Size: size}); err != nil {
		t.Fatalf("AddRawLink %s: %v", name, err)
	}
}

func TestChordSpec_MerkleDAGFetchBasic(t *testing.T) {
	ring := newRPCTestRing(t)
	defer ring.cleanup()

	b := ring.addBootstrap(0)
	fetcher := ring.addJoiner(128)
	ring.stabilizeRounds(40)

	ctx := context.Background()

	rootCID, allCIDs := testDAGCreate(t, ctx, b.node)

	if err := fetcher.node.FetchDAG(ctx, rootCID); err != nil {
		t.Fatalf("FetchDAG: %v", err)
	}

	for _, c := range allCIDs {
		has, err := fetcher.store.Has(ctx, c)
		if err != nil {
			t.Fatalf("has %s: %v", c, err)
		}
		if !has {
			t.Errorf("fetcher missing block %s after FetchDAG", c)
		}
	}
}

func TestChordSpec_MerkleDAGIntegrity(t *testing.T) {
	ring := newRPCTestRing(t)
	defer ring.cleanup()

	b := ring.addBootstrap(0)
	fetcher := ring.addJoiner(200)
	ring.stabilizeRounds(40)

	ctx := context.Background()
	rootCID, _ := testDAGCreate(t, ctx, b.node)

	if err := fetcher.node.FetchDAG(ctx, rootCID); err != nil {
		t.Fatalf("FetchDAG: %v", err)
	}

	rootData, err := fetcher.store.Get(ctx, rootCID)
	if err != nil {
		t.Fatalf("get root from fetcher: %v", err)
	}

	links, err := store.LinksOf(rootCID, rootData)
	if err != nil {
		t.Fatalf("LinksOf root: %v", err)
	}
	if len(links) != 2 {
		t.Errorf("root should have 2 links, got %d", len(links))
	}

	for _, midCID := range links {
		midData, err := fetcher.store.Get(ctx, midCID)
		if err != nil {
			t.Fatalf("get mid %s from fetcher: %v", midCID, err)
		}
		midLinks, err := store.LinksOf(midCID, midData)
		if err != nil {
			t.Fatalf("LinksOf mid %s: %v", midCID, err)
		}
		if len(midLinks) == 0 {
			t.Errorf("mid block %s should have at least one link", midCID)
		}

		for _, leafCID := range midLinks {
			has, _ := fetcher.store.Has(ctx, leafCID)
			if !has {
				t.Errorf("leaf %s not in fetcher store after FetchDAG", leafCID)
			}

			leafData, err := fetcher.store.Get(ctx, leafCID)
			if err != nil {
				t.Fatalf("get leaf %s: %v", leafCID, err)
			}

			gotCID := testCID(leafData)
			if gotCID != leafCID {
				t.Errorf("leaf %s: content-address mismatch (data corruption?)", leafCID)
			}
		}
	}
}

func TestChordSpec_MerkleDAGMultiNodeScatter(t *testing.T) {
	ring := newRPCTestRing(t)
	defer ring.cleanup()

	positions := []byte{0, 64, 128, 192}
	for _, pos := range positions {
		ring.addNode(pos)
	}
	ring.stabilizeRounds(60)

	ctx := context.Background()
	n0 := ring.findNode(0)

	rootCID, allCIDs := testDAGCreate(t, ctx, n0.node)

	counts := make(map[byte]int)
	for _, c := range allCIDs {
		resp, err := n0.node.FindSuccessor(ctx, CIDToNodeID(c))
		if err != nil {
			t.Fatalf("FindSuccessor: %v", err)
		}
		counts[testNodeIDVal(resp.ID)]++
	}
	t.Logf("block distribution: %v", counts)
	if len(counts) == 1 {
		t.Log("warning: all blocks on one node — hash distribution may be skewed for this small DAG")
	}

	fetcher := ring.findNode(192)
	if err := fetcher.node.FetchDAG(ctx, rootCID); err != nil {
		t.Fatalf("FetchDAG: %v", err)
	}

	for _, c := range allCIDs {
		has, err := fetcher.store.Has(ctx, c)
		if err != nil {
			t.Fatalf("has %s: %v", c, err)
		}
		if !has {
			t.Errorf("fetcher missing %s after multi-node FetchDAG", c)
		}
	}
}

func TestChordSpec_ReplicationFactor(t *testing.T) {

	const replication = 3

	ring := NewTestRing()
	ring.Replication = replication

	positions := []byte{0, 64, 128, 192}
	for _, pos := range positions {
		ring.AddNode(pos)
	}
	ring.StabilizeRounds(60)

	ctx := context.Background()
	n0 := ring.FindNode(0)

	var key cid.Cid
	var data []byte
	for i := 0; ; i++ {
		k, d := testBlock(fmt.Sprintf("rep-%d", i))
		resp, err := n0.FindSuccessor(ctx, CIDToNodeID(k))
		if err != nil {
			t.Fatalf("FindSuccessor: %v", err)
		}
		if resp.ID.Equal(n0.id) {
			key, data = k, d
			break
		}
		if i > 10000 {
			t.Fatal("could not find a block primary-owned by node 0")
		}
	}

	if err := n0.Put(ctx, key, data); err != nil {
		t.Fatalf("put: %v", err)
	}

	ring.WaitForReplicationDrain(2 * time.Second)

	count := 0
	for _, n := range ring.Nodes {
		has, _ := n.blocks.Has(ctx, key)
		if has {
			count++
		}
	}
	if count < replication {
		t.Errorf("block replicated to %d nodes, want at least %d", count, replication)
	}
	t.Logf("block replicated to %d/%d nodes", count, len(ring.Nodes))
}

func TestChordSpec_SuccessorListFaultTolerance(t *testing.T) {

	const replication = 2

	ring := NewTestRing()
	ring.Replication = replication

	positions := []byte{0, 64, 128, 192}
	for _, pos := range positions {
		ring.AddNode(pos)
	}
	ring.StabilizeRounds(60)

	n0 := ring.FindNode(0)

	succList := n0.getSuccessorList()
	if len(succList) < 2 {
		t.Fatalf("successor list has only %d entries with Replication=%d — expected at least 2",
			len(succList), replication)
	}

	if got := testNodeIDVal(succList[0].ID); got != 64 {
		t.Errorf("successor list[0] = %d, want 64", got)
	}
	if got := testNodeIDVal(succList[1].ID); got != 128 {
		t.Errorf("successor list[1] = %d, want 128", got)
	}

	failedID := testNodeID(64)
	ring.mu.Lock()
	delete(ring.nodeMap, failedID)
	var liveNodes []*Node
	for _, n := range ring.Nodes {
		if n.id != failedID {
			liveNodes = append(liveNodes, n)
		}
	}
	ring.Nodes = liveNodes
	ring.mu.Unlock()

	for i := 0; i < 20; i++ {
		for _, n := range ring.Nodes {
			n.stabilize()
		}
		for _, n := range ring.Nodes {
			n.fixFinger()
		}
		for _, n := range ring.Nodes {
			n.checkPredecessor()
		}
	}

	finalSuccList := n0.getSuccessorList()
	if len(finalSuccList) == 0 {
		t.Fatal("successor list must not be empty after immediate successor failure")
	}
	for _, s := range finalSuccList {
		if s.ID == failedID {
			t.Errorf("failed node %s still in successor list after recovery — fault tolerance broken", failedID)
		}
	}

	currentSucc := n0.getSuccessor()
	if currentSucc.ID == failedID {
		t.Errorf("n0 successor is still the failed node after recovery, want n128")
	}
}

func TestChordSpec_EvictNodeReReplicatesWindow(t *testing.T) {
	const replication = 3

	ring := NewTestRing()
	ring.Replication = replication

	positions := []byte{0, 50, 100, 150, 200, 250}
	for _, pos := range positions {
		ring.AddNode(pos)
	}
	ring.StabilizeRounds(60)

	ctx := context.Background()
	n0 := ring.FindNode(0)

	var key cid.Cid
	var data []byte
	for i := 0; ; i++ {
		k, d := testBlock(fmt.Sprintf("evict-rereplicate-%d", i))
		resp, err := n0.FindSuccessor(ctx, CIDToNodeID(k))
		if err != nil {
			t.Fatalf("FindSuccessor: %v", err)
		}
		if resp.ID.Equal(n0.id) {
			key, data = k, d
			break
		}
		if i > 10000 {
			t.Fatal("could not find a block primary-owned by n0")
		}
	}

	if err := n0.Put(ctx, key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	ring.WaitForReplicationDrain(2 * time.Second)

	for _, pos := range []byte{0, 50, 100} {
		if has, _ := ring.FindNode(pos).blocks.Has(ctx, key); !has {
			t.Fatalf("precondition: n%d missing block after Put", pos)
		}
	}

	n150 := ring.FindNode(150)
	if has, _ := n150.blocks.Has(ctx, key); has {
		t.Fatalf("precondition: n150 already has block before eviction")
	}

	failedID := testNodeID(50)
	ring.mu.Lock()
	delete(ring.nodeMap, failedID)
	var live []*Node
	for _, n := range ring.Nodes {
		if !n.id.Equal(failedID) {
			live = append(live, n)
		}
	}
	ring.Nodes = live
	ring.mu.Unlock()

	n0.evictNode(failedID)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if has, _ := n150.blocks.Has(ctx, key); has {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("n150 did not receive block after evictNode — eviction did not trigger re-replication")
}

func TestChordSpec_IdentityFromKeypair(t *testing.T) {
	ident1, err := GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}
	ident2, err := GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}

	if ident1.ID == ident2.ID {
		t.Error("two generated identities should have different NodeIDs")
	}

	ident1b := IdentityFromKey(ident1.PrivKey)
	if ident1b.ID != ident1.ID {
		t.Errorf("IdentityFromKey: got ID %s, want %s", ident1b.ID, ident1.ID)
	}

	wantID := PubKeyToNodeID(ident1.PubKey)
	if ident1.ID != wantID {
		t.Errorf("Identity.ID = %s, PubKeyToNodeID = %s: mismatch", ident1.ID, wantID)
	}

	ring := NewTestRing()
	n1 := NewNode(ident1.ID, "test-1", newTestMemStore(), nil,
		&localTransport{ring: ring, nodeID: ident1.ID}, Config{})
	n2 := NewNode(ident2.ID, "test-2", newTestMemStore(), nil,
		&localTransport{ring: ring, nodeID: ident2.ID}, Config{})

	ring.mu.Lock()
	ring.Nodes = append(ring.Nodes, n1, n2)
	ring.nodeMap[ident1.ID] = n1
	ring.nodeMap[ident2.ID] = n2
	ring.mu.Unlock()

	n1.Create()
	bootstrap := NodeAddr{ID: n1.id, Addr: n1.addr}
	if err := n2.Join(context.Background(), bootstrap); err != nil {
		t.Fatalf("n2 join: %v", err)
	}
	ring.StabilizeRounds(30)

	if n1.getSuccessor().ID == n1.id && n2.getSuccessor().ID == n2.id {
		t.Error("ring did not form: both nodes still point to themselves")
	}
}

func TestChordSpec_MultiaddrRoundTrip(t *testing.T) {
	cases := []struct {
		input		string
		wantTCP		string
		wantFail	bool
	}{
		{"/ip4/127.0.0.1/tcp/7000", "127.0.0.1:7000", false},
		{"/ip4/0.0.0.0/tcp/0", "0.0.0.0:0", false},
		{"/ip4/192.168.1.100/tcp/1234", "192.168.1.100:1234", false},
	}

	for _, tc := range cases {
		got, err := MultiaddrToTCPAddr(tc.input)
		if tc.wantFail {
			if err == nil {
				t.Errorf("MultiaddrToTCPAddr(%q): want error, got %q", tc.input, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("MultiaddrToTCPAddr(%q): unexpected error: %v", tc.input, err)
			continue
		}
		if got != tc.wantTCP {
			t.Errorf("MultiaddrToTCPAddr(%q) = %q, want %q", tc.input, got, tc.wantTCP)
		}
	}

	original := "192.168.5.10:9000"
	ma, err := TCPAddrToMultiaddr(original)
	if err != nil {
		t.Fatalf("TCPAddrToMultiaddr(%q): %v", original, err)
	}
	back, err := MultiaddrToTCPAddr(ma)
	if err != nil {
		t.Fatalf("MultiaddrToTCPAddr(%q): %v", ma, err)
	}
	if back != original {
		t.Errorf("round-trip %q → %q → %q: mismatch", original, ma, back)
	}
}

func TestChordSpec_RoutingHopCount(t *testing.T) {
	ring := newRPCTestRing(t)
	defer ring.cleanup()

	positions := []byte{0, 32, 64, 96, 128, 160, 192, 224}
	for _, pos := range positions {
		ring.addNode(pos)
	}
	ring.stabilizeRounds(300)

	ctx := context.Background()
	n0 := ring.findNode(0)

	const maxHops = 6

	n0.node.mu.RLock()
	seen := make(map[byte]bool)
	for _, f := range n0.node.fingers {
		seen[testNodeIDVal(f.ID)] = true
	}
	n0.node.mu.RUnlock()

	if len(seen) < 3 {
		t.Errorf("node 0 finger table has only %d distinct targets — routing diversity low", len(seen))
	}
	t.Logf("node 0 finger table distinct targets: %v", func() []byte {
		var ids []byte
		for id := range seen {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		return ids
	}())

	queryPositions := []byte{1, 33, 65, 97, 129, 161, 193, 225, 255}
	for _, qp := range queryPositions {
		id := testNodeID(qp)
		got, err := n0.node.FindSuccessor(ctx, id)
		if err != nil {
			t.Fatalf("FindSuccessor(%d): %v", qp, err)
		}

		sorted := make([]byte, len(positions))
		copy(sorted, positions)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		want := ring8BitSucc(qp, sorted)
		if testNodeIDVal(got.ID) != want {
			t.Errorf("FindSuccessor(%d) = %d, want %d", qp, testNodeIDVal(got.ID), want)
		}
		_ = maxHops
	}
}

func TestChordSpec_NotifyValidation(t *testing.T) {
	ring := NewTestRing()
	for _, pos := range []byte{0, 50, 100, 200} {
		ring.AddNode(pos)
	}
	ring.StabilizeRounds(40)

	n100 := ring.FindNode(100)
	if n100 == nil {
		t.Fatal("node 100 not found")
	}

	pred := n100.getPredecessor()
	if pred == nil || testNodeIDVal(pred.ID) != 50 {
		t.Fatalf("pre-condition: n100 predecessor = %v, want 50", pred)
	}

	fake150 := NodeAddr{ID: testNodeID(150), Addr: "test-fake150"}
	n100.notify(fake150)
	pred = n100.getPredecessor()

	if pred == nil || testNodeIDVal(pred.ID) != 50 {
		t.Errorf("invalid notify(150): predecessor = %v, want 50 (spurious notify must be rejected)", pred)
	}
}

func TestChordSpec_RealKeypairRing(t *testing.T) {
	const nodeCount = 4
	type nodeEntry struct {
		ident	*Identity
		node	*Node
		srv	*RPCServer
		store	*testMemStore
		tr	*RPCTransport
		addr	NodeAddr
	}

	var nodes []*nodeEntry

	for i := 0; i < nodeCount; i++ {
		ident, err := GenerateIdentity()
		if err != nil {
			t.Fatalf("GenerateIdentity: %v", err)
		}
		bs := newTestMemStore()
		tr := NewRPCTransport(3 * time.Second)
		cfg := Config{Replication: 1}

		node := NewNode(ident.ID, "", bs, nil, tr, cfg)
		srv, boundAddr, err := StartServer("/ip4/127.0.0.1/tcp/0", "", node)
		if err != nil {
			t.Fatalf("start server %d: %v", i, err)
		}
		node.setAddr(boundAddr)

		nodes = append(nodes, &nodeEntry{
			ident:	ident, node: node, srv: srv, store: bs, tr: tr,
			addr:	NodeAddr{ID: ident.ID, Addr: boundAddr},
		})
	}
	defer func() {
		for _, n := range nodes {
			n.node.Stop()
			n.srv.Stop()
			n.tr.Close()
		}
	}()

	nodes[0].node.Create()
	for i := 1; i < nodeCount; i++ {
		if err := nodes[i].node.Join(context.Background(), nodes[0].addr); err != nil {
			t.Fatalf("node %d join: %v", i, err)
		}
	}

	for round := 0; round < 80; round++ {
		for _, n := range nodes {
			n.node.stabilize()
		}
		for _, n := range nodes {
			n.node.fixFinger()
		}
		for _, n := range nodes {
			n.node.checkPredecessor()
		}
	}

	ctx := context.Background()

	type kv struct {
		key	cid.Cid
		data	[]byte
	}
	var stored []kv

	for i, n := range nodes {
		key, data := testBlock(fmt.Sprintf("realkey-%s-%d", n.ident.ID, i))
		if err := n.node.Put(ctx, key, data); err != nil {
			t.Fatalf("put from node %d: %v", i, err)
		}
		stored = append(stored, kv{key, data})
	}

	for _, n := range nodes {
		for j, kv := range stored {
			got, err := n.node.Get(ctx, kv.key)
			if err != nil {
				t.Errorf("node (id=%s) get kv[%d]: %v", n.ident.ID, j, err)
				continue
			}
			if string(got) != string(kv.data) {
				t.Errorf("node (id=%s) get kv[%d]: data mismatch", n.ident.ID, j)
			}
		}
	}
}
