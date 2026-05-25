package dht

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/mjagos0/datarings/store"
)

type localTransport struct {
	ring	*TestRing
	nodeID	NodeID
}

var _ Transport = (*localTransport)(nil)

func (t *localTransport) resolve(target NodeAddr) (*Node, error) {
	node := t.ring.getNode(t.nodeID, target.ID)
	if node == nil {
		return nil, fmt.Errorf("node %s unreachable", target.ID)
	}
	return node, nil
}

func (t *localTransport) FindSuccessor(ctx context.Context, target NodeAddr, id NodeID) (NodeAddr, error) {
	node, err := t.resolve(target)
	if err != nil {
		return NodeAddr{}, err
	}
	return node.FindSuccessor(ctx, id)
}

func (t *localTransport) ClosestPrecedingNode(_ context.Context, target NodeAddr, id NodeID) (NodeAddr, NodeAddr, error) {
	node, err := t.resolve(target)
	if err != nil {
		return NodeAddr{}, NodeAddr{}, err
	}
	return node.closestPrecedingNode(id), node.getSuccessor(), nil
}

func (t *localTransport) GetPredecessor(_ context.Context, target NodeAddr) (*NodeAddr, error) {
	node, err := t.resolve(target)
	if err != nil {
		return nil, err
	}
	return node.getPredecessor(), nil
}

func (t *localTransport) GetSuccessorList(_ context.Context, target NodeAddr) ([]NodeAddr, error) {
	node, err := t.resolve(target)
	if err != nil {
		return nil, err
	}
	return node.getSuccessorList(), nil
}

func (t *localTransport) Notify(_ context.Context, target NodeAddr, caller NodeAddr) error {
	node, err := t.resolve(target)
	if err != nil {
		return err
	}
	node.notify(caller)
	return nil
}

func (t *localTransport) PutBlock(_ context.Context, target NodeAddr, key cid.Cid, data []byte, rootCID cid.Cid, rootExpiry int64) error {
	node, err := t.resolve(target)
	if err != nil {
		return err
	}
	ctx := context.Background()

	storedLocally := false
	if rv := node.ringStore(); rv != nil {
		if putErr := rv.PutWithRoot(ctx, key, data, rootCID, rootExpiry); putErr != nil {
			if !store.IsStorageFull(putErr) {
				return putErr
			}
		} else {
			storedLocally = true
		}
	} else {
		if putErr := node.blocks.Put(ctx, key, data); putErr != nil {
			if !store.IsStorageFull(putErr) {
				return putErr
			}
		} else {
			storedLocally = true
		}
	}

	if storedLocally {
		node.replicateAsync(context.Background(), key, data, rootCID, rootExpiry)
		return nil
	}
	replicaOK, _ := node.replicateAndReport(ctx, key, data, rootCID, rootExpiry)
	if replicaOK == 0 {
		return store.ErrStorageFull
	}
	return nil
}

func (t *localTransport) PushBlocks(_ context.Context, target NodeAddr, keys []cid.Cid, data [][]byte, blockRoots [][]store.NetworkRootEntry) error {
	node, err := t.resolve(target)
	if err != nil {
		return err
	}
	ctx := context.Background()
	rv := node.ringStore()
	stored := 0
	for i, key := range keys {

		var firstRootCID cid.Cid
		var firstRootExp int64
		hasRootCtx := false
		if rv != nil && i < len(blockRoots) && len(blockRoots[i]) > 0 {
			if rc, derr := cid.Decode(blockRoots[i][0].CID); derr == nil {
				firstRootCID = rc
				firstRootExp = blockRoots[i][0].ExpiresAt
				hasRootCtx = true
			}
		}

		if hasRootCtx {
			if err := rv.PutWithRoot(ctx, key, data[i], firstRootCID, firstRootExp); err != nil {
				if store.IsStorageFull(err) {
					continue
				}
				return err
			}
			for j := 1; j < len(blockRoots[i]); j++ {
				nr := blockRoots[i][j]
				rv.AddRootWithExpiry(nr.CID, nr.ExpiresAt)
				rv.AddBlockRootIndex(key, nr.CID)
			}
		} else {
			if err := node.blocks.Put(ctx, key, data[i]); err != nil {
				if store.IsStorageFull(err) {
					continue
				}
				return err
			}
		}
		stored++
	}
	if stored == 0 && len(keys) > 0 {
		return store.ErrStorageFull
	}
	return nil
}

func (t *localTransport) ReconcileBlocks(_ context.Context, target NodeAddr, keys []cid.Cid) ([]cid.Cid, error) {
	node, err := t.resolve(target)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	missing := make([]cid.Cid, 0, len(keys))
	for _, k := range keys {
		has, err := node.blocks.Has(ctx, k)
		if err != nil || !has {
			missing = append(missing, k)
		}
	}
	return missing, nil
}

func (t *localTransport) FetchBlock(_ context.Context, target NodeAddr, key cid.Cid) ([]byte, error) {
	node, err := t.resolve(target)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	data, err := node.blocks.Get(ctx, key)
	if err == nil {
		return data, nil
	}

	if node.localBlocks != nil {
		if d, lerr := node.localBlocks.Get(ctx, key); lerr == nil {
			return d, nil
		}
	}

	node.mu.RLock()
	succs := make([]NodeAddr, len(node.successorList))
	copy(succs, node.successorList)
	node.mu.RUnlock()
	for _, s := range succs {
		if s.ID.Equal(node.id) {
			continue
		}
		peer := t.ring.getNode(node.id, s.ID)
		if peer == nil {
			continue
		}
		if d, ferr := peer.blocks.Get(ctx, key); ferr == nil {
			return d, nil
		}
	}
	return nil, fmt.Errorf("block not found: %s", key)
}

func (t *localTransport) HasBlock(_ context.Context, target NodeAddr, key cid.Cid) (bool, error) {
	node, err := t.resolve(target)
	if err != nil {
		return false, err
	}
	ctx := context.Background()
	if has, err := node.blocks.Has(ctx, key); err == nil && has {
		return true, nil
	}
	if node.localBlocks != nil {
		return node.localBlocks.Has(ctx, key)
	}
	return node.blocks.Has(ctx, key)
}

func (t *localTransport) RemoveBlock(_ context.Context, target NodeAddr, key cid.Cid) error {
	node, err := t.resolve(target)
	if err != nil {
		return err
	}
	return node.blocks.Delete(context.Background(), key)
}

func (t *localTransport) TransferKeys(_ context.Context, target NodeAddr, from, to NodeID) ([]cid.Cid, [][]byte, [][]store.NetworkRootEntry, error) {
	node, err := t.resolve(target)
	if err != nil {
		return nil, nil, nil, err
	}
	ctx := context.Background()
	keys, data, blockRoots, err := node.getKeysInRange(ctx, from, to)
	if err != nil {
		return nil, nil, nil, err
	}

	rv := node.ringStore()
	for _, k := range keys {
		if rv != nil {
			rv.DropBlock(k)
			if node.networkBlocks != nil && !node.networkBlocks.BlockHasLiveRootAnyRing(k) {
				_ = node.networkBlocks.Delete(ctx, k)
			}
		} else {
			_ = node.blocks.Delete(ctx, k)
		}
	}
	return keys, data, blockRoots, nil
}

func (t *localTransport) DeleteCID(_ context.Context, target NodeAddr, key cid.Cid, propagate bool) error {
	node, err := t.resolve(target)
	if err != nil {
		return err
	}
	node.handleDeleteCID(key, propagate)
	return nil
}

func (t *localTransport) NotifyLeave(_ context.Context, target NodeAddr, self NodeAddr, successor NodeAddr, predecessor NodeAddr) error {
	node, err := t.resolve(target)
	if err != nil {
		return err
	}
	node.handleNotifyLeave(self, successor, predecessor)
	return nil
}

func (t *localTransport) Ping(_ context.Context, target NodeAddr) error {
	if !t.ring.canReach(t.nodeID, target.ID) {
		return fmt.Errorf("node %s unreachable", target.ID)
	}
	return nil
}

var _ RecordTransport = (*localTransport)(nil)

func (t *localTransport) PutRecord(_ context.Context, target NodeAddr, key NodeID, data []byte, callerID NodeID) error {
	node, err := t.resolve(target)
	if err != nil {
		return err
	}
	if err := node.storeRecord(key, data, callerID); err != nil {
		return err
	}
	ctx := context.Background()
	node.replicateRecordBestEffort(ctx, key, data)
	return nil
}

func (t *localTransport) GetRecord(_ context.Context, target NodeAddr, key NodeID) ([]byte, error) {
	node, err := t.resolve(target)
	if err != nil {
		return nil, err
	}
	return node.localRecordGet(key)
}

func (t *localTransport) PushRecords(_ context.Context, target NodeAddr, keys []NodeID, data [][]byte) error {
	node, err := t.resolve(target)
	if err != nil {
		return err
	}
	for i, k := range keys {
		node.localRecordPut(k, data[i])
	}
	return nil
}

func (t *localTransport) TransferRecords(_ context.Context, target NodeAddr, from, to NodeID) ([]NodeID, [][]byte, error) {
	node, err := t.resolve(target)
	if err != nil {
		return nil, nil, err
	}
	keys, data := node.getRecordsInRange(from, to)
	return keys, data, nil
}

type TestRing struct {
	mu		sync.RWMutex
	Nodes		[]*Node
	nodeMap		map[NodeID]*Node
	blocked		map[[2]NodeID]bool
	Replication	int
}

func NewTestRing() *TestRing {
	return &TestRing{
		nodeMap:	make(map[NodeID]*Node),
		blocked:	make(map[[2]NodeID]bool),
		Replication:	1,
	}
}

func (r *TestRing) AddNode(pos byte) *Node {
	id := testNodeID(pos)
	bs := newTestMemStore()
	lt := &localTransport{ring: r, nodeID: id}
	cfg := Config{Replication: r.Replication}
	node := NewNode(id, fmt.Sprintf("test-%d", pos), bs, nil, lt, cfg)

	r.mu.Lock()
	r.Nodes = append(r.Nodes, node)
	r.nodeMap[id] = node
	existing := r.Nodes[:len(r.Nodes)-1]
	r.mu.Unlock()

	if len(existing) == 0 {
		node.Create()
	} else {
		bootstrap := NodeAddr{ID: existing[0].id, Addr: existing[0].addr}
		_ = node.Join(context.Background(), bootstrap)
	}

	return node
}

func (r *TestRing) FindNode(pos byte) *Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.nodeMap[testNodeID(pos)]
}

func (r *TestRing) RemoveNode(pos byte) {
	id := testNodeID(pos)
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.nodeMap, id)
	for i, node := range r.Nodes {
		if node.id.Equal(id) {
			r.Nodes = append(r.Nodes[:i], r.Nodes[i+1:]...)
			break
		}
	}
}

func (r *TestRing) Partition(a, b byte) {
	idA, idB := testNodeID(a), testNodeID(b)
	r.mu.Lock()
	r.blocked[[2]NodeID{idA, idB}] = true
	r.blocked[[2]NodeID{idB, idA}] = true
	r.mu.Unlock()
}

func (r *TestRing) Heal(a, b byte) {
	idA, idB := testNodeID(a), testNodeID(b)
	r.mu.Lock()
	delete(r.blocked, [2]NodeID{idA, idB})
	delete(r.blocked, [2]NodeID{idB, idA})
	r.mu.Unlock()
}

func (r *TestRing) canReach(from, to NodeID) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.nodeMap[to] != nil && !r.blocked[[2]NodeID{from, to}]
}

func (r *TestRing) getNode(from, to NodeID) *Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.blocked[[2]NodeID{from, to}] {
		return nil
	}
	return r.nodeMap[to]
}

func (r *TestRing) StabilizeAll() {
	r.mu.RLock()
	nodes := make([]*Node, len(r.Nodes))
	copy(nodes, r.Nodes)
	r.mu.RUnlock()

	for _, n := range nodes {
		n.stabilize()
	}
	for _, n := range nodes {
		n.fixFinger()
	}
	for _, n := range nodes {
		n.checkPredecessor()
	}
}

func (r *TestRing) StabilizeRounds(rounds int) {
	for i := 0; i < rounds; i++ {
		r.StabilizeAll()
	}
	r.WaitForKeyTransferDrain()
}

func (r *TestRing) WaitForKeyTransferDrain() {
	r.mu.RLock()
	nodes := make([]*Node, len(r.Nodes))
	copy(nodes, r.Nodes)
	r.mu.RUnlock()
	for _, n := range nodes {
		n.xferWG.Wait()
	}
}

func (r *TestRing) WaitForReplicationDrain(timeout time.Duration) bool {
	r.mu.RLock()
	nodes := make([]*Node, len(r.Nodes))
	copy(nodes, r.Nodes)
	r.mu.RUnlock()
	for _, n := range nodes {
		if !n.WaitForReplicationDrain(timeout) {
			return false
		}
	}
	return true
}

func (r *TestRing) DumpAll() string {
	r.mu.RLock()
	nodes := make([]*Node, len(r.Nodes))
	copy(nodes, r.Nodes)
	r.mu.RUnlock()

	var out string
	for _, n := range nodes {
		n.mu.RLock()
		predStr := "nil"
		if n.predecessor != nil {
			predStr = fmt.Sprintf("%d", testNodeIDVal(n.predecessor.ID))
		}
		out += fmt.Sprintf("Node %3d: succ=%3d pred=%s blocks=%d\n",
			testNodeIDVal(n.id),
			testNodeIDVal(n.successor.ID),
			predStr,
			n.DataCount(),
		)
		n.mu.RUnlock()
	}
	return out
}

func testNodeID(pos byte) NodeID {
	var id NodeID
	id[0] = pos
	return id
}

func testNodeIDVal(id NodeID) byte {
	return id[0]
}

type testMemStore struct {
	mu	sync.Mutex
	blocks	map[cid.Cid][]byte
}

func newTestMemStore() *testMemStore {
	return &testMemStore{blocks: make(map[cid.Cid][]byte)}
}

func (m *testMemStore) Put(_ context.Context, key cid.Cid, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.blocks[key] = cp
	return nil
}

func (m *testMemStore) Get(_ context.Context, key cid.Cid) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.blocks[key]
	if !ok {
		return nil, fmt.Errorf("block not found: %s", key)
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, nil
}

func (m *testMemStore) Delete(_ context.Context, key cid.Cid) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.blocks, key)
	return nil
}

func (m *testMemStore) Has(_ context.Context, key cid.Cid) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.blocks[key]
	return ok, nil
}

func (m *testMemStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	m.mu.Lock()
	keys := make([]cid.Cid, 0, len(m.blocks))
	for k := range m.blocks {
		keys = append(keys, k)
	}
	m.mu.Unlock()

	ch := make(chan cid.Cid, len(keys))
	go func() {
		defer close(ch)
		for _, k := range keys {
			select {
			case ch <- k:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}
