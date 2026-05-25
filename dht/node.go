package dht

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/proto"

	dhtpb "github.com/mjagos0/datarings/dht/dhtpb"
	"github.com/mjagos0/datarings/eventlog"
	"github.com/mjagos0/datarings/metrics"
	"github.com/mjagos0/datarings/store"
)

type recType int

const (
	recUnknown	recType	= iota
	recPeer
	recGroup
	recProvider
)

type recordMeta struct {
	storedAt	time.Time
	ttl		time.Duration
	typ		recType
}

func (m recordMeta) isExpiredAt(now time.Time) bool {
	return m.ttl > 0 && now.Sub(m.storedAt) > m.ttl
}

var ErrVersionConflict = errors.New("record version conflict: stored version is newer, refetch and retry")

const versionConflictPrefix = "record version conflict"

func isVersionConflict(err error) bool {
	return err != nil && (errors.Is(err, ErrVersionConflict) ||
		strings.Contains(err.Error(), versionConflictPrefix))
}

const fingerCount = 160

type Node struct {
	id	NodeID
	addr	string

	mu		sync.RWMutex
	successor	NodeAddr
	successorList	[]NodeAddr
	predecessor	*NodeAddr

	predecessorList	[]NodeAddr
	prevPredID	NodeID
	fingers		[fingerCount]NodeAddr

	nextFinger	int

	blocks			store.BlockStore
	localBlocks		store.BlockStore
	networkBlocks		*store.NetworkBlockStore
	dag			ipld.DAGService
	transport		Transport
	replication		int
	successorListSize	int

	ringID	string

	met	*metrics.Registry
	metRing	string

	addrRefresher	func(id NodeID) string

	recordsMu	sync.RWMutex
	records		map[NodeID][]byte
	recordsMeta	map[NodeID]recordMeta

	providerEntryTimes	map[NodeID]map[NodeID]time.Time

	peerRecordTTL		time.Duration
	groupRecordTTL		time.Duration
	providerRecordTTL	time.Duration
	recordPurgePeriod	time.Duration

	nowFunc	func() time.Time

	backgroundPaused	atomic.Bool

	storageStatusFn	func() (usedBytes, maxBytes int64)

	replicationSem		chan struct{}
	replicationsPending	sync.WaitGroup
	replicationsInFlight	atomic.Int64
	replicationsDropped	atomic.Int64

	replicateToSuccessorsSlot	chan struct{}
	replicateToSuccessorsDropped	atomic.Int64

	blocksInFlightSem	*semaphore.Weighted
	blocksInFlightCap	int64
	blocksInFlightActive	atomic.Int64
	blocksInFlightMax	atomic.Int64

	stopCh	chan struct{}
	wg	sync.WaitGroup

	xferWG	sync.WaitGroup
}

const defaultReplicationConcurrency = 64

const defaultPushBlocksChunkSize = 16

const defaultBlocksInFlight = 64

var _ DHT = (*Node)(nil)

func NewNode(id NodeID, addr string, blocks store.BlockStore, dag ipld.DAGService, t Transport, cfg Config) *Node {
	self := NodeAddr{ID: id, Addr: addr}
	n := &Node{
		id:				id,
		addr:				addr,
		blocks:				blocks,
		dag:				dag,
		transport:			t,
		replication:			cfg.replication(),
		successorListSize:		cfg.successorListSize(),
		ringID:				store.RingPublic,
		records:			make(map[NodeID][]byte),
		recordsMeta:			make(map[NodeID]recordMeta),
		providerEntryTimes:		make(map[NodeID]map[NodeID]time.Time),
		peerRecordTTL:			cfg.peerRecordTTL(),
		groupRecordTTL:			cfg.groupRecordTTL(),
		providerRecordTTL:		cfg.providerRecordTTL(),
		recordPurgePeriod:		cfg.recordPurgePeriod(),
		nowFunc:			time.Now,
		stopCh:				make(chan struct{}),
		replicationSem:			make(chan struct{}, defaultReplicationConcurrency),
		blocksInFlightSem:		semaphore.NewWeighted(defaultBlocksInFlight),
		blocksInFlightCap:		defaultBlocksInFlight,
		replicateToSuccessorsSlot:	make(chan struct{}, 1),
	}

	n.successor = self
	n.successorList = []NodeAddr{self}
	for i := range n.fingers {
		n.fingers[i] = self
	}
	return n
}

func (n *Node) Create() {
	self := NodeAddr{ID: n.id, Addr: n.addr}
	n.mu.Lock()
	n.successor = self
	n.successorList = []NodeAddr{self}
	n.predecessor = nil
	for i := range n.fingers {
		n.fingers[i] = self
	}
	n.mu.Unlock()
	slog.Info("chord: initialized single-node ring", "node", n.id)
}

func (n *Node) LocalNode() NodeAddr {
	return NodeAddr{ID: n.id, Addr: n.addr}
}

func (n *Node) now() time.Time {
	if n.nowFunc != nil {
		return n.nowFunc()
	}
	return time.Now()
}

func (n *Node) SetNowFunc(fn func() time.Time) {
	n.recordsMu.Lock()
	defer n.recordsMu.Unlock()
	if fn == nil {
		n.nowFunc = time.Now
	} else {
		n.nowFunc = fn
	}
}

func (n *Node) SetAddrRefresher(fn func(id NodeID) string) {
	n.mu.Lock()
	n.addrRefresher = fn
	n.mu.Unlock()
}

func (n *Node) SetMetrics(m *metrics.Registry, ring string) {
	n.met = m
	n.metRing = ring
}

func (n *Node) SetStorageStatus(fn func() (usedBytes, maxBytes int64)) {
	n.storageStatusFn = fn
}

func (n *Node) SetNetworkBlocks(nbs *store.NetworkBlockStore) {
	n.networkBlocks = nbs
	n.blocks = nbs
}

func (n *Node) SetRingID(ringID string) {
	if ringID == "" {
		ringID = store.RingPublic
	}
	n.mu.Lock()
	n.ringID = ringID
	n.mu.Unlock()
}

func (n *Node) RingID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.ringID
}

func (n *Node) ringStore() *store.RingView {
	if n.networkBlocks == nil {
		return nil
	}
	return n.networkBlocks.Ring(n.ringID)
}

func (n *Node) NetworkBlocks() *store.NetworkBlockStore {
	return n.networkBlocks
}

func (n *Node) SetLocalBlocks(bs store.BlockStore) {
	n.localBlocks = bs
}

func (n *Node) localOrRingBlocks() store.BlockStore {
	if n.localBlocks != nil {
		return n.localBlocks
	}
	return n.blocks
}

func (n *Node) JoinPeer(ctx context.Context, peerMultiaddr string) error {
	bootstrap := NodeAddr{
		ID:	AddrToNodeID(peerMultiaddr),
		Addr:	peerMultiaddr,
	}
	return n.Join(ctx, bootstrap)
}

func (n *Node) Join(ctx context.Context, bootstrap NodeAddr) error {
	succ, err := n.transport.FindSuccessor(ctx, bootstrap, n.id)
	if err != nil {
		return fmt.Errorf("find successor: %w", err)
	}

	n.mu.Lock()
	n.successor = succ
	n.successorList = []NodeAddr{succ}
	n.mu.Unlock()
	slog.Info("chord: joined ring via bootstrap", "node", n.id, "bootstrap", bootstrap.ID, "successor", succ.ID)

	pred, err := n.transport.GetPredecessor(ctx, succ)
	if err == nil && pred != nil && !pred.ID.Equal(n.id) {

		keys, data, blockRoots, err := n.transport.TransferKeys(ctx, succ, pred.ID, n.id)
		if err == nil {
			rv := n.ringStore()
			for i, key := range keys {
				if rv != nil {
					_ = n.networkBlocks.Put(ctx, key, data[i])

					if i < len(blockRoots) {
						for _, nr := range blockRoots[i] {
							rv.AddRootWithExpiry(nr.CID, nr.ExpiresAt)
							rv.AddBlockRootIndex(key, nr.CID)
						}
					}
				} else {
					_ = n.blocks.Put(ctx, key, data[i])
				}
			}
			if len(keys) > 0 {
				slog.Debug("chord: received blocks from successor on join", "node", n.id, "count", len(keys))
			}
		}

		if rt, ok := n.transport.(RecordTransport); ok {
			rKeys, rData, rErr := rt.TransferRecords(ctx, succ, pred.ID, n.id)
			if rErr == nil {
				for i, k := range rKeys {
					n.localRecordPut(k, rData[i])
				}
				if len(rKeys) > 0 {
					slog.Debug("chord: received records from successor on join", "node", n.id, "count", len(rKeys))
				}
			}
		}
	}

	return nil
}

func (n *Node) Leave(ctx context.Context) error {

	n.PauseBackground()

	succ := n.getSuccessor()
	pred := n.getPredecessor()
	self := NodeAddr{ID: n.id, Addr: n.addr}

	predID := ""
	if pred != nil {
		predID = pred.ID.String()
	}
	eventlog.Emit("node_leaving", map[string]any{
		"ring":		n.metRing,
		"successor":	succ.ID.String(),
		"predecessor":	predID,
	})

	if !succ.ID.Equal(n.id) {

		var keys []cid.Cid
		var data [][]byte
		var blockRoots [][]store.NetworkRootEntry
		if rv := n.ringStore(); rv != nil {
			ringKeys, kerr := rv.Blocks(ctx)
			if kerr == nil {
				for _, key := range ringKeys {
					block, err := n.blocks.Get(ctx, key)
					if err != nil {
						continue
					}
					keys = append(keys, key)
					data = append(data, block)
					blockRoots = append(blockRoots, rv.RootsForBlockEntries(key))
				}
			}
		} else {
			ch, err := n.blocks.AllKeysChan(ctx)
			if err == nil {
				for key := range ch {
					block, err := n.blocks.Get(ctx, key)
					if err == nil {
						keys = append(keys, key)
						data = append(data, block)
					}
				}
				blockRoots = make([][]store.NetworkRootEntry, len(keys))
			}
		}
		if len(keys) > 0 {
			slog.Info("chord: transferring blocks to successor on leave", "node", n.id, "successor", succ.ID, "count", len(keys))
			_ = n.transport.PushBlocks(ctx, succ, keys, data, blockRoots)
		}

		if rt, ok := n.transport.(RecordTransport); ok {
			n.recordsMu.RLock()
			var rKeys []NodeID
			var rData [][]byte
			for k, v := range n.records {
				cp := make([]byte, len(v))
				copy(cp, v)
				rKeys = append(rKeys, k)
				rData = append(rData, cp)
			}
			n.recordsMu.RUnlock()
			if len(rKeys) > 0 {
				slog.Info("chord: transferring records to successor on leave", "node", n.id, "successor", succ.ID, "count", len(rKeys))
				_ = rt.PushRecords(ctx, succ, rKeys, rData)
			}
		}

		predAddr := self
		if pred != nil {
			predAddr = *pred
		}
		_ = n.transport.NotifyLeave(ctx, succ, self, succ, predAddr)

		if pred != nil && !pred.ID.Equal(n.id) {
			_ = n.transport.NotifyLeave(ctx, *pred, self, succ, predAddr)
		}
	}

	n.Stop()
	return nil
}

func (n *Node) handleNotifyLeave(leaver, successor, predecessor NodeAddr) {
	n.mu.Lock()
	defer n.mu.Unlock()

	role := "none"
	if n.successor.ID.Equal(leaver.ID) {
		role = "successor"
	}
	if n.predecessor != nil && n.predecessor.ID.Equal(leaver.ID) {
		if role == "successor" {
			role = "both"
		} else {
			role = "predecessor"
		}
	}
	eventlog.Emit("notify_leave_received", map[string]any{
		"ring":			n.metRing,
		"from":			leaver.ID.String(),
		"role":			role,
		"new_successor":	successor.ID.String(),
		"new_predecessor":	predecessor.ID.String(),
	})

	if n.successor.ID.Equal(leaver.ID) {
		slog.Info("chord: successor left, adopting its successor", "node", n.id, "old", leaver.ID, "new", successor.ID)
		n.successor = successor

		newList := []NodeAddr{successor}
		for _, s := range n.successorList {
			if !s.ID.Equal(leaver.ID) && !s.ID.Equal(successor.ID) && len(newList) < n.successorListSize {
				newList = append(newList, s)
			}
		}
		n.successorList = newList
	}

	if n.predecessor != nil && n.predecessor.ID.Equal(leaver.ID) {
		slog.Info("chord: predecessor left, adopting its predecessor", "node", n.id, "old", leaver.ID, "new", predecessor.ID)
		if predecessor.ID.Equal(leaver.ID) {

			n.predecessor = nil
		} else {
			n.predecessor = &predecessor
		}
	}
}

func (n *Node) Stop() {
	select {
	case <-n.stopCh:
	default:
		close(n.stopCh)
	}
	if !n.WaitForReplicationDrain(5 * time.Second) {
		slog.Warn("chord: replication drain timed out on shutdown",
			"node", n.id, "ring", n.ringID, "in_flight", n.ReplicationsInFlight())
	}
	n.xferWG.Wait()
	n.wg.Wait()
}

func (n *Node) StopCh() <-chan struct{}	{ return n.stopCh }

func (n *Node) Put(ctx context.Context, key cid.Cid, data []byte) error {
	return n.PutWithRoot(ctx, key, data, cid.Undef, 0)
}

func (n *Node) PutWithRoot(ctx context.Context, key cid.Cid, data []byte, rootCID cid.Cid, rootExpiry int64) error {

	eventlog.Emit("block_put_started", map[string]any{
		"ring":	n.metRing,
		"cid":	key.String(),
	})
	id := CIDToNodeID(key)
	responsible, err := n.FindSuccessor(ctx, id)
	if err != nil {
		return fmt.Errorf("route put: %w", err)
	}

	if responsible.ID.Equal(n.id) {
		if rv := n.ringStore(); rv != nil {
			if err := rv.PutWithRoot(ctx, key, data, rootCID, rootExpiry); err != nil {
				return err
			}
		} else {
			if err := n.blocks.Put(ctx, key, data); err != nil {
				return err
			}
		}
		if n.met != nil {
			n.met.BlocksStoredByType.WithLabelValues(n.metRing, "primary").Inc()
		}
		eventlog.Emit("block_stored", map[string]any{
			"ring":	n.metRing,
			"type":	"primary",
			"cid":	key.String(),
			"size":	len(data),
		})
		slog.Debug("chord: stored block locally", "node", n.id, "cid", key)

		n.replicateAsync(context.Background(), key, data, rootCID, rootExpiry)
		return nil
	}

	slog.Debug("chord: routing put to responsible node", "node", n.id, "cid", key, "responsible", responsible.ID)
	if err := n.transport.PutBlock(ctx, responsible, key, data, rootCID, rootExpiry); err != nil {
		reason := "rpc_error"
		if store.IsStorageFull(err) {
			reason = "storage_full"
		}
		eventlog.Emit("block_offload_failed", map[string]any{
			"ring":		n.metRing,
			"cid":		key.String(),
			"size":		len(data),
			"responsible":	responsible.ID.String(),
			"reason":	reason,
		})
		return err
	}
	eventlog.Emit("block_offloaded", map[string]any{
		"ring":		n.metRing,
		"cid":		key.String(),
		"size":		len(data),
		"responsible":	responsible.ID.String(),
	})
	return nil
}

func (n *Node) Get(ctx context.Context, key cid.Cid) ([]byte, error) {
	id := CIDToNodeID(key)
	responsible, err := n.FindSuccessor(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("route get: %w", err)
	}

	if responsible.ID.Equal(n.id) {
		data, err := n.blocks.Get(ctx, key)
		if err == nil {
			return data, nil
		}

		if n.localBlocks != nil {
			data, err = n.localBlocks.Get(ctx, key)
			if err == nil {
				return data, nil
			}
		}

		slog.Debug("chord: local miss for owned block, fanning out to successors", "node", n.id, "cid", key)
		n.mu.RLock()
		succs := make([]NodeAddr, len(n.successorList))
		copy(succs, n.successorList)
		n.mu.RUnlock()

		var targets []NodeAddr
		for _, s := range succs {
			if !s.ID.Equal(n.id) {
				targets = append(targets, s)
			}
		}
		if len(targets) == 0 {
			return nil, fmt.Errorf("block not found: %s", key)
		}
		hits := make(chan []byte, len(targets))
		misses := make(chan struct{}, len(targets))
		for _, target := range targets {
			go func(target NodeAddr) {

				if err := n.acquireBlockSlots(ctx, 1); err != nil {
					misses <- struct{}{}
					return
				}
				defer n.releaseBlockSlots(1)
				if d, ferr := n.transport.FetchBlock(ctx, target, key); ferr == nil && d != nil {
					hits <- d
				} else {
					misses <- struct{}{}
				}
			}(target)
		}
		missCount := 0
		for {
			select {
			case d := <-hits:
				return d, nil
			case <-misses:
				missCount++
				if missCount == len(targets) {
					return nil, fmt.Errorf("block not found: %s", key)
				}
			}
		}
	}

	slog.Info("chord: fetching block from peer", "cid", key, "peer", responsible.ID, "addr", responsible.Addr)
	return n.transport.FetchBlock(ctx, responsible, key)
}

func (n *Node) Has(ctx context.Context, key cid.Cid) (bool, error) {
	id := CIDToNodeID(key)
	responsible, err := n.FindSuccessor(ctx, id)
	if err != nil {
		return false, fmt.Errorf("route has: %w", err)
	}

	if responsible.ID.Equal(n.id) {
		return n.blocks.Has(ctx, key)
	}

	return n.transport.HasBlock(ctx, responsible, key)
}

func (n *Node) Remove(ctx context.Context, key cid.Cid) error {
	id := CIDToNodeID(key)
	responsible, err := n.FindSuccessor(ctx, id)
	if err != nil {
		return fmt.Errorf("route remove: %w", err)
	}

	if responsible.ID.Equal(n.id) {
		return n.blocks.Delete(ctx, key)
	}

	return n.transport.RemoveBlock(ctx, responsible, key)
}

const maxLookupHops = 160

func (n *Node) FindSuccessor(ctx context.Context, id NodeID) (NodeAddr, error) {
	self := NodeAddr{ID: n.id, Addr: n.addr}
	succ := n.getSuccessor()

	if id.BetweenRightInclusive(n.id, succ.ID) {
		return succ, nil
	}

	candidate := n.closestPrecedingNode(id)
	if candidate.ID.Equal(n.id) {
		return self, nil
	}

	for hop := 0; hop < maxLookupHops; hop++ {
		if ctx.Err() != nil {
			return NodeAddr{}, ctx.Err()
		}

		cpn, cSucc, err := n.transport.ClosestPrecedingNode(ctx, candidate, id)
		if err != nil {

			slog.Debug("chord: iterative lookup RPC failed, evicting dead node",
				"node", n.id, "dead", candidate.ID, "err", err)
			n.evictNode(candidate.ID)

			candidate = n.closestPrecedingNode(id)
			if candidate.ID.Equal(n.id) {
				return self, nil
			}
			continue
		}

		if id.BetweenRightInclusive(candidate.ID, cSucc.ID) {
			return cSucc, nil
		}

		if cpn.ID.Equal(candidate.ID) {
			return cSucc, nil
		}

		candidate = cpn
	}

	slog.Warn("chord: iterative lookup reached max hops", "node", n.id, "id", id)
	return candidate, nil
}

func (n *Node) closestPrecedingNode(id NodeID) NodeAddr {
	n.mu.RLock()
	defer n.mu.RUnlock()

	best := NodeAddr{ID: n.id, Addr: n.addr}

	for i := fingerCount - 1; i >= 0; i-- {
		f := n.fingers[i]
		if f.ID.BetweenExclusive(n.id, id) {
			best = f
			break
		}
	}

	for _, s := range n.successorList {
		if s.ID.BetweenExclusive(n.id, id) {

			if best.ID.Equal(n.id) || s.ID.BetweenExclusive(best.ID, id) {
				best = s
			}
		}
	}

	return best
}

func (n *Node) evictNode(dead NodeID) {
	n.mu.Lock()

	self := NodeAddr{ID: n.id, Addr: n.addr}

	for i, f := range n.fingers {
		if f.ID.Equal(dead) {
			n.fingers[i] = self
		}
	}

	oldList := make([]NodeAddr, len(n.successorList))
	copy(oldList, n.successorList)

	newList := make([]NodeAddr, 0, len(n.successorList))
	for _, s := range n.successorList {
		if !s.ID.Equal(dead) {
			newList = append(newList, s)
		}
	}
	n.successorList = newList
	n.mu.Unlock()

	if len(oldList) != len(newList) {
		n.triggerReplicateToNewSuccessors(context.Background(), oldList, newList)
	}
}

func (n *Node) notify(caller NodeAddr) {
	n.mu.Lock()

	if n.predecessor == nil || caller.ID.BetweenExclusive(n.predecessor.ID, n.id) {
		n.acceptPredecessor(caller)
		return
	}

	stalePred := *n.predecessor
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err := n.transport.Ping(ctx, stalePred)
	cancel()
	if err == nil {
		return
	}

	slog.Info("chord: predecessor unreachable during notify, clearing",
		"node", n.id, "predecessor", stalePred.ID, "error", err)
	eventlog.Emit("predecessor_failure_detected", map[string]any{
		"ring":		n.metRing,
		"failed":	stalePred.ID.String(),
	})

	n.mu.Lock()

	if n.predecessor != nil && n.predecessor.ID.Equal(stalePred.ID) {
		n.predecessor = nil
	}

	if n.predecessor == nil || caller.ID.BetweenExclusive(n.predecessor.ID, n.id) {
		n.acceptPredecessor(caller)
		return
	}
	n.mu.Unlock()
}

func (n *Node) acceptPredecessor(caller NodeAddr) {
	oldPred := n.predecessor
	n.predecessor = &caller
	n.mu.Unlock()

	if oldPred == nil || !oldPred.ID.Equal(caller.ID) {
		oldID := ""
		if oldPred != nil {
			oldID = oldPred.ID.String()
		}
		eventlog.Emit("predecessor_accepted", map[string]any{
			"ring":			n.metRing,
			"from":			caller.ID.String(),
			"old_predecessor":	oldID,
		})
	}

	if oldPred == nil {
		slog.Info("chord: predecessor set", "node", n.id, "predecessor", caller.ID)
	} else {
		slog.Info("chord: predecessor updated", "node", n.id, "old", oldPred.ID, "new", caller.ID)
	}

	if caller.ID.Equal(n.id) {
		return
	}
	var from NodeID
	if oldPred != nil {
		if caller.ID.Equal(oldPred.ID) {
			return
		}
		from = oldPred.ID
	} else {
		from = n.id
	}
	n.xferWG.Add(2)
	go func() {
		defer n.xferWG.Done()
		n.transferKeysRange(caller, from, caller.ID)
	}()
	go func() {
		defer n.xferWG.Done()
		n.transferRecordsRange(caller, from, caller.ID)
	}()
}

func (n *Node) transferKeysRange(target NodeAddr, from, to NodeID) {
	ctx := context.Background()

	keys, err := n.enumerateRingKeysInRange(ctx, from, to, true)
	if err != nil || len(keys) == 0 {
		return
	}

	slog.Debug("chord: transferring key range to new predecessor", "node", n.id, "target", target.ID, "count", len(keys))
	n.pushBlocksChunked(ctx, []NodeAddr{target}, keys, "transferKeysRange")

}

func (n *Node) getKeysInRange(ctx context.Context, from, to NodeID) ([]cid.Cid, [][]byte, [][]store.NetworkRootEntry, error) {
	return n.collectRingBlocksInRange(ctx, from, to)
}

func (n *Node) enumerateRingKeysInRange(ctx context.Context, from, to NodeID, filterByRange bool) ([]cid.Cid, error) {
	rv := n.ringStore()
	var keys []cid.Cid

	if rv != nil {
		ringKeys, err := rv.Blocks(ctx)
		if err != nil {
			return nil, err
		}
		for _, key := range ringKeys {
			if filterByRange {
				ringPos := CIDToNodeID(key)
				if !ringPos.BetweenRightInclusive(from, to) {
					continue
				}
			}
			keys = append(keys, key)
		}
		return keys, nil
	}

	ch, err := n.blocks.AllKeysChan(ctx)
	if err != nil {
		return nil, err
	}
	for key := range ch {
		if filterByRange {
			ringPos := CIDToNodeID(key)
			if !ringPos.BetweenRightInclusive(from, to) {
				continue
			}
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func (n *Node) acquireBlockSlots(ctx context.Context, weight int64) error {
	if weight <= 0 {
		return nil
	}

	if weight > n.blocksInFlightCap {
		return fmt.Errorf("blocksInFlight: requested weight %d exceeds pool capacity %d", weight, n.blocksInFlightCap)
	}
	if err := n.blocksInFlightSem.Acquire(ctx, weight); err != nil {
		return err
	}
	cur := n.blocksInFlightActive.Add(weight)
	for {
		prev := n.blocksInFlightMax.Load()
		if cur <= prev || n.blocksInFlightMax.CompareAndSwap(prev, cur) {
			break
		}
	}
	return nil
}

func (n *Node) releaseBlockSlots(weight int64) {
	if weight <= 0 {
		return
	}
	n.blocksInFlightActive.Add(-weight)
	n.blocksInFlightSem.Release(weight)
}

func (n *Node) pushBlocksChunked(ctx context.Context, targets []NodeAddr, keys []cid.Cid, reason string) {
	if len(keys) == 0 || len(targets) == 0 {
		return
	}
	for _, target := range targets {
		missing, err := n.transport.ReconcileBlocks(ctx, target, keys)
		if err != nil {
			slog.Debug("chord: reconcile failed; falling back to full push",
				"node", n.id, "target", target.ID, "reason", reason, "error", err)
			missing = keys
		}
		if len(missing) == 0 {
			slog.Debug("chord: reconcile: target already holds all blocks; skipping push",
				"node", n.id, "target", target.ID, "reason", reason, "count", len(keys))
			continue
		}
		n.pushBlocksChunkedToTarget(ctx, target, missing, reason)
	}
}

func (n *Node) pushBlocksChunkedToTarget(ctx context.Context, target NodeAddr, keys []cid.Cid, reason string) {
	if len(keys) == 0 {
		return
	}
	rv := n.ringStore()
	cs := defaultPushBlocksChunkSize
	for start := 0; start < len(keys); start += cs {
		end := start + cs
		if end > len(keys) {
			end = len(keys)
		}
		chunkKeys := keys[start:end]
		weight := int64(len(chunkKeys))
		if err := n.acquireBlockSlots(ctx, weight); err != nil {
			slog.Debug("chord: chunked push aborted acquiring slots",
				"node", n.id, "reason", reason, "error", err)
			return
		}

		validKeys := make([]cid.Cid, 0, len(chunkKeys))
		chunkData := make([][]byte, 0, len(chunkKeys))
		chunkRoots := make([][]store.NetworkRootEntry, 0, len(chunkKeys))
		for _, key := range chunkKeys {
			block, err := n.blocks.Get(ctx, key)
			if err != nil {

				continue
			}
			validKeys = append(validKeys, key)
			chunkData = append(chunkData, block)
			if rv != nil {
				chunkRoots = append(chunkRoots, rv.RootsForBlockEntries(key))
			} else {
				chunkRoots = append(chunkRoots, nil)
			}
		}
		if len(validKeys) > 0 {
			if err := n.transport.PushBlocks(ctx, target, validKeys, chunkData, chunkRoots); err != nil {
				slog.Debug("chord: chunked push failed",
					"node", n.id,
					"target", target.ID,
					"reason", reason,
					"chunk_start", start,
					"chunk_size", len(validKeys),
					"error", err)
			}
		}
		n.releaseBlockSlots(weight)
	}
}

func (n *Node) collectRingBlocksInRange(ctx context.Context, from, to NodeID) ([]cid.Cid, [][]byte, [][]store.NetworkRootEntry, error) {
	rv := n.ringStore()
	var keys []cid.Cid
	var data [][]byte
	var blockRoots [][]store.NetworkRootEntry

	if rv != nil {
		ringKeys, err := rv.Blocks(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		for _, key := range ringKeys {
			ringPos := CIDToNodeID(key)
			if !ringPos.BetweenRightInclusive(from, to) {
				continue
			}
			block, err := n.blocks.Get(ctx, key)
			if err != nil {
				continue
			}
			keys = append(keys, key)
			data = append(data, block)
			blockRoots = append(blockRoots, rv.RootsForBlockEntries(key))
		}
		return keys, data, blockRoots, nil
	}

	ch, err := n.blocks.AllKeysChan(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	for key := range ch {
		ringPos := CIDToNodeID(key)
		if !ringPos.BetweenRightInclusive(from, to) {
			continue
		}
		block, err := n.blocks.Get(ctx, key)
		if err != nil {
			continue
		}
		keys = append(keys, key)
		data = append(data, block)
	}
	blockRoots = make([][]store.NetworkRootEntry, len(keys))
	return keys, data, blockRoots, nil
}

func (n *Node) triggerReplicateToNewSuccessors(ctx context.Context, oldList, newList []NodeAddr) {
	select {
	case n.replicateToSuccessorsSlot <- struct{}{}:
	default:
		n.replicateToSuccessorsDropped.Add(1)
		slog.Debug("chord: replicateToNewSuccessors already in flight, dropping round",
			"node", n.id)
		return
	}
	go func() {
		defer func() { <-n.replicateToSuccessorsSlot }()
		n.replicateToNewSuccessors(ctx, oldList, newList)
	}()
}

func (n *Node) replicateToNewSuccessors(ctx context.Context, oldList, newList []NodeAddr) {
	if n.replication <= 1 {
		return
	}

	pred := n.getPredecessor()
	filterByRange := pred != nil
	var predID NodeID
	if filterByRange {
		predID = pred.ID
	}

	n.mu.Lock()
	predChanged := false
	prevPredID := n.prevPredID
	if pred != nil {
		if !pred.ID.Equal(n.prevPredID) {
			predChanged = true
			n.prevPredID = pred.ID
		}
	}
	n.mu.Unlock()

	if predChanged && pred != nil {
		eventlog.Emit("primary_promoted", map[string]any{
			"ring":			n.metRing,
			"old_predecessor":	prevPredID.String(),
			"new_predecessor":	pred.ID.String(),
		})
	}

	oldWindow := make(map[NodeID]bool, n.replication-1)
	for i := 0; i < n.replication-1 && i < len(oldList); i++ {
		oldWindow[oldList[i].ID] = true
	}

	var targets []NodeAddr
	for i := 0; i < n.replication-1 && i < len(newList); i++ {
		s := newList[i]
		if s.ID.Equal(n.id) {
			continue
		}
		if predChanged || !oldWindow[s.ID] {
			targets = append(targets, s)
		}
	}
	if len(targets) == 0 {
		return
	}

	keys, err := n.enumerateRingKeysInRange(ctx, predID, n.id, filterByRange)
	if err != nil || len(keys) == 0 {
		return
	}

	slog.Debug("chord: re-replicating blocks to new successors",
		"node", n.id,
		"targets", len(targets),
		"count", len(keys))

	n.pushBlocksChunked(ctx, targets, keys, "replicateToNewSuccessors")
}

func (n *Node) replicateAndReport(ctx context.Context, key cid.Cid, data []byte, rootCID cid.Cid, rootExpiry int64) (int, int) {
	if n.replication <= 1 {
		return 0, 0
	}

	n.mu.RLock()
	succs := make([]NodeAddr, len(n.successorList))
	copy(succs, n.successorList)
	n.mu.RUnlock()

	var blockRoots [][]store.NetworkRootEntry
	if rootCID.Defined() {
		blockRoots = [][]store.NetworkRootEntry{{{CID: rootCID.String(), ExpiresAt: rootExpiry}}}
	} else {
		blockRoots = [][]store.NetworkRootEntry{nil}
	}

	var targets []NodeAddr
	for i := 0; i < n.replication-1 && i < len(succs); i++ {
		if !succs[i].ID.Equal(n.id) {
			targets = append(targets, succs[i])
		}
	}
	if len(targets) == 0 {
		return 0, 0
	}

	results := make([]error, len(targets))
	var wg sync.WaitGroup
	for i, target := range targets {
		wg.Add(1)
		go func(i int, target NodeAddr) {
			defer wg.Done()
			results[i] = n.transport.PushBlocks(ctx, target, []cid.Cid{key}, [][]byte{data}, blockRoots)
		}(i, target)
	}
	wg.Wait()

	ok := 0
	for _, err := range results {
		if err == nil {
			ok++
		}
	}
	return ok, len(targets)
}

func (n *Node) replicateAsync(ctx context.Context, key cid.Cid, data []byte, rootCID cid.Cid, rootExpiry int64) {
	if n.replication <= 1 {
		return
	}

	n.mu.RLock()
	succs := make([]NodeAddr, len(n.successorList))
	copy(succs, n.successorList)
	n.mu.RUnlock()

	var blockRoots [][]store.NetworkRootEntry
	if rootCID.Defined() {
		blockRoots = [][]store.NetworkRootEntry{{{CID: rootCID.String(), ExpiresAt: rootExpiry}}}
	} else {
		blockRoots = [][]store.NetworkRootEntry{nil}
	}

	for i := 0; i < n.replication-1 && i < len(succs); i++ {
		target := succs[i]
		if target.ID.Equal(n.id) {
			continue
		}

		select {
		case n.replicationSem <- struct{}{}:
		default:
			n.replicationsDropped.Add(1)
			slog.Warn("chord: replication queue full, deferring to stabilization",
				"node", n.id, "ring", n.ringID, "target", target.ID, "cid", key)
			continue
		}
		n.replicationsPending.Add(1)
		n.replicationsInFlight.Add(1)
		go func(target NodeAddr) {
			defer func() {
				<-n.replicationSem
				n.replicationsInFlight.Add(-1)
				n.replicationsPending.Done()
			}()
			if err := n.transport.PushBlocks(ctx, target, []cid.Cid{key}, [][]byte{data}, blockRoots); err != nil {
				slog.Warn("chord: async replication push failed",
					"node", n.id, "ring", n.ringID, "target", target.ID, "cid", key, "error", err)
			}
		}(target)
	}
}

func (n *Node) WaitForReplicationDrain(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		n.replicationsPending.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (n *Node) ReplicationsInFlight() int	{ return int(n.replicationsInFlight.Load()) }

func (n *Node) ReplicationsDroppedTotal() int64	{ return n.replicationsDropped.Load() }

func (n *Node) DeleteCID(ctx context.Context, rootCID cid.Cid) error {

	if rv := n.ringStore(); rv != nil {
		rv.RemoveRoot(rootCID)
	}

	visited := make(map[cid.Cid]struct{})
	return n.deleteCIDWalk(ctx, rootCID, rootCID, visited)
}

func (n *Node) deleteCIDWalk(ctx context.Context, rootCID, c cid.Cid, visited map[cid.Cid]struct{}) error {
	if _, seen := visited[c]; seen {
		return nil
	}
	visited[c] = struct{}{}

	id := CIDToNodeID(c)
	responsible, err := n.FindSuccessor(ctx, id)
	if err != nil {
		slog.Debug("chord: DeleteCID route failed", "node", n.id, "cid", c, "error", err)
		return nil
	}

	if responsible.ID.Equal(n.id) {

		n.handleDeleteCID(rootCID, true)
	} else {
		if err := n.transport.DeleteCID(ctx, responsible, rootCID, true); err != nil {
			slog.Warn("chord: DeleteCID propagation failed", "node", n.id, "target", responsible.ID, "cid", rootCID, "error", err)
		}
	}

	data, err := n.localOrRingBlocks().Get(ctx, c)
	if err != nil {
		data, err = n.blocks.Get(ctx, c)
	}
	if err != nil {
		return nil
	}
	children, err := store.LinksOf(c, data)
	if err != nil {
		return nil
	}
	for _, child := range children {
		if err := n.deleteCIDWalk(ctx, rootCID, child, visited); err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) refreshPredecessorList(ctx context.Context) {
	pred := n.getPredecessor()
	if pred == nil {
		n.mu.Lock()
		n.predecessorList = nil
		n.mu.Unlock()
		return
	}

	list := []NodeAddr{*pred}
	cur := *pred

	for i := 1; i < n.successorListSize; i++ {
		if cur.ID.Equal(n.id) {
			break
		}
		nextPred, err := n.transport.GetPredecessor(ctx, cur)
		if err != nil || nextPred == nil {
			break
		}
		if nextPred.ID.Equal(n.id) {
			break
		}
		dup := false
		for _, e := range list {
			if e.ID.Equal(nextPred.ID) {
				dup = true
				break
			}
		}
		if dup {
			break
		}
		list = append(list, *nextPred)
		cur = *nextPred
	}

	n.mu.Lock()
	n.predecessorList = list
	n.mu.Unlock()
}

func (n *Node) PruneOutOfWindowBlocks(ctx context.Context) (int, error) {
	rv := n.ringStore()
	if rv == nil || n.replication <= 1 {
		return 0, nil
	}

	n.mu.RLock()
	pred := n.predecessor
	predList := append([]NodeAddr{}, n.predecessorList...)
	n.mu.RUnlock()

	if pred == nil || len(predList) < n.replication {

		return 0, nil
	}

	rangeLow := predList[n.replication-1].ID
	rangeHigh := n.id

	keys, err := rv.Blocks(ctx)
	if err != nil {
		return 0, err
	}

	dropped := 0
	for _, key := range keys {
		cidPos := CIDToNodeID(key)
		if cidPos.BetweenRightInclusive(rangeLow, rangeHigh) {
			continue
		}

		rv.DropBlock(key)
		if n.networkBlocks != nil && !n.networkBlocks.BlockHasLiveRootAnyRing(key) {
			_ = n.networkBlocks.Delete(ctx, key)
		}
		dropped++
	}
	if dropped > 0 {
		slog.Info("chord: pruned out-of-window blocks", "node", n.id, "ring", n.ringID,
			"dropped", dropped, "window_low", rangeLow, "window_high", rangeHigh)
	}
	return dropped, nil
}

func (n *Node) handleDeleteCID(rootCID cid.Cid, propagate bool) {
	if rv := n.ringStore(); rv != nil {
		rv.RemoveRoot(rootCID)
		slog.Info("chord: removed network root", "node", n.id, "ring", n.ringID, "cid", rootCID)
	}

	if !propagate {
		return
	}

	n.mu.RLock()
	succs := make([]NodeAddr, len(n.successorList))
	copy(succs, n.successorList)
	n.mu.RUnlock()

	ctx := context.Background()
	for i := 0; i < n.replication-1 && i < len(succs); i++ {
		succ := succs[i]
		if !succ.ID.Equal(n.id) {
			_ = n.transport.DeleteCID(ctx, succ, rootCID, false)
		}
	}
}

func (n *Node) getSuccessor() NodeAddr {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.successor
}

func (n *Node) getPredecessor() *NodeAddr {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.predecessor
}

func (n *Node) getSuccessorList() []NodeAddr {
	n.mu.RLock()
	defer n.mu.RUnlock()
	out := make([]NodeAddr, len(n.successorList))
	copy(out, n.successorList)
	return out
}

func (n *Node) SetAddrPublic(addr string)	{ n.setAddr(addr) }

func (n *Node) setAddr(addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	oldSelf := NodeAddr{ID: n.id, Addr: n.addr}
	newSelf := NodeAddr{ID: n.id, Addr: addr}
	n.addr = addr

	if n.successor == oldSelf {
		n.successor = newSelf
	}
	for i, s := range n.successorList {
		if s == oldSelf {
			n.successorList[i] = newSelf
		}
	}
	for i, f := range n.fingers {
		if f == oldSelf {
			n.fingers[i] = newSelf
		}
	}
	if n.predecessor != nil && *n.predecessor == oldSelf {
		n.predecessor = &newSelf
	}
}

func (n *Node) RecordPut(ctx context.Context, key NodeID, data []byte) error {
	responsible, err := n.FindSuccessor(ctx, key)
	if err != nil {
		return fmt.Errorf("route record put: %w", err)
	}
	if responsible.ID.Equal(n.id) {
		if err := n.storeRecord(key, data, n.id); err != nil {
			return err
		}
		n.replicateRecordBestEffort(ctx, key, data)
		return nil
	}
	rt, ok := n.transport.(RecordTransport)
	if !ok {
		return fmt.Errorf("transport does not support record operations")
	}
	return rt.PutRecord(ctx, responsible, key, data, n.id)
}

func (n *Node) replicateRecordBestEffort(ctx context.Context, key NodeID, data []byte) {
	if n.replication <= 1 {
		return
	}
	rt, ok := n.transport.(RecordTransport)
	if !ok {
		return
	}
	n.mu.RLock()
	succs := make([]NodeAddr, len(n.successorList))
	copy(succs, n.successorList)
	n.mu.RUnlock()

	for i := 0; i < n.replication-1 && i < len(succs); i++ {
		succ := succs[i]
		if !succ.ID.Equal(n.id) {
			_ = rt.PushRecords(ctx, succ, []NodeID{key}, [][]byte{data})
		}
	}
}

func (n *Node) getRecordsInRange(from, to NodeID) ([]NodeID, [][]byte) {
	n.recordsMu.RLock()
	defer n.recordsMu.RUnlock()

	var keys []NodeID
	var data [][]byte
	for k, v := range n.records {
		if k.BetweenRightInclusive(from, to) {
			cp := make([]byte, len(v))
			copy(cp, v)
			keys = append(keys, k)
			data = append(data, cp)
		}
	}
	return keys, data
}

func (n *Node) transferRecordsRange(target NodeAddr, from, to NodeID) {
	rt, ok := n.transport.(RecordTransport)
	if !ok {
		return
	}
	keys, data := n.getRecordsInRange(from, to)
	if len(keys) == 0 {
		return
	}
	ctx := context.Background()
	_ = rt.PushRecords(ctx, target, keys, data)
}

func (n *Node) storeRecord(key NodeID, data []byte, callerID NodeID) error {
	typ := classifyRecord(data)

	switch typ {
	case recProvider:
		return n.storeProviderRecord(key, data, callerID)
	case recPeer, recGroup:
		return n.storeSignedRecord(key, data, typ)
	default:
		n.localRecordPut(key, data)
		return nil
	}
}

func (n *Node) storeProviderRecord(key NodeID, data []byte, callerID NodeID) error {
	var rawPR dhtpb.ProviderRecord
	if err := proto.Unmarshal(data, &rawPR); err != nil {
		return fmt.Errorf("decode provider record: %w", err)
	}

	var pr ProviderRecord
	copy(pr.ContentHash[:], rawPR.ContentHash)
	copy(pr.Provider[:], rawPR.Provider)

	n.recordsMu.Lock()
	defer n.recordsMu.Unlock()

	if callerID != pr.Provider {
		return fmt.Errorf("provider record rejected: caller %s does not match advertised provider %s", callerID, pr.Provider)
	}

	now := n.now()

	var existing []ProviderRecord
	if d, ok := n.records[key]; ok {
		all, _ := DecodeProviderRecords(d)
		entryTimes := n.providerEntryTimes[key]
		for _, e := range all {
			if t, ok := entryTimes[e.Provider]; ok && now.Sub(t) <= n.providerRecordTTL {
				existing = append(existing, e)
			}
		}
	}

	out := existing[:0]
	for _, e := range existing {
		if e.Provider != pr.Provider {
			out = append(out, e)
		}
	}
	out = append(out, pr)

	if n.providerEntryTimes[key] == nil {
		n.providerEntryTimes[key] = make(map[NodeID]time.Time)
	}
	n.providerEntryTimes[key][pr.Provider] = now

	merged, err := encodeProviderRecordListWithTimestamps(out, n.providerEntryTimes[key])
	if err != nil {
		return fmt.Errorf("encode provider record list: %w", err)
	}
	n.records[key] = merged
	n.recordsMeta[key] = recordMeta{storedAt: now, ttl: 0, typ: recProvider}
	return nil
}

func (n *Node) storeSignedRecord(key NodeID, data []byte, typ recType) error {
	var incomingVersion uint64

	if typ == recPeer {
		pir, err := DecodePeerIdentityRecord(data)
		if err != nil {
			return fmt.Errorf("decode peer identity record: %w", err)
		}
		if err := pir.Verify(); err != nil {
			return fmt.Errorf("peer identity record rejected: %w", err)
		}
		incomingVersion = pir.Data.Version
	} else {
		gir, err := DecodeGroupIdentityRecord(data)
		if err != nil {
			return fmt.Errorf("decode group identity record: %w", err)
		}
		if err := gir.Verify(); err != nil {
			return fmt.Errorf("group identity record rejected: %w", err)
		}
		incomingVersion = gir.Data.Version
	}

	n.recordsMu.Lock()
	defer n.recordsMu.Unlock()

	now := n.now()
	if raw, ok := n.records[key]; ok {
		storedMeta := n.recordsMeta[key]
		if !storedMeta.isExpiredAt(now) {
			if storedVer := extractRecordVersion(raw); storedVer >= incomingVersion {
				slog.Debug("chord: record version conflict rejected", "node", n.id, "key", key, "stored_version", storedVer, "incoming_version", incomingVersion)
				return fmt.Errorf("%w: stored version %d, incoming version %d",
					ErrVersionConflict, storedVer, incomingVersion)
			}
		}
	}

	cp := make([]byte, len(data))
	copy(cp, data)
	n.records[key] = cp
	n.recordsMeta[key] = recordMeta{storedAt: now, ttl: n.ttlForType(typ), typ: typ}
	slog.Debug("chord: signed record stored", "node", n.id, "key", key, "version", incomingVersion, "ttl", n.ttlForType(typ))
	return nil
}

func extractRecordVersion(data []byte) uint64 {
	if pir, err := DecodePeerIdentityRecord(data); err == nil && pir.Data.PeerID != (NodeID{}) {
		return pir.Data.Version
	}
	if gir, err := DecodeGroupIdentityRecord(data); err == nil && gir.Data.GroupID != (NodeID{}) {
		return gir.Data.Version
	}
	return 0
}

func (n *Node) RecordGet(ctx context.Context, key NodeID) ([]byte, error) {
	responsible, err := n.FindSuccessor(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("route record get: %w", err)
	}
	if responsible.ID.Equal(n.id) {
		return n.localRecordGet(key)
	}
	rt, ok := n.transport.(RecordTransport)
	if !ok {
		return nil, fmt.Errorf("transport does not support record operations")
	}
	return rt.GetRecord(ctx, responsible, key)
}

func classifyRecord(data []byte) recType {
	var rawPR dhtpb.ProviderRecord
	if err := proto.Unmarshal(data, &rawPR); err == nil &&
		len(rawPR.ContentHash) == 20 && len(rawPR.Provider) == 20 {
		return recProvider
	}
	var rawPRL dhtpb.ProviderRecordList
	if err := proto.Unmarshal(data, &rawPRL); err == nil &&
		len(rawPRL.Providers) > 0 &&
		len(rawPRL.Providers[0].ContentHash) == 20 &&
		len(rawPRL.Providers[0].Provider) == 20 {
		return recProvider
	}
	var rawSR dhtpb.SignedRecord
	if err := proto.Unmarshal(data, &rawSR); err == nil &&
		len(rawSR.Pubkey) == 32 && len(rawSR.Signature) == 64 && len(rawSR.Data) > 0 {
		var peerData PeerIdentityData
		if cbor.Unmarshal(rawSR.Data, &peerData) == nil && peerData.PeerID != (NodeID{}) {
			return recPeer
		}
		return recGroup
	}
	return recUnknown
}

func (n *Node) ttlForType(typ recType) time.Duration {
	switch typ {
	case recPeer:
		return n.peerRecordTTL
	case recGroup:
		return n.groupRecordTTL
	case recProvider:
		return 0
	default:
		return 0
	}
}

func (n *Node) localRecordPut(key NodeID, data []byte) {
	incomingVer := extractRecordVersion(data)

	cp := make([]byte, len(data))
	copy(cp, data)

	n.recordsMu.Lock()
	defer n.recordsMu.Unlock()

	now := n.now()

	if incomingVer > 0 {
		if existing, ok := n.records[key]; ok {
			storedMeta := n.recordsMeta[key]
			if !storedMeta.isExpiredAt(now) {
				if storedVer := extractRecordVersion(existing); storedVer >= incomingVer {
					slog.Debug("chord: replica push dropped (stale version)", "node", n.id, "key", key, "stored", storedVer, "incoming", incomingVer)
					return
				}
			}
		}
	}

	typ := classifyRecord(data)
	n.records[key] = cp
	n.recordsMeta[key] = recordMeta{storedAt: now, ttl: n.ttlForType(typ), typ: typ}

	var rawPRL dhtpb.ProviderRecordList
	if err := proto.Unmarshal(data, &rawPRL); err == nil && len(rawPRL.Providers) > 0 {
		hasTimestamps := len(rawPRL.Timestamps) == len(rawPRL.Providers)
		if n.providerEntryTimes[key] == nil {
			n.providerEntryTimes[key] = make(map[NodeID]time.Time)
		}
		for i, p := range rawPRL.Providers {
			if len(p.Provider) != 20 {
				continue
			}
			var provID NodeID
			copy(provID[:], p.Provider)

			var entryTime time.Time
			if hasTimestamps && rawPRL.Timestamps[i] > 0 {
				entryTime = time.Unix(0, rawPRL.Timestamps[i])
			} else {
				entryTime = now
			}

			if n.providerRecordTTL > 0 && now.Sub(entryTime) > n.providerRecordTTL {
				continue
			}
			n.providerEntryTimes[key][provID] = entryTime
		}
	}
}

func (n *Node) localRecordGet(key NodeID) ([]byte, error) {
	now := n.now()

	n.recordsMu.RLock()
	data, ok := n.records[key]
	meta := n.recordsMeta[key]
	entryTimes := n.providerEntryTimes[key]
	n.recordsMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("record not found at %s", key)
	}

	if entryTimes == nil {
		if meta.isExpiredAt(now) {
			return nil, fmt.Errorf("record at %s has expired (TTL %s)", key, meta.ttl)
		}
		cp := make([]byte, len(data))
		copy(cp, data)
		return cp, nil
	}

	all, err := DecodeProviderRecords(data)
	if err != nil {
		return nil, fmt.Errorf("decode provider records at %s: %w", key, err)
	}
	var live []ProviderRecord
	for _, e := range all {
		if t, ok := entryTimes[e.Provider]; ok && now.Sub(t) <= n.providerRecordTTL {
			live = append(live, e)
		}
	}
	if len(live) == 0 {
		return nil, fmt.Errorf("record not found at %s", key)
	}
	return encodeProviderRecordList(live)
}

func (n *Node) purgeExpiredRecords() {
	now := n.now()

	n.recordsMu.Lock()
	defer n.recordsMu.Unlock()

	for key, meta := range n.recordsMeta {
		if meta.typ == recProvider {
			continue
		}
		if meta.isExpiredAt(now) {
			slog.Debug("chord: purging expired record", "node", n.id, "key", key, "ttl", meta.ttl)
			delete(n.records, key)
			delete(n.recordsMeta, key)
		}
	}

	for key, entryTimes := range n.providerEntryTimes {

		for provID, storedAt := range entryTimes {
			if now.Sub(storedAt) > n.providerRecordTTL {
				slog.Debug("chord: purging expired provider entry", "node", n.id, "key", key, "provider", provID)
				delete(entryTimes, provID)
			}
		}

		if len(entryTimes) == 0 {

			delete(n.records, key)
			delete(n.recordsMeta, key)
			delete(n.providerEntryTimes, key)
			continue
		}

		existing, err := DecodeProviderRecords(n.records[key])
		if err != nil {
			continue
		}
		var survivors []ProviderRecord
		for _, e := range existing {
			if _, alive := entryTimes[e.Provider]; alive {
				survivors = append(survivors, e)
			}
		}
		if merged, err := encodeProviderRecordList(survivors); err == nil {
			n.records[key] = merged
		}
	}
}

func (n *Node) DataCount() int {
	ctx := context.Background()
	ch, err := n.blocks.AllKeysChan(ctx)
	if err != nil {
		return 0
	}
	count := 0
	for range ch {
		count++
	}
	return count
}

type NodeState struct {
	ID		string		`json:"id"`
	Addr		string		`json:"addr"`
	Successor	NodeAddr	`json:"successor"`
	SuccessorList	[]NodeAddr	`json:"successor_list"`
	Predecessor	*NodeAddr	`json:"predecessor"`

	Fingers	[]NodeAddr	`json:"fingers"`

	BlockCount	int	`json:"block_count"`
	RecordCount	int	`json:"record_count"`

	StorageUsedBytes	int64	`json:"storage_used_bytes"`
	StorageMaxBytes		int64	`json:"storage_max_bytes"`

	RingID			string	`json:"ring_id"`
	RingBlockCount		int	`json:"ring_block_count"`
	RingStorageUsedBytes	int64	`json:"ring_storage_used_bytes"`
	RingStorageMaxBytes	int64	`json:"ring_storage_max_bytes"`
	RingNetworkRootCount	int	`json:"ring_network_root_count"`
}

func (n *Node) StateJSON() []byte {
	s := n.State()
	data, _ := json.Marshal(s)
	return data
}

func (n *Node) State() NodeState {
	n.mu.RLock()
	succ := n.successor
	succList := make([]NodeAddr, len(n.successorList))
	copy(succList, n.successorList)
	var pred *NodeAddr
	if n.predecessor != nil {
		p := *n.predecessor
		pred = &p
	}
	fingers := n.fingers[:]
	n.mu.RUnlock()

	n.recordsMu.RLock()
	recCount := len(n.records)
	n.recordsMu.RUnlock()

	state := NodeState{
		ID:		n.id.String(),
		Addr:		n.addr,
		Successor:	succ,
		SuccessorList:	succList,
		Predecessor:	pred,
		Fingers:	fingers,
		BlockCount:	n.DataCount(),
		RecordCount:	recCount,
		RingID:		n.ringID,
	}
	if n.storageStatusFn != nil {
		state.StorageUsedBytes, state.StorageMaxBytes = n.storageStatusFn()
	}
	if rv := n.ringStore(); rv != nil {
		state.RingBlockCount = int(rv.BlockCount())
		state.RingStorageUsedBytes = rv.UsedBytes()
		state.RingStorageMaxBytes = rv.Quota()
		state.RingNetworkRootCount = rv.RootCount()
	}
	return state
}
