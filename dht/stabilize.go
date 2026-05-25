package dht

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/mjagos0/datarings/eventlog"
)

func jitteredSleep(base time.Duration, stop <-chan struct{}) bool {
	half := base / 2
	jitter := time.Duration(rand.Int63n(int64(base)))
	d := half + jitter
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-stop:
		return false
	case <-t.C:
		return true
	}
}

func (n *Node) PauseBackground()	{ n.backgroundPaused.Store(true) }

func (n *Node) ResumeBackground()	{ n.backgroundPaused.Store(false) }

func (n *Node) BackgroundPaused() bool	{ return n.backgroundPaused.Load() }

func (n *Node) StartBackground(cfg Config) {
	goroutines := 2
	purgePeriod := cfg.recordPurgePeriod()
	if purgePeriod > 0 {
		goroutines = 3
	}
	n.wg.Add(goroutines)

	go func() {
		defer n.wg.Done()
		base := cfg.stabilizeInterval()
		for jitteredSleep(base, n.stopCh) {
			if !n.backgroundPaused.Load() {
				start := time.Now()
				n.stabilize()
				if n.met != nil {
					n.met.StabilizeRoundsTotal.WithLabelValues(n.metRing).Inc()
					n.met.StabilizeDurationSeconds.WithLabelValues(n.metRing).Observe(time.Since(start).Seconds())
				}
			}
		}
	}()

	go func() {
		defer n.wg.Done()
		base := cfg.fixFingersInterval()

		for i := 0; i < fingerCount; i++ {
			select {
			case <-n.stopCh:
				return
			default:
			}
			if !n.backgroundPaused.Load() {
				n.fixFinger()
			}
		}

		for jitteredSleep(base, n.stopCh) {
			if !n.backgroundPaused.Load() {
				n.fixFinger()
			}
		}
	}()

	if purgePeriod > 0 {
		go func() {
			defer n.wg.Done()
			tick := time.NewTicker(purgePeriod)
			defer tick.Stop()
			for {
				select {
				case <-n.stopCh:
					return
				case <-tick.C:
					n.purgeExpiredRecords()
				}
			}
		}()
	}
}

func (n *Node) stabilize() {
	ctx := context.Background()
	succ := n.getSuccessor()

	n.mu.RLock()
	savedOldList := make([]NodeAddr, len(n.successorList))
	copy(savedOldList, n.successorList)
	n.mu.RUnlock()

	pred, err := n.transport.GetPredecessor(ctx, succ)
	if err != nil {

		slog.Debug("chord: successor unreachable during stabilize", "node", n.id, "successor", succ.ID, "error", err)
		n.handleSuccessorFailure(ctx)

		succ = n.getSuccessor()
		if succ.ID.Equal(n.id) {
			return
		}
		n.refreshSuccessorListAndReplicate(ctx, succ, savedOldList)
		return
	}

	if pred != nil && pred.ID.BetweenExclusive(n.id, succ.ID) {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := n.transport.Ping(pingCtx, *pred)
		cancel()
		if err == nil {
			n.mu.Lock()
			n.successor = *pred
			n.mu.Unlock()
			slog.Info("chord: successor updated via stabilize", "node", n.id, "old", succ.ID, "new", pred.ID)
			succ = *pred
		} else {
			slog.Debug("chord: candidate successor unreachable, keeping current",
				"node", n.id, "candidate", pred.ID, "error", err)
		}
	}

	_ = n.transport.Notify(ctx, succ, NodeAddr{ID: n.id, Addr: n.addr})

	if succ.ID.Equal(n.id) {
		n.tryReconnect(ctx)
		return
	}

	n.refreshSuccessorListAndReplicate(ctx, succ, savedOldList)
}

func (n *Node) refreshSuccessorListAndReplicate(ctx context.Context, succ NodeAddr, oldList []NodeAddr) {
	succList, err := n.transport.GetSuccessorList(ctx, succ)
	if err != nil {
		return
	}

	n.mu.Lock()
	list := make([]NodeAddr, 0, n.successorListSize)
	list = append(list, succ)
	for _, s := range succList {
		if len(list) >= n.successorListSize {
			break
		}
		list = append(list, s)
	}
	n.successorList = list
	n.mu.Unlock()

	n.triggerReplicateToNewSuccessors(ctx, oldList, list)

	n.refreshPredecessorList(ctx)
}

func (n *Node) handleSuccessorFailure(ctx context.Context) {
	n.mu.Lock()
	defer n.mu.Unlock()

	oldSuccID := ""
	if len(n.successorList) > 0 {
		oldSuccID = n.successorList[0].ID.String()
	}

	for i, s := range n.successorList {
		if s.ID.Equal(n.id) {
			continue
		}
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := n.transport.Ping(pingCtx, s)
		cancel()
		if err == nil {
			slog.Info("chord: failed over to backup successor", "node", n.id, "successor", s.ID)
			eventlog.Emit("successor_failure_detected", map[string]any{
				"ring":			n.metRing,
				"failed":		oldSuccID,
				"new_successor":	s.ID.String(),
			})
			n.successor = s
			n.successorList = n.successorList[i:]
			return
		}
		slog.Debug("chord: backup successor unreachable", "node", n.id, "successor", s.ID, "error", err)
	}

	slog.Warn("chord: all successors unreachable, forming single-node ring", "node", n.id)
	self := NodeAddr{ID: n.id, Addr: n.addr}
	newList := []NodeAddr{self}
	for _, s := range n.successorList {
		if !s.ID.Equal(n.id) {
			newList = append(newList, s)
		}
	}
	n.successor = self
	n.successorList = newList
}

func (n *Node) tryReconnect(ctx context.Context) bool {
	n.mu.RLock()
	list := make([]NodeAddr, len(n.successorList))
	copy(list, n.successorList)
	refresh := n.addrRefresher
	n.mu.RUnlock()

	for _, s := range list {
		if s.ID.Equal(n.id) {
			continue
		}
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := n.transport.Ping(pingCtx, s)
		cancel()
		if err == nil {
			slog.Info("chord: partition healed — reconnecting to peer", "node", n.id, "peer", s.ID)
			n.mu.Lock()
			n.successor = s
			n.successorList = []NodeAddr{s}
			n.mu.Unlock()
			return true
		}

		if refresh == nil {
			continue
		}
		newAddr := refresh(s.ID)
		if newAddr == "" || newAddr == s.Addr {
			continue
		}
		updated := NodeAddr{ID: s.ID, Addr: newAddr}
		pingCtx2, cancel2 := context.WithTimeout(ctx, 2*time.Second)
		err2 := n.transport.Ping(pingCtx2, updated)
		cancel2()
		if err2 == nil {
			slog.Info("chord: reconnected to peer at refreshed address",
				"node", n.id, "peer", s.ID, "old_addr", s.Addr, "new_addr", newAddr)
			n.mu.Lock()
			n.successor = updated
			n.successorList = []NodeAddr{updated}
			n.mu.Unlock()
			return true
		}
	}
	return false
}

func (n *Node) fixFinger() {
	ctx := context.Background()

	n.mu.Lock()
	i := n.nextFinger
	n.nextFinger = (n.nextFinger + 1) % fingerCount
	n.mu.Unlock()

	start := n.id.fingerStart(i)
	succ, err := n.FindSuccessor(ctx, start)
	if err != nil {
		slog.Debug("chord: fixFinger failed", "node", n.id, "index", i, "error", err)
		if n.met != nil {
			n.met.FixFingerErrors.WithLabelValues(n.metRing).Inc()
		}
		return
	}

	n.mu.Lock()
	old := n.fingers[i]
	n.fingers[i] = succ
	n.mu.Unlock()

	if old.ID != succ.ID {
		slog.Debug("chord: finger updated", "node", n.id, "index", i, "old", old.ID, "new", succ.ID)
		eventlog.Emit("finger_updated", map[string]any{
			"ring":		n.metRing,
			"index":	i,
			"old":		old.ID.String(),
			"new":		succ.ID.String(),
			"new_addr":	succ.Addr,
		})
	}
}

func (n *Node) checkPredecessor() {
	pred := n.getPredecessor()
	if pred == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := n.transport.Ping(ctx, *pred); err != nil {
		slog.Info("chord: predecessor unreachable, clearing", "node", n.id, "predecessor", pred.ID, "error", err)
		eventlog.Emit("predecessor_failure_detected", map[string]any{
			"ring":		n.metRing,
			"failed":	pred.ID.String(),
		})
		n.mu.Lock()
		n.predecessor = nil
		n.mu.Unlock()
	}
}

func (n *Node) Stabilize()	{ n.stabilize() }

func (n *Node) FixAllFingers() {
	for i := 0; i < fingerCount; i++ {
		n.fixFinger()
	}
}

func (n *Node) CheckPredecessor()	{ n.checkPredecessor() }

func (n *Node) StabilizeFull() {
	start := time.Now()
	n.checkPredecessor()
	n.stabilize()
	n.FixAllFingers()
	if n.met != nil {
		dur := time.Since(start).Seconds()
		n.met.StabilizeDurationSeconds.WithLabelValues(n.metRing).Observe(dur)
		n.met.StabilizeRoundsTotal.WithLabelValues(n.metRing).Inc()
	}
}

func (n *Node) RecordKeys() []string {
	n.recordsMu.RLock()
	defer n.recordsMu.RUnlock()
	keys := make([]string, 0, len(n.records))
	for k := range n.records {
		keys = append(keys, k.String())
	}
	return keys
}

func (n *Node) HasRecord(keyHex string) bool {
	b, err := hex.DecodeString(keyHex)
	if err != nil || len(b) != 20 {
		return false
	}
	var id NodeID
	copy(id[:], b)
	n.recordsMu.RLock()
	defer n.recordsMu.RUnlock()
	_, ok := n.records[id]
	return ok
}

func (n *Node) HasLocalBlock(cidStr string) (bool, error) {
	c, err := cid.Decode(cidStr)
	if err != nil {
		return false, fmt.Errorf("invalid CID %q: %w", cidStr, err)
	}
	ctx := context.Background()

	if has, err := n.blocks.Has(ctx, c); err == nil && has {
		return true, nil
	}
	if n.localBlocks != nil {
		return n.localBlocks.Has(ctx, c)
	}
	return n.blocks.Has(ctx, c)
}

func (n *Node) DeleteLocalBlock(cidStr string) error {
	c, err := cid.Decode(cidStr)
	if err != nil {
		return fmt.Errorf("invalid CID %q: %w", cidStr, err)
	}
	ctx := context.Background()

	err1 := n.blocks.Delete(ctx, c)
	if n.localBlocks != nil {
		err2 := n.localBlocks.Delete(ctx, c)
		if err1 != nil {
			return err2
		}
	}
	return err1
}

func (n *Node) RingBlockRange(ctx context.Context) (low, high NodeID, ok bool) {
	high = n.id

	n.mu.RLock()
	pred := n.predecessor
	k := n.replication
	n.mu.RUnlock()

	if pred == nil {
		return NodeID{}, high, false
	}

	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	err := n.transport.Ping(pingCtx, *pred)
	cancel()
	if err != nil {
		slog.Info("chord: predecessor unreachable during RingBlockRange, clearing",
			"node", n.id, "predecessor", pred.ID, "error", err)
		n.mu.Lock()
		if n.predecessor != nil && n.predecessor.ID.Equal(pred.ID) {
			n.predecessor = nil
		}
		n.mu.Unlock()
		return NodeID{}, high, false
	}

	low = pred.ID
	current := *pred
	for i := 1; i < k; i++ {
		prevPred, err := n.transport.GetPredecessor(ctx, current)
		if err != nil || prevPred == nil {
			return NodeID{}, high, false
		}
		low = prevPred.ID
		current = *prevPred
	}

	return low, high, true
}

func (n *Node) IsRingBlock(ctx context.Context, c cid.Cid) bool {
	low, high, ok := n.RingBlockRange(ctx)
	if !ok {
		return true
	}
	return CIDToNodeID(c).BetweenRightInclusive(low, high)
}

func (n *Node) DeleteRecord(keyHex string) bool {
	b, err := hex.DecodeString(keyHex)
	if err != nil || len(b) != 20 {
		return false
	}
	var id NodeID
	copy(id[:], b)
	n.recordsMu.Lock()
	defer n.recordsMu.Unlock()
	if _, ok := n.records[id]; !ok {
		return false
	}
	delete(n.records, id)
	delete(n.recordsMeta, id)
	return true
}

func (n *Node) RecordKeysJSON() []byte {
	keys := n.RecordKeys()
	data, _ := json.Marshal(keys)
	return data
}
