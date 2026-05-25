package dht

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/rpc"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/mjagos0/datarings/eventlog"
	"github.com/mjagos0/datarings/metrics"
	"github.com/mjagos0/datarings/store"
)

type ArgsFindSuccessor struct{ ID NodeID }
type ReplyFindSuccessor struct{ Node NodeAddr }

type ArgsGetPredecessor struct{}
type ReplyGetPredecessor struct {
	Node	NodeAddr
	HasNode	bool
	SelfID	NodeID
}

type ArgsGetSuccessorList struct{}
type ReplyGetSuccessorList struct{ List []NodeAddr }

type ArgsNotify struct{ Caller NodeAddr }
type ReplyNotify struct{}

type ArgsPutBlock struct {
	Key		[]byte
	Data		[]byte
	RootCID		[]byte
	RootTTLNanos	int64
}
type ReplyPutBlock struct{}

type ArgsPushBlocks struct {
	Keys		[][]byte
	Data		[][]byte
	BlockRootCIDs	[][]string
	BlockRootExpiry	[][]int64
}
type ReplyPushBlocks struct{}

type ArgsReconcileBlocks struct {
	Keys [][]byte
}
type ReplyReconcileBlocks struct {
	MissingIndices []uint32
}

type ArgsFetchBlock struct{ Key []byte }
type ReplyFetchBlock struct {
	Data	[]byte
	Found	bool
}

type ArgsHasBlock struct{ Key []byte }
type ReplyHasBlock struct{ Has bool }

type ArgsRemoveBlock struct{ Key []byte }
type ReplyRemoveBlock struct{}

type ArgsTransferKeys struct{ From, To NodeID }
type ReplyTransferKeys struct {
	Keys		[][]byte
	Data		[][]byte
	BlockRootCIDs	[][]string
	BlockRootExpiry	[][]int64
}

type ArgsDeleteCID struct {
	CID		[]byte
	Propagate	bool
}
type ReplyDeleteCID struct{}

type ArgsNotifyLeave struct {
	Self		NodeAddr
	Successor	NodeAddr
	Predecessor	NodeAddr
}
type ReplyNotifyLeave struct{}

type ArgsClosestPrecedingNode struct{ ID NodeID }
type ReplyClosestPrecedingNode struct {
	Predecessor	NodeAddr
	Successor	NodeAddr
}

type ArgsPing struct{}
type ReplyPing struct{}

type ArgsPutRecord struct {
	Key		NodeID
	Data		[]byte
	CallerID	NodeID
}
type ReplyPutRecord struct{}

type ArgsGetRecord struct{ Key NodeID }
type ReplyGetRecord struct {
	Data	[]byte
	Found	bool
}

type ArgsPushRecords struct {
	Keys	[]NodeID
	Data	[][]byte
}
type ReplyPushRecords struct{}

type ArgsTransferRecords struct{ From, To NodeID }
type ReplyTransferRecords struct {
	Keys	[]NodeID
	Data	[][]byte
}

type RPCServer struct {
	listener net.Listener
}

func StartServer(listenAddr, advertiseAddr string, node *Node) (*RPCServer, string, error) {
	tcpAddr, err := MultiaddrToTCPAddr(listenAddr)
	if err != nil {
		return nil, "", fmt.Errorf("dht rpc parse addr %q: %w", listenAddr, err)
	}

	network := "tcp"
	if strings.Contains(listenAddr, "/ip4/") {
		network = "tcp4"
	}
	l, err := listenReuse(network, tcpAddr)
	if err != nil {
		return nil, "", fmt.Errorf("dht rpc listen %s: %w", tcpAddr, err)
	}

	srv := rpc.NewServer()
	if err := srv.RegisterName("ChordNode", &rpcHandler{node: node}); err != nil {
		l.Close()
		return nil, "", fmt.Errorf("dht rpc register: %w", err)
	}

	s := &RPCServer{listener: l}
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go srv.ServeConn(conn)
		}
	}()

	boundMA, err := effectiveMultiaddr(l.Addr().String(), advertiseAddr)
	if err != nil {

		boundMA = l.Addr().String()
	}
	return s, boundMA, nil
}

func (s *RPCServer) Stop() {
	s.listener.Close()
}

type rpcHandler struct {
	node			*Node
	authenticatedPeerID	*NodeID
}

func (h *rpcHandler) FindSuccessor(args *ArgsFindSuccessor, reply *ReplyFindSuccessor) error {
	ctx := context.Background()
	n, err := h.node.FindSuccessor(ctx, args.ID)
	if err != nil {
		return err
	}
	reply.Node = n
	return nil
}

func (h *rpcHandler) GetPredecessor(_ *ArgsGetPredecessor, reply *ReplyGetPredecessor) error {
	reply.SelfID = h.node.id
	pred := h.node.getPredecessor()
	if pred != nil {
		reply.Node = *pred
		reply.HasNode = true
	}
	return nil
}

func (h *rpcHandler) GetSuccessorList(_ *ArgsGetSuccessorList, reply *ReplyGetSuccessorList) error {
	reply.List = h.node.getSuccessorList()
	return nil
}

func (h *rpcHandler) Notify(args *ArgsNotify, _ *ReplyNotify) error {

	if h.authenticatedPeerID != nil && args.Caller.ID != *h.authenticatedPeerID {
		return fmt.Errorf("notify rejected: caller ID %s does not match authenticated peer %s",
			args.Caller.ID, *h.authenticatedPeerID)
	}
	h.node.notify(args.Caller)
	return nil
}

func (h *rpcHandler) PutBlock(args *ArgsPutBlock, _ *ReplyPutBlock) error {
	ctx := context.Background()
	c, err := cid.Cast(args.Key)
	if err != nil {
		return fmt.Errorf("invalid CID: %w", err)
	}

	var rootCID cid.Cid
	var rootExpiry int64
	if len(args.RootCID) > 0 {
		if rc, err := cid.Cast(args.RootCID); err == nil {
			rootCID = rc
			rootExpiry = args.RootTTLNanos
		}
	}

	storedLocally := false
	emitRefused := func() {
		eventlog.Emit("block_store_refused", map[string]any{
			"ring":		h.node.metRing,
			"type":		"primary",
			"cid":		c.String(),
			"size":		len(args.Data),
			"reason":	"storage_full",
		})
	}
	if rv := h.node.ringStore(); rv != nil {
		if err := rv.PutWithRoot(ctx, c, args.Data, rootCID, rootExpiry); err != nil {
			if store.IsStorageFull(err) {
				slog.Warn("chord: PutBlock local store full, will try replicas", "node", h.node.id, "cid", c)
				emitRefused()
			} else {
				return err
			}
		} else {
			storedLocally = true
		}
	} else {
		if err := h.node.blocks.Put(ctx, c, args.Data); err != nil {
			if store.IsStorageFull(err) {
				slog.Warn("chord: PutBlock local store full, will try replicas", "node", h.node.id, "cid", c)
				emitRefused()
			} else {
				return err
			}
		} else {
			storedLocally = true
		}
	}

	if storedLocally {
		eventlog.Emit("block_stored", map[string]any{
			"ring":	h.node.metRing,
			"type":	"primary",
			"cid":	c.String(),
			"size":	len(args.Data),
		})

		h.node.replicateAsync(context.Background(), c, args.Data, rootCID, rootExpiry)
		return nil
	}
	replicaOK, replicaTotal := h.node.replicateAndReport(ctx, c, args.Data, rootCID, rootExpiry)
	if replicaOK == 0 {
		eventlog.Emit("block_fanout_failed", map[string]any{
			"ring":		h.node.metRing,
			"cid":		c.String(),
			"size":		len(args.Data),
			"succeeded":	replicaOK,
			"total":	replicaTotal,
		})
		return store.ErrStorageFull
	}
	eventlog.Emit("block_fanout_succeeded", map[string]any{
		"ring":		h.node.metRing,
		"cid":		c.String(),
		"size":		len(args.Data),
		"succeeded":	replicaOK,
		"total":	replicaTotal,
	})
	return nil
}

func (h *rpcHandler) PushBlocks(args *ArgsPushBlocks, _ *ReplyPushBlocks) error {
	ctx := context.Background()
	rv := h.node.ringStore()
	stored := 0
	for i, rawKey := range args.Keys {
		c, err := cid.Cast(rawKey)
		if err != nil {
			return fmt.Errorf("invalid CID: %w", err)
		}

		var firstRootCID cid.Cid
		var firstRootExp int64
		hasRootCtx := false
		if rv != nil && i < len(args.BlockRootCIDs) && len(args.BlockRootCIDs[i]) > 0 {
			if rc, derr := cid.Decode(args.BlockRootCIDs[i][0]); derr == nil {
				firstRootCID = rc
				if 0 < len(args.BlockRootExpiry[i]) {
					firstRootExp = args.BlockRootExpiry[i][0]
				}
				hasRootCtx = true
			}
		}

		if hasRootCtx {
			if err := rv.PutWithRoot(ctx, c, args.Data[i], firstRootCID, firstRootExp); err != nil {
				if store.IsStorageFull(err) {
					slog.Warn("chord: PushBlocks skipped block — storage full", "node", h.node.id, "cid", c)
					eventlog.Emit("block_store_refused", map[string]any{
						"ring":		h.node.metRing,
						"type":		"replica",
						"cid":		c.String(),
						"size":		len(args.Data[i]),
						"reason":	"storage_full",
					})
					continue
				}
				return err
			}

			for j := 1; j < len(args.BlockRootCIDs[i]); j++ {
				rootCID := args.BlockRootCIDs[i][j]
				var exp int64
				if j < len(args.BlockRootExpiry[i]) {
					exp = args.BlockRootExpiry[i][j]
				}
				rv.AddRootWithExpiry(rootCID, exp)
				rv.AddBlockRootIndex(c, rootCID)
			}
		} else {

			if err := h.node.blocks.Put(ctx, c, args.Data[i]); err != nil {
				if store.IsStorageFull(err) {
					slog.Warn("chord: PushBlocks skipped block — storage full", "node", h.node.id, "cid", c)
					eventlog.Emit("block_store_refused", map[string]any{
						"ring":		h.node.metRing,
						"type":		"replica",
						"cid":		c.String(),
						"size":		len(args.Data[i]),
						"reason":	"storage_full",
					})
					continue
				}
				return err
			}
		}
		eventlog.Emit("block_stored", map[string]any{
			"ring":	h.node.metRing,
			"type":	"replica",
			"cid":	c.String(),
			"size":	len(args.Data[i]),
		})
		stored++
	}
	if stored == 0 && len(args.Keys) > 0 {
		return store.ErrStorageFull
	}
	return nil
}

func (h *rpcHandler) ReconcileBlocks(args *ArgsReconcileBlocks, reply *ReplyReconcileBlocks) error {
	ctx := context.Background()
	reply.MissingIndices = reply.MissingIndices[:0]
	for i, rawKey := range args.Keys {
		c, err := cid.Cast(rawKey)
		if err != nil {
			reply.MissingIndices = append(reply.MissingIndices, uint32(i))
			continue
		}
		has, err := h.node.blocks.Has(ctx, c)
		if err != nil || !has {
			reply.MissingIndices = append(reply.MissingIndices, uint32(i))
		}
	}
	return nil
}

func (h *rpcHandler) FetchBlock(args *ArgsFetchBlock, reply *ReplyFetchBlock) error {
	ctx := context.Background()
	c, err := cid.Cast(args.Key)
	if err != nil {
		return fmt.Errorf("invalid CID: %w", err)
	}

	data, err := h.node.blocks.Get(ctx, c)
	if err == nil {
		reply.Data = data
		reply.Found = true
		return nil
	}
	if h.node.localBlocks != nil {
		data, err = h.node.localBlocks.Get(ctx, c)
		if err == nil {
			reply.Data = data
			reply.Found = true
			return nil
		}
	}

	var targets []NodeAddr
	for _, s := range h.node.getSuccessorList() {
		if !s.ID.Equal(h.node.id) {
			targets = append(targets, s)
		}
	}
	if len(targets) == 0 {
		return nil
	}

	hits := make(chan []byte, len(targets))
	misses := make(chan struct{}, len(targets))

	for _, target := range targets {
		go func(target NodeAddr) {
			has, herr := h.node.transport.HasBlock(ctx, target, c)
			if herr != nil || !has {
				misses <- struct{}{}
				return
			}

			if err := h.node.acquireBlockSlots(ctx, 1); err != nil {
				misses <- struct{}{}
				return
			}
			defer h.node.releaseBlockSlots(1)
			d, ferr := h.node.transport.FetchBlock(ctx, target, c)
			if ferr != nil || d == nil {
				misses <- struct{}{}
				return
			}
			hits <- d
		}(target)
	}

	missCount := 0
	for {
		select {
		case d := <-hits:
			reply.Data = d
			reply.Found = true
			return nil
		case <-misses:
			missCount++
			if missCount == len(targets) {
				return nil
			}
		}
	}
}

func (h *rpcHandler) HasBlock(args *ArgsHasBlock, reply *ReplyHasBlock) error {
	ctx := context.Background()
	c, err := cid.Cast(args.Key)
	if err != nil {
		return fmt.Errorf("invalid CID: %w", err)
	}
	has, err := h.node.blocks.Has(ctx, c)
	if err != nil {
		return err
	}
	reply.Has = has
	return nil
}

func (h *rpcHandler) RemoveBlock(args *ArgsRemoveBlock, _ *ReplyRemoveBlock) error {
	ctx := context.Background()
	c, err := cid.Cast(args.Key)
	if err != nil {
		return fmt.Errorf("invalid CID: %w", err)
	}
	return h.node.blocks.Delete(ctx, c)
}

func (h *rpcHandler) TransferKeys(args *ArgsTransferKeys, reply *ReplyTransferKeys) error {
	ctx := context.Background()
	keys, data, blockRoots, err := h.node.getKeysInRange(ctx, args.From, args.To)
	if err != nil {
		return err
	}
	reply.BlockRootCIDs = make([][]string, len(keys))
	reply.BlockRootExpiry = make([][]int64, len(keys))
	for i, k := range keys {
		reply.Keys = append(reply.Keys, k.Bytes())
		entries := blockRoots[i]
		cidStrs := make([]string, len(entries))
		expiries := make([]int64, len(entries))
		for j, e := range entries {
			cidStrs[j] = e.CID
			expiries[j] = e.ExpiresAt
		}
		reply.BlockRootCIDs[i] = cidStrs
		reply.BlockRootExpiry[i] = expiries
	}
	reply.Data = data

	rv := h.node.ringStore()
	for _, k := range keys {
		if rv != nil {
			rv.DropBlock(k)
			if h.node.networkBlocks != nil && !h.node.networkBlocks.BlockHasLiveRootAnyRing(k) {
				_ = h.node.networkBlocks.Delete(ctx, k)
			}
		} else {
			_ = h.node.blocks.Delete(ctx, k)
		}
	}
	return nil
}

func (h *rpcHandler) DeleteCID(args *ArgsDeleteCID, _ *ReplyDeleteCID) error {
	c, err := cid.Cast(args.CID)
	if err != nil {
		return fmt.Errorf("invalid CID: %w", err)
	}
	h.node.handleDeleteCID(c, args.Propagate)
	return nil
}

func (h *rpcHandler) NotifyLeave(args *ArgsNotifyLeave, _ *ReplyNotifyLeave) error {
	h.node.handleNotifyLeave(args.Self, args.Successor, args.Predecessor)
	return nil
}

func (h *rpcHandler) ClosestPrecedingNode(args *ArgsClosestPrecedingNode, reply *ReplyClosestPrecedingNode) error {
	reply.Predecessor = h.node.closestPrecedingNode(args.ID)
	reply.Successor = h.node.getSuccessor()
	return nil
}

func (h *rpcHandler) Ping(_ *ArgsPing, _ *ReplyPing) error {
	return nil
}

func (h *rpcHandler) PutRecord(args *ArgsPutRecord, _ *ReplyPutRecord) error {
	callerID := args.CallerID

	if h.authenticatedPeerID != nil {
		callerID = *h.authenticatedPeerID
	}
	if err := h.node.storeRecord(args.Key, args.Data, callerID); err != nil {
		return err
	}

	ctx := context.Background()
	h.node.replicateRecordBestEffort(ctx, args.Key, args.Data)
	return nil
}

func (h *rpcHandler) GetRecord(args *ArgsGetRecord, reply *ReplyGetRecord) error {
	data, err := h.node.localRecordGet(args.Key)
	if err != nil {
		return nil
	}
	reply.Data = data
	reply.Found = true
	return nil
}

func (h *rpcHandler) PushRecords(args *ArgsPushRecords, _ *ReplyPushRecords) error {

	for i, k := range args.Keys {
		h.node.localRecordPut(k, args.Data[i])
	}
	return nil
}

func (h *rpcHandler) TransferRecords(args *ArgsTransferRecords, reply *ReplyTransferRecords) error {

	keys, data := h.node.getRecordsInRange(args.From, args.To)
	reply.Keys = keys
	reply.Data = data
	return nil
}

type RPCTransport struct {
	mu	sync.Mutex
	pool	map[string]*rpc.Client
	timeout	time.Duration
	met	*metrics.Registry
	ring	string
}

func NewRPCTransport(dialTimeout time.Duration) *RPCTransport {
	return &RPCTransport{
		pool:		make(map[string]*rpc.Client),
		timeout:	dialTimeout,
	}
}

func (t *RPCTransport) SetMetrics(m *metrics.Registry, ring string) {
	t.met = m
	t.ring = ring
}

func (t *RPCTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for addr, c := range t.pool {
		c.Close()
		delete(t.pool, addr)
	}
}

func (t *RPCTransport) getClient(addr string) (*rpc.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if c, ok := t.pool[addr]; ok {
		return c, nil
	}

	tcpAddr, err := MultiaddrToTCPAddr(addr)
	if err != nil {
		return nil, fmt.Errorf("parse peer addr %q: %w", addr, err)
	}

	conn, err := net.DialTimeout("tcp", tcpAddr, t.timeout)
	if err != nil {
		return nil, err
	}
	c := rpc.NewClient(conn)
	t.pool[addr] = c
	return c, nil
}

func (t *RPCTransport) call(addr, method string, args, reply interface{}) error {
	c, err := t.getClient(addr)
	if err != nil {
		return err
	}
	err = c.Call(method, args, reply)
	if err != nil {

		t.mu.Lock()
		delete(t.pool, addr)
		t.mu.Unlock()

		if err == rpc.ErrShutdown {
			c2, err2 := t.getClient(addr)
			if err2 != nil {
				return err
			}
			err = c2.Call(method, args, reply)
			if err != nil {
				t.mu.Lock()
				delete(t.pool, addr)
				t.mu.Unlock()
			}
			return err
		}
	}
	return err
}

var _ Transport = (*RPCTransport)(nil)

func (t *RPCTransport) FindSuccessor(_ context.Context, target NodeAddr, id NodeID) (NodeAddr, error) {
	if t.met != nil {
		t.met.DHTRPC.WithLabelValues("find_successor").Inc()
	}
	var reply ReplyFindSuccessor
	if err := t.call(target.Addr, "ChordNode.FindSuccessor", &ArgsFindSuccessor{ID: id}, &reply); err != nil {
		return NodeAddr{}, err
	}
	return reply.Node, nil
}

func (t *RPCTransport) ClosestPrecedingNode(_ context.Context, target NodeAddr, id NodeID) (NodeAddr, NodeAddr, error) {
	if t.met != nil {
		t.met.DHTRPC.WithLabelValues("closest_preceding_node").Inc()
	}
	var reply ReplyClosestPrecedingNode
	if err := t.call(target.Addr, "ChordNode.ClosestPrecedingNode", &ArgsClosestPrecedingNode{ID: id}, &reply); err != nil {
		return NodeAddr{}, NodeAddr{}, err
	}
	return reply.Predecessor, reply.Successor, nil
}

func (t *RPCTransport) GetPredecessor(_ context.Context, target NodeAddr) (*NodeAddr, error) {
	if t.met != nil {
		t.met.DHTRPC.WithLabelValues("get_predecessor").Inc()
	}
	var reply ReplyGetPredecessor
	if err := t.call(target.Addr, "ChordNode.GetPredecessor", &ArgsGetPredecessor{}, &reply); err != nil {
		return nil, err
	}

	var zeroID NodeID
	if reply.SelfID != zeroID && reply.SelfID != target.ID {
		return nil, fmt.Errorf("identity mismatch: expected %s, got %s", target.ID, reply.SelfID)
	}
	if !reply.HasNode {
		return nil, nil
	}
	n := reply.Node
	return &n, nil
}

func (t *RPCTransport) GetSuccessorList(_ context.Context, target NodeAddr) ([]NodeAddr, error) {
	if t.met != nil {
		t.met.DHTRPC.WithLabelValues("get_successor_list").Inc()
	}
	var reply ReplyGetSuccessorList
	if err := t.call(target.Addr, "ChordNode.GetSuccessorList", &ArgsGetSuccessorList{}, &reply); err != nil {
		return nil, err
	}
	return reply.List, nil
}

func (t *RPCTransport) Notify(_ context.Context, target NodeAddr, caller NodeAddr) error {
	if t.met != nil {
		t.met.DHTRPC.WithLabelValues("notify").Inc()
	}
	return t.call(target.Addr, "ChordNode.Notify", &ArgsNotify{Caller: caller}, &ReplyNotify{})
}

func (t *RPCTransport) PutBlock(_ context.Context, target NodeAddr, key cid.Cid, data []byte, rootCID cid.Cid, rootExpiry int64) error {
	args := &ArgsPutBlock{Key: key.Bytes(), Data: data, RootTTLNanos: rootExpiry}
	if rootCID.Defined() {
		args.RootCID = rootCID.Bytes()
	}
	if t.met == nil {
		return t.call(target.Addr, "ChordNode.PutBlock", args, &ReplyPutBlock{})
	}
	start := time.Now()
	err := t.call(target.Addr, "ChordNode.PutBlock", args, &ReplyPutBlock{})
	dur := time.Since(start).Seconds()
	if err != nil {
		t.met.PushErrors.WithLabelValues(t.ring).Inc()
		if store.IsStorageFull(err) {
			t.met.QuotaRejectionsTotal.WithLabelValues(t.ring).Inc()
		}
	} else {
		t.met.BlocksPushed.WithLabelValues(t.ring).Inc()
		t.met.PushBytes.WithLabelValues(t.ring).Add(float64(len(data)))
		t.met.PushDuration.WithLabelValues(t.ring).Observe(dur)
		t.met.BlocksStoredByType.WithLabelValues(t.ring, "primary").Inc()
		if dur > 0 {
			t.met.BlockPushSpeedBytesPerSec.WithLabelValues(t.ring).Observe(float64(len(data)) / dur)
		}
	}
	return err
}

func (t *RPCTransport) PushBlocks(_ context.Context, target NodeAddr, keys []cid.Cid, data [][]byte, blockRoots [][]store.NetworkRootEntry) error {
	brCIDs := make([][]string, len(keys))
	brExp := make([][]int64, len(keys))
	for i, roots := range blockRoots {
		cidStrs := make([]string, len(roots))
		expiries := make([]int64, len(roots))
		for j, e := range roots {
			cidStrs[j] = e.CID
			expiries[j] = e.ExpiresAt
		}
		brCIDs[i] = cidStrs
		brExp[i] = expiries
	}
	args := &ArgsPushBlocks{
		Keys:			make([][]byte, len(keys)),
		Data:			data,
		BlockRootCIDs:		brCIDs,
		BlockRootExpiry:	brExp,
	}
	for i, k := range keys {
		args.Keys[i] = k.Bytes()
	}
	if t.met == nil {
		return t.call(target.Addr, "ChordNode.PushBlocks", args, &ReplyPushBlocks{})
	}
	start := time.Now()
	err := t.call(target.Addr, "ChordNode.PushBlocks", args, &ReplyPushBlocks{})
	dur := time.Since(start).Seconds()
	if err != nil {
		t.met.PushErrors.WithLabelValues(t.ring).Inc()
		if store.IsStorageFull(err) {
			t.met.QuotaRejectionsTotal.WithLabelValues(t.ring).Inc()
		}
	} else {
		totalBytes := 0
		for _, d := range data {
			totalBytes += len(d)
		}
		t.met.BlocksPushed.WithLabelValues(t.ring).Add(float64(len(keys)))
		t.met.PushBytes.WithLabelValues(t.ring).Add(float64(totalBytes))
		t.met.PushDuration.WithLabelValues(t.ring).Observe(dur)
		t.met.BlocksStoredByType.WithLabelValues(t.ring, "replica").Add(float64(len(keys)))
		if dur > 0 && totalBytes > 0 {
			t.met.BlockPushSpeedBytesPerSec.WithLabelValues(t.ring).Observe(float64(totalBytes) / dur)
		}
	}
	return err
}

func (t *RPCTransport) ReconcileBlocks(_ context.Context, target NodeAddr, keys []cid.Cid) ([]cid.Cid, error) {
	args := &ArgsReconcileBlocks{Keys: make([][]byte, len(keys))}
	for i, k := range keys {
		args.Keys[i] = k.Bytes()
	}
	var reply ReplyReconcileBlocks
	if err := t.call(target.Addr, "ChordNode.ReconcileBlocks", args, &reply); err != nil {
		return nil, err
	}
	missing := make([]cid.Cid, 0, len(reply.MissingIndices))
	for _, idx := range reply.MissingIndices {
		if int(idx) < len(keys) {
			missing = append(missing, keys[idx])
		}
	}
	return missing, nil
}

func (t *RPCTransport) FetchBlock(_ context.Context, target NodeAddr, key cid.Cid) ([]byte, error) {
	if t.met == nil {
		var reply ReplyFetchBlock
		if err := t.call(target.Addr, "ChordNode.FetchBlock", &ArgsFetchBlock{Key: key.Bytes()}, &reply); err != nil {
			return nil, err
		}
		if !reply.Found {
			return nil, fmt.Errorf("block not found: %s", key)
		}
		return reply.Data, nil
	}
	start := time.Now()
	var reply ReplyFetchBlock
	err := t.call(target.Addr, "ChordNode.FetchBlock", &ArgsFetchBlock{Key: key.Bytes()}, &reply)
	dur := time.Since(start).Seconds()
	if err != nil {
		t.met.FetchErrors.WithLabelValues(t.ring).Inc()
		return nil, err
	}
	if !reply.Found {
		t.met.FetchErrors.WithLabelValues(t.ring).Inc()
		return nil, fmt.Errorf("block not found: %s", key)
	}
	t.met.BlocksFetched.WithLabelValues(t.ring).Inc()
	t.met.FetchBytes.WithLabelValues(t.ring).Add(float64(len(reply.Data)))
	t.met.FetchDuration.WithLabelValues(t.ring).Observe(dur)
	if dur > 0 && len(reply.Data) > 0 {
		t.met.BlockFetchSpeedBytesPerSec.WithLabelValues(t.ring).Observe(float64(len(reply.Data)) / dur)
	}
	return reply.Data, nil
}

func (t *RPCTransport) HasBlock(_ context.Context, target NodeAddr, key cid.Cid) (bool, error) {
	var reply ReplyHasBlock
	if err := t.call(target.Addr, "ChordNode.HasBlock", &ArgsHasBlock{Key: key.Bytes()}, &reply); err != nil {
		return false, err
	}
	return reply.Has, nil
}

func (t *RPCTransport) RemoveBlock(_ context.Context, target NodeAddr, key cid.Cid) error {
	return t.call(target.Addr, "ChordNode.RemoveBlock", &ArgsRemoveBlock{Key: key.Bytes()}, &ReplyRemoveBlock{})
}

func (t *RPCTransport) TransferKeys(_ context.Context, target NodeAddr, from, to NodeID) ([]cid.Cid, [][]byte, [][]store.NetworkRootEntry, error) {
	var reply ReplyTransferKeys
	if err := t.call(target.Addr, "ChordNode.TransferKeys", &ArgsTransferKeys{From: from, To: to}, &reply); err != nil {
		return nil, nil, nil, err
	}
	keys := make([]cid.Cid, len(reply.Keys))
	for i, rawKey := range reply.Keys {
		c, err := cid.Cast(rawKey)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("invalid CID in transfer reply: %w", err)
		}
		keys[i] = c
	}
	blockRoots := make([][]store.NetworkRootEntry, len(keys))
	for i := range keys {
		if i < len(reply.BlockRootCIDs) {
			roots := make([]store.NetworkRootEntry, len(reply.BlockRootCIDs[i]))
			for j, cidStr := range reply.BlockRootCIDs[i] {
				var exp int64
				if i < len(reply.BlockRootExpiry) && j < len(reply.BlockRootExpiry[i]) {
					exp = reply.BlockRootExpiry[i][j]
				}
				roots[j] = store.NetworkRootEntry{CID: cidStr, ExpiresAt: exp}
			}
			blockRoots[i] = roots
		}
	}
	return keys, reply.Data, blockRoots, nil
}

func (t *RPCTransport) DeleteCID(_ context.Context, target NodeAddr, key cid.Cid, propagate bool) error {
	return t.call(target.Addr, "ChordNode.DeleteCID", &ArgsDeleteCID{CID: key.Bytes(), Propagate: propagate}, &ReplyDeleteCID{})
}

func (t *RPCTransport) NotifyLeave(_ context.Context, target NodeAddr, self NodeAddr, successor NodeAddr, predecessor NodeAddr) error {
	return t.call(target.Addr, "ChordNode.NotifyLeave", &ArgsNotifyLeave{Self: self, Successor: successor, Predecessor: predecessor}, &ReplyNotifyLeave{})
}

func (t *RPCTransport) Ping(_ context.Context, target NodeAddr) error {
	return t.call(target.Addr, "ChordNode.Ping", &ArgsPing{}, &ReplyPing{})
}

var _ RecordTransport = (*RPCTransport)(nil)

func (t *RPCTransport) PutRecord(_ context.Context, target NodeAddr, key NodeID, data []byte, callerID NodeID) error {
	return t.call(target.Addr, "ChordNode.PutRecord", &ArgsPutRecord{Key: key, Data: data, CallerID: callerID}, &ReplyPutRecord{})
}

func (t *RPCTransport) GetRecord(_ context.Context, target NodeAddr, key NodeID) ([]byte, error) {
	var reply ReplyGetRecord
	if err := t.call(target.Addr, "ChordNode.GetRecord", &ArgsGetRecord{Key: key}, &reply); err != nil {
		return nil, err
	}
	if !reply.Found {
		return nil, fmt.Errorf("record not found at %s", key)
	}
	return reply.Data, nil
}

func (t *RPCTransport) PushRecords(_ context.Context, target NodeAddr, keys []NodeID, data [][]byte) error {
	return t.call(target.Addr, "ChordNode.PushRecords", &ArgsPushRecords{Keys: keys, Data: data}, &ReplyPushRecords{})
}

func (t *RPCTransport) TransferRecords(_ context.Context, target NodeAddr, from, to NodeID) ([]NodeID, [][]byte, error) {
	var reply ReplyTransferRecords
	if err := t.call(target.Addr, "ChordNode.TransferRecords", &ArgsTransferRecords{From: from, To: to}, &reply); err != nil {
		return nil, nil, err
	}
	return reply.Keys, reply.Data, nil
}
