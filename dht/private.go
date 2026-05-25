package dht

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"

	"github.com/mjagos0/datarings/metrics"
	"github.com/mjagos0/datarings/store"
)

const maxGroupRecordRetries = 5

type PrivateDring struct {
	node		*Node
	grp		*GroupIdentity
	peerID		NodeID
	srv		*RPCServer
	multiaddr	string
	tr		*privateRPCTransport

	verifiedMu	*sync.RWMutex
	verified	map[NodeID]bool

	groupRecordCachePath	string

	publicDring	*PublicDring
}

func NewPrivateDring(grp *GroupIdentity, peerIdent *Identity, blocks store.BlockStore, dag ipld.DAGService, cfg Config) *PrivateDring {
	verified := make(map[NodeID]bool)
	verifiedMu := &sync.RWMutex{}

	tr := &privateRPCTransport{
		psk:		grp.PSK,
		selfID:		peerIdent.ID,
		timeout:	5 * time.Second,
		pool:		make(map[NodeID]*rpc.Client),
		poolAddrs:	make(map[NodeID]string),
		verified:	verified,
		verifiedMu:	verifiedMu,
	}
	node := NewNode(peerIdent.ID, "", blocks, dag, tr, cfg)

	node.SetRingID(grp.GroupID.String())
	return &PrivateDring{
		node:		node,
		grp:		grp,
		peerID:		peerIdent.ID,
		tr:		tr,
		verified:	verified,
		verifiedMu:	verifiedMu,
	}
}

func (p *PrivateDring) SetMetrics(m *metrics.Registry) {
	p.tr.SetMetrics(m, p.grp.GroupID.String())
}

func (p *PrivateDring) Node() *Node	{ return p.node }

func (p *PrivateDring) GroupID() NodeID	{ return p.grp.GroupID }

func (p *PrivateDring) GroupIdentity() *GroupIdentity	{ return p.grp }

func (p *PrivateDring) Multiaddr() string	{ return p.multiaddr }

func (p *PrivateDring) StartServer(listenAddr, advertiseAddr string) (string, error) {
	srv, boundAddr, err := startPrivateRPCServer(listenAddr, advertiseAddr, p.node, p.grp.PSK, p.peerID, p.verified, p.verifiedMu)
	if err != nil {
		return "", err
	}
	p.srv = srv
	p.multiaddr = boundAddr
	p.node.setAddr(boundAddr)
	return boundAddr, nil
}

func (p *PrivateDring) Create()	{ p.node.Create() }

func (p *PrivateDring) Join(ctx context.Context, peer NodeAddr) error {
	return p.node.Join(ctx, peer)
}

func (p *PrivateDring) JoinViaPublicDring(ctx context.Context, publicDring *PublicDring) error {
	rec, err := publicDring.LookupGroup(ctx, p.grp.GroupID)
	if err != nil {
		return fmt.Errorf("lookup group on public dring: %w", err)
	}
	if len(rec.Data.Peers) == 0 {
		return fmt.Errorf("group %s has no peers in public record", p.grp.GroupID)
	}

	groupIDHex := p.grp.GroupID.String()
	slog.Debug("private dring: attempting join via group members", "group_id", p.grp.GroupID, "member_count", len(rec.Data.Peers))

	var lastErr error
	var knownPeers []NodeAddr
	var hadStaleRef bool
	for _, member := range rec.Data.Peers {
		if member.ID == p.peerID {
			continue
		}
		peerRec, err := publicDring.LookupPeer(ctx, member.ID)
		if err != nil {
			slog.Debug("private dring: could not fetch peer record", "group_id", p.grp.GroupID, "peer", member.ID, "error", err)
			lastErr = fmt.Errorf("lookup peer %s: %w", member.ID, err)
			continue
		}
		addr, ok := peerRec.Data.GroupAddrs[groupIDHex]
		if !ok || addr == "" {
			slog.Debug("private dring: peer has no address for group", "group_id", p.grp.GroupID, "peer", member.ID)
			lastErr = fmt.Errorf("peer %s has no private-dring address for group %s", member.ID, p.grp.GroupID)
			continue
		}
		bootstrap := NodeAddr{ID: member.ID, Addr: addr}
		knownPeers = append(knownPeers, bootstrap)
		slog.Debug("private dring: trying group member", "group_id", p.grp.GroupID, "peer", member.ID, "addr", addr)
		if err := p.Join(ctx, bootstrap); err != nil {
			slog.Debug("private dring: member unreachable", "group_id", p.grp.GroupID, "peer", member.ID, "error", err)
			lastErr = err
			continue
		}

		if p.node.getSuccessor().ID == p.peerID {
			slog.Debug("private dring: join returned stale self-reference, trying next peer",
				"group_id", p.grp.GroupID, "peer", member.ID)
			lastErr = fmt.Errorf("join via %s returned stale self-reference", member.ID)
			hadStaleRef = true
			continue
		}
		slog.Info("private dring: joined via member", "group_id", p.grp.GroupID, "peer", member.ID)
		return nil
	}

	if hadStaleRef {
		slog.Info("private dring: could not join any peer (stale self-ref), creating seeded ring",
			"group_id", p.grp.GroupID, "known_peers", len(knownPeers))
	} else if lastErr != nil {
		slog.Info("private dring: all peers unreachable, creating seeded ring for auto-reconnect",
			"group_id", p.grp.GroupID, "known_peers", len(knownPeers))
	} else {
		slog.Info("private dring: no other members, creating new ring", "group_id", p.grp.GroupID)
	}
	p.Create()
	if len(knownPeers) > 0 {
		p.node.mu.Lock()
		p.node.successorList = append(p.node.successorList, knownPeers...)
		p.node.mu.Unlock()
	}
	return nil
}

func (p *PrivateDring) UpdateGroupRecord(ctx context.Context, publicDring *PublicDring) error {
	for attempt := 0; attempt < maxGroupRecordRetries; attempt++ {
		if attempt > 0 {

			base := time.Duration(attempt) * 50 * time.Millisecond
			jitter := time.Duration(rand.Int63n(int64(base)))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(base + jitter):
			}
		}

		var currentPeers []GroupMember
		var currentVersion uint64
		if rec, err := publicDring.LookupGroup(ctx, p.grp.GroupID); err == nil {
			currentPeers = rec.Data.Peers
			currentVersion = rec.Data.Version
		} else if cached, cerr := p.loadCachedGroupRecord(); cerr == nil {

			slog.Info("private dring: group record missing from ring, recovering from disk cache",
				"group_id", p.grp.GroupID, "cached_version", cached.Data.Version)
			currentPeers = cached.Data.Peers
			currentVersion = cached.Data.Version
		}

		var out []GroupMember
		for _, m := range currentPeers {
			if m.ID != p.peerID {
				out = append(out, m)
			}
		}
		out = append(out, GroupMember{ID: p.peerID})

		newVersion := currentVersion + 1
		err := publicDring.PublishGroup(ctx, p.grp, newVersion, out)
		if err == nil {
			slog.Info("private dring: group record updated", "group_id", p.grp.GroupID, "version", newVersion, "members", len(out))

			if cacheRec, signErr := NewGroupIdentityRecord(p.grp, newVersion, out); signErr == nil {
				if cacheErr := p.cacheGroupRecord(cacheRec); cacheErr != nil {
					slog.Warn("private dring: failed to cache group record", "group_id", p.grp.GroupID, "error", cacheErr)
				}
			}
			return nil
		}
		if !isVersionConflict(err) {
			return err
		}

		slog.Info("private dring: group record version conflict, retrying", "group_id", p.grp.GroupID, "attempt", attempt+1)
	}
	return fmt.Errorf("update group record: too many concurrent updates, try again")
}

func (p *PrivateDring) removeFromGroupRecord(ctx context.Context, publicDring *PublicDring) error {
	for attempt := 0; attempt < maxGroupRecordRetries; attempt++ {
		if attempt > 0 {
			base := time.Duration(attempt) * 50 * time.Millisecond
			jitter := time.Duration(rand.Int63n(int64(base)))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(base + jitter):
			}
		}

		rec, err := publicDring.LookupGroup(ctx, p.grp.GroupID)
		if err != nil {
			return fmt.Errorf("lookup group record: %w", err)
		}

		var out []GroupMember
		for _, m := range rec.Data.Peers {
			if m.ID != p.peerID {
				out = append(out, m)
			}
		}

		err = publicDring.PublishGroup(ctx, p.grp, rec.Data.Version+1, out)
		if err == nil {
			return nil
		}
		if !isVersionConflict(err) {
			return err
		}
	}
	return fmt.Errorf("remove from group record: too many concurrent updates, try again")
}

func (p *PrivateDring) IsVerified(peerID NodeID) bool {
	p.verifiedMu.RLock()
	defer p.verifiedMu.RUnlock()
	return p.verified[peerID]
}

func (p *PrivateDring) VerifiedPeers() []NodeID {
	p.verifiedMu.RLock()
	defer p.verifiedMu.RUnlock()
	out := make([]NodeID, 0, len(p.verified))
	for id := range p.verified {
		out = append(out, id)
	}
	return out
}

func (p *PrivateDring) ShareDAG(ctx context.Context, root cid.Cid) error {
	return p.node.ShareDAG(ctx, root)
}

func (p *PrivateDring) ShareDAGWithTTL(ctx context.Context, root cid.Cid, ttl time.Duration) error {
	return p.node.ShareDAGWithTTL(ctx, root, ttl)
}

func (p *PrivateDring) FetchDAG(ctx context.Context, root cid.Cid) error {
	return p.node.FetchDAG(ctx, root)
}

func (p *PrivateDring) DeleteCID(ctx context.Context, rootCID cid.Cid) error {
	return p.node.DeleteCID(ctx, rootCID)
}

func (p *PrivateDring) PruneOutOfWindowBlocks(ctx context.Context) (int, error) {
	return p.node.PruneOutOfWindowBlocks(ctx)
}

func (p *PrivateDring) StartBackground(cfg Config) {
	p.node.StartBackground(cfg)
}

func (p *PrivateDring) Leave(ctx context.Context) error {
	err := p.node.Leave(ctx)
	if p.srv != nil {
		p.srv.Stop()
	}
	return err
}

func (p *PrivateDring) LeaveGroup(ctx context.Context, publicDring *PublicDring) error {

	_ = p.removeFromGroupRecord(ctx, publicDring)

	publicDring.UnregisterGroupAddrProvider(p.grp.GroupID)

	err := p.Leave(ctx)

	p.tr.Close()

	return err
}

func (p *PrivateDring) TransportPoolState() map[string]string {
	return p.tr.PoolState()
}

func (p *PrivateDring) Stop() {
	p.node.Stop()
	if p.srv != nil {
		p.srv.Stop()
	}
}

func (p *PrivateDring) SetGroupRecordCachePath(path string) {
	p.groupRecordCachePath = path
}

func (p *PrivateDring) SetPublicDring(pub *PublicDring) {
	p.publicDring = pub
	pub.RegisterGroupRepublisher(func(ctx context.Context) error {
		return p.UpdateGroupRecord(ctx, pub)
	})
	pub.RegisterGroupAddrProvider(p.grp.GroupID, func() string {
		return p.multiaddr
	})

	groupIDHex := p.grp.GroupID.String()
	p.node.SetAddrRefresher(func(peerID NodeID) string {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rec, err := pub.LookupPeer(ctx, peerID)
		if err != nil {
			return ""
		}
		return rec.Data.GroupAddrs[groupIDHex]
	})
}

func (p *PrivateDring) cacheGroupRecord(rec *GroupIdentityRecord) error {
	if p.groupRecordCachePath == "" {
		return nil
	}
	data, err := rec.Encode()
	if err != nil {
		return fmt.Errorf("encode group record for cache: %w", err)
	}
	if err := os.WriteFile(p.groupRecordCachePath, data, 0600); err != nil {
		return fmt.Errorf("write group record cache %s: %w", p.groupRecordCachePath, err)
	}
	slog.Debug("private dring: group record cached to disk", "group_id", p.grp.GroupID, "path", p.groupRecordCachePath)
	return nil
}

func (p *PrivateDring) loadCachedGroupRecord() (*GroupIdentityRecord, error) {
	if p.groupRecordCachePath == "" {
		return nil, fmt.Errorf("no cache path configured")
	}
	data, err := os.ReadFile(p.groupRecordCachePath)
	if err != nil {
		return nil, fmt.Errorf("read group record cache: %w", err)
	}
	rec, err := DecodeGroupIdentityRecord(data)
	if err != nil {
		return nil, fmt.Errorf("decode cached group record: %w", err)
	}
	return rec, nil
}

func startPrivateRPCServer(
	addr, advertiseAddr string,
	node *Node,
	psk []byte,
	selfID NodeID,
	verified map[NodeID]bool,
	mu *sync.RWMutex,
) (*RPCServer, string, error) {
	tcpAddr, err := MultiaddrToTCPAddr(addr)
	if err != nil {
		return nil, "", fmt.Errorf("parse addr %q: %w", addr, err)
	}
	network := "tcp"
	if strings.Contains(addr, "/ip4/") {
		network = "tcp4"
	}
	l, err := listenReuse(network, tcpAddr)
	if err != nil {
		return nil, "", fmt.Errorf("listen %s: %w", tcpAddr, err)
	}

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				clientID, err := PerformServerAuth(c, psk, selfID)
				if err != nil {
					slog.Debug("private dring: auth failed", "remote", c.RemoteAddr(), "error", err)
					c.Close()
					return
				}
				slog.Info("private dring: peer authenticated", "peer", clientID, "remote", c.RemoteAddr())
				mu.Lock()
				verified[clientID] = true
				mu.Unlock()

				connSrv := rpc.NewServer()
				if err := connSrv.RegisterName("ChordNode", &rpcHandler{
					node:			node,
					authenticatedPeerID:	&clientID,
				}); err != nil {
					c.Close()
					return
				}
				connSrv.ServeConn(c)
			}(conn)
		}
	}()

	boundMA, err := effectiveMultiaddr(l.Addr().String(), advertiseAddr)
	if err != nil {
		boundMA = l.Addr().String()
	}
	return &RPCServer{listener: l}, boundMA, nil
}

type privateRPCTransport struct {
	psk	[]byte
	selfID	NodeID
	timeout	time.Duration

	mu		sync.Mutex
	pool		map[NodeID]*rpc.Client
	poolAddrs	map[NodeID]string
	verified	map[NodeID]bool
	verifiedMu	*sync.RWMutex

	met	*metrics.Registry
	ring	string
}

func (t *privateRPCTransport) SetMetrics(m *metrics.Registry, ring string) {
	t.met = m
	t.ring = ring
}

var _ Transport = (*privateRPCTransport)(nil)

func (t *privateRPCTransport) getClient(target NodeAddr) (*rpc.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if c, ok := t.pool[target.ID]; ok {
		return c, nil
	}

	tcpAddr, err := MultiaddrToTCPAddr(target.Addr)
	if err != nil {
		return nil, fmt.Errorf("parse peer addr %q: %w", target.Addr, err)
	}
	conn, err := net.DialTimeout("tcp", tcpAddr, t.timeout)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", target.Addr, err)
	}

	if err := PerformClientAuth(conn, t.psk, t.selfID, target.ID); err != nil {
		slog.Debug("private dring: outbound auth failed", "peer", target.ID, "addr", target.Addr, "error", err)
		conn.Close()
		return nil, fmt.Errorf("auth with %s: %w", target.Addr, err)
	}
	slog.Debug("private dring: outbound auth succeeded", "peer", target.ID, "addr", target.Addr)

	t.verifiedMu.Lock()
	t.verified[target.ID] = true
	t.verifiedMu.Unlock()

	c := rpc.NewClient(conn)
	t.pool[target.ID] = c
	t.poolAddrs[target.ID] = target.Addr
	return c, nil
}

func (t *privateRPCTransport) call(target NodeAddr, method string, args, reply interface{}) error {
	c, err := t.getClient(target)
	if err != nil {
		return err
	}
	err = c.Call(method, args, reply)
	if err != nil {
		t.mu.Lock()
		delete(t.pool, target.ID)
		delete(t.poolAddrs, target.ID)
		t.mu.Unlock()

		if err == rpc.ErrShutdown {
			c2, err2 := t.getClient(target)
			if err2 != nil {
				return err
			}
			err = c2.Call(method, args, reply)
			if err != nil {
				t.mu.Lock()
				delete(t.pool, target.ID)
				delete(t.poolAddrs, target.ID)
				t.mu.Unlock()
			}
			return err
		}
	}
	return err
}

func (t *privateRPCTransport) PoolState() map[string]string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]string, len(t.pool))
	for id := range t.pool {
		addr := t.poolAddrs[id]
		if addr == "" {
			addr = "<unknown>"
		}
		out[id.String()] = addr
	}
	return out
}

func (t *privateRPCTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, c := range t.pool {
		c.Close()
		delete(t.pool, id)
		delete(t.poolAddrs, id)
	}
}

func (t *privateRPCTransport) FindSuccessor(ctx context.Context, target NodeAddr, id NodeID) (NodeAddr, error) {
	var reply ReplyFindSuccessor
	if err := t.call(target, "ChordNode.FindSuccessor", &ArgsFindSuccessor{ID: id}, &reply); err != nil {
		return NodeAddr{}, err
	}
	return reply.Node, nil
}

func (t *privateRPCTransport) ClosestPrecedingNode(_ context.Context, target NodeAddr, id NodeID) (NodeAddr, NodeAddr, error) {
	var reply ReplyClosestPrecedingNode
	if err := t.call(target, "ChordNode.ClosestPrecedingNode", &ArgsClosestPrecedingNode{ID: id}, &reply); err != nil {
		return NodeAddr{}, NodeAddr{}, err
	}
	return reply.Predecessor, reply.Successor, nil
}

func (t *privateRPCTransport) GetPredecessor(_ context.Context, target NodeAddr) (*NodeAddr, error) {
	var reply ReplyGetPredecessor
	if err := t.call(target, "ChordNode.GetPredecessor", &ArgsGetPredecessor{}, &reply); err != nil {
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

func (t *privateRPCTransport) GetSuccessorList(_ context.Context, target NodeAddr) ([]NodeAddr, error) {
	var reply ReplyGetSuccessorList
	if err := t.call(target, "ChordNode.GetSuccessorList", &ArgsGetSuccessorList{}, &reply); err != nil {
		return nil, err
	}
	return reply.List, nil
}

func (t *privateRPCTransport) Notify(_ context.Context, target NodeAddr, caller NodeAddr) error {
	if t.met != nil {
		t.met.DHTRPC.WithLabelValues("notify").Inc()
	}
	return t.call(target, "ChordNode.Notify", &ArgsNotify{Caller: caller}, &ReplyNotify{})
}

func (t *privateRPCTransport) PutBlock(_ context.Context, target NodeAddr, key cid.Cid, data []byte, rootCID cid.Cid, rootExpiry int64) error {
	args := &ArgsPutBlock{Key: key.Bytes(), Data: data, RootTTLNanos: rootExpiry}
	if rootCID.Defined() {
		args.RootCID = rootCID.Bytes()
	}
	if t.met == nil {
		return t.call(target, "ChordNode.PutBlock", args, &ReplyPutBlock{})
	}
	start := time.Now()
	err := t.call(target, "ChordNode.PutBlock", args, &ReplyPutBlock{})
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

func (t *privateRPCTransport) PushBlocks(_ context.Context, target NodeAddr, keys []cid.Cid, data [][]byte, blockRoots [][]store.NetworkRootEntry) error {
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
		return t.call(target, "ChordNode.PushBlocks", args, &ReplyPushBlocks{})
	}
	start := time.Now()
	err := t.call(target, "ChordNode.PushBlocks", args, &ReplyPushBlocks{})
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

func (t *privateRPCTransport) ReconcileBlocks(_ context.Context, target NodeAddr, keys []cid.Cid) ([]cid.Cid, error) {
	args := &ArgsReconcileBlocks{Keys: make([][]byte, len(keys))}
	for i, k := range keys {
		args.Keys[i] = k.Bytes()
	}
	var reply ReplyReconcileBlocks
	if err := t.call(target, "ChordNode.ReconcileBlocks", args, &reply); err != nil {
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

func (t *privateRPCTransport) FetchBlock(_ context.Context, target NodeAddr, key cid.Cid) ([]byte, error) {
	if t.met == nil {
		var reply ReplyFetchBlock
		if err := t.call(target, "ChordNode.FetchBlock", &ArgsFetchBlock{Key: key.Bytes()}, &reply); err != nil {
			return nil, err
		}
		if !reply.Found {
			return nil, fmt.Errorf("block not found: %s", key)
		}
		return reply.Data, nil
	}
	start := time.Now()
	var reply ReplyFetchBlock
	err := t.call(target, "ChordNode.FetchBlock", &ArgsFetchBlock{Key: key.Bytes()}, &reply)
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

func (t *privateRPCTransport) HasBlock(_ context.Context, target NodeAddr, key cid.Cid) (bool, error) {
	var reply ReplyHasBlock
	if err := t.call(target, "ChordNode.HasBlock", &ArgsHasBlock{Key: key.Bytes()}, &reply); err != nil {
		return false, err
	}
	return reply.Has, nil
}

func (t *privateRPCTransport) RemoveBlock(_ context.Context, target NodeAddr, key cid.Cid) error {
	return t.call(target, "ChordNode.RemoveBlock", &ArgsRemoveBlock{Key: key.Bytes()}, &ReplyRemoveBlock{})
}

func (t *privateRPCTransport) TransferKeys(_ context.Context, target NodeAddr, from, to NodeID) ([]cid.Cid, [][]byte, [][]store.NetworkRootEntry, error) {
	var reply ReplyTransferKeys
	if err := t.call(target, "ChordNode.TransferKeys", &ArgsTransferKeys{From: from, To: to}, &reply); err != nil {
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

func (t *privateRPCTransport) DeleteCID(_ context.Context, target NodeAddr, key cid.Cid, propagate bool) error {
	return t.call(target, "ChordNode.DeleteCID", &ArgsDeleteCID{CID: key.Bytes(), Propagate: propagate}, &ReplyDeleteCID{})
}

func (t *privateRPCTransport) NotifyLeave(_ context.Context, target NodeAddr, self NodeAddr, successor NodeAddr, predecessor NodeAddr) error {
	return t.call(target, "ChordNode.NotifyLeave", &ArgsNotifyLeave{Self: self, Successor: successor, Predecessor: predecessor}, &ReplyNotifyLeave{})
}

func (t *privateRPCTransport) Ping(_ context.Context, target NodeAddr) error {
	return t.call(target, "ChordNode.Ping", &ArgsPing{}, &ReplyPing{})
}
