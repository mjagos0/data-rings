package dht

import (
	"context"
	"net"
	"sort"
	"testing"
	"time"
)

func newTCPListener() (net.Listener, error) {
	return net.Listen("tcp", "127.0.0.1:0")
}

func dialTCP(addr string) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, 3*time.Second)
}

func makePublicDring(t *testing.T, ring *TestRing, pos byte) (*PublicDring, *Identity) {
	t.Helper()
	ident, err := GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}

	id := ident.ID
	bs := newTestMemStore()
	lt := &localTransport{ring: ring, nodeID: id}
	cfg := Config{Replication: 1}
	node := NewNode(id, testAddr(pos), bs, nil, lt, cfg)

	ring.mu.Lock()
	ring.Nodes = append(ring.Nodes, node)
	ring.nodeMap[id] = node
	existing := ring.Nodes[:len(ring.Nodes)-1]
	ring.mu.Unlock()

	if len(existing) == 0 {
		node.Create()
	} else {
		bootstrap := NodeAddr{ID: existing[0].id, Addr: existing[0].addr}
		_ = node.Join(context.Background(), bootstrap)
	}

	return NewPublicDring(node, ident), ident
}

func testAddr(pos byte) string	{ return testNodeID(pos).String() }

func TestPublicDring_PeerIdentityRecord_PublishAndLookup(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()

	pd, ident := makePublicDring(t, ring, 0)
	ring.StabilizeRounds(10)

	if err := pd.PublishSelf(ctx); err != nil {
		t.Fatalf("PublishSelf: %v", err)
	}

	rec, err := pd.LookupPeer(ctx, ident.ID)
	if err != nil {
		t.Fatalf("LookupPeer: %v", err)
	}
	if rec.Data.PeerID != ident.ID {
		t.Errorf("PeerID mismatch")
	}
}

func TestPublicDring_GroupIdentityRecord_PublishAndLookup(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()
	pd, ident := makePublicDring(t, ring, 0)
	ring.StabilizeRounds(10)

	grp, err := GenerateGroupIdentity()
	if err != nil {
		t.Fatalf("GenerateGroupIdentity: %v", err)
	}

	peers := []GroupMember{{ID: ident.ID}}
	if err := pd.PublishGroup(ctx, grp, 1, peers); err != nil {
		t.Fatalf("PublishGroup: %v", err)
	}

	rec, err := pd.LookupGroup(ctx, grp.GroupID)
	if err != nil {
		t.Fatalf("LookupGroup: %v", err)
	}
	if rec.Data.GroupID != grp.GroupID {
		t.Errorf("GroupID mismatch")
	}
	if len(rec.Data.Peers) != 1 {
		t.Errorf("expected 1 peer, got %d", len(rec.Data.Peers))
	}
	if rec.Data.Peers[0].ID != ident.ID {
		t.Errorf("peer ID mismatch")
	}
}

func TestPublicDring_GroupIdentityRecord_InvalidSig_RejectedAtLookup(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()
	pd, ident := makePublicDring(t, ring, 0)
	ring.StabilizeRounds(10)

	grp, _ := GenerateGroupIdentity()
	peers := []GroupMember{{ID: ident.ID}}

	rec, err := NewGroupIdentityRecord(grp, 1, peers)
	if err != nil {
		t.Fatalf("NewGroupIdentityRecord: %v", err)
	}
	rec.Signature[0] ^= 0xFF

	data, _ := rec.Encode()

	pd.node.localRecordPut(grp.GroupID, data)

	if _, err := pd.LookupGroup(ctx, grp.GroupID); err == nil {
		t.Fatal("expected LookupGroup to reject tampered record with invalid signature")
	}
}

func TestPublicDring_ProviderRecord_PublishAndFind(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()
	pd, _ := makePublicDring(t, ring, 0)
	ring.StabilizeRounds(10)

	_, data := testBlock("provider-test-content")
	c := testCID(data)

	if err := pd.AnnounceProvider(ctx, c); err != nil {
		t.Fatalf("PublishProvider: %v", err)
	}

	providers, err := pd.FindProviders(ctx, c)
	if err != nil {
		t.Fatalf("FindProviders: %v", err)
	}
	if len(providers) != 1 {
		t.Fatalf("expected 1 provider, got %d", len(providers))
	}
}

func TestPublicDring_ProviderRecord_IdempotentRePublish(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()
	pd, _ := makePublicDring(t, ring, 0)
	ring.StabilizeRounds(10)

	_, data := testBlock("idempotent-content")
	c := testCID(data)

	for i := 0; i < 5; i++ {
		if err := pd.AnnounceProvider(ctx, c); err != nil {
			t.Fatalf("PublishProvider (iter %d): %v", i, err)
		}
	}

	providers, _ := pd.FindProviders(ctx, c)
	if len(providers) != 1 {
		t.Errorf("expected exactly 1 provider after 5 re-publishes, got %d", len(providers))
	}
}

func TestPublicDring_ProviderRecord_MultipleProviders(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()

	pd1, _ := makePublicDring(t, ring, 0)
	pd2, _ := makePublicDring(t, ring, 100)
	ring.StabilizeRounds(30)

	_, data := testBlock("shared-content")
	c := testCID(data)

	if err := pd1.AnnounceProvider(ctx, c); err != nil {
		t.Fatalf("pd1.PublishProvider: %v", err)
	}
	if err := pd2.AnnounceProvider(ctx, c); err != nil {
		t.Fatalf("pd2.PublishProvider: %v", err)
	}

	providers, err := pd1.FindProviders(ctx, c)
	if err != nil {
		t.Fatalf("FindProviders: %v", err)
	}
	if len(providers) != 2 {
		t.Errorf("expected 2 providers, got %d", len(providers))
	}
}

func TestPublicDring_RecordRouting(t *testing.T) {

	ring := NewTestRing()
	ctx := context.Background()

	pds := make([]*PublicDring, 0, 4)
	for _, pos := range []byte{0, 64, 128, 192} {
		pd, _ := makePublicDring(t, ring, pos)
		pds = append(pds, pd)
	}
	ring.StabilizeRounds(30)

	for _, pd := range pds {
		if err := pd.PublishSelf(ctx); err != nil {
			t.Fatalf("PublishSelf: %v", err)
		}
	}

	for _, reader := range pds {
		for _, publisher := range pds {
			_, err := reader.LookupPeer(ctx, publisher.identity.ID)
			if err != nil {
				t.Errorf("node lookup failed: publisher=%s reader=%s err=%v",
					publisher.identity.ID, reader.identity.ID, err)
			}
		}
	}
}

func TestPublicDring_RPC_PeerAndGroupRecords(t *testing.T) {
	ring := newRPCTestRing(t)
	defer ring.cleanup()

	b := ring.addBootstrap(0)
	bIdent, _ := GenerateIdentity()
	bPublic := NewPublicDring(b.node, bIdent)
	b.node.setAddr(b.multiaddr)

	n2 := ring.addJoiner(128)
	n2Ident, _ := GenerateIdentity()
	n2Public := NewPublicDring(n2.node, n2Ident)

	ring.stabilizeRounds(40)

	ctx := context.Background()

	if err := bPublic.PublishSelf(ctx); err != nil {
		t.Fatalf("b.PublishSelf: %v", err)
	}
	if err := n2Public.PublishSelf(ctx); err != nil {
		t.Fatalf("n2.PublishSelf: %v", err)
	}

	if _, err := n2Public.LookupPeer(ctx, bIdent.ID); err != nil {
		t.Errorf("n2 lookup b: %v", err)
	}
	if _, err := bPublic.LookupPeer(ctx, n2Ident.ID); err != nil {
		t.Errorf("b lookup n2: %v", err)
	}

	grp, _ := GenerateGroupIdentity()
	if err := bPublic.PublishGroup(ctx, grp, 1, []GroupMember{{ID: bIdent.ID}}); err != nil {
		t.Fatalf("PublishGroup: %v", err)
	}
	rec, err := n2Public.LookupGroup(ctx, grp.GroupID)
	if err != nil {
		t.Fatalf("n2 LookupGroup: %v", err)
	}
	if rec.Data.GroupID != grp.GroupID {
		t.Errorf("GroupID mismatch")
	}
}

func TestPublicDring_RPC_ProviderRecords(t *testing.T) {
	ring := newRPCTestRing(t)
	defer ring.cleanup()

	bIdent, _ := GenerateIdentity()
	b := ring.addNodeWithIdent(bIdent)
	bPublic := NewPublicDring(b.node, bIdent)

	n2Ident, _ := GenerateIdentity()
	n2 := ring.addNodeWithIdent(n2Ident)
	n2Public := NewPublicDring(n2.node, n2Ident)

	ring.stabilizeRounds(40)

	ctx := context.Background()
	_, data := testBlock("provider-rpc-test")
	c := testCID(data)

	if err := bPublic.AnnounceProvider(ctx, c); err != nil {
		t.Fatalf("b.PublishProvider: %v", err)
	}
	if err := n2Public.AnnounceProvider(ctx, c); err != nil {
		t.Fatalf("n2.PublishProvider: %v", err)
	}

	providers, err := bPublic.FindProviders(ctx, c)
	if err != nil {
		t.Fatalf("FindProviders: %v", err)
	}

	providerIDs := make([]string, len(providers))
	for i, p := range providers {
		providerIDs[i] = p.Provider.String()
	}
	sort.Strings(providerIDs)

	wantIDs := []string{bIdent.ID.String(), n2Ident.ID.String()}
	sort.Strings(wantIDs)

	if len(providerIDs) != 2 {
		t.Errorf("expected 2 providers, got %d: %v", len(providerIDs), providerIDs)
	}
}

func TestPublicDring_LookupPeer_NotFound(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()
	pd, _ := makePublicDring(t, ring, 0)
	ring.StabilizeRounds(10)

	unknown, _ := GenerateIdentity()
	_, err := pd.LookupPeer(ctx, unknown.ID)
	if err == nil {
		t.Fatal("expected error when looking up unpublished peer")
	}
}

func TestPublicDring_RecordTransferOnJoin(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()

	pd0, ident0 := makePublicDring(t, ring, 0)
	ring.StabilizeRounds(10)

	if err := pd0.PublishSelf(ctx); err != nil {
		t.Fatalf("PublishSelf: %v", err)
	}

	pd128, _ := makePublicDring(t, ring, 128)
	ring.StabilizeRounds(20)

	if _, err := pd128.LookupPeer(ctx, ident0.ID); err != nil {
		t.Errorf("LookupPeer after join: %v", err)
	}
}

func TestProviderRecord_SenderMustMatchProvider(t *testing.T) {
	ring := NewTestRing()
	pd1, ident1 := makePublicDring(t, ring, 0)
	pd2, ident2 := makePublicDring(t, ring, 128)
	ring.StabilizeRounds(10)
	_ = pd2

	_, raw := testBlock("sender-validation-content")
	c := testCID(raw)
	key := CIDToNodeID(c)

	pr1 := ProviderRecord{ContentHash: key, Provider: ident1.ID}
	prData1, _ := pr1.Encode()
	if err := pd1.node.storeRecord(key, prData1, ident1.ID); err != nil {
		t.Fatalf("valid ProviderRecord (own provider) rejected: %v", err)
	}

	pr2 := ProviderRecord{ContentHash: key, Provider: ident2.ID}
	prData2, _ := pr2.Encode()
	if err := pd1.node.storeRecord(key, prData2, ident1.ID); err == nil {
		t.Fatal("expected rejection: caller cannot advertise a different peer as provider")
	}

	if err := pd1.node.storeRecord(key, prData2, ident2.ID); err != nil {
		t.Fatalf("valid ProviderRecord (matching provider) rejected: %v", err)
	}

	if err := pd1.node.storeRecord(key, prData2, NodeID{}); err == nil {
		t.Fatal("expected rejection: zero callerID should not bypass provider validation")
	}
}

func TestProviderRecord_SenderMustMatchProvider_RPC(t *testing.T) {
	ring := newRPCTestRing(t)
	defer ring.cleanup()

	bIdent, _ := GenerateIdentity()
	b := ring.addNodeWithIdent(bIdent)
	bPublic := NewPublicDring(b.node, bIdent)

	n2Ident, _ := GenerateIdentity()
	n2 := ring.addNodeWithIdent(n2Ident)
	n2Public := NewPublicDring(n2.node, n2Ident)

	ring.stabilizeRounds(40)

	ctx := context.Background()
	_, data := testBlock("rpc-sender-validation")
	c := testCID(data)

	if err := bPublic.AnnounceProvider(ctx, c); err != nil {
		t.Fatalf("b.AnnounceProvider: %v", err)
	}
	if err := n2Public.AnnounceProvider(ctx, c); err != nil {
		t.Fatalf("n2.AnnounceProvider: %v", err)
	}

	providers, err := bPublic.FindProviders(ctx, c)
	if err != nil {
		t.Fatalf("FindProviders: %v", err)
	}
	if len(providers) != 2 {
		t.Errorf("expected 2 providers, got %d", len(providers))
	}
	for _, pr := range providers {
		if pr.Provider != bIdent.ID && pr.Provider != n2Ident.ID {
			t.Errorf("unexpected provider ID: %s", pr.Provider)
		}
	}
}

func TestAuth_Timeout(t *testing.T) {

	l, err := newTCPListener()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()

	go func() {
		conn, _ := l.Accept()
		if conn != nil {
			time.Sleep(30 * time.Second)
			conn.Close()
		}
	}()

	selfID := testNodeID(1)
	peerID := testNodeID(2)
	psk := []byte("test-psk-bytes-32-bytes-exactly!")

	conn, err := dialTCP(l.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	err = PerformClientAuth(conn, psk, selfID, peerID)
	if err == nil {
		t.Fatal("expected auth to fail/timeout when server is silent")
	}
}

func TestPublicDring_PeerIdentityRecord_InvalidSig_RejectedAtLookup(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()
	pd, ident := makePublicDring(t, ring, 0)
	ring.StabilizeRounds(10)

	rec, err := NewPeerIdentityRecord(ident, 1, testAddr(0), nil)
	if err != nil {
		t.Fatalf("NewPeerIdentityRecord: %v", err)
	}
	rec.Signature[0] ^= 0xFF

	data, _ := rec.Encode()

	pd.node.localRecordPut(ident.ID, data)

	if _, err := pd.LookupPeer(ctx, ident.ID); err == nil {
		t.Fatal("expected LookupPeer to reject tampered PeerIdentityRecord with invalid signature")
	}
}

func TestPublicDring_RecordReplication_PrimaryPlusKMinusOne(t *testing.T) {
	const k = 3
	cfg := Config{Replication: k}
	ring := NewTestRing()

	var publisher *PublicDring
	var publisherIdent *Identity
	for i := 0; i < 5; i++ {
		pd, ident := makePublicDringWithCfg(t, ring, cfg, nil)
		if i == 0 {
			publisher = pd
			publisherIdent = ident
		}
	}
	ring.StabilizeRounds(60)

	ctx := context.Background()
	if err := publisher.PublishSelf(ctx); err != nil {
		t.Fatalf("PublishSelf: %v", err)
	}

	key := publisherIdent.ID
	count := 0
	for _, n := range ring.Nodes {
		n.recordsMu.RLock()
		_, has := n.records[key]
		n.recordsMu.RUnlock()
		if has {
			count++
		}
	}

	if count < k {
		t.Errorf("PeerIdentityRecord replicated to %d nodes, want at least k=%d total copies (primary + k-1 replicas)",
			count, k)
	}
	t.Logf("PeerIdentityRecord present on %d/%d nodes (k=%d)", count, len(ring.Nodes), k)
}
