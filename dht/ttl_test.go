package dht

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu	sync.Mutex
	now	time.Time
}

func newFakeClock() *fakeClock {

	return &fakeClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func purgeRing(ring *TestRing) {
	ring.mu.Lock()
	nodes := append([]*Node(nil), ring.Nodes...)
	ring.mu.Unlock()
	for _, n := range nodes {
		n.purgeExpiredRecords()
	}
}

func shortTTLConfig() Config {
	return Config{
		Replication:			1,
		PeerRecordTTL:			3 * time.Second,
		GroupRecordTTL:			3 * time.Second,
		ProviderRecordTTL:		3 * time.Second,
		RecordPurgePeriod:		500 * time.Millisecond,
		PeerRepublishInterval:		1 * time.Second,
		ProviderRepublishInterval:	1 * time.Second,
	}
}

func makePublicDringWithCfg(t *testing.T, ring *TestRing, cfg Config, clock *fakeClock) (*PublicDring, *Identity) {
	t.Helper()
	ident, err := GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}
	id := ident.ID
	bs := newTestMemStore()
	lt := &localTransport{ring: ring, nodeID: id}
	node := NewNode(id, testAddr(id[0]), bs, nil, lt, cfg)
	if clock != nil {
		node.SetNowFunc(clock.Now)
	}

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

func TestTTL_PeerIdentityRecord_Expires(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()
	cfg := shortTTLConfig()
	clock := newFakeClock()

	pd, ident := makePublicDringWithCfg(t, ring, cfg, clock)
	ring.StabilizeRounds(10)

	if err := pd.PublishSelf(ctx); err != nil {
		t.Fatalf("PublishSelf: %v", err)
	}

	if _, err := pd.LookupPeer(ctx, ident.ID); err != nil {
		t.Fatalf("LookupPeer immediately after publish: %v", err)
	}

	clock.Advance(cfg.PeerRecordTTL + time.Second)
	purgeRing(ring)

	if _, err := pd.LookupPeer(ctx, ident.ID); err == nil {
		t.Fatal("expected LookupPeer to fail after TTL expiry, but it succeeded")
	}
}

func TestTTL_PeerIdentityRecord_Republished(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()
	cfg := shortTTLConfig()
	clock := newFakeClock()

	pd, ident := makePublicDringWithCfg(t, ring, cfg, clock)
	ring.StabilizeRounds(10)

	if err := pd.PublishSelf(ctx); err != nil {
		t.Fatalf("PublishSelf: %v", err)
	}

	clock.Advance(cfg.PeerRecordTTL + time.Second)
	purgeRing(ring)
	if err := pd.PublishSelf(ctx); err != nil {
		t.Fatalf("PublishSelf (republish): %v", err)
	}

	rec, err := pd.LookupPeer(ctx, ident.ID)
	if err != nil {
		t.Fatalf("LookupPeer after republish: %v", err)
	}
	if rec.Data.PeerID != ident.ID {
		t.Errorf("republished record has wrong PeerID")
	}
	if rec.Data.Version <= 1 {
		t.Errorf("expected version > 1 after republish, got %d", rec.Data.Version)
	}
}

func TestTTL_ProviderRecord_Expires(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()
	cfg := shortTTLConfig()
	clock := newFakeClock()

	pd, _ := makePublicDringWithCfg(t, ring, cfg, clock)
	ring.StabilizeRounds(10)

	c, _ := testBlock("ttl-provider-expire")

	if err := pd.AnnounceProvider(ctx, c); err != nil {
		t.Fatalf("AnnounceProvider: %v", err)
	}

	if _, err := pd.FindProviders(ctx, c); err != nil {
		t.Fatalf("FindProviders immediately after announce: %v", err)
	}

	clock.Advance(cfg.ProviderRecordTTL + time.Second)
	purgeRing(ring)

	if _, err := pd.FindProviders(ctx, c); err == nil {
		t.Fatal("expected FindProviders to fail after ProviderRecord TTL expiry")
	}
}

func TestTTL_ProviderRecord_Republished(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()
	cfg := shortTTLConfig()
	clock := newFakeClock()

	pd, _ := makePublicDringWithCfg(t, ring, cfg, clock)
	ring.StabilizeRounds(10)

	c, _ := testBlock("ttl-provider-republish")

	if err := pd.AnnounceProvider(ctx, c); err != nil {
		t.Fatalf("AnnounceProvider: %v", err)
	}

	clock.Advance(cfg.ProviderRecordTTL + time.Second)
	purgeRing(ring)
	if err := pd.AnnounceProvider(ctx, c); err != nil {
		t.Fatalf("AnnounceProvider (republish): %v", err)
	}

	providers, err := pd.FindProviders(ctx, c)
	if err != nil {
		t.Fatalf("FindProviders after republish: %v", err)
	}
	if len(providers) == 0 {
		t.Fatal("expected at least one provider after republish")
	}
}

func TestTTL_GroupIdentityRecord_Expires(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()
	cfg := shortTTLConfig()
	clock := newFakeClock()

	pd, ident := makePublicDringWithCfg(t, ring, cfg, clock)
	ring.StabilizeRounds(10)

	grp, err := GenerateGroupIdentity()
	if err != nil {
		t.Fatalf("GenerateGroupIdentity: %v", err)
	}

	peers := []GroupMember{{ID: ident.ID}}
	if err := pd.PublishGroup(ctx, grp, 1, peers); err != nil {
		t.Fatalf("PublishGroup: %v", err)
	}

	if _, err := pd.LookupGroup(ctx, grp.GroupID); err != nil {
		t.Fatalf("LookupGroup immediately after publish: %v", err)
	}

	clock.Advance(cfg.GroupRecordTTL + time.Second)
	purgeRing(ring)

	if _, err := pd.LookupGroup(ctx, grp.GroupID); err == nil {
		t.Fatal("expected LookupGroup to fail after GroupIdentityRecord TTL expiry")
	}
}

func TestTTL_GroupIdentityRecord_DiskCacheRecovery(t *testing.T) {

	cfg := shortTTLConfig()
	ring := NewTestRing()
	clock := newFakeClock()

	pubIdent, _ := GenerateIdentity()
	bs := newTestMemStore()
	lt := &localTransport{ring: ring, nodeID: pubIdent.ID}
	pubNode := NewNode(pubIdent.ID, testAddr(pubIdent.ID[0]), bs, nil, lt, cfg)
	pubNode.SetNowFunc(clock.Now)
	ring.mu.Lock()
	ring.Nodes = append(ring.Nodes, pubNode)
	ring.nodeMap[pubIdent.ID] = pubNode
	ring.mu.Unlock()
	pubNode.Create()

	pubDring := NewPublicDring(pubNode, pubIdent)
	ring.StabilizeRounds(10)

	ctx := context.Background()

	grp, err := GenerateGroupIdentity()
	if err != nil {
		t.Fatalf("GenerateGroupIdentity: %v", err)
	}

	tmpDir := t.TempDir()
	cacheFile := filepath.Join(tmpDir, grp.GroupID.String()+".record")

	privDring := NewPrivateDring(grp, pubIdent, newTestMemStore(), nil, cfg)
	_, err = privDring.StartServer("/ip4/127.0.0.1/tcp/0", "")
	if err != nil {
		t.Fatalf("StartServer: %v", err)
	}
	defer privDring.Stop()

	privDring.SetGroupRecordCachePath(cacheFile)
	privDring.Create()

	if err := privDring.UpdateGroupRecord(ctx, pubDring); err != nil {
		t.Fatalf("initial UpdateGroupRecord: %v", err)
	}

	if _, err := os.Stat(cacheFile); os.IsNotExist(err) {
		t.Fatal("expected disk cache file to exist after UpdateGroupRecord")
	}

	rec1, err := pubDring.LookupGroup(ctx, grp.GroupID)
	if err != nil {
		t.Fatalf("LookupGroup before expiry: %v", err)
	}
	v1 := rec1.Data.Version

	clock.Advance(cfg.GroupRecordTTL + time.Second)
	pubNode.purgeExpiredRecords()

	if _, err := pubDring.LookupGroup(ctx, grp.GroupID); err == nil {
		t.Fatal("expected LookupGroup to fail after TTL expiry")
	}

	if err := privDring.UpdateGroupRecord(ctx, pubDring); err != nil {
		t.Fatalf("UpdateGroupRecord after expiry (cache recovery): %v", err)
	}

	rec2, err := pubDring.LookupGroup(ctx, grp.GroupID)
	if err != nil {
		t.Fatalf("LookupGroup after cache recovery: %v", err)
	}
	if rec2.Data.Version <= v1 {
		t.Errorf("expected version > %d after cache recovery, got %d", v1, rec2.Data.Version)
	}

	if len(rec2.Data.Peers) == 0 {
		t.Error("expected non-empty peer list after cache recovery")
	}
}

func TestTTL_MultiNode_PeerRecordExpireAndRepublish(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()
	cfg := shortTTLConfig()
	clock := newFakeClock()

	pd1, ident1 := makePublicDringWithCfg(t, ring, cfg, clock)
	pd2, ident2 := makePublicDringWithCfg(t, ring, cfg, clock)
	ring.StabilizeRounds(20)

	if err := pd1.PublishSelf(ctx); err != nil {
		t.Fatalf("pd1.PublishSelf: %v", err)
	}
	if err := pd2.PublishSelf(ctx); err != nil {
		t.Fatalf("pd2.PublishSelf: %v", err)
	}

	clock.Advance(cfg.PeerRecordTTL + time.Second)
	purgeRing(ring)
	if err := pd1.PublishSelf(ctx); err != nil {
		t.Fatalf("pd1.PublishSelf (republish): %v", err)
	}
	if err := pd2.PublishSelf(ctx); err != nil {
		t.Fatalf("pd2.PublishSelf (republish): %v", err)
	}

	if _, err := pd2.LookupPeer(ctx, ident1.ID); err != nil {
		t.Errorf("pd2 LookupPeer(ident1) after republish: %v", err)
	}
	if _, err := pd1.LookupPeer(ctx, ident2.ID); err != nil {
		t.Errorf("pd1 LookupPeer(ident2) after republish: %v", err)
	}
}

func TestTTL_SignedRecord_VersionMonotonicity(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()
	cfg := shortTTLConfig()

	pd, ident := makePublicDringWithCfg(t, ring, cfg, nil)
	ring.StabilizeRounds(10)

	if err := pd.PublishSelf(ctx); err != nil {
		t.Fatalf("PublishSelf 1: %v", err)
	}
	r1, err := pd.LookupPeer(ctx, ident.ID)
	if err != nil {
		t.Fatalf("LookupPeer after publish 1: %v", err)
	}

	if err := pd.PublishSelf(ctx); err != nil {
		t.Fatalf("PublishSelf 2: %v", err)
	}
	r2, err := pd.LookupPeer(ctx, ident.ID)
	if err != nil {
		t.Fatalf("LookupPeer after publish 2: %v", err)
	}

	if r2.Data.Version <= r1.Data.Version {
		t.Errorf("expected version to increase: v1=%d v2=%d", r1.Data.Version, r2.Data.Version)
	}
}

func TestTTL_VersionConflict_SameVersionRejected(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()
	cfg := shortTTLConfig()

	pd, ident := makePublicDringWithCfg(t, ring, cfg, nil)
	ring.StabilizeRounds(10)

	const fixedVersion = 5
	rec, err := NewPeerIdentityRecord(ident, fixedVersion, testAddr(0), nil)
	if err != nil {
		t.Fatalf("NewPeerIdentityRecord: %v", err)
	}
	data, _ := rec.Encode()
	if err := pd.node.RecordPut(ctx, ident.ID, data); err != nil {
		t.Fatalf("initial RecordPut v%d: %v", fixedVersion, err)
	}

	if err := pd.node.RecordPut(ctx, ident.ID, data); err == nil {
		t.Error("expected version conflict error when re-submitting the same version, got nil")
	}
}

func TestTTL_VersionConflict_LowerVersionRejected(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()
	cfg := shortTTLConfig()

	pd, ident := makePublicDringWithCfg(t, ring, cfg, nil)
	ring.StabilizeRounds(10)

	const higherVersion = 10
	recHigh, err := NewPeerIdentityRecord(ident, higherVersion, testAddr(0), nil)
	if err != nil {
		t.Fatalf("NewPeerIdentityRecord v%d: %v", higherVersion, err)
	}
	dataHigh, _ := recHigh.Encode()
	if err := pd.node.RecordPut(ctx, ident.ID, dataHigh); err != nil {
		t.Fatalf("RecordPut v%d: %v", higherVersion, err)
	}

	const lowerVersion = 3
	recLow, err := NewPeerIdentityRecord(ident, lowerVersion, testAddr(0), nil)
	if err != nil {
		t.Fatalf("NewPeerIdentityRecord v%d: %v", lowerVersion, err)
	}
	dataLow, _ := recLow.Encode()
	if err := pd.node.RecordPut(ctx, ident.ID, dataLow); err == nil {
		t.Errorf("expected rejection for lower-version record (v%d < v%d), got nil",
			lowerVersion, higherVersion)
	}
}

func TestTTL_SignedRecord_VersionCheckWaivedOnExpiry(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()
	cfg := shortTTLConfig()
	clock := newFakeClock()

	pd, ident := makePublicDringWithCfg(t, ring, cfg, clock)
	ring.StabilizeRounds(10)

	const highVersion = 50
	recHigh, err := NewPeerIdentityRecord(ident, highVersion, testAddr(0), nil)
	if err != nil {
		t.Fatalf("NewPeerIdentityRecord v%d: %v", highVersion, err)
	}
	dataHigh, _ := recHigh.Encode()
	if err := pd.node.RecordPut(ctx, ident.ID, dataHigh); err != nil {
		t.Fatalf("RecordPut v%d: %v", highVersion, err)
	}

	clock.Advance(cfg.PeerRecordTTL + time.Second)
	purgeRing(ring)

	if _, err := pd.LookupPeer(ctx, ident.ID); err == nil {
		t.Fatal("expected record to be absent after TTL expiry, but LookupPeer succeeded")
	}

	recLow, err := NewPeerIdentityRecord(ident, 1, testAddr(0), nil)
	if err != nil {
		t.Fatalf("NewPeerIdentityRecord v1: %v", err)
	}
	dataLow, _ := recLow.Encode()
	if err := pd.node.RecordPut(ctx, ident.ID, dataLow); err != nil {
		t.Errorf("expected version check to be waived for expired record (v1 after v%d expired): %v",
			highVersion, err)
	}

	rec, err := pd.LookupPeer(ctx, ident.ID)
	if err != nil {
		t.Fatalf("LookupPeer after re-publish: %v", err)
	}
	if rec.Data.Version != 1 {
		t.Errorf("expected version 1 after re-publish, got %d", rec.Data.Version)
	}
}

func TestTTL_PeerIdentityRecord_RestartVersionRecovery(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()
	cfg := Config{Replication: 1}

	pd, ident := makePublicDringWithCfg(t, ring, cfg, nil)
	ring.StabilizeRounds(10)

	const preRestartVersion = uint64(10)
	recHigh, err := NewPeerIdentityRecord(ident, preRestartVersion, "/ip4/127.0.0.1/tcp/7000", nil)
	if err != nil {
		t.Fatalf("NewPeerIdentityRecord v%d: %v", preRestartVersion, err)
	}
	dataHigh, _ := recHigh.Encode()
	if err := pd.node.RecordPut(ctx, ident.ID, dataHigh); err != nil {
		t.Fatalf("RecordPut v%d: %v", preRestartVersion, err)
	}

	recV1, _ := NewPeerIdentityRecord(ident, 1, "/ip4/127.0.0.1/tcp/7001", nil)
	dataV1, _ := recV1.Encode()
	err = pd.node.RecordPut(ctx, ident.ID, dataV1)
	if err == nil {
		t.Fatal("expected version conflict when re-publishing at version 1 after higher version, got nil")
	}
	if !isVersionConflict(err) {
		t.Fatalf("expected ErrVersionConflict, got: %v", err)
	}

	stored, err := pd.LookupPeer(ctx, ident.ID)
	if err != nil {
		t.Fatalf("LookupPeer for version recovery: %v", err)
	}
	if stored.Data.Version != preRestartVersion {
		t.Errorf("stored version = %d, want %d", stored.Data.Version, preRestartVersion)
	}

	recoveryVersion := stored.Data.Version + 1
	recRecovery, err := NewPeerIdentityRecord(ident, recoveryVersion, "/ip4/127.0.0.1/tcp/7001", nil)
	if err != nil {
		t.Fatalf("NewPeerIdentityRecord recovery: %v", err)
	}
	dataRecovery, _ := recRecovery.Encode()
	if err := pd.node.RecordPut(ctx, ident.ID, dataRecovery); err != nil {
		t.Fatalf("RecordPut at storedVersion+1 (%d) failed: %v", recoveryVersion, err)
	}

	final, err := pd.LookupPeer(ctx, ident.ID)
	if err != nil {
		t.Fatalf("LookupPeer after recovery: %v", err)
	}
	if final.Data.Version != recoveryVersion {
		t.Errorf("expected version %d after recovery, got %d", recoveryVersion, final.Data.Version)
	}
}

func TestTTL_ProviderRecord_PerEntryExpiry(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()
	cfg := shortTTLConfig()
	clock := newFakeClock()

	pdA, _ := makePublicDringWithCfg(t, ring, cfg, clock)
	pdB, identB := makePublicDringWithCfg(t, ring, cfg, clock)
	ring.StabilizeRounds(10)

	c, _ := testBlock("ttl-per-entry")

	if err := pdA.AnnounceProvider(ctx, c); err != nil {
		t.Fatalf("pdA AnnounceProvider: %v", err)
	}
	if err := pdB.AnnounceProvider(ctx, c); err != nil {
		t.Fatalf("pdB AnnounceProvider: %v", err)
	}

	providers, err := pdA.FindProviders(ctx, c)
	if err != nil {
		t.Fatalf("FindProviders before expiry: %v", err)
	}
	if len(providers) != 2 {
		t.Fatalf("expected 2 providers before expiry, got %d", len(providers))
	}

	clock.Advance(cfg.ProviderRecordTTL + time.Second)
	if err := pdA.AnnounceProvider(ctx, c); err != nil {
		t.Fatalf("pdA AnnounceProvider (republish): %v", err)
	}
	purgeRing(ring)

	providers, err = pdA.FindProviders(ctx, c)
	if err != nil {
		t.Fatalf("FindProviders after partial expiry: %v", err)
	}
	if len(providers) != 1 {
		t.Errorf("expected exactly 1 provider after pdB expired, got %d", len(providers))
	}
	if len(providers) > 0 && providers[0].Provider == identB.ID {
		t.Errorf("expected pdB's entry to have expired, but it is still present")
	}
}

func TestTTL_GroupIdentityRecord_FreshPublishWhenNoCacheAndNoRingRecord(t *testing.T) {
	ring := NewTestRing()
	cfg := Config{Replication: 1}

	pubIdent, _ := GenerateIdentity()
	bs := newTestMemStore()
	lt := &localTransport{ring: ring, nodeID: pubIdent.ID}
	pubNode := NewNode(pubIdent.ID, testAddr(pubIdent.ID[0]), bs, nil, lt, cfg)
	ring.mu.Lock()
	ring.Nodes = append(ring.Nodes, pubNode)
	ring.nodeMap[pubIdent.ID] = pubNode
	ring.mu.Unlock()
	pubNode.Create()
	publicDring := NewPublicDring(pubNode, pubIdent)

	ctx := context.Background()
	grp, err := GenerateGroupIdentity()
	if err != nil {
		t.Fatalf("GenerateGroupIdentity: %v", err)
	}

	peerIdent, _ := GenerateIdentity()
	privDring := NewPrivateDring(grp, peerIdent, newTestMemStore(), nil, cfg)
	_, err = privDring.StartServer("/ip4/127.0.0.1/tcp/0", "")
	if err != nil {
		t.Fatalf("StartServer: %v", err)
	}
	defer privDring.Stop()
	privDring.Create()

	if err := privDring.UpdateGroupRecord(ctx, publicDring); err != nil {
		t.Fatalf("UpdateGroupRecord (fresh, no cache, no ring record): %v", err)
	}

	rec, err := publicDring.LookupGroup(ctx, grp.GroupID)
	if err != nil {
		t.Fatalf("LookupGroup after fresh publish: %v", err)
	}
	if rec.Data.Version != 1 {
		t.Errorf("expected version 1 for fresh publish (no prior record), got %d", rec.Data.Version)
	}
	if len(rec.Data.Peers) != 1 {
		t.Errorf("expected exactly 1 member (the local peer) for fresh publish, got %d", len(rec.Data.Peers))
	}
	if rec.Data.Peers[0].ID != peerIdent.ID {
		t.Errorf("expected local peer %s as sole member, got %s", peerIdent.ID, rec.Data.Peers[0].ID)
	}
}
