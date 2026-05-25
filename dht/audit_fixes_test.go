package dht

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestPublishSelf_VersionConflictRecovery(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()
	pd, _ := makePublicDring(t, ring, 0)
	ring.StabilizeRounds(10)

	for i := 0; i < 5; i++ {
		if err := pd.PublishSelf(ctx); err != nil {
			t.Fatalf("PublishSelf round %d: %v", i, err)
		}
	}

	pd2 := NewPublicDring(pd.node, pd.identity)

	if err := pd2.PublishSelf(ctx); err != nil {
		t.Fatalf("PublishSelf after simulated restart should recover from version conflict: %v", err)
	}

	rec, err := pd2.LookupPeer(ctx, pd2.identity.ID)
	if err != nil {
		t.Fatalf("LookupPeer: %v", err)
	}
	if rec.Data.Version <= 5 {
		t.Errorf("expected version > 5 after recovery, got %d", rec.Data.Version)
	}
}

func TestUnregisterGroupAddrProvider(t *testing.T) {
	ring := NewTestRing()
	pd, _ := makePublicDring(t, ring, 0)
	ring.StabilizeRounds(10)

	grp, err := GenerateGroupIdentity()
	if err != nil {
		t.Fatalf("GenerateGroupIdentity: %v", err)
	}

	pd.RegisterGroupAddrProvider(grp.GroupID, func() string {
		return "/ip4/1.2.3.4/tcp/9000"
	})

	addrs := pd.collectGroupAddrs()
	if _, ok := addrs[grp.GroupID.String()]; !ok {
		t.Fatal("expected group addr to be present after registration")
	}

	pd.UnregisterGroupAddrProvider(grp.GroupID)

	addrs = pd.collectGroupAddrs()
	if _, ok := addrs[grp.GroupID.String()]; ok {
		t.Fatal("expected group addr to be absent after unregistration")
	}
}

func TestLeaveGroup_CleansUpGroupAddrs(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()
	pd, ident := makePublicDring(t, ring, 0)
	ring.StabilizeRounds(10)

	grp, _ := GenerateGroupIdentity()

	bs := newTestMemStore()
	cfg := Config{Replication: 1}
	privDring := NewPrivateDring(grp, ident, bs, nil, cfg)
	privDring.Create()

	pd.RegisterGroupAddrProvider(grp.GroupID, func() string {
		return "/ip4/1.2.3.4/tcp/9001"
	})

	addrs := pd.collectGroupAddrs()
	if _, ok := addrs[grp.GroupID.String()]; !ok {
		t.Fatal("expected group addr before leave")
	}

	_ = privDring.LeaveGroup(ctx, pd)

	addrs = pd.collectGroupAddrs()
	if _, ok := addrs[grp.GroupID.String()]; ok {
		t.Fatal("expected group addr to be removed after LeaveGroup")
	}
}

func TestProviderRecord_ZeroCallerID_Rejected(t *testing.T) {
	ring := NewTestRing()
	pd, ident := makePublicDring(t, ring, 0)
	ring.StabilizeRounds(10)

	_, raw := testBlock("zero-caller-test")
	c := testCID(raw)
	key := CIDToNodeID(c)

	pr := ProviderRecord{ContentHash: key, Provider: ident.ID}
	data, _ := pr.Encode()

	if err := pd.node.storeRecord(key, data, NodeID{}); err == nil {
		t.Fatal("expected zero callerID to be rejected")
	}

	if err := pd.node.storeRecord(key, data, ident.ID); err != nil {
		t.Fatalf("matching callerID should be accepted: %v", err)
	}
}

func TestLocalRecordPut_ExpiredVersionWaiver(t *testing.T) {
	ring := NewTestRing()
	cfg := Config{
		Replication:	1,
		PeerRecordTTL:	100 * time.Millisecond,
	}
	pd, ident := makePublicDringWithCfg(t, ring, cfg, nil)
	ring.StabilizeRounds(10)

	ctx := context.Background()

	rec5, err := NewPeerIdentityRecord(ident, 5, pd.node.addr, nil)
	if err != nil {
		t.Fatalf("NewPeerIdentityRecord v5: %v", err)
	}
	data5, _ := rec5.Encode()
	pd.node.localRecordPut(ident.ID, data5)

	rec1, _ := NewPeerIdentityRecord(ident, 1, pd.node.addr, nil)
	data1, _ := rec1.Encode()

	pd.node.localRecordPut(ident.ID, data1)
	stored, err := pd.LookupPeer(ctx, ident.ID)
	if err != nil {
		t.Fatalf("LookupPeer: %v", err)
	}
	if stored.Data.Version != 5 {
		t.Errorf("expected version 5 while not expired, got %d", stored.Data.Version)
	}

	time.Sleep(200 * time.Millisecond)

	pd.node.localRecordPut(ident.ID, data1)

	pd.node.recordsMu.RLock()
	rawStored := pd.node.records[ident.ID]
	pd.node.recordsMu.RUnlock()

	if rawStored == nil {
		t.Fatal("expected record to be stored after expiry waiver")
	}
	ver := extractRecordVersion(rawStored)
	if ver != 1 {
		t.Errorf("expected version 1 after expired waiver, got %d", ver)
	}
}

func TestNotify_FirstPredecessor_TransfersKeys(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()

	ring.AddNode(0)
	ring.StabilizeRounds(10)
	n0 := ring.FindNode(0)

	for i := 0; i < 20; i++ {
		key, data := testBlock(fmt.Sprintf("notify-transfer-%d", i))
		if err := n0.Put(ctx, key, data); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	initial := n0.DataCount()
	if initial != 20 {
		t.Fatalf("expected 20 initial blocks, got %d", initial)
	}

	ring.AddNode(128)
	ring.StabilizeRounds(20)

	n128 := ring.FindNode(128)
	total := n0.DataCount() + n128.DataCount()
	if total != 20 {
		t.Fatalf("expected 20 total blocks after join, got %d (node0=%d, node128=%d)",
			total, n0.DataCount(), n128.DataCount())
	}
	if n128.DataCount() == 0 {
		t.Error("node 128 should have received some blocks")
	}
}

func TestProviderEntryTimestamps_Propagated(t *testing.T) {

	ring := NewTestRing()
	cfg := Config{
		Replication:		2,
		ProviderRecordTTL:	70 * time.Minute,
	}
	pd, ident := makePublicDringWithCfg(t, ring, cfg, nil)
	ring.StabilizeRounds(10)

	ctx := context.Background()

	if err := pd.AnnounceProvider(ctx, testCID([]byte("timestamp-test"))); err != nil {
		t.Fatalf("AnnounceProvider: %v", err)
	}

	c := testCID([]byte("timestamp-test"))
	key := CIDToNodeID(c)

	pd.node.recordsMu.RLock()
	data := pd.node.records[key]
	pd.node.recordsMu.RUnlock()

	if data == nil {
		t.Fatal("expected provider record to be stored")
	}

	rs, err := DecodeProviderRecords(data)
	if err != nil {
		t.Fatalf("DecodeProviderRecords: %v", err)
	}
	if len(rs) == 0 {
		t.Fatal("expected at least one provider record")
	}
	if rs[0].Provider != ident.ID {
		t.Errorf("provider mismatch: got %s, want %s", rs[0].Provider, ident.ID)
	}

	pd2, _ := makePublicDringWithCfg(t, ring, cfg, nil)
	ring.StabilizeRounds(10)

	pd2.node.localRecordPut(key, data)

	pd2.node.recordsMu.RLock()
	entryTimes := pd2.node.providerEntryTimes[key]
	pd2.node.recordsMu.RUnlock()

	if entryTimes == nil {
		t.Fatal("expected entry times to be set on replica")
	}
	entryTime, ok := entryTimes[ident.ID]
	if !ok {
		t.Fatal("expected entry time for provider")
	}

	if time.Since(entryTime) < 0 {
		t.Error("entry time should not be in the future")
	}
}

func TestPublishSelf_ConcurrentFromMultiplePeers(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()

	const n = 5
	pds := make([]*PublicDring, n)
	for i := 0; i < n; i++ {
		pds[i], _ = makePublicDring(t, ring, byte(i*32))
	}
	ring.StabilizeRounds(20)

	var wg sync.WaitGroup
	errs := make(chan error, n*3)

	for _, pd := range pds {
		pd := pd
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := 0; round < 3; round++ {
				if err := pd.PublishSelf(ctx); err != nil {
					errs <- err
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("PublishSelf failed: %v", err)
	}
}
