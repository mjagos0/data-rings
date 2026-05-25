//go:build system

package system_test

import (
	"strings"
	"testing"

	"github.com/mjagos0/datarings/testrig"
)

func TestScenario_PerRing_FullOneFreeOther(t *testing.T) {
	const (
		ringFullName	= "ring-full"
		ringFreeName	= "ring-free"
		ringFullQuota	= int64(1024)
		ringFreeQuota	= int64(1024 * 1024)
	)

	c := testrig.NewCluster(t, testrig.StateFreshPublic)
	_ = c.Nodes8()
	c.Setup()

	peer := c.NodeByName("node1")

	createRingAndJoin(t, []*testrig.TestNode{peer}, ringFullName)
	createRingAndJoin(t, []*testrig.TestNode{peer}, ringFreeName)
	stabilizeRing(t, []*testrig.TestNode{peer}, ringFullName, 1)
	stabilizeRing(t, []*testrig.TestNode{peer}, ringFreeName, 1)

	if err := setRingQuota(t, peer, ringFullName, ringFullQuota); err != nil {
		t.Fatalf("set %s quota: %v", ringFullName, err)
	}
	if err := setRingQuota(t, peer, ringFreeName, ringFreeQuota); err != nil {
		t.Fatalf("set %s quota: %v", ringFreeName, err)
	}
	t.Logf("[isolation] per-ring quotas: %s=%d  %s=%d", ringFullName, ringFullQuota, ringFreeName, ringFreeQuota)

	ringFullID := privateRingGroupID(t, peer, ringFullName)
	ringFreeID := privateRingGroupID(t, peer, ringFreeName)

	smallData := []byte("seed-bytes-to-bring-ring-full-near-cap")
	if _, err := peer.StoreFile(smallData, ringFullName); err != nil {
		t.Fatalf("seed store in %s: %v", ringFullName, err)
	}
	usedFull, _, err := ringScopedStorage(t, peer, ringFullID)
	if err != nil {
		t.Fatalf("read %s storage: %v", ringFullName, err)
	}
	if usedFull == 0 {
		t.Fatalf("[isolation] %s usage stayed at 0 after seed store", ringFullName)
	}
	if usedFull > ringFullQuota {
		t.Fatalf("[isolation] %s usage %d already exceeds quota %d after seed",
			ringFullName, usedFull, ringFullQuota)
	}
	t.Logf("[isolation] after seed: %s used=%d (quota %d)", ringFullName, usedFull, ringFullQuota)

	largeData := make([]byte, 4*1024)
	for i := range largeData {
		largeData[i] = byte(i % 251)
	}

	cidFull, errFull := peer.StoreFile(largeData, ringFullName)
	if errFull == nil {
		t.Fatalf("[isolation] %s: large store should have been rejected by per-ring cap (CID=%s)", ringFullName, cidFull)
	}
	if !strings.Contains(errFull.Error(), "storage full") {
		t.Errorf("[isolation] %s: rejection reason should be storage full, got: %v", ringFullName, errFull)
	} else {
		t.Logf("[isolation] %s: store rejected as expected: %v", ringFullName, errFull)
	}

	cidFree, errFree := peer.StoreFile(largeData, ringFreeName)
	if errFree != nil {
		t.Fatalf("[isolation] %s: store should succeed (per-ring cap %d), got: %v",
			ringFreeName, ringFreeQuota, errFree)
	}
	t.Logf("[isolation] %s: store succeeded, CID=%s", ringFreeName, cidFree)

	usedFullAfter, _, err := ringScopedStorage(t, peer, ringFullID)
	if err != nil {
		t.Fatalf("read %s storage post: %v", ringFullName, err)
	}
	if usedFullAfter > ringFullQuota {
		t.Errorf("[isolation] %s usage %d > quota %d — quota was breached",
			ringFullName, usedFullAfter, ringFullQuota)
	}

	usedFree, _, err := ringScopedStorage(t, peer, ringFreeID)
	if err != nil {
		t.Fatalf("read %s storage post: %v", ringFreeName, err)
	}
	if usedFree < int64(len(largeData)) {
		t.Errorf("[isolation] %s usage %d < file size %d — large file did not land",
			ringFreeName, usedFree, len(largeData))
	}
	t.Logf("[isolation] final: %s used=%d (≤quota %d), %s used=%d (under quota %d)",
		ringFullName, usedFullAfter, ringFullQuota, ringFreeName, usedFree, ringFreeQuota)

	freeBlocks, err := ringScopedBlockCIDs(t, peer, ringFreeID)
	if err != nil {
		t.Fatalf("read %s blocks: %v", ringFreeName, err)
	}
	if len(freeBlocks) == 0 {
		t.Errorf("[isolation] %s: zero blocks visible after successful store", ringFreeName)
	}

	fullBlocks, err := ringScopedBlockCIDs(t, peer, ringFullID)
	if err != nil {
		t.Fatalf("read %s blocks: %v", ringFullName, err)
	}
	for _, c := range fullBlocks {
		if c == cidFree {
			t.Errorf("[isolation] %s holds %s, the CID published only to %s — cross-ring leak",
				ringFullName, c, ringFreeName)
		}
	}
}

func TestScenario_AggregateQuota_BlocksAcrossRings(t *testing.T) {
	const (
		ringAName	= "agg-ring-A"
		ringBName	= "agg-ring-B"
		aggregateCap	= "2048"
	)

	c := testrig.NewCluster(t, testrig.StateFreshPublic)
	c.DaemonArgs = []string{"--storage-max", aggregateCap}
	_ = c.Nodes8()
	c.Setup()

	peer := c.NodeByName("node1")

	createRingAndJoin(t, []*testrig.TestNode{peer}, ringAName)
	createRingAndJoin(t, []*testrig.TestNode{peer}, ringBName)
	stabilizeRing(t, []*testrig.TestNode{peer}, ringAName, 1)
	stabilizeRing(t, []*testrig.TestNode{peer}, ringBName, 1)

	ringAID := privateRingGroupID(t, peer, ringAName)
	ringBID := privateRingGroupID(t, peer, ringBName)

	firstData := make([]byte, 1024)
	for i := range firstData {
		firstData[i] = byte(i)
	}
	if _, err := peer.StoreFile(firstData, ringAName); err != nil {
		t.Fatalf("[agg] seed store in %s: %v", ringAName, err)
	}

	largeData := make([]byte, 2*1024)
	for i := range largeData {
		largeData[i] = byte(i + 1)
	}

	_, err := peer.StoreFile(largeData, ringBName)
	if err == nil {
		t.Fatalf("[agg] %s: store should have been rejected by aggregate cap, succeeded", ringBName)
	}
	if !strings.Contains(err.Error(), "storage full") {
		t.Errorf("[agg] %s: rejection reason should be storage full, got: %v", ringBName, err)
	} else {
		t.Logf("[agg] %s: store rejected as expected: %v", ringBName, err)
	}

	usedA, _, err := ringScopedStorage(t, peer, ringAID)
	if err != nil {
		t.Fatalf("read %s storage: %v", ringAName, err)
	}
	if usedA == 0 {
		t.Errorf("[agg] %s used=0 after successful seed store", ringAName)
	}
	usedB, _, err := ringScopedStorage(t, peer, ringBID)
	if err != nil {
		t.Fatalf("read %s storage: %v", ringBName, err)
	}
	if int64(usedB) >= int64(len(largeData)) {
		t.Errorf("[agg] %s used=%d ≥ rejected file size %d — large write was not blocked",
			ringBName, usedB, len(largeData))
	}

	state, err := peer.PublicState()
	if err != nil {
		t.Fatalf("PublicState: %v", err)
	}
	if state.StorageMaxBytes != 2048 {
		t.Errorf("[agg] aggregate StorageMaxBytes=%d, want 2048", state.StorageMaxBytes)
	}
	if state.StorageUsedBytes > state.StorageMaxBytes {
		t.Errorf("[agg] aggregate used %d > max %d — quota was breached",
			state.StorageUsedBytes, state.StorageMaxBytes)
	}
	t.Logf("[agg] final: aggregate used=%d/%d, %s used=%d, %s used=%d",
		state.StorageUsedBytes, state.StorageMaxBytes, ringAName, usedA, ringBName, usedB)
}
