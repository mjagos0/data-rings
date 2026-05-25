//go:build system

package system_test

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/mjagos0/datarings/testrig"
)

const (
	xringRingAName	= "ring-A"
	xringRingBName	= "ring-B"
	xringChunkSize	= 262144
	xringNumChunks	= 3
	xringFileSize	= xringChunkSize * xringNumChunks
)

func TestScenario_PerRing_Placement_Counts_Metrics(t *testing.T) {
	c := testrig.NewCluster(t, testrig.StateFreshPublic)
	_ = c.Nodes8()
	c.Setup()

	ringA := []*testrig.TestNode{
		c.NodeByName("node1"), c.NodeByName("node2"),
		c.NodeByName("node3"), c.NodeByName("node4"),
	}
	ringB := []*testrig.TestNode{
		c.NodeByName("node1"), c.NodeByName("node5"),
		c.NodeByName("node6"), c.NodeByName("node7"),
	}
	shared := c.NodeByName("node1")
	outsider := c.NodeByName("node8")

	ringAKey := createRingAndJoin(t, ringA, xringRingAName)
	stabilizeRing(t, ringA, xringRingAName, len(ringA)+testrig.DefaultSuccListSize-2)
	ringBKey := createRingAndJoin(t, ringB, xringRingBName)
	if ringAKey == ringBKey {
		t.Fatal("ring keys collided")
	}
	stabilizeRing(t, ringB, xringRingBName, len(ringB)+testrig.DefaultSuccListSize-2)

	ringAGroupID := privateRingGroupID(t, shared, xringRingAName)
	ringBGroupID := privateRingGroupID(t, shared, xringRingBName)
	t.Logf("[per-ring] ring-A group=%s, ring-B group=%s", ringAGroupID, ringBGroupID)

	fileData := make([]byte, xringFileSize)
	if _, err := rand.Read(fileData); err != nil {
		t.Fatalf("generate file data: %v", err)
	}
	rootCID, dagBlocks := buildDAGBlocks(t, fileData)
	cidStr, err := shared.StoreFile(fileData, xringRingAName)
	if err != nil {
		t.Fatalf("store file in ring A: %v", err)
	}
	if cidStr != rootCID {
		t.Fatalf("root CID mismatch: got %s want %s", cidStr, rootCID)
	}
	t.Logf("[per-ring] stored CID=%s in ring-A (%d blocks)", cidStr, len(dagBlocks))

	ringABlockMHs := make(map[string]bool, len(dagBlocks))
	for _, b := range dagBlocks {
		ringABlockMHs[b.MultihashHex] = true
	}

	ringAReplicaNames := map[string]bool{}
	for _, blk := range dagBlocks {
		for _, n := range c.ReplicaNodesForHashIn(blk.RingKey, ringA, 3) {
			ringAReplicaNames[n.Name()] = true
		}
	}
	t.Logf("[per-ring] ring-A replicas (across all DAG blocks): %v", sortedKeys(ringAReplicaNames))

	for _, n := range ringA {
		got, err := ringScopedBlockCIDs(t, n, ringAGroupID)
		if err != nil {
			t.Fatalf("%s ring-A blocks: %v", n.Name(), err)
		}
		gotMH := mhSet(got)
		expected := ringAReplicaNames[n.Name()]
		if expected {
			missing := 0
			for _, blk := range dagBlocks {
				if !gotMH[blk.MultihashHex] {

					mine := false
					for _, r := range c.ReplicaNodesForHashIn(blk.RingKey, ringA, 3) {
						if r.Name() == n.Name() {
							mine = true
							break
						}
					}
					if mine {
						missing++
					}
				}
			}
			if missing > 0 {
				t.Errorf("[per-ring] %s ring-A: missing %d blocks it should hold (have %d)",
					n.Name(), missing, len(got))
			} else {
				t.Logf("  [ok] %s ring-A: holds %d blocks (all expected ones present)", n.Name(), len(got))
			}
		} else if len(got) != 0 {
			t.Errorf("[per-ring] %s ring-A: holds %d blocks but is not a ring-A replica for any DAG block",
				n.Name(), len(got))
		}
	}

	for _, n := range ringB {
		got, err := ringScopedBlockCIDs(t, n, ringBGroupID)
		if err != nil {
			t.Fatalf("%s ring-B blocks: %v", n.Name(), err)
		}
		var leaked []string
		for _, c := range got {
			mh := cidToMultihashHex(c)
			if ringABlockMHs[mh] {
				leaked = append(leaked, c)
			}
		}
		if len(leaked) > 0 {
			t.Errorf("[per-ring] %s ring-B: holds %d ring-A blocks (cross-ring leak): %v",
				n.Name(), len(leaked), leaked)
		} else {
			t.Logf("  [ok] %s ring-B: 0 ring-A blocks (held %d ring-B blocks total)", n.Name(), len(got))
		}
	}

	for _, ringID := range []string{ringAGroupID, ringBGroupID} {
		got, err := ringScopedBlockCIDs(t, outsider, ringID)
		if err != nil {
			t.Fatalf("%s ring=%s: %v", outsider.Name(), ringID, err)
		}
		if len(got) != 0 {
			t.Errorf("[per-ring] %s ring=%s: outsider holds %d blocks (should be 0)",
				outsider.Name(), ringID, len(got))
		}
	}

	for _, n := range ringA {
		entry, ok := n.PrivateState(xringRingAName)
		if !ok {
			t.Fatalf("%s: not in ring-A PrivateState", n.Name())
		}
		if entry.Node.RingID != ringAGroupID {
			t.Errorf("[per-ring] %s ring-A NodeState.RingID=%q want %q",
				n.Name(), entry.Node.RingID, ringAGroupID)
		}

		used, max, err := ringScopedStorage(t, n, ringAGroupID)
		if err != nil {
			t.Fatalf("%s storage(ring-A): %v", n.Name(), err)
		}
		if int64(entry.Node.RingStorageUsedBytes) != used {
			t.Errorf("[per-ring] %s ring-A used: NodeState=%d vs /debug/rings=%d",
				n.Name(), entry.Node.RingStorageUsedBytes, used)
		}
		if entry.Node.RingStorageMaxBytes != max {
			t.Errorf("[per-ring] %s ring-A max: NodeState=%d vs /debug/rings=%d",
				n.Name(), entry.Node.RingStorageMaxBytes, max)
		}
		t.Logf("  [ok] %s ring-A: blocks=%d used=%d roots=%d quota=%d",
			n.Name(), entry.Node.RingBlockCount, entry.Node.RingStorageUsedBytes,
			entry.Node.RingNetworkRootCount, entry.Node.RingStorageMaxBytes)
	}

	{
		entry, ok := shared.PrivateState(xringRingBName)
		if !ok {
			t.Fatalf("shared node not in ring-B PrivateState")
		}
		if entry.Node.RingStorageUsedBytes != 0 {
			t.Errorf("[per-ring] shared node ring-B used=%d, want 0 (no ring-B writes)",
				entry.Node.RingStorageUsedBytes)
		}
		if entry.Node.RingBlockCount != 0 {
			t.Errorf("[per-ring] shared node ring-B blocks=%d, want 0",
				entry.Node.RingBlockCount)
		}
		if entry.Node.RingNetworkRootCount != 0 {
			t.Errorf("[per-ring] shared node ring-B roots=%d, want 0",
				entry.Node.RingNetworkRootCount)
		}
	}

	rings := ringSummariesAt(t, shared)
	want := map[string]bool{ringAGroupID: false, ringBGroupID: false}
	for _, r := range rings {
		if _, ok := want[r.Ring]; ok {
			want[r.Ring] = true
		}
	}
	for ring, found := range want {
		if !found {
			t.Errorf("[per-ring] /debug/rings on shared node missing ring=%s", ring)
		}
	}

	for _, n := range ringA {
		if !ringAReplicaNames[n.Name()] {
			continue
		}
		used, max, err := ringScopedStorage(t, n, ringAGroupID)
		if err != nil {
			t.Fatalf("%s storage(ring-A): %v", n.Name(), err)
		}
		if used <= 0 {
			t.Errorf("[per-ring] %s ring-A: used_bytes=%d on a replica node, want > 0", n.Name(), used)
		}
		if max != 0 {
			t.Errorf("[per-ring] %s ring-A: max_bytes=%d, want 0 (default unlimited)", n.Name(), max)
		}
	}
}

func TestScenario_PerRing_QuotaEnforcement(t *testing.T) {
	c := testrig.NewCluster(t, testrig.StateFreshPublic)
	_ = c.Nodes8()
	c.Setup()

	ringA := []*testrig.TestNode{
		c.NodeByName("node1"), c.NodeByName("node2"),
		c.NodeByName("node3"), c.NodeByName("node4"),
	}
	const ringName = "ring-A-quota"
	createRingAndJoin(t, ringA, ringName)
	stabilizeRing(t, ringA, ringName, len(ringA)+testrig.DefaultSuccListSize-2)

	const quota = int64(64 * 1024)
	for _, n := range ringA {
		if err := setRingQuota(t, n, ringName, quota); err != nil {
			t.Fatalf("set ring quota on %s: %v", n.Name(), err)
		}
	}
	t.Logf("[quota] set ring-A per-ring quota = %d bytes on all 4 members", quota)

	if _, max, err := ringScopedStorage(t, ringA[0], privateRingGroupID(t, ringA[0], ringName)); err != nil {
		t.Fatalf("read ring storage: %v", err)
	} else if max != quota {
		t.Errorf("read-back quota=%d, want %d", max, quota)
	}

	fileData := make([]byte, 256*1024)
	if _, err := rand.Read(fileData); err != nil {
		t.Fatalf("generate data: %v", err)
	}
	_, err := ringA[0].StoreFile(fileData, ringName)
	if err == nil {
		t.Fatalf("[quota] expected ErrStorageFull on store, got nil")
	}
	t.Logf("[quota] store rejected by per-ring caps: %v", err)

	groupID := privateRingGroupID(t, ringA[0], ringName)
	for _, n := range ringA {
		used, _, err := ringScopedStorage(t, n, groupID)
		if err != nil {
			t.Fatalf("read ring storage: %v", err)
		}
		if used == 0 {
			continue
		}
		if err := setRingQuota(t, n, ringName, used-1); err == nil {
			t.Errorf("[quota] %s: setting cap below current usage (%d → %d) should fail",
				n.Name(), used, used-1)
		} else {
			t.Logf("[quota] %s: correctly refused to shrink cap below usage: %v", n.Name(), err)
		}
		return
	}
	t.Log("[quota] no member retained any bytes; skipping shrink-below-usage check")
}

func createRingAndJoin(t *testing.T, members []*testrig.TestNode, name string) string {
	t.Helper()
	out, err := members[0].Exec("ring", "create")
	if err != nil {
		t.Fatalf("ring create on %s: %v\n%s", members[0].Name(), err, out)
	}
	key := testrig.ParseGroupKey(out)
	if key == "" {
		t.Fatalf("could not parse group key: %s", out)
	}
	for _, n := range members {
		if out, err := n.Exec("ring", "join", key, name); err != nil {
			t.Fatalf("%s join %s: %v\n%s", n.Name(), name, err, out)
		}
	}
	return key
}

func privateRingGroupID(t *testing.T, n *testrig.TestNode, name string) string {
	t.Helper()
	entry, ok := n.PrivateState(name)
	if !ok {
		t.Fatalf("%s: ring %q not in PrivateState", n.Name(), name)
	}
	return entry.GroupID
}

func ringScopedBlockCIDs(t *testing.T, n *testrig.TestNode, ringID string) ([]string, error) {
	t.Helper()
	resp, err := http.Get(nodeAPI(t, n) + "/debug/rings/" + ringID + "/network-blocks")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, body)
	}
	var out struct {
		Ring	string		`json:"ring"`
		Count	int		`json:"count"`
		CIDs	[]string	`json:"cids"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.CIDs, nil
}

func ringScopedStorage(t *testing.T, n *testrig.TestNode, ringID string) (used, max int64, err error) {
	t.Helper()
	resp, err := http.Get(nodeAPI(t, n) + "/debug/rings/" + ringID + "/storage")
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return 0, 0, fmt.Errorf("status %d: %s", resp.StatusCode, body)
	}
	var out struct {
		Used	int64	`json:"used_bytes"`
		Max	int64	`json:"max_bytes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, 0, err
	}
	return out.Used, out.Max, nil
}

func ringSummariesAt(t *testing.T, n *testrig.TestNode) []struct {
	Ring		string	`json:"ring"`
	RootCount	int	`json:"root_count"`
	BlockCount	int	`json:"block_count"`
	UsedBytes	int64	`json:"used_bytes"`
	MaxBytes	int64	`json:"max_bytes"`
} {
	t.Helper()
	resp, err := http.Get(nodeAPI(t, n) + "/debug/rings")
	if err != nil {
		t.Fatalf("/debug/rings: %v", err)
	}
	defer resp.Body.Close()
	var out struct {
		Rings []struct {
			Ring		string	`json:"ring"`
			RootCount	int	`json:"root_count"`
			BlockCount	int	`json:"block_count"`
			UsedBytes	int64	`json:"used_bytes"`
			MaxBytes	int64	`json:"max_bytes"`
		} `json:"rings"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode /debug/rings: %v", err)
	}
	return out.Rings
}

func setRingQuota(t *testing.T, n *testrig.TestNode, groupRef string, max int64) error {
	t.Helper()
	body := strings.NewReader(fmt.Sprintf(`{"max_bytes": %d}`, max))
	req, _ := http.NewRequest(http.MethodPut, nodeAPI(t, n)+"/ring/"+groupRef+"/quota", body)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return nil
	}
	b, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("status %d: %s", resp.StatusCode, b)
}

func nodeAPI(t *testing.T, n *testrig.TestNode) string {
	t.Helper()
	out, err := n.Exec("config", "show")
	if err == nil && strings.Contains(out, "api_addr") {

	}

	idx := -1
	for i, name := range testrigAllNodes() {
		if name == n.Name() {
			idx = i
			break
		}
	}
	if idx < 0 {
		t.Fatalf("nodeAPI: unknown node %q", n.Name())
	}
	return fmt.Sprintf("http://127.0.0.1:%d", 17400+idx)
}

func testrigAllNodes() []string {
	return []string{"node1", "node2", "node3", "node4", "node5", "node6", "node7", "node8"}
}

func mhSet(cids []string) map[string]bool {
	out := make(map[string]bool, len(cids))
	for _, c := range cids {
		out[cidToMultihashHex(c)] = true
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}
