//go:build system

package system_test

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/mjagos0/datarings/testrig"
)

const bctRingName = "block-count-ring"

func TestScenario_BlockCount_PersistedCountersMatchAndSurviveRestart(t *testing.T) {
	c := testrig.NewCluster(t, testrig.StateFreshPublic)
	_ = c.Nodes8()
	c.Setup()

	members := []*testrig.TestNode{
		c.NodeByName("node1"), c.NodeByName("node2"),
		c.NodeByName("node3"), c.NodeByName("node4"),
	}
	createRingAndJoin(t, members, bctRingName)
	stabilizeRing(t, members, bctRingName, len(members)+testrig.DefaultSuccListSize-2)
	groupID := privateRingGroupID(t, members[0], bctRingName)

	const (
		chunkSize	= 262144
		numChunks	= 3
	)
	fileData := make([]byte, chunkSize*numChunks)
	if _, err := rand.Read(fileData); err != nil {
		t.Fatalf("generate data: %v", err)
	}
	rootCID, err := members[0].StoreFile(fileData, bctRingName)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	t.Logf("[blockcount] stored rootCID=%s", rootCID)

	for _, n := range members {
		storageCount, err := ringScopedStorageBlockCount(t, n, groupID)
		if err != nil {
			t.Fatalf("%s: storage block_count: %v", n.Name(), err)
		}
		indexedCIDs, err := ringScopedBlockCIDs(t, n, groupID)
		if err != nil {
			t.Fatalf("%s: ring blocks: %v", n.Name(), err)
		}
		if int64(len(indexedCIDs)) != storageCount {
			t.Errorf("[blockcount] %s ring=%s: persisted block_count=%d vs indexed=%d",
				n.Name(), bctRingName, storageCount, len(indexedCIDs))
		} else {
			t.Logf("  [ok] %s ring=%s: block_count=%d matches index", n.Name(), bctRingName, storageCount)
		}
	}

	for _, n := range members {
		aggCount, err := aggregateBlockCountAt(t, n)
		if err != nil {
			t.Fatalf("%s: aggregate block_count: %v", n.Name(), err)
		}
		actual, err := listNetworkBlocksAt(t, n)
		if err != nil {
			t.Fatalf("%s: network-blocks: %v", n.Name(), err)
		}
		if int64(len(actual)) != aggCount {
			t.Errorf("[blockcount] %s aggregate: persisted=%d vs listed=%d",
				n.Name(), aggCount, len(actual))
		} else {
			t.Logf("  [ok] %s aggregate: block_count=%d matches flatfs", n.Name(), aggCount)
		}
	}

	var victim *testrig.TestNode
	for _, n := range members {
		cnt, _ := ringScopedStorageBlockCount(t, n, groupID)
		if cnt > 0 {
			victim = n
			break
		}
	}
	if victim == nil {
		t.Fatalf("no member holds any block — replication may be broken")
	}
	preBlockCount, err := ringScopedStorageBlockCount(t, victim, groupID)
	if err != nil {
		t.Fatalf("pre-restart block_count: %v", err)
	}
	preUsed, _, err := ringScopedStorage(t, victim, groupID)
	if err != nil {
		t.Fatalf("pre-restart used: %v", err)
	}
	preAggCount, err := aggregateBlockCountAt(t, victim)
	if err != nil {
		t.Fatalf("pre-restart agg: %v", err)
	}
	preAggBytes, err := aggregateUsedBytesAt(t, victim)
	if err != nil {
		t.Fatalf("pre-restart agg bytes: %v", err)
	}
	t.Logf("[blockcount] %s pre-restart: ring_blocks=%d ring_used=%d agg_blocks=%d agg_used=%d",
		victim.Name(), preBlockCount, preUsed, preAggCount, preAggBytes)

	if err := victim.Stop(); err != nil {
		t.Fatalf("stop %s: %v", victim.Name(), err)
	}
	if err := victim.Start(); err != nil {
		t.Fatalf("start %s: %v", victim.Name(), err)
	}
	if err := waitForAPIReady(t, victim, 10*time.Second); err != nil {
		t.Fatalf("%s: API not ready after restart: %v", victim.Name(), err)
	}

	postBlockCount, err := ringScopedStorageBlockCount(t, victim, groupID)
	if err != nil {
		t.Fatalf("post-restart block_count: %v", err)
	}
	postUsed, _, err := ringScopedStorage(t, victim, groupID)
	if err != nil {
		t.Fatalf("post-restart used: %v", err)
	}
	postAggCount, err := aggregateBlockCountAt(t, victim)
	if err != nil {
		t.Fatalf("post-restart agg: %v", err)
	}
	postAggBytes, err := aggregateUsedBytesAt(t, victim)
	if err != nil {
		t.Fatalf("post-restart agg bytes: %v", err)
	}
	if postBlockCount != preBlockCount {
		t.Errorf("[blockcount] ring block_count lost across restart: pre=%d post=%d",
			preBlockCount, postBlockCount)
	}
	if postUsed != preUsed {
		t.Errorf("[blockcount] ring used_bytes lost across restart: pre=%d post=%d",
			preUsed, postUsed)
	}
	if postAggCount != preAggCount {
		t.Errorf("[blockcount] aggregate block_count lost across restart: pre=%d post=%d",
			preAggCount, postAggCount)
	}
	if postAggBytes != preAggBytes {
		t.Errorf("[blockcount] aggregate used_bytes lost across restart: pre=%d post=%d",
			preAggBytes, postAggBytes)
	}

}

func TestScenario_BlockCount_IdempotentPutDoesNotInflateCounter(t *testing.T) {
	c := testrig.NewCluster(t, testrig.StateFreshPublic)
	_ = c.Nodes8()
	c.Setup()

	members := []*testrig.TestNode{
		c.NodeByName("node1"), c.NodeByName("node2"),
		c.NodeByName("node3"), c.NodeByName("node4"),
	}
	createRingAndJoin(t, members, bctRingName)
	stabilizeRing(t, members, bctRingName, len(members)+testrig.DefaultSuccListSize-2)

	baseline := sumAggregateBlockCount(t, members)
	if baseline != 0 {
		t.Fatalf("baseline non-zero: %d", baseline)
	}

	data := make([]byte, 128*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("gen: %v", err)
	}
	rootA, err := members[0].StoreFile(data, bctRingName)
	if err != nil {
		t.Fatalf("first store: %v", err)
	}

	c.WaitForReplicationDrain(15 * time.Second)

	afterFirst := sumAggregateBlockCount(t, members)
	if afterFirst <= baseline {
		t.Fatalf("[blockcount] aggregate did not increase on first store: baseline=%d after=%d",
			baseline, afterFirst)
	}
	flatfsFirst := sumNetworkBlocks(t, members)

	rootB, err := members[0].StoreFile(data, bctRingName)
	if err != nil {
		t.Fatalf("second store: %v", err)
	}
	if rootA != rootB {
		t.Fatalf("content addressing broken: %s vs %s", rootA, rootB)
	}
	c.WaitForReplicationDrain(15 * time.Second)

	afterSecond := sumAggregateBlockCount(t, members)
	if afterSecond != afterFirst {
		t.Errorf("[blockcount] idempotent double-store inflated counter: first=%d second=%d",
			afterFirst, afterSecond)
	}
	flatfsSecond := sumNetworkBlocks(t, members)
	if flatfsSecond != flatfsFirst {
		t.Errorf("[blockcount] idempotent double-store changed flatfs population: first=%d second=%d",
			flatfsFirst, flatfsSecond)
	}
	if afterSecond != int64(flatfsSecond) {
		t.Errorf("[blockcount] counter diverged from flatfs: counter=%d flatfs=%d",
			afterSecond, flatfsSecond)
	} else {
		t.Logf("  [ok] idempotent double-store: counter=%d == flatfs=%d (summed across %d members)",
			afterSecond, flatfsSecond, len(members))
	}
}

func sumAggregateBlockCount(t *testing.T, nodes []*testrig.TestNode) int64 {
	t.Helper()
	var total int64
	for _, n := range nodes {
		v, err := aggregateBlockCountAt(t, n)
		if err != nil {
			t.Fatalf("%s: aggregate block_count: %v", n.Name(), err)
		}
		total += v
	}
	return total
}

func sumNetworkBlocks(t *testing.T, nodes []*testrig.TestNode) int {
	t.Helper()
	var total int
	for _, n := range nodes {
		cids, err := listNetworkBlocksAt(t, n)
		if err != nil {
			t.Fatalf("%s: list blocks: %v", n.Name(), err)
		}
		total += len(cids)
	}
	return total
}

func ringScopedStorageBlockCount(t *testing.T, n *testrig.TestNode, ringID string) (int64, error) {
	t.Helper()
	resp, err := http.Get(nodeAPI(t, n) + "/debug/rings/" + ringID + "/storage")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("status %d: %s", resp.StatusCode, body)
	}
	var out struct {
		BlockCount int64 `json:"block_count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	return out.BlockCount, nil
}

func aggregateBlockCountAt(t *testing.T, n *testrig.TestNode) (int64, error) {
	t.Helper()
	resp, err := http.Get(nodeAPI(t, n) + "/debug/rings")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var out struct {
		AggregateBlockCount int64 `json:"aggregate_block_count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	return out.AggregateBlockCount, nil
}

func aggregateUsedBytesAt(t *testing.T, n *testrig.TestNode) (int64, error) {
	t.Helper()
	resp, err := http.Get(nodeAPI(t, n) + "/debug/rings")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var out struct {
		AggregateUsedBytes int64 `json:"aggregate_used_bytes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	return out.AggregateUsedBytes, nil
}

func listNetworkBlocksAt(t *testing.T, n *testrig.TestNode) ([]string, error) {
	t.Helper()
	resp, err := http.Get(nodeAPI(t, n) + "/debug/network-blocks")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, b)
	}
	var out struct {
		CIDs []string `json:"cids"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.CIDs, nil
}

func waitForAPIReady(t *testing.T, n *testrig.TestNode, deadline time.Duration) error {
	t.Helper()
	base := nodeAPI(t, n)
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		resp, err := http.Get(base + "/debug/rings")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("API at %s not ready within %v", base, deadline)
}
