//go:build system

package system_test

import (
	"crypto/rand"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/mjagos0/datarings/testrig"
)

const (
	mibibyte		= 1 << 20
	largeReplicationFact	= 3

	leafChunkSize	= 256 * 1024
)

func genRandomFile(t *testing.T, size int) []byte {
	t.Helper()
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		t.Fatalf("crypto/rand: %v", err)
	}
	return buf
}

func sumRingStorage(t *testing.T, nodes []*testrig.TestNode, group string) int64 {
	t.Helper()
	var total int64
	for _, n := range nodes {
		used, err := n.RingStorageUsedBytes(group)
		if err != nil {
			t.Fatalf("%s: RingStorageUsedBytes: %v", n.Name(), err)
		}
		total += used
	}
	return total
}

func sumRingBlockCount(t *testing.T, nodes []*testrig.TestNode, group string) int {
	t.Helper()
	total := 0
	for _, n := range nodes {
		c, err := n.RingBlockCount(group)
		if err != nil {
			t.Fatalf("%s: RingBlockCount: %v", n.Name(), err)
		}
		total += c
	}
	return total
}

func formatBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.2f MiB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.2f KiB", float64(b)/float64(1<<10))
	}
	return fmt.Sprintf("%d B", b)
}

func TestScenario_LargeFile_100MB_TotalRingStorage(t *testing.T) {
	const fileSize = 100 * mibibyte

	fileData := genRandomFile(t, fileSize)

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	rootCID, dagBlocks := buildDAGBlocks(t, fileData)
	var expectedTotal int64
	for _, blk := range dagBlocks {
		expectedTotal += int64(blk.Size) * largeReplicationFact
	}
	expectedBlockInstances := len(dagBlocks) * largeReplicationFact

	cidStr, err := nodes[0].StoreFile(fileData, group)
	if err != nil {
		t.Fatalf("[100mb-store] store file: %v", err)
	}
	if cidStr != rootCID {
		t.Fatalf("[100mb-store] root CID mismatch: got %s, pre-computed %s", cidStr, rootCID)
	}
	t.Logf("[100mb-store] stored 100 MiB CID=%s (%d DAG blocks)", cidStr, len(dagBlocks))

	c.WaitForReplicationDrain(30 * time.Second)

	totalRing := sumRingStorage(t, nodes, group)
	totalBlocks := sumRingBlockCount(t, nodes, group)

	for _, n := range nodes {
		used, _ := n.RingStorageUsedBytes(group)
		count, _ := n.RingBlockCount(group)
		t.Logf("  %s: ring blocks=%d used=%s", n.Name(), count, formatBytes(used))
	}
	t.Logf("[100mb-store] total ring storage: %s (%d block-instances)",
		formatBytes(totalRing), totalBlocks)
	t.Logf("[100mb-store] expected exactly:   %s (%d DAG blocks × k=%d)",
		formatBytes(expectedTotal), len(dagBlocks), largeReplicationFact)

	if totalRing != expectedTotal {
		t.Errorf("[100mb-store] total ring storage = %d (%s), want exactly %d (%s)",
			totalRing, formatBytes(totalRing), expectedTotal, formatBytes(expectedTotal))
	}
	if totalBlocks != expectedBlockInstances {
		t.Errorf("[100mb-store] total block-instances = %d, want exactly %d (%d DAG blocks × k=%d)",
			totalBlocks, expectedBlockInstances, len(dagBlocks), largeReplicationFact)
	}
}

func TestScenario_LargeFile_500MB_EvenDistribution(t *testing.T) {
	const fileSize = 500 * mibibyte

	const altTmp = "/home/mjagos/data-rings-tests-tmp"
	if err := os.MkdirAll(altTmp, 0755); err != nil {
		t.Fatalf("create alt TMPDIR %s: %v", altTmp, err)
	}
	t.Setenv("TMPDIR", altTmp)

	fileData := genRandomFile(t, fileSize)

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	rootCID, dagBlocks := buildDAGBlocks(t, fileData)
	var expectedTotalBytes int64
	for _, blk := range dagBlocks {
		expectedTotalBytes += int64(blk.Size) * largeReplicationFact
	}
	expectedTotalInstances := len(dagBlocks) * largeReplicationFact

	cidStr, err := nodes[0].StoreFile(fileData, group)
	if err != nil {
		t.Fatalf("[500mb-dist] store file: %v", err)
	}
	if cidStr != rootCID {
		t.Fatalf("[500mb-dist] root CID mismatch: got %s, pre-computed %s", cidStr, rootCID)
	}
	t.Logf("[500mb-dist] stored 500 MiB CID=%s (%d DAG blocks)", cidStr, len(dagBlocks))

	c.WaitForReplicationDrain(60 * time.Second)

	type nodeStats struct {
		name	string
		blocks	int
		bytes	int64
	}
	stats := make([]nodeStats, 0, len(nodes))
	var totalBlocks int
	var totalBytes int64
	for _, n := range nodes {
		blocks, err := n.RingBlockCount(group)
		if err != nil {
			t.Fatalf("%s: RingBlockCount: %v", n.Name(), err)
		}
		bytes, err := n.RingStorageUsedBytes(group)
		if err != nil {
			t.Fatalf("%s: RingStorageUsedBytes: %v", n.Name(), err)
		}
		stats = append(stats, nodeStats{n.Name(), blocks, bytes})
		totalBlocks += blocks
		totalBytes += bytes
	}

	meanBlocks := float64(totalBlocks) / float64(len(nodes))
	meanBytes := float64(totalBytes) / float64(len(nodes))

	t.Logf("[500mb-dist] totals  : blocks=%d bytes=%s", totalBlocks, formatBytes(totalBytes))
	t.Logf("[500mb-dist] expected: blocks=%d bytes=%s",
		expectedTotalInstances, formatBytes(expectedTotalBytes))
	t.Logf("[500mb-dist] means   : blocks=%.0f bytes=%s", meanBlocks, formatBytes(int64(meanBytes)))

	if totalBytes != expectedTotalBytes {
		t.Errorf("[500mb-dist] total bytes = %d (%s), want exactly %d (%s)",
			totalBytes, formatBytes(totalBytes), expectedTotalBytes, formatBytes(expectedTotalBytes))
	}
	if totalBlocks != expectedTotalInstances {
		t.Errorf("[500mb-dist] total block-instances = %d, want exactly %d (%d DAG blocks × k=%d)",
			totalBlocks, expectedTotalInstances, len(dagBlocks), largeReplicationFact)
	}

	const tolerance = 0.20
	for _, s := range stats {
		blockDev := math.Abs(float64(s.blocks)-meanBlocks) / meanBlocks
		bytesDev := math.Abs(float64(s.bytes)-meanBytes) / meanBytes
		t.Logf("  %s: blocks=%d (Δ%.1f%%) bytes=%s (Δ%.1f%%)",
			s.name, s.blocks, blockDev*100, formatBytes(s.bytes), bytesDev*100)
		if blockDev > tolerance {
			t.Errorf("[500mb-dist] %s: block count %d deviates %.1f%% from mean %.0f (limit ±%.0f%%)",
				s.name, s.blocks, blockDev*100, meanBlocks, tolerance*100)
		}
		if bytesDev > tolerance {
			t.Errorf("[500mb-dist] %s: bytes %d deviates %.1f%% from mean %.0f (limit ±%.0f%%)",
				s.name, s.bytes, bytesDev*100, meanBytes, tolerance*100)
		}
	}
}

func TestScenario_LargeFile_100MB_DurabilityUnderChurn(t *testing.T) {
	const fileSize = 100 * mibibyte

	fileData := genRandomFile(t, fileSize)

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	cidStr, err := nodes[0].StoreFile(fileData, group)
	if err != nil {
		t.Fatalf("[churn] store file: %v", err)
	}
	t.Logf("[churn] stored 100 MiB CID=%s", cidStr)

	expectedReplicated := int64(fileSize) * largeReplicationFact

	initial := sumRingStorage(t, nodes, group)
	t.Logf("[churn] initial total ring storage: %s (expected ~%s)",
		formatBytes(initial), formatBytes(expectedReplicated))
	if initial < expectedReplicated {
		t.Fatalf("[churn] initial total %s < expected %s — file did not fully replicate",
			formatBytes(initial), formatBytes(expectedReplicated))
	}

	victims := []string{"node3", "node5", "node7", "node4"}
	for i, name := range victims {
		victim := c.NodeByName(name)
		t.Logf("[churn-cycle-%d] killing %s", i+1, victim.Name())
		if err := victim.Stop(); err != nil {
			t.Fatalf("stop %s: %v", victim.Name(), err)
		}

		c.StabilizePrivate(group, testrig.DefaultSuccListSize-1)

		t.Logf("[churn-cycle-%d] restarting %s", i+1, victim.Name())
		if err := victim.Start(); err != nil {
			t.Fatalf("restart %s: %v", victim.Name(), err)
		}

		c.Stabilize(len(nodes) + testrig.DefaultSuccListSize - 2)
		c.RepublishSelf()
		_, _ = victim.Exec("ring", "leave", group)
		if out, err := victim.Exec("ring", "join", c.GroupKey(), group); err != nil {
			t.Fatalf("[churn-cycle-%d] %s rejoin: %v\n%s", i+1, victim.Name(), err, out)
		}
		c.StabilizePrivate(group, testrig.DefaultSuccListSize-1)
	}

	t.Log("[churn] running 30 extra stabilization rounds")
	c.StabilizePrivate(group, 30)

	t.Log("[churn] running GC on every node")
	for _, n := range nodes {
		if out, err := n.Exec("gc"); err != nil {
			t.Errorf("[churn] gc on %s: %v\n%s", n.Name(), err, out)
		}
	}

	final := sumRingStorage(t, nodes, group)
	for _, n := range nodes {
		used, _ := n.RingStorageUsedBytes(group)
		count, _ := n.RingBlockCount(group)
		t.Logf("  %s: ring blocks=%d used=%s", n.Name(), count, formatBytes(used))
	}
	t.Logf("[churn] final total ring storage: %s (initial %s, expected ~%s)",
		formatBytes(final), formatBytes(initial), formatBytes(expectedReplicated))

	const tol = 0.05
	minBytes := int64(float64(expectedReplicated) * (1 - tol))
	maxBytes := int64(float64(expectedReplicated) * (1 + tol))
	if final < minBytes {
		t.Errorf("[churn] final %s < min %s — churn dropped data",
			formatBytes(final), formatBytes(minBytes))
	}
	if final > maxBytes {
		t.Errorf("[churn] final %s > max %s — churn produced over-replication that GC did not clean up",
			formatBytes(final), formatBytes(maxBytes))
	}

	for _, n := range nodes {
		if err := n.FetchCID(cidStr, group); err != nil {
			t.Errorf("[churn] %s: fetch failed after churn+GC: %v", n.Name(), err)
		}
	}
}
