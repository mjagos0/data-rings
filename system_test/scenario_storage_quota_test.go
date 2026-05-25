//go:build system

package system_test

import (
	"crypto/rand"
	"strings"
	"testing"

	"github.com/mjagos0/datarings/testrig"
)

func TestScenario_StorageQuota_RejectWhenFull(t *testing.T) {
	const quotaBytes = 256 * 1024

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	c.DaemonArgs = []string{"--storage-max", "262144"}
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()

	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	for _, n := range nodes {
		maxBytes, err := n.StorageMax()
		if err != nil {
			t.Fatalf("%s: StorageMax: %v", n.Name(), err)
		}
		if maxBytes != quotaBytes {
			t.Errorf("%s: StorageMaxBytes=%d, want %d", n.Name(), maxBytes, quotaBytes)
		}
	}

	smallData := make([]byte, 1024)
	rand.Read(smallData)
	cidStr, err := nodes[0].StoreFile(smallData, group)
	if err != nil {
		t.Fatalf("store small file: %v", err)
	}
	t.Logf("stored small file: CID=%s", cidStr)

	anyUsed := false
	for _, n := range nodes {
		u, _ := n.StorageUsed()
		if u > 0 {
			anyUsed = true
			break
		}
	}
	if !anyUsed {
		t.Error("no node has non-zero storage after storing a file")
	}

	var storageFull bool
	for i := 0; i < 20; i++ {
		data := make([]byte, 64*1024)
		rand.Read(data)
		_, err := nodes[0].StoreFile(data, group)
		if err != nil {
			if strings.Contains(err.Error(), "storage full") {
				t.Logf("storage full after %d large files: %v", i+1, err)
				storageFull = true
				break
			}
			t.Logf("non-quota error after %d files: %v", i+1, err)
			storageFull = true
			break
		}
	}

	if !storageFull {
		t.Error("expected storage full error after filling 256KB quota with 64KB files, but all writes succeeded")
	}

	var anyNearFull bool
	for _, n := range nodes {
		u, err := n.StorageUsed()
		if err != nil {
			continue
		}
		m, _ := n.StorageMax()
		pct := float64(u) / float64(m) * 100
		t.Logf("  %s: %d / %d bytes (%.0f%%)", n.Name(), u, m, pct)
		if float64(u) > float64(m)*0.5 {
			anyNearFull = true
		}
	}
	if !anyNearFull {
		t.Error("expected at least one node to be >50%% full")
	}
}

func TestScenario_StorageQuota_ReportedInState(t *testing.T) {
	const quotaBytes int64 = 100 * 1024 * 1024

	c := testrig.NewCluster(t, testrig.StateFreshPublic)
	c.DaemonArgs = []string{"--storage-max", "104857600"}
	nodes := c.Nodes8()
	c.Setup()

	for _, n := range nodes {
		s, err := n.PublicState()
		if err != nil {
			t.Fatalf("%s: PublicState: %v", n.Name(), err)
		}
		if s.StorageMaxBytes != quotaBytes {
			t.Errorf("%s: StorageMaxBytes=%d, want %d", n.Name(), s.StorageMaxBytes, quotaBytes)
		}

		if s.StorageUsedBytes > 1024*1024 {
			t.Errorf("%s: StorageUsedBytes=%d, expected <1MB for fresh node", n.Name(), s.StorageUsedBytes)
		}
		t.Logf("  [ok] %s: used=%d max=%d", n.Name(), s.StorageUsedBytes, s.StorageMaxBytes)
	}
}
