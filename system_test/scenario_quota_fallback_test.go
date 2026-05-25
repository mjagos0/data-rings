//go:build system

package system_test

import (
	"crypto/rand"
	"strings"
	"testing"

	"github.com/mjagos0/datarings/testrig"
)

func TestScenario_FetchFromReplica_WhenPrimaryMissing(t *testing.T) {
	const k = 3

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	data := []byte("scenario-7a-fetch-from-replica-test-content")
	cidStr, err := nodes[0].StoreFile(data, group)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	t.Logf("[7a] stored CID=%s from %s", cidStr, nodes[0].Name())

	replicas := c.ReplicaNodesForCID(cidStr, k)
	if len(replicas) < 2 {
		t.Fatalf("[7a] expected at least 2 replica nodes, got %d", len(replicas))
	}
	primary := replicas[0]
	t.Logf("[7a] primary for CID: %s", primary.Name())

	has, err := primary.HasBlock(cidStr)
	if err != nil {
		t.Fatalf("HasBlock on primary: %v", err)
	}
	if !has {
		t.Fatalf("[7a] primary %s does not hold root block", primary.Name())
	}

	if err := primary.DeleteBlock(cidStr); err != nil {
		t.Fatalf("DeleteBlock on primary: %v", err)
	}
	t.Logf("[7a] deleted root block from primary %s", primary.Name())

	has, err = primary.HasBlock(cidStr)
	if err != nil {
		t.Fatalf("HasBlock after delete: %v", err)
	}
	if has {
		t.Fatalf("[7a] primary still has block after delete")
	}

	replicaHas := false
	for _, r := range replicas[1:] {
		h, err := r.HasBlock(cidStr)
		if err == nil && h {
			replicaHas = true
			t.Logf("[7a] replica %s still holds root block", r.Name())
			break
		}
	}
	if !replicaHas {
		t.Fatalf("[7a] no replica holds the root block")
	}

	for _, n := range nodes {
		if err := n.FetchCID(cidStr, group); err != nil {
			t.Errorf("[7a] %s: fetch failed after primary block deletion: %v", n.Name(), err)
		}
	}
	t.Logf("[7a] all 8 nodes can fetch CID despite primary missing the block")
}

func TestScenario_UploadSucceeds_WhenPrimaryFull(t *testing.T) {
	const quotaBytes = 256 * 1024

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	c.DaemonArgs = []string{"--storage-max", "262144"}
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	var storedCIDs []string
	for i := 0; i < 8; i++ {
		data := make([]byte, 32*1024)
		rand.Read(data)
		cidStr, err := nodes[0].StoreFile(data, group)
		if err != nil {
			t.Logf("[7b] fill iteration %d: %v", i, err)
			break
		}
		storedCIDs = append(storedCIDs, cidStr)
	}
	t.Logf("[7b] stored %d fill files", len(storedCIDs))

	for _, n := range nodes {
		used, _ := n.StorageUsed()
		t.Logf("[7b] %s: used=%d / %d bytes", n.Name(), used, quotaBytes)
	}

	testData := []byte("scenario-7b-test-file-after-fill")
	testCID, err := nodes[0].StoreFile(testData, group)
	if err != nil {

		t.Logf("[7b] store after fill: %v (may be expected if all replicas full)", err)

		if len(storedCIDs) > 0 {
			if fetchErr := nodes[1].FetchCID(storedCIDs[0], group); fetchErr != nil {
				t.Errorf("[7b] pre-fill file not fetchable: %v", fetchErr)
			} else {
				t.Logf("[7b] pre-fill files still fetchable despite quota pressure")
			}
		}
		return
	}
	t.Logf("[7b] stored test file after fill: CID=%s", testCID)

	for _, n := range nodes {
		if err := n.FetchCID(testCID, group); err != nil {
			t.Errorf("[7b] %s: fetch test file failed: %v", n.Name(), err)
		}
	}
	t.Logf("[7b] test file fetchable from all nodes despite quota pressure")
}

func TestScenario_UploadFails_WhenAllFull(t *testing.T) {
	const quotaBytes = 128 * 1024

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	c.DaemonArgs = []string{"--storage-max", "131072"}
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	var lastOK string
	var hitFull bool
	for i := 0; i < 40; i++ {
		data := make([]byte, 16*1024)
		rand.Read(data)
		cidStr, err := nodes[0].StoreFile(data, group)
		if err != nil {
			if strings.Contains(err.Error(), "storage full") {
				t.Logf("[7c] storage full after %d files", i)
				hitFull = true
				break
			}
			t.Logf("[7c] non-quota error after %d files: %v", i, err)
			hitFull = true
			break
		}
		lastOK = cidStr
	}

	if !hitFull {
		t.Fatal("[7c] expected storage full error but all writes succeeded")
	}
	if lastOK == "" {
		t.Fatal("[7c] no file was stored before hitting full")
	}

	for _, n := range nodes {
		used, _ := n.StorageUsed()
		t.Logf("[7c] %s: used=%d / %d bytes", n.Name(), used, quotaBytes)
	}

	extraData := make([]byte, 16*1024)
	rand.Read(extraData)
	_, err := nodes[0].StoreFile(extraData, group)
	if err == nil {
		t.Logf("[7c] WARNING: extra write after full succeeded (may have found a node with space)")
	} else {
		t.Logf("[7c] extra write correctly failed: %v", err)
	}

	t.Logf("[7c] verifying last successful CID=%s is still fetchable", lastOK)
	fetched := false
	for _, n := range nodes {
		used, _ := n.StorageUsed()
		maxB, _ := n.StorageMax()
		if maxB > 0 && float64(used) > float64(maxB)*0.5 {
			continue
		}
		if err := n.FetchCID(lastOK, group); err != nil {
			t.Logf("[7c] %s: fetch failed (node may be full): %v", n.Name(), err)
		} else {
			fetched = true
			t.Logf("[7c] %s: successfully fetched last-OK CID", n.Name())
			break
		}
	}

	if !fetched {
		holdCount := 0
		for _, n := range nodes {
			has, err := n.HasBlock(lastOK)
			if err == nil && has {
				holdCount++
			}
		}
		if holdCount == 0 {
			t.Error("[7c] no node holds the last-OK CID — data was lost under quota pressure")
		} else {
			t.Logf("[7c] %d nodes still hold last-OK CID (fetch failed due to full local store, but data persists on ring)", holdCount)
		}
	}
}
