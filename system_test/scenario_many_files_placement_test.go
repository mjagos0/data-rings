//go:build system

package system_test

import (
	"fmt"
	"testing"

	"github.com/mjagos0/datarings/testrig"
)

func TestScenario_ManyFiles_ExactPlacement_BulkDelete(t *testing.T) {
	const (
		k		= 3
		numFiles	= 5
	)

	type fileInfo struct {
		data		[]byte
		rootCID		string
		blocks		[]blockInfo
		storNode	int
	}

	files := make([]fileInfo, numFiles)
	for i := 0; i < numFiles; i++ {

		data := make([]byte, 1024)
		for j := range data {
			data[j] = byte((i*37 + j*13) & 0xFF)
		}

		copy(data, []byte(fmt.Sprintf("file-%d-scenario-8-many-files-exact-", i)))

		rootCID, blocks := buildDAGBlocks(t, data)

		if len(blocks) != 2 {
			t.Fatalf("file %d: expected 2 DAG blocks, got %d", i, len(blocks))
		}

		files[i] = fileInfo{
			data:		data,
			rootCID:	rootCID,
			blocks:		blocks,
			storNode:	i % numFiles,
		}
		t.Logf("[many-8] file[%d]: root=%s, blocks=%d, publisher=node%d",
			i, testrig.NodeIDHexToShort(rootCID), len(blocks), files[i].storNode+1)
		for j, blk := range blocks {
			kind := "internal"
			if blk.IsLeaf {
				kind = "leaf"
			}
			t.Logf("[many-8]   block[%d.%d]: CID=%s size=%d type=%s",
				i, j, testrig.NodeIDHexToShort(blk.CID), blk.Size, kind)
		}
	}

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	c.DaemonArgs = []string{"--storage-max", "104857600"}
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	expectedBlockMHs := make(map[string]map[string]bool)
	expectedBlockCount := make(map[string]int)
	expectedBytes := make(map[string]int64)
	expectedRoots := make(map[string]map[string]bool)

	for _, n := range nodes {
		expectedBlockMHs[n.Name()] = make(map[string]bool)
		expectedRoots[n.Name()] = make(map[string]bool)
	}

	for _, f := range files {
		for _, blk := range f.blocks {
			replicas := c.ReplicaNodesForHash(blk.RingKey, k)
			for _, n := range replicas {
				expectedBlockMHs[n.Name()][blk.MultihashHex] = true
				expectedBlockCount[n.Name()]++
				expectedBytes[n.Name()] += int64(blk.Size)
				expectedRoots[n.Name()][f.rootCID] = true
			}
		}
	}

	for _, n := range nodes {
		t.Logf("[many-8] expected %s: %d blocks, %d bytes, %d roots",
			n.Name(), expectedBlockCount[n.Name()], expectedBytes[n.Name()],
			len(expectedRoots[n.Name()]))
	}

	storedCIDs := make([]string, numFiles)
	for i, f := range files {
		publisher := nodes[f.storNode]
		cidStr, err := publisher.StoreFile(f.data, group)
		if err != nil {
			t.Fatalf("store file %d from %s: %v", i, publisher.Name(), err)
		}
		storedCIDs[i] = cidStr
		t.Logf("[many-8] stored file[%d]: CID=%s from %s", i, cidStr, publisher.Name())

		if cidStr != f.rootCID {
			t.Fatalf("[many-8] file[%d] root CID mismatch: got %s, pre-computed %s",
				i, cidStr, f.rootCID)
		}
	}

	t.Log("[many-8] verifying network block placement per node")
	for _, n := range nodes {
		actualCIDs, err := n.NetworkBlockCIDs()
		if err != nil {
			t.Fatalf("%s: NetworkBlockCIDs: %v", n.Name(), err)
		}

		actualMHSet := make(map[string]bool, len(actualCIDs))
		for _, c := range actualCIDs {
			actualMHSet[cidToMultihashHex(c)] = true
		}
		expMHSet := expectedBlockMHs[n.Name()]

		for mh := range expMHSet {
			if !actualMHSet[mh] {
				t.Errorf("[many-8] %s: expected network block mh=%s not found",
					n.Name(), testrig.NodeIDHexToShort(mh))
			}
		}

		for _, c := range actualCIDs {
			mh := cidToMultihashHex(c)
			if !expMHSet[mh] {
				t.Errorf("[many-8] %s: unexpected network block %s (mh=%s)",
					n.Name(), testrig.NodeIDHexToShort(c), testrig.NodeIDHexToShort(mh))
			}
		}

		expCount := expectedBlockCount[n.Name()]
		if len(actualCIDs) != expCount {
			t.Errorf("[many-8] %s: network block count=%d, want %d",
				n.Name(), len(actualCIDs), expCount)
		} else {
			t.Logf("  [ok] %s: %d network blocks", n.Name(), len(actualCIDs))
		}
	}

	t.Log("[many-8] verifying storage used per node")
	for _, n := range nodes {
		actual, err := n.StorageUsed()
		if err != nil {
			t.Fatalf("%s: StorageUsed: %v", n.Name(), err)
		}
		exp := expectedBytes[n.Name()]
		if actual != exp {
			t.Errorf("[many-8] %s: storage_used_bytes=%d, want %d", n.Name(), actual, exp)
		} else {
			t.Logf("  [ok] %s: storage=%d bytes", n.Name(), actual)
		}
	}

	t.Log("[many-8] verifying exact network roots per node")
	for _, n := range nodes {
		roots, err := n.NetworkRoots()
		if err != nil {
			t.Fatalf("%s: NetworkRoots: %v", n.Name(), err)
		}

		actualRootSet := make(map[string]bool, len(roots))
		for _, r := range roots {
			actualRootSet[r] = true
		}
		expRootSet := expectedRoots[n.Name()]

		for rootCID := range expRootSet {
			if !actualRootSet[rootCID] {
				t.Errorf("[many-8] %s: missing network root %s",
					n.Name(), testrig.NodeIDHexToShort(rootCID))
			}
		}

		for _, rootCID := range roots {
			if !expRootSet[rootCID] {
				t.Errorf("[many-8] %s: unexpected extra network root %s",
					n.Name(), testrig.NodeIDHexToShort(rootCID))
			}
		}

		if len(roots) != len(expRootSet) {
			t.Errorf("[many-8] %s: network root count=%d, want exactly %d",
				n.Name(), len(roots), len(expRootSet))
		} else {
			t.Logf("  [ok] %s: exactly %d network roots", n.Name(), len(roots))
		}
	}

	t.Log("[many-8] verifying fetch from all nodes for all files")
	for i, f := range files {
		for _, n := range nodes {
			if err := n.FetchCID(f.rootCID, group); err != nil {
				t.Errorf("[many-8] %s: fetch file[%d] CID=%s failed: %v",
					n.Name(), i, testrig.NodeIDHexToShort(f.rootCID), err)
			}
		}
	}

	t.Log("[many-8] deleting all file CIDs from network")
	for i, f := range files {
		publisher := nodes[f.storNode]
		if err := publisher.DeleteCID(f.rootCID, group); err != nil {
			t.Fatalf("delete file[%d] CID: %v", i, err)
		}
	}

	for _, n := range nodes {
		for _, f := range files {
			n.Exec("rm", f.rootCID)
		}
	}

	t.Log("[many-8] running GC on all nodes")
	for _, n := range nodes {
		out, err := n.Exec("gc")
		if err != nil {
			t.Fatalf("gc on %s: %v\n%s", n.Name(), err, out)
		}
		t.Logf("  %s: %s", n.Name(), out)
	}

	t.Log("[many-8] verifying network roots cleared")
	allRootCIDs := make(map[string]bool, numFiles)
	for _, f := range files {
		allRootCIDs[f.rootCID] = true
	}

	for _, n := range nodes {
		roots, err := n.NetworkRoots()
		if err != nil {
			t.Logf("%s: NetworkRoots: %v", n.Name(), err)
			continue
		}
		for _, r := range roots {
			if allRootCIDs[r] {
				t.Errorf("[many-8] %s: still has network root %s after delete+GC",
					n.Name(), testrig.NodeIDHexToShort(r))
			}
		}
	}

	t.Log("[many-8] verifying network blocks cleared")
	allBlockMHs := make(map[string]bool)
	for _, f := range files {
		for _, blk := range f.blocks {
			allBlockMHs[blk.MultihashHex] = true
		}
	}

	for _, n := range nodes {
		netBlocks, err := n.NetworkBlockCIDs()
		if err != nil {
			t.Logf("%s: NetworkBlockCIDs: %v", n.Name(), err)
			continue
		}
		for _, c := range netBlocks {
			mh := cidToMultihashHex(c)
			if allBlockMHs[mh] {
				t.Errorf("[many-8] %s: still holds network block %s after delete+GC",
					n.Name(), testrig.NodeIDHexToShort(c))
			}
		}
		if len(netBlocks) == 0 {
			t.Logf("  [ok] %s: 0 network blocks", n.Name())
		}
	}

	t.Log("[many-8] verifying storage is zero")
	for _, n := range nodes {
		used, err := n.StorageUsed()
		if err != nil {
			t.Logf("%s: StorageUsed: %v", n.Name(), err)
			continue
		}
		if used != 0 {
			t.Errorf("[many-8] %s: storage_used_bytes=%d after delete+GC, want 0",
				n.Name(), used)
		} else {
			t.Logf("  [ok] %s: storage=0", n.Name())
		}
	}

	t.Log("[many-8] verifying no file is fetchable")
	unfetchable := 0
	for i, f := range files {
		for _, n := range nodes {
			err := n.FetchCID(f.rootCID, group)
			if err != nil {
				unfetchable++
			} else {
				t.Errorf("[many-8] %s: fetch file[%d] SUCCEEDED after delete+GC",
					n.Name(), i)
			}
		}
	}
	totalChecks := numFiles * len(nodes)
	if unfetchable == totalChecks {
		t.Logf("[many-8] all %d fetch attempts correctly failed (%d files × %d nodes)",
			totalChecks, numFiles, len(nodes))
	}
}
