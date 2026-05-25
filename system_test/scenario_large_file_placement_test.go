//go:build system

package system_test

import (
	"crypto/rand"
	"sort"
	"testing"

	"github.com/mjagos0/datarings/testrig"
)

func TestScenario_LargeFile_ExactPlacement_DeleteGC(t *testing.T) {
	const (
		k		= 3
		chunkSize	= 262144
		numChunks	= 3
		fileSize	= chunkSize * numChunks
	)

	fileData := make([]byte, fileSize)

	if _, err := rand.Read(fileData); err != nil {
		t.Fatalf("generate file data: %v", err)
	}

	precomputedRootCID, dagBlocks := buildDAGBlocks(t, fileData)
	t.Logf("[large-7] pre-computed DAG: root=%s, blocks=%d", precomputedRootCID, len(dagBlocks))

	var totalLeafBytes int
	for i, blk := range dagBlocks {
		kind := "internal"
		if blk.IsLeaf {
			kind = "leaf"
			totalLeafBytes += blk.Size
		}
		t.Logf("[large-7]   block[%d]: CID=%s ringKey=%s size=%d type=%s",
			i, testrig.NodeIDHexToShort(blk.CID), testrig.NodeIDHexToShort(blk.RingKey), blk.Size, kind)
	}

	if len(dagBlocks) != 5 {
		t.Fatalf("[large-7] expected 5 DAG blocks, got %d", len(dagBlocks))
	}
	if totalLeafBytes != fileSize {
		t.Fatalf("[large-7] leaf bytes=%d, expected %d", totalLeafBytes, fileSize)
	}

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)

	c.DaemonArgs = []string{"--storage-max", "104857600"}
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	publisher := nodes[0]

	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	expected := computeExpectedPlacement(t, c, dagBlocks, k)

	for _, n := range nodes {
		exp := expected[n.Name()]
		t.Logf("[large-7] expected %s: %d blocks, %d bytes",
			n.Name(), exp.Count, exp.Bytes)
	}

	nodesWithRoot := make(map[string]bool)
	for _, n := range nodes {
		if expected[n.Name()].Count > 0 {
			nodesWithRoot[n.Name()] = true
		}
	}

	cidStr, err := publisher.StoreFile(fileData, group)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	t.Logf("[large-7] stored CID=%s from %s", cidStr, publisher.Name())

	if cidStr != precomputedRootCID {
		t.Fatalf("[large-7] root CID mismatch: got %s, pre-computed %s", cidStr, precomputedRootCID)
	}
	t.Logf("[large-7] root CID matches pre-computation")

	t.Log("[large-7] verifying network block placement per node")
	verifyNetworkBlockPlacement(t, nodes, expected, publisher.Name())

	t.Log("[large-7] verifying storage used per node")
	for _, n := range nodes {
		exp := expected[n.Name()]
		actual, err := n.StorageUsed()
		if err != nil {
			t.Fatalf("%s: StorageUsed: %v", n.Name(), err)
		}
		if actual != exp.Bytes {
			t.Errorf("[large-7] %s: storage_used_bytes=%d, want %d", n.Name(), actual, exp.Bytes)
		} else {
			t.Logf("  [ok] %s: storage=%d bytes", n.Name(), actual)
		}
	}

	t.Log("[large-7] verifying exact network roots per node")
	for _, n := range nodes {
		roots, err := n.NetworkRoots()
		if err != nil {
			t.Fatalf("%s: NetworkRoots: %v", n.Name(), err)
		}

		shouldHave := nodesWithRoot[n.Name()]

		if shouldHave {

			found := false
			for _, r := range roots {
				if r == cidStr {
					found = true
				} else {
					t.Errorf("[large-7] %s: unexpected extra network root %s (expected only %s)",
						n.Name(), testrig.NodeIDHexToShort(r), testrig.NodeIDHexToShort(cidStr))
				}
			}
			if !found {
				t.Errorf("[large-7] %s: expected network root %s not found among %d roots",
					n.Name(), testrig.NodeIDHexToShort(cidStr), len(roots))
			}
			if len(roots) != 1 {
				t.Errorf("[large-7] %s: network root count=%d, want exactly 1", n.Name(), len(roots))
			} else {
				t.Logf("  [ok] %s: exactly 1 network root (correct)", n.Name())
			}
		} else {

			if len(roots) != 0 {
				t.Errorf("[large-7] %s: expected 0 network roots, got %d: %v",
					n.Name(), len(roots), roots)
			} else {
				t.Logf("  [ok] %s: 0 network roots (correct)", n.Name())
			}
		}
	}

	t.Log("[large-7] verifying fetch from all nodes")
	for _, n := range nodes {
		if err := n.FetchCID(cidStr, group); err != nil {
			t.Errorf("[large-7] %s: fetch failed: %v", n.Name(), err)
		}
	}

	t.Log("[large-7] deleting CID from network")
	if err := publisher.DeleteCID(cidStr, group); err != nil {
		t.Fatalf("delete-cid: %v", err)
	}

	for _, n := range nodes {
		n.Exec("rm", cidStr)
	}

	t.Log("[large-7] running GC on all nodes")
	for _, n := range nodes {
		out, err := n.Exec("gc")
		if err != nil {
			t.Fatalf("gc on %s: %v\n%s", n.Name(), err, out)
		}
		t.Logf("  %s: %s", n.Name(), out)
	}

	t.Log("[large-7] verifying network roots cleared")
	for _, n := range nodes {
		roots, err := n.NetworkRoots()
		if err != nil {
			t.Logf("%s: NetworkRoots: %v", n.Name(), err)
			continue
		}
		for _, r := range roots {
			if r == cidStr {
				t.Errorf("[large-7] %s: still has network root %s after delete+GC", n.Name(), testrig.NodeIDHexToShort(cidStr))
			}
		}
	}

	t.Log("[large-7] verifying network blocks cleared")
	blockMHSet := make(map[string]bool, len(dagBlocks))
	for _, blk := range dagBlocks {
		blockMHSet[blk.MultihashHex] = true
	}

	for _, n := range nodes {
		netBlocks, err := n.NetworkBlockCIDs()
		if err != nil {
			t.Logf("%s: NetworkBlockCIDs: %v", n.Name(), err)
			continue
		}
		for _, c := range netBlocks {
			mh := cidToMultihashHex(c)
			if blockMHSet[mh] {
				t.Errorf("[large-7] %s: still holds network block %s after delete+GC", n.Name(), testrig.NodeIDHexToShort(c))
			}
		}
		if len(netBlocks) == 0 {
			t.Logf("  [ok] %s: 0 network blocks", n.Name())
		}
	}

	t.Log("[large-7] verifying storage is zero (aggregate + per-ring)")
	for _, n := range nodes {
		used, err := n.StorageUsed()
		if err != nil {
			t.Logf("%s: StorageUsed: %v", n.Name(), err)
			continue
		}
		if used != 0 {
			t.Errorf("[large-7] %s: storage_used_bytes=%d after delete+GC, want 0", n.Name(), used)
		}
		ringUsed, err := n.RingStorageUsedBytes(group)
		if err != nil {
			t.Logf("%s: RingStorageUsedBytes(%s): %v", n.Name(), group, err)
			continue
		}
		if ringUsed != 0 {
			t.Errorf("[large-7] %s: ring %s used=%d after delete+GC, want 0",
				n.Name(), group, ringUsed)
		}
		ringRoots, err := n.RingNetworkRootCount(group)
		if err == nil && ringRoots != 0 {
			t.Errorf("[large-7] %s: ring %s root count=%d after delete+GC, want 0",
				n.Name(), group, ringRoots)
		}
		if used == 0 && ringUsed == 0 {
			t.Logf("  [ok] %s: aggregate=0 ring=0", n.Name())
		}
	}

	t.Log("[large-7] verifying CID is not fetchable")

	fetchFailed := 0
	for _, n := range nodes {
		err := n.FetchCID(cidStr, group)
		if err != nil {
			fetchFailed++
		} else {
			t.Errorf("[large-7] %s: fetch SUCCEEDED after delete+GC — file should be gone", n.Name())
		}
	}
	if fetchFailed == len(nodes) {
		t.Logf("[large-7] CID correctly unfetchable from all %d nodes", len(nodes))
	}
}

func TestScenario_LargeFile_ReplicaConsistency(t *testing.T) {
	const (
		k		= 3
		chunkSize	= 262144
		numChunks	= 3
		fileSize	= chunkSize * numChunks
	)

	fileData := make([]byte, fileSize)
	if _, err := rand.Read(fileData); err != nil {
		t.Fatalf("generate file data: %v", err)
	}

	_, dagBlocks := buildDAGBlocks(t, fileData)
	totalBlocks := len(dagBlocks)

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	c.DaemonArgs = []string{"--storage-max", "104857600"}
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	cidStr, err := nodes[0].StoreFile(fileData, group)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	t.Logf("[replica-7b] stored CID=%s, %d DAG blocks", cidStr, totalBlocks)

	assignment := c.Assignment()
	for _, blk := range dagBlocks {
		replicas := c.ReplicaNodesForHash(blk.RingKey, k)
		if len(replicas) != k {
			t.Errorf("block %s: got %d replicas, want %d",
				testrig.NodeIDHexToShort(blk.CID), len(replicas), k)
		}

		seen := make(map[string]bool)
		for _, r := range replicas {
			if seen[r.Name()] {
				t.Errorf("block %s: duplicate replica %s", testrig.NodeIDHexToShort(blk.CID), r.Name())
			}
			seen[r.Name()] = true
		}

		replicaNames := testrig.NodeNamesFromSlice(replicas)
		t.Logf("  block %s → replicas %v (ring key %s)",
			testrig.NodeIDHexToShort(blk.CID), replicaNames,
			testrig.NodeIDHexToShort(blk.RingKey))

		for _, r := range replicas {
			has, err := r.HasNetworkBlock(blk.CID)
			if err != nil {
				t.Fatalf("%s: HasNetworkBlock: %v", r.Name(), err)
			}
			if !has {
				t.Errorf("%s: replica for block %s does not hold it",
					r.Name(), testrig.NodeIDHexToShort(blk.CID))
			}
		}

		replicaNameSet := make(map[string]bool, len(replicas))
		for _, r := range replicas {
			replicaNameSet[r.Name()] = true
		}
		publisher := nodes[0].Name()
		for _, n := range nodes {
			if replicaNameSet[n.Name()] || n.Name() == publisher {
				continue
			}
			has, err := n.HasNetworkBlock(blk.CID)
			if err != nil {
				t.Fatalf("%s: HasNetworkBlock: %v", n.Name(), err)
			}
			if has {
				t.Errorf("[replica-7b] %s: non-replica holds block %s (should only be on %v)",
					n.Name(), testrig.NodeIDHexToShort(blk.CID), replicaNames)
			}
		}

		replicaIDs := make([]string, len(replicas))
		for i, r := range replicas {
			replicaIDs[i] = r.ID()
		}
		sortedNodeIDs := make([]string, len(nodes))
		for i, n := range nodes {
			sortedNodeIDs[i] = assignment.NodeToID[n.Name()]
		}
		sort.Strings(sortedNodeIDs)

		primaryIdx := -1
		for i, id := range sortedNodeIDs {
			if id == replicaIDs[0] {
				primaryIdx = i
				break
			}
		}
		if primaryIdx == -1 {
			t.Fatalf("primary %s not found in sorted ring", replicaIDs[0])
		}
		for j := 1; j < k; j++ {
			expectedIdx := (primaryIdx + j) % len(sortedNodeIDs)
			if sortedNodeIDs[expectedIdx] != replicaIDs[j] {
				t.Errorf("block %s: replica[%d]=%s, expected ring position %d=%s",
					testrig.NodeIDHexToShort(blk.CID), j,
					testrig.NodeIDHexToShort(replicaIDs[j]),
					expectedIdx,
					testrig.NodeIDHexToShort(sortedNodeIDs[expectedIdx]))
			}
		}
	}

	totalInstances := 0
	for _, n := range nodes {
		cids, err := n.NetworkBlockCIDs()
		if err != nil {
			t.Fatalf("%s: NetworkBlockCIDs: %v", n.Name(), err)
		}
		totalInstances += len(cids)
		t.Logf("  %s: %d network blocks", n.Name(), len(cids))
	}
	expectedInstances := totalBlocks * k
	if totalInstances != expectedInstances {
		t.Errorf("[replica-7b] total block instances=%d, want %d (%d blocks × %d replicas)",
			totalInstances, expectedInstances, totalBlocks, k)
	} else {
		t.Logf("[replica-7b] total block instances: %d = %d×%d (correct)", totalInstances, totalBlocks, k)
	}
}
