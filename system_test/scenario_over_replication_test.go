//go:build system

package system_test

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/mjagos0/datarings/testrig"
)

func TestScenario_NoOverReplication_AfterAbruptLeave(t *testing.T) {
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
	totalBlocks := len(dagBlocks)
	t.Logf("[over-repl] pre-computed DAG: root=%s, blocks=%d", precomputedRootCID, totalBlocks)

	for i, blk := range dagBlocks {
		kind := "internal"
		if blk.IsLeaf {
			kind = "leaf"
		}
		t.Logf("[over-repl]   block[%d]: CID=%s ringKey=%s size=%d type=%s",
			i, testrig.NodeIDHexToShort(blk.CID), testrig.NodeIDHexToShort(blk.RingKey), blk.Size, kind)
	}

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	publisher := nodes[0]

	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	cidStr, err := publisher.StoreFile(fileData, group)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	t.Logf("[over-repl] stored CID=%s from %s", cidStr, publisher.Name())

	if cidStr != precomputedRootCID {
		t.Fatalf("[over-repl] root CID mismatch: got %s, pre-computed %s", cidStr, precomputedRootCID)
	}

	expected := computeExpectedPlacement(t, c, dagBlocks, k)
	t.Log("[over-repl] verifying initial placement (8-node ring)")
	verifyNetworkBlockPlacement(t, nodes, expected, publisher.Name())

	initialInstances := countTotalBlockInstances(t, nodes)
	expectedInstances := totalBlocks * k
	if initialInstances != expectedInstances {
		t.Errorf("[over-repl] initial total block instances=%d, want %d",
			initialInstances, expectedInstances)
	} else {
		t.Logf("[over-repl] initial total block instances: %d = %d×%d (correct)",
			initialInstances, totalBlocks, k)
	}

	crashNode := nodes[len(nodes)-1]
	t.Logf("[over-repl] abruptly stopping %s", crashNode.Name())
	if err := crashNode.Stop(); err != nil {
		t.Fatalf("stop %s: %v", crashNode.Name(), err)
	}

	var remainingNodes []*testrig.TestNode
	var remainingIdents []testrig.PoolIdentity
	for _, n := range nodes {
		if n.Name() != crashNode.Name() {
			remainingNodes = append(remainingNodes, n)
			remainingIdents = append(remainingIdents, n.GetIdentity())
		}
	}

	t.Log("[over-repl] stabilizing ring (S-1 rounds)")
	c.StabilizePrivate(group, testrig.DefaultSuccListSize-1)

	time.Sleep(3 * time.Second)

	postInstances := countTotalBlockInstances(t, remainingNodes)

	postExpected := totalBlocks * k
	if postInstances > postExpected {
		t.Errorf("[over-repl] OVER-REPLICATION DETECTED: total block instances=%d, want <=%d (%d blocks × %d replicas); blocks have spread beyond their replica set",
			postInstances, postExpected, totalBlocks, k)
	} else if postInstances < postExpected {

		t.Logf("[over-repl] WARNING: under-replicated: total=%d, want=%d",
			postInstances, postExpected)
	} else {
		t.Logf("[over-repl] post-leave total block instances: %d = %d×%d (correct, no over-replication)",
			postInstances, totalBlocks, k)
	}

	t.Log("[over-repl] verifying per-block replica count on 7-node ring")
	for _, blk := range dagBlocks {
		holders := 0
		var holderNames []string
		for _, n := range remainingNodes {
			has, err := n.HasNetworkBlock(blk.CID)
			if err != nil {
				t.Fatalf("%s: HasNetworkBlock: %v", n.Name(), err)
			}
			if has {
				holders++
				holderNames = append(holderNames, n.Name())
			}
		}

		if holders > k {
			t.Errorf("[over-repl] block %s: held by %d nodes %v, want <=%d (over-replicated)",
				testrig.NodeIDHexToShort(blk.CID), holders, holderNames, k)
		} else if holders < k {
			t.Logf("[over-repl] block %s: held by %d nodes %v (under-replicated, expected %d)",
				testrig.NodeIDHexToShort(blk.CID), holders, holderNames, k)
		} else {
			t.Logf("  [ok] block %s: held by %d nodes %v",
				testrig.NodeIDHexToShort(blk.CID), holders, holderNames)
		}
	}

	for _, blk := range dagBlocks {
		ringHolders := 0
		for _, n := range remainingNodes {
			ringCIDs, err := n.RingNetworkBlockCIDs(group)
			if err != nil {
				t.Fatalf("%s: RingNetworkBlockCIDs(%s): %v", n.Name(), group, err)
			}
			for _, c := range ringCIDs {
				if cidToMultihashHex(c) == blk.MultihashHex {
					ringHolders++
					break
				}
			}
		}
		if ringHolders > k {
			t.Errorf("[over-repl] block mh=%s: ring %s lists it on %d nodes, want <=%d",
				testrig.NodeIDHexToShort(blk.MultihashHex), group, ringHolders, k)
		}
	}

	t.Log("[over-repl] verifying fetch from all remaining nodes")
	for _, n := range remainingNodes {
		if err := n.FetchCID(cidStr, group); err != nil {
			t.Errorf("[over-repl] %s: fetch CID failed: %v", n.Name(), err)
		}
	}
}

func countTotalBlockInstances(t *testing.T, nodes []*testrig.TestNode) int {
	t.Helper()
	total := 0
	for _, n := range nodes {
		cids, err := n.NetworkBlockCIDs()
		if err != nil {
			t.Fatalf("%s: NetworkBlockCIDs: %v", n.Name(), err)
		}
		t.Logf("  %s: %d network blocks", n.Name(), len(cids))
		total += len(cids)
	}
	return total
}
