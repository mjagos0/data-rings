//go:build system

package system_test

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/mjagos0/datarings/testrig"
)

func TestScenario_CrossRing_ReplicationLeakage(t *testing.T) {
	const (
		ringAName	= "ring-A"
		ringBName	= "ring-B"
		k		= 3

		chunkSize	= 262144
		numChunks	= 3
		fileSize	= chunkSize * numChunks
	)

	c := testrig.NewCluster(t, testrig.StateFreshPublic)
	_ = c.Nodes8()
	c.Setup()

	ringAMembers := []*testrig.TestNode{
		c.NodeByName("node1"),
		c.NodeByName("node2"),
		c.NodeByName("node3"),
		c.NodeByName("node4"),
	}
	ringBMembers := []*testrig.TestNode{
		c.NodeByName("node1"),
		c.NodeByName("node5"),
		c.NodeByName("node6"),
		c.NodeByName("node7"),
	}

	ringBOnly := []*testrig.TestNode{
		c.NodeByName("node5"),
		c.NodeByName("node6"),
		c.NodeByName("node7"),
	}

	outsider := c.NodeByName("node8")

	out, err := ringAMembers[0].Exec("ring", "create")
	if err != nil {
		t.Fatalf("ring-A create on %s: %v\n%s", ringAMembers[0].Name(), err, out)
	}
	ringAKey := testrig.ParseGroupKey(out)
	if ringAKey == "" {
		t.Fatalf("could not parse ring-A group key from output: %s", out)
	}
	t.Logf("[xring] ring-A group key created on %s", ringAMembers[0].Name())

	for _, n := range ringAMembers {
		if out, err := n.Exec("ring", "join", ringAKey, ringAName); err != nil {
			t.Fatalf("%s join ring-A: %v\n%s", n.Name(), err, out)
		}
	}
	stabilizeRing(t, ringAMembers, ringAName,
		len(ringAMembers)+testrig.DefaultSuccListSize-2)

	fileData := make([]byte, fileSize)
	if _, err := rand.Read(fileData); err != nil {
		t.Fatalf("generate file data: %v", err)
	}
	rootCID, dagBlocks := buildDAGBlocks(t, fileData)
	t.Logf("[xring] pre-computed DAG: root=%s blocks=%d", rootCID, len(dagBlocks))

	cidStr, err := ringAMembers[0].StoreFile(fileData, ringAName)
	if err != nil {
		t.Fatalf("store file in ring-A: %v", err)
	}
	if cidStr != rootCID {
		t.Fatalf("[xring] root CID mismatch: got %s, want %s", cidStr, rootCID)
	}
	t.Logf("[xring] stored CID=%s in ring-A from %s", cidStr, ringAMembers[0].Name())

	ringABlockMHs := make(map[string]bool, len(dagBlocks))
	for _, blk := range dagBlocks {
		ringABlockMHs[blk.MultihashHex] = true
	}

	for _, n := range append(append([]*testrig.TestNode{}, ringBOnly...), outsider) {
		cids, err := n.NetworkBlockCIDs()
		if err != nil {
			t.Fatalf("%s: NetworkBlockCIDs (pre-ring-B): %v", n.Name(), err)
		}
		if len(cids) != 0 {
			t.Fatalf("[xring] %s: holds %d network block(s) before ring-B exists; "+
				"setup precondition violated: %v", n.Name(), len(cids), cids)
		}
		roots, err := n.NetworkRoots()
		if err != nil {
			t.Fatalf("%s: NetworkRoots (pre-ring-B): %v", n.Name(), err)
		}
		if len(roots) != 0 {
			t.Fatalf("[xring] %s: has %d network root(s) before ring-B exists: %v",
				n.Name(), len(roots), roots)
		}
	}

	out, err = ringBMembers[0].Exec("ring", "create")
	if err != nil {
		t.Fatalf("ring-B create on %s: %v\n%s", ringBMembers[0].Name(), err, out)
	}
	ringBKey := testrig.ParseGroupKey(out)
	if ringBKey == "" {
		t.Fatalf("could not parse ring-B group key from output: %s", out)
	}
	if ringBKey == ringAKey {
		t.Fatalf("ring-A and ring-B group keys collided")
	}
	t.Logf("[xring] ring-B group key created on %s", ringBMembers[0].Name())

	for _, n := range ringBMembers {
		if out, err := n.Exec("ring", "join", ringBKey, ringBName); err != nil {
			t.Fatalf("%s join ring-B: %v\n%s", n.Name(), err, out)
		}
	}

	stabilizeRing(t, ringBMembers, ringBName,
		len(ringBMembers)+testrig.DefaultSuccListSize-2)

	time.Sleep(2 * time.Second)

	leakedAny := false
	for _, n := range ringBOnly {
		cids, err := n.NetworkBlockCIDs()
		if err != nil {
			t.Fatalf("%s: NetworkBlockCIDs (post-ring-B): %v", n.Name(), err)
		}
		var leaked []string
		for _, c := range cids {
			mh := cidToMultihashHex(c)
			if ringABlockMHs[mh] {
				leaked = append(leaked, c)
			}
		}
		if len(leaked) > 0 {
			leakedAny = true
			t.Errorf("[xring] %s: holds %d ring-A block(s) it never should have seen — "+
				"replicateToNewSuccessors on ring-B leaked ring-A data via the shared "+
				"block store: %v", n.Name(), len(leaked), leaked)
		} else {
			t.Logf("  [ok] %s: 0 ring-A blocks (held %d unrelated network blocks total)",
				n.Name(), len(cids))
		}

		roots, err := n.NetworkRoots()
		if err != nil {
			t.Fatalf("%s: NetworkRoots (post-ring-B): %v", n.Name(), err)
		}
		for _, r := range roots {
			if r == cidStr {
				leakedAny = true
				t.Errorf("[xring] %s: registry holds ring-A network root %s — "+
					"PushBlocks over ring-B's transport propagated it from node1",
					n.Name(), r)
			}
		}
	}

	if cids, err := outsider.NetworkBlockCIDs(); err != nil {
		t.Fatalf("%s: NetworkBlockCIDs (outsider): %v", outsider.Name(), err)
	} else if len(cids) != 0 {
		t.Errorf("[xring] %s: outsider gained %d network block(s): %v",
			outsider.Name(), len(cids), cids)
	}

	if !leakedAny {
		t.Log("[xring] no cross-ring leakage observed — bug appears fixed")
	}
}

func stabilizeRing(t *testing.T, members []*testrig.TestNode, groupRef string, rounds int) {
	t.Helper()
	for r := 0; r < rounds; r++ {
		for _, n := range members {
			if err := n.ForceStabilizePrivate(groupRef); err != nil {
				t.Logf("[stabilize] %s ring=%s round=%d: %v", n.Name(), groupRef, r, err)
			}
		}
	}
}
