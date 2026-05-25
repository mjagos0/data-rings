//go:build system

package system_test

import (
	"testing"

	"github.com/mjagos0/datarings/testrig"
)

func addFileLocally(t *testing.T, n *testrig.TestNode, data []byte) string {
	t.Helper()
	cidStr, err := n.AddLocal(data)
	if err != nil {
		t.Fatalf("add on %s: %v", n.Name(), err)
	}
	return cidStr
}

func TestScenario_LocalBlocks_NotReplicated(t *testing.T) {
	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()

	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	data := []byte("local-only-block-must-not-replicate-to-other-nodes!")
	cidStr := addFileLocally(t, nodes[0], data)
	t.Logf("[local-1] node1 added CID=%s locally (not pushed to ring)", cidStr)

	c.StabilizePrivate(group, 4)
	t.Log("[local-2] ran 4 private stabilization rounds")

	has, err := nodes[0].HasBlock(cidStr)
	if err != nil {
		t.Fatalf("node1 HasBlock: %v", err)
	}
	if !has {
		t.Fatal("[local-3] node1 does not hold root block — add failed?")
	}
	t.Log("[local-3] node1 holds root block (expected, in LocalBlocks)")

	leaked := false
	for _, n := range nodes[1:] {
		has, err := n.HasBlock(cidStr)
		if err != nil {
			t.Errorf("%s: HasBlock: %v", n.Name(), err)
			continue
		}
		if has {
			t.Errorf("[local-4] %s: holds root block — LOCAL BLOCKS LEAKED to ring!", n.Name())
			leaked = true
		} else {
			t.Logf("  [ok] %s: does not hold root block (correct)", n.Name())
		}
	}
	if !leaked {
		t.Log("[local-4] confirmed: no local blocks leaked to other nodes")
	}
}

func TestScenario_LocalBlocks_NotOverReplicated(t *testing.T) {
	const k = 3

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	data := []byte("store-and-pub-local-blocks-isolation-test!")
	cidStr, err := nodes[0].StoreFile(data, group)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	t.Logf("[iso-1] stored CID=%s from %s", cidStr, nodes[0].Name())

	replicaNodes := c.ReplicaNodesForCID(cidStr, k)
	replicaSet := testrig.NameSet(replicaNodes)
	t.Logf("[iso-2] expected replicas: %v", testrig.NodeNamesFromSlice(replicaNodes))

	c.StabilizePrivate(group, 4)

	holders := 0
	publisher := nodes[0].Name()
	for _, n := range nodes {
		has, err := n.HasBlock(cidStr)
		if err != nil {
			t.Errorf("%s: HasBlock: %v", n.Name(), err)
			continue
		}
		if has {
			holders++
			if !replicaSet[n.Name()] && n.Name() != publisher {
				t.Errorf("[iso-3] %s: holds block but is neither replica nor publisher — over-replication", n.Name())
			}
		}
	}

	maxExpected := k + 1
	if replicaSet[publisher] {
		maxExpected = k
	}
	if holders > maxExpected {
		t.Errorf("[iso-4] block held by %d nodes, expected at most %d (k=%d replicas + publisher)", holders, maxExpected, k)
	} else {
		t.Logf("[iso-4] block held by %d nodes, max expected %d — no over-replication", holders, maxExpected)
	}
}
