//go:build system

package system_test

import (
	"testing"
	"time"

	"github.com/mjagos0/datarings/testrig"
)

func TestScenario_Predecessor_CrashAndRestart(t *testing.T) {
	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	topo8 := c.Topology()

	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)
	t.Log("[pred-9a] === Baseline: 8-node private ring ===")
	verifyPrivateFingers(t, nodes, c.Assignment(), topo8, group)

	crashNode := c.NodeByName("node4")
	t.Logf("[pred-9a] crashing %s", crashNode.Name())

	entry, ok := c.NodeByName("node5").PrivateState(group)
	if !ok {
		t.Fatal("node5: not in private ring")
	}
	if entry.Node.Predecessor == nil {
		t.Fatal("[pred-9a] node5: predecessor is nil before crash")
	}
	t.Logf("[pred-9a] node5 predecessor before crash: %s (expect %s)",
		testrig.NodeIDHexToShort(entry.Node.Predecessor.ID), testrig.NodeIDHexToShort(crashNode.ID()))
	if entry.Node.Predecessor.ID != crashNode.ID() {
		t.Errorf("[pred-9a] node5 predecessor before crash: %s, want %s",
			testrig.NodeIDHexToShort(entry.Node.Predecessor.ID), crashNode.Name())
	}

	if err := crashNode.Stop(); err != nil {
		t.Fatalf("stop %s: %v", crashNode.Name(), err)
	}

	t.Log("[pred-9a] stabilizing after crash (S-1=4 rounds)")
	c.StabilizePrivate(group, testrig.DefaultSuccListSize-1)

	remaining := make([]*testrig.TestNode, 0, 7)
	remainingIdents := make([]testrig.PoolIdentity, 0, 7)
	for _, n := range nodes {
		if n.Name() != crashNode.Name() {
			remaining = append(remaining, n)
			remainingIdents = append(remainingIdents, n.GetIdentity())
		}
	}
	topo7 := testrig.ComputeTopology(remainingIdents, testrig.DefaultSuccListSize)
	assignment7 := testrig.AssignIdentities(
		testrig.TestNodeNames(remaining), remainingIdents, testrig.DefaultSuccListSize,
	)

	t.Log("[pred-9a] === After crash: 7-node topology ===")
	verifyPrivateFingers(t, remaining, assignment7, topo7, group)

	entry, ok = c.NodeByName("node5").PrivateState(group)
	if !ok {
		t.Fatal("node5: not in private ring after crash")
	}
	if entry.Node.Predecessor == nil {
		t.Errorf("[pred-9a] node5: predecessor is nil after crash stabilization")
	} else {
		t.Logf("[pred-9a] node5 predecessor after crash: %s",
			testrig.NodeIDHexToShort(entry.Node.Predecessor.ID))
		expectedPred := topo7.Predecessors[c.NodeByName("node5").ID()]
		if entry.Node.Predecessor.ID != expectedPred {
			t.Errorf("[pred-9a] node5 predecessor: %s, want %s",
				testrig.NodeIDHexToShort(entry.Node.Predecessor.ID),
				testrig.NodeIDHexToShort(expectedPred))
		}
	}

	t.Logf("[pred-9a] restarting %s", crashNode.Name())
	if err := crashNode.Start(); err != nil {
		t.Fatalf("restart %s: %v", crashNode.Name(), err)
	}

	t.Log("[pred-9a] stabilizing public ring after restart (N+S-2=11 rounds)")
	c.Stabilize(len(nodes) + testrig.DefaultSuccListSize - 2)
	c.RepublishSelf()

	t.Logf("[pred-9a] %s: leaving stale private ring singleton", crashNode.Name())
	crashNode.Exec("ring", "leave", group)
	t.Logf("[pred-9a] %s: rejoining private ring", crashNode.Name())
	out, err := crashNode.Exec("ring", "join", c.GroupKey(), group)
	if err != nil {
		t.Fatalf("rejoin: %v\n%s", err, out)
	}

	t.Log("[pred-9a] stabilizing private ring for 8-node convergence (N+S-2=11 rounds)")
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	t.Log("[pred-9a] === After restart: 8-node topology restored ===")
	verifyPrivateFingers(t, nodes, c.Assignment(), topo8, group)
}

func TestScenario_Predecessor_GCWithDeadPredecessor(t *testing.T) {
	const k = 3

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()

	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	data := []byte("scenario-9b-gc-dead-predecessor-payload")
	cidStr, err := nodes[0].StoreFile(data, group)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	publisher := nodes[0]
	t.Logf("[pred-9b] stored CID=%s from %s", cidStr, publisher.Name())

	var target *testrig.TestNode
	for _, n := range nodes {
		if n.Name() == publisher.Name() {
			continue
		}
		has, err := n.HasBlock(cidStr)
		if err != nil {
			t.Fatalf("%s: HasBlock: %v", n.Name(), err)
		}
		if has {
			target = n
			break
		}
	}
	if target == nil {
		t.Fatal("[pred-9b] no non-publisher replica found")
	}

	entry, ok := target.PrivateState(group)
	if !ok {
		t.Fatalf("%s: not in private ring", target.Name())
	}
	if entry.Node.Predecessor == nil {
		t.Fatalf("%s: predecessor is nil", target.Name())
	}
	predNode := c.NodeByID(entry.Node.Predecessor.ID)
	if predNode == nil {
		t.Fatalf("predecessor node %s not found in cluster",
			testrig.NodeIDHexToShort(entry.Node.Predecessor.ID))
	}
	t.Logf("[pred-9b] target=%s, predecessor=%s", target.Name(), predNode.Name())

	t.Logf("[pred-9b] killing predecessor %s", predNode.Name())
	if err := predNode.Stop(); err != nil {
		t.Fatalf("stop %s: %v", predNode.Name(), err)
	}

	t.Logf("[pred-9b] running GC on %s (predecessor is dead, no stabilization)", target.Name())
	out, err := target.Exec("gc")
	if err != nil {
		t.Fatalf("gc on %s: %v\n%s", target.Name(), err, out)
	}
	t.Logf("[pred-9b] GC output: %s", out)

	has, err := target.HasBlock(cidStr)
	if err != nil {
		t.Fatalf("HasBlock after GC: %v", err)
	}
	if !has {
		t.Errorf("[pred-9b] %s: lost root block after GC with dead predecessor — "+
			"RingBlockRange should have returned ok=false, keeping all blocks", target.Name())
	} else {
		t.Logf("[pred-9b] %s: correctly preserved root block (GC was conservative due to dead predecessor)", target.Name())
	}

	if err := predNode.Start(); err != nil {
		t.Logf("restart %s: %v (non-fatal)", predNode.Name(), err)
	}
}

func TestScenario_UnreliableNode_CrashRejoinCycles(t *testing.T) {
	const k = 3

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()

	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	data := []byte("scenario-9c-unreliable-node-five-cycles-payload")
	cidStr, err := nodes[0].StoreFile(data, group)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	t.Logf("[unreliable-9c] stored CID=%s from %s", cidStr, nodes[0].Name())

	replicasBefore := c.ReplicaNodesForCID(cidStr, k)
	t.Logf("[unreliable-9c] initial replicas: %v", testrig.NodeNamesFromSlice(replicasBefore))
	for _, n := range replicasBefore {
		has, _ := n.HasBlock(cidStr)
		if !has {
			t.Fatalf("[unreliable-9c] %s: missing root block before cycles", n.Name())
		}
	}

	unreliable := c.NodeByName("node7")
	t.Logf("[unreliable-9c] unreliable node: %s", unreliable.Name())

	type cycle struct {
		roundsAfterKill		int
		roundsAfterRestart	int
	}
	schedule := []cycle{
		{2, 2},
		{1, 1},
		{0, 3},
		{3, 0},
		{2, 2},
	}

	for i, cyc := range schedule {
		t.Logf("[unreliable-9c] === Cycle %d: kill(%d rounds) + restart(%d rounds) ===",
			i+1, cyc.roundsAfterKill, cyc.roundsAfterRestart)

		if err := unreliable.Stop(); err != nil {
			t.Fatalf("cycle %d: stop %s: %v", i+1, unreliable.Name(), err)
		}
		if cyc.roundsAfterKill > 0 {
			c.StabilizePrivate(group, cyc.roundsAfterKill)
		}

		if err := unreliable.Start(); err != nil {
			t.Fatalf("cycle %d: restart %s: %v", i+1, unreliable.Name(), err)
		}
		c.Stabilize(len(nodes) + testrig.DefaultSuccListSize - 2)
		c.RepublishSelf()
		unreliable.Exec("ring", "leave", group)
		out, err := unreliable.Exec("ring", "join", c.GroupKey(), group)
		if err != nil {
			t.Fatalf("cycle %d: rejoin: %v\n%s", i+1, err, out)
		}

		if cyc.roundsAfterRestart > 0 {
			c.StabilizePrivate(group, cyc.roundsAfterRestart)
		}
	}

	t.Log("[unreliable-9c] === Full stabilization (N+S-2=11 rounds) ===")
	c.Stabilize(len(nodes) + testrig.DefaultSuccListSize - 2)
	c.RepublishSelf()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	time.Sleep(2 * time.Second)

	topo8 := c.Topology()
	t.Log("[unreliable-9c] === Final topology verification ===")
	verifyPrivateFingers(t, nodes, c.Assignment(), topo8, group)

	entry, ok := unreliable.PrivateState(group)
	if !ok {
		t.Fatalf("%s: not in private ring after cycles", unreliable.Name())
	}
	expectedPred := topo8.Predecessors[unreliable.ID()]
	expectedSucc := topo8.Successors[unreliable.ID()]

	if entry.Node.Predecessor == nil {
		t.Errorf("[unreliable-9c] %s: predecessor is nil, want %s",
			unreliable.Name(), testrig.NodeIDHexToShort(expectedPred))
	} else if entry.Node.Predecessor.ID != expectedPred {
		t.Errorf("[unreliable-9c] %s: predecessor=%s, want %s",
			unreliable.Name(),
			testrig.NodeIDHexToShort(entry.Node.Predecessor.ID),
			testrig.NodeIDHexToShort(expectedPred))
	} else {
		t.Logf("[unreliable-9c] %s: predecessor=%s (correct)",
			unreliable.Name(), testrig.NodeIDHexToShort(entry.Node.Predecessor.ID))
	}

	if entry.Node.Successor.ID != expectedSucc {
		t.Errorf("[unreliable-9c] %s: successor=%s, want %s",
			unreliable.Name(),
			testrig.NodeIDHexToShort(entry.Node.Successor.ID),
			testrig.NodeIDHexToShort(expectedSucc))
	} else {
		t.Logf("[unreliable-9c] %s: successor=%s (correct)",
			unreliable.Name(), testrig.NodeIDHexToShort(entry.Node.Successor.ID))
	}

	replicasAfter := c.ReplicaNodesForCID(cidStr, k)
	replicaSet := testrig.NameSet(replicasAfter)
	t.Logf("[unreliable-9c] expected replicas: %v", testrig.NodeNamesFromSlice(replicasAfter))

	for _, n := range replicasAfter {
		has, err := n.HasBlock(cidStr)
		if err != nil {
			t.Errorf("%s: HasBlock: %v", n.Name(), err)
			continue
		}
		if !has {
			t.Errorf("[unreliable-9c] %s: expected replica missing root block after cycles", n.Name())
		}
	}

	publisher := nodes[0].Name()
	for _, n := range nodes {
		if replicaSet[n.Name()] || n.Name() == publisher {
			continue
		}
		has, _ := n.HasBlock(cidStr)
		if has {
			t.Errorf("[unreliable-9c] %s: holds root block but is not a replica (over-replication)", n.Name())
		}
	}

	t.Log("[unreliable-9c] verifying fetch from all 8 nodes")
	for _, n := range nodes {
		if err := n.FetchCID(cidStr, group); err != nil {
			t.Errorf("[unreliable-9c] %s: fetch failed: %v", n.Name(), err)
		}
	}
}
