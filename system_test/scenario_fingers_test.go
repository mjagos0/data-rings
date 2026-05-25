//go:build system

package system_test

import (
	"testing"

	"github.com/mjagos0/datarings/testrig"
)

func TestScenario_Fingers_PublicRing(t *testing.T) {
	c := testrig.NewCluster(t, testrig.StateFreshPublic)
	nodes := c.Nodes8()
	c.Setup()

	topo := c.Topology()
	assignment := c.Assignment()

	t.Log("--- Expected Topology ---")
	for _, node := range topo.Nodes {
		name := assignment.IDToNode[node.NodeIDHex]
		succName := assignment.IDToNode[topo.Successors[node.NodeIDHex]]
		predName := assignment.IDToNode[topo.Predecessors[node.NodeIDHex]]
		t.Logf("  %s (%s): succ=%s pred=%s unique_fingers=%d",
			name, testrig.NodeIDHexToShort(node.NodeIDHex), succName, predName,
			len(topo.UniqueFingers[node.NodeIDHex]))
	}

	verifyPublicFingers(t, nodes, topo)
}

func TestScenario_Fingers_AfterGracefulLeave(t *testing.T) {
	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	topo8 := c.Topology()

	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	t.Log("[fingers-2b] === Baseline: 8-node private ring ===")
	verifyPrivateFingers(t, nodes, c.Assignment(), topo8, group)

	leavingNode := nodes[7]
	t.Logf("[fingers-2b] %s gracefully leaving", leavingNode.Name())
	out, err := leavingNode.Exec("ring", "leave", group)
	if err != nil {
		t.Errorf("leave failed: %v\n%s", err, out)
	}

	remainingNodes := nodes[:7]
	remainingIdents := make([]testrig.PoolIdentity, 7)
	for i, n := range remainingNodes {
		remainingIdents[i] = n.GetIdentity()
	}
	topo7 := testrig.ComputeTopology(remainingIdents, testrig.DefaultSuccListSize)
	assignment7 := testrig.AssignIdentities(testrig.TestNodeNames(remainingNodes), remainingIdents, testrig.DefaultSuccListSize)

	c.StabilizePrivate(group, testrig.DefaultSuccListSize-1)

	t.Log("[fingers-2b] === Post-leave: 7-node private ring ===")
	verifyPrivateFingers(t, remainingNodes, assignment7, topo7, group)

	_, ok := leavingNode.PrivateState(group)
	if ok {
		t.Errorf("%s: still in group after graceful leave", leavingNode.Name())
	}
}

func TestScenario_Fingers_AfterAbruptLeave(t *testing.T) {
	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	topo8 := c.Topology()

	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	t.Log("[fingers-2c] === Baseline: 8-node private ring ===")
	verifyPrivateFingers(t, nodes, c.Assignment(), topo8, group)

	crashNode := nodes[7]
	t.Logf("[fingers-2c] abruptly stopping %s", crashNode.Name())
	if err := crashNode.Stop(); err != nil {
		t.Fatalf("stop %s: %v", crashNode.Name(), err)
	}

	remainingNodes := nodes[:7]
	remainingIdents := make([]testrig.PoolIdentity, 7)
	for i, n := range remainingNodes {
		remainingIdents[i] = n.GetIdentity()
	}
	topo7 := testrig.ComputeTopology(remainingIdents, testrig.DefaultSuccListSize)
	assignment7 := testrig.AssignIdentities(testrig.TestNodeNames(remainingNodes), remainingIdents, testrig.DefaultSuccListSize)

	c.StabilizePrivate(group, testrig.DefaultSuccListSize-1)

	t.Log("[fingers-2c] === Post-crash: 7-node private ring ===")
	verifyPrivateFingers(t, remainingNodes, assignment7, topo7, group)

	t.Logf("[fingers-2c] restarting %s", crashNode.Name())
	if err := crashNode.Start(); err != nil {
		t.Logf("restart %s: %v (non-fatal)", crashNode.Name(), err)
	}
}
