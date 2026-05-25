//go:build system

package system_test

import (
	"strings"
	"testing"
	"time"

	"github.com/mjagos0/datarings/testrig"
)

func TestScenario_CascadingFailure_TwoReplicasCrash(t *testing.T) {
	const k = 3

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()

	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	data := []byte("scenario-7a-cascading-failure-two-replicas-crash-payload")
	cidStr, err := nodes[0].StoreFile(data, group)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	t.Logf("[cascade-7a] stored CID=%s from %s", cidStr, nodes[0].Name())

	replicaNodes := c.ReplicaNodesForCID(cidStr, k)
	if len(replicaNodes) < k {
		t.Fatalf("[cascade-7a] expected %d replicas, got %d", k, len(replicaNodes))
	}
	t.Logf("[cascade-7a] replicas: R1=%s (primary), R2=%s, R3=%s",
		replicaNodes[0].Name(), replicaNodes[1].Name(), replicaNodes[2].Name())

	for i, n := range replicaNodes {
		has, err := n.HasBlock(cidStr)
		if err != nil {
			t.Fatalf("R%d (%s): HasBlock: %v", i+1, n.Name(), err)
		}
		if !has {
			t.Fatalf("[cascade-7a] R%d (%s): expected to hold root block, but does not", i+1, n.Name())
		}
	}

	primary := replicaNodes[0]
	crashNode1 := replicaNodes[1]
	crashNode2 := replicaNodes[2]
	t.Logf("[cascade-7a] crashing R2=%s and R3=%s; primary R1=%s survives",
		crashNode1.Name(), crashNode2.Name(), primary.Name())

	if err := crashNode1.Stop(); err != nil {
		t.Fatalf("stop R2 (%s): %v", crashNode1.Name(), err)
	}
	if err := crashNode2.Stop(); err != nil {
		t.Fatalf("stop R3 (%s): %v", crashNode2.Name(), err)
	}

	has, err := primary.HasBlock(cidStr)
	if err != nil {
		t.Fatalf("R1 (%s): HasBlock after crashes: %v", primary.Name(), err)
	}
	if !has {
		t.Fatalf("[cascade-7a] R1 (%s): lost root block — single surviving copy is gone", primary.Name())
	}
	t.Logf("[cascade-7a] R1 (%s): still holds root block (sole surviving copy)", primary.Name())

	crashSet := map[string]bool{crashNode1.Name(): true, crashNode2.Name(): true}
	var remainingNodes []*testrig.TestNode
	var remainingIdents []testrig.PoolIdentity
	for _, n := range nodes {
		if !crashSet[n.Name()] {
			remainingNodes = append(remainingNodes, n)
			remainingIdents = append(remainingIdents, n.GetIdentity())
		}
	}
	t.Logf("[cascade-7a] remaining %d nodes: %v",
		len(remainingNodes), testrig.TestNodeNames(remainingNodes))

	t.Log("[cascade-7a] stabilizing ring (S-1=4 rounds)")
	c.StabilizePrivate(group, testrig.DefaultSuccListSize-1)

	time.Sleep(2 * time.Second)

	newReplicas := testrig.ReplicaNodes(
		testrig.CIDToRingKeyHex(cidStr), remainingIdents, k,
	)
	var newReplicaNodes []*testrig.TestNode
	for _, nodeIDHex := range newReplicas {
		n := c.NodeByID(nodeIDHex)
		if n != nil {
			newReplicaNodes = append(newReplicaNodes, n)
		}
	}
	t.Logf("[cascade-7a] new 6-node replicas: %v", testrig.NodeNamesFromSlice(newReplicaNodes))

	for _, n := range newReplicaNodes {
		has, err := n.HasBlock(cidStr)
		if err != nil {
			t.Errorf("%s: HasBlock: %v", n.Name(), err)
			continue
		}
		if !has {
			t.Errorf("[cascade-7a] %s: new replica does NOT hold root block (re-replication failed)", n.Name())
		} else {
			t.Logf("  [ok] %s: holds root block (new replica)", n.Name())
		}
	}

	holdCount := 0
	for _, n := range remainingNodes {
		has, err := n.HasBlock(cidStr)
		if err != nil {
			t.Errorf("%s: HasBlock: %v", n.Name(), err)
			continue
		}
		if has {
			holdCount++
		}
	}
	t.Logf("[cascade-7a] total holders among remaining 6: %d (need >= %d)", holdCount, k)
	if holdCount < k {
		t.Errorf("[cascade-7a] only %d of 6 remaining nodes hold root block, want >= %d", holdCount, k)
	}

	t.Log("[cascade-7a] verifying fetch from all remaining nodes")
	for _, n := range remainingNodes {
		if err := n.FetchCID(cidStr, group); err != nil {
			t.Errorf("[cascade-7a] %s: fetch CID failed: %v", n.Name(), err)
		}
	}

	topo6 := testrig.ComputeTopology(remainingIdents, testrig.DefaultSuccListSize)
	assignment6 := testrig.AssignIdentities(
		testrig.TestNodeNames(remainingNodes), remainingIdents, testrig.DefaultSuccListSize,
	)
	t.Log("[cascade-7a] === Post-crash: 6-node private ring topology ===")
	verifyPrivateFingers(t, remainingNodes, assignment6, topo6, group)
}

func TestScenario_CascadingFailure_MultiFile(t *testing.T) {
	const k = 3

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()

	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	data1 := []byte("scenario-7b-cascade-multi-file1-from-node1")
	cid1, err := nodes[0].StoreFile(data1, group)
	if err != nil {
		t.Fatalf("store file1: %v", err)
	}
	t.Logf("[cascade-7b] file1 CID=%s from %s", cid1, nodes[0].Name())

	data2 := []byte("scenario-7b-cascade-multi-file2-from-node5")
	cid2, err := nodes[4].StoreFile(data2, group)
	if err != nil {
		t.Fatalf("store file2: %v", err)
	}
	t.Logf("[cascade-7b] file2 CID=%s from %s", cid2, nodes[4].Name())

	replicas1 := c.ReplicaNodesForCID(cid1, k)
	replicas2 := c.ReplicaNodesForCID(cid2, k)
	t.Logf("[cascade-7b] CID1 replicas: %v", testrig.NodeNamesFromSlice(replicas1))
	t.Logf("[cascade-7b] CID2 replicas: %v", testrig.NodeNamesFromSlice(replicas2))

	for i, n := range replicas1 {
		has, err := n.HasBlock(cid1)
		if err != nil {
			t.Fatalf("CID1 R%d (%s): HasBlock: %v", i+1, n.Name(), err)
		}
		if !has {
			t.Errorf("[cascade-7b] CID1 R%d (%s): missing root block", i+1, n.Name())
		}
	}
	for i, n := range replicas2 {
		has, err := n.HasBlock(cid2)
		if err != nil {
			t.Fatalf("CID2 R%d (%s): HasBlock: %v", i+1, n.Name(), err)
		}
		if !has {
			t.Errorf("[cascade-7b] CID2 R%d (%s): missing root block", i+1, n.Name())
		}
	}

	crashNode1 := replicas1[1]
	crashNode2 := replicas1[2]
	t.Logf("[cascade-7b] crashing CID1 R2=%s, R3=%s (primary %s survives)",
		crashNode1.Name(), crashNode2.Name(), replicas1[0].Name())

	if err := crashNode1.Stop(); err != nil {
		t.Fatalf("stop %s: %v", crashNode1.Name(), err)
	}
	if err := crashNode2.Stop(); err != nil {
		t.Fatalf("stop %s: %v", crashNode2.Name(), err)
	}

	crashSet := map[string]bool{crashNode1.Name(): true, crashNode2.Name(): true}
	var remainingNodes []*testrig.TestNode
	for _, n := range nodes {
		if !crashSet[n.Name()] {
			remainingNodes = append(remainingNodes, n)
		}
	}

	t.Log("[cascade-7b] stabilizing ring (S-1=4 rounds)")
	c.StabilizePrivate(group, testrig.DefaultSuccListSize-1)
	time.Sleep(2 * time.Second)

	hold1 := 0
	for _, n := range remainingNodes {
		has, err := n.HasBlock(cid1)
		if err != nil {
			t.Errorf("%s: HasBlock(CID1): %v", n.Name(), err)
			continue
		}
		if has {
			hold1++
			t.Logf("  [cascade-7b] %s: holds CID1 root block", n.Name())
		}
	}
	if hold1 < k {
		t.Errorf("[cascade-7b] CID1: only %d of 6 remaining hold root block, want >= %d", hold1, k)
	}

	hold2 := 0
	for _, n := range remainingNodes {
		has, err := n.HasBlock(cid2)
		if err != nil {
			t.Errorf("%s: HasBlock(CID2): %v", n.Name(), err)
			continue
		}
		if has {
			hold2++
		}
	}
	if hold2 < 1 {
		t.Errorf("[cascade-7b] CID2: no remaining node holds root block")
	}
	t.Logf("[cascade-7b] CID2: %d of 6 remaining hold root block", hold2)

	t.Log("[cascade-7b] verifying fetch from all remaining nodes")
	for _, n := range remainingNodes {
		if err := n.FetchCID(cid1, group); err != nil {
			t.Errorf("[cascade-7b] %s: fetch CID1 failed: %v", n.Name(), err)
		}
		if err := n.FetchCID(cid2, group); err != nil {
			t.Errorf("[cascade-7b] %s: fetch CID2 failed: %v", n.Name(), err)
		}
	}
}

func TestScenario_CascadingFailure_SequentialCrashes(t *testing.T) {
	const k = 3

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()

	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	data := []byte("scenario-7c-sequential-three-crashes-payload")
	cidStr, err := nodes[0].StoreFile(data, group)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	t.Logf("[cascade-7c] stored CID=%s from %s", cidStr, nodes[0].Name())

	replicaNodes := c.ReplicaNodesForCID(cidStr, k)
	t.Logf("[cascade-7c] initial replicas: %v", testrig.NodeNamesFromSlice(replicaNodes))
	for _, n := range replicaNodes {
		has, err := n.HasBlock(cidStr)
		if err != nil {
			t.Fatalf("%s: HasBlock: %v", n.Name(), err)
		}
		if !has {
			t.Fatalf("[cascade-7c] %s: expected replica missing root block", n.Name())
		}
	}

	alive := make([]*testrig.TestNode, len(nodes))
	copy(alive, nodes)

	keyHex := testrig.CIDToRingKeyHex(cidStr)

	for crash := 1; crash <= 3; crash++ {

		aliveIdents := make([]testrig.PoolIdentity, len(alive))
		for i, n := range alive {
			aliveIdents[i] = n.GetIdentity()
		}
		currentReplicas := testrig.ReplicaNodes(keyHex, aliveIdents, k)

		victimID := currentReplicas[len(currentReplicas)-1]
		victim := c.NodeByID(victimID)
		if victim == nil {
			t.Fatalf("[cascade-7c] crash #%d: replica %s not found", crash, testrig.NodeIDHexToShort(victimID))
		}

		t.Logf("[cascade-7c] crash #%d: killing %s (R%d of %d replicas); primary %s survives",
			crash, victim.Name(), len(currentReplicas),
			len(currentReplicas), c.NodeByID(currentReplicas[0]).Name())

		if err := victim.Stop(); err != nil {
			t.Fatalf("stop %s: %v", victim.Name(), err)
		}

		var newAlive []*testrig.TestNode
		for _, n := range alive {
			if n.Name() != victim.Name() {
				newAlive = append(newAlive, n)
			}
		}
		alive = newAlive

		t.Logf("[cascade-7c] crash #%d: stabilizing %d-node ring (S-1=4 rounds)",
			crash, len(alive))
		c.StabilizePrivate(group, testrig.DefaultSuccListSize-1)
		time.Sleep(2 * time.Second)

		holdCount := 0
		for _, n := range alive {
			has, err := n.HasBlock(cidStr)
			if err != nil {
				continue
			}
			if has {
				holdCount++
			}
		}

		minExpected := k
		if len(alive) < k {
			minExpected = len(alive)
		}
		t.Logf("[cascade-7c] crash #%d: %d of %d alive nodes hold root block (need >= %d)",
			crash, holdCount, len(alive), minExpected)

		if holdCount < minExpected {
			t.Errorf("[cascade-7c] crash #%d: only %d hold root block, want >= %d",
				crash, holdCount, minExpected)
		}
	}

	t.Logf("[cascade-7c] verifying fetch from %d remaining nodes", len(alive))
	for _, n := range alive {
		if err := n.FetchCID(cidStr, group); err != nil {
			t.Errorf("[cascade-7c] %s: fetch failed: %v", n.Name(), err)
		}
	}
}

func TestScenario_CascadingFailure_TotalReplicaLoss(t *testing.T) {
	const k = 3

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()

	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	data := []byte("scenario-7d-total-replica-loss-irrecoverable-payload")
	cidStr, err := nodes[0].StoreFile(data, group)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	t.Logf("[cascade-7d] stored CID=%s from %s", cidStr, nodes[0].Name())

	var holders []*testrig.TestNode
	holderSet := make(map[string]bool)
	for _, n := range nodes {
		has, err := n.HasBlock(cidStr)
		if err != nil {
			t.Fatalf("%s: HasBlock: %v", n.Name(), err)
		}
		if has {
			holders = append(holders, n)
			holderSet[n.Name()] = true
		}
	}
	t.Logf("[cascade-7d] holders (%d): %v", len(holders), testrig.NodeNamesFromSlice(holders))

	if len(holders) < k {
		t.Fatalf("[cascade-7d] expected >= %d holders, got %d", k, len(holders))
	}

	for _, n := range holders {
		t.Logf("[cascade-7d] killing %s (holds root block)", n.Name())
		if err := n.Stop(); err != nil {
			t.Fatalf("stop %s: %v", n.Name(), err)
		}
	}

	var remainingNodes []*testrig.TestNode
	for _, n := range nodes {
		if !holderSet[n.Name()] {
			remainingNodes = append(remainingNodes, n)
		}
	}
	t.Logf("[cascade-7d] remaining %d nodes: %v",
		len(remainingNodes), testrig.TestNodeNames(remainingNodes))

	if len(remainingNodes) == 0 {
		t.Fatal("[cascade-7d] all nodes were holders — no surviving nodes to test")
	}

	t.Log("[cascade-7d] stabilizing ring (S-1=4 rounds)")
	c.StabilizePrivate(group, testrig.DefaultSuccListSize-1)
	time.Sleep(2 * time.Second)

	for _, n := range remainingNodes {
		has, err := n.HasBlock(cidStr)
		if err != nil {
			t.Errorf("%s: HasBlock: %v", n.Name(), err)
			continue
		}
		if has {
			t.Errorf("[cascade-7d] %s: holds root block — should be irrecoverably lost", n.Name())
		} else {
			t.Logf("  [ok] %s: does not hold root block (correct, data is lost)", n.Name())
		}
	}

	t.Log("[cascade-7d] verifying fetch fails on all surviving nodes")
	for _, n := range remainingNodes {
		err := n.FetchCID(cidStr, group)
		if err == nil {
			t.Errorf("[cascade-7d] %s: fetch succeeded — should have failed (data is lost)", n.Name())
		} else if !strings.Contains(err.Error(), "not found") &&
			!strings.Contains(err.Error(), "block not found") &&
			!strings.Contains(err.Error(), "fetch") {
			t.Logf("[cascade-7d] %s: fetch failed with: %v", n.Name(), err)
		} else {
			t.Logf("  [ok] %s: fetch correctly failed: %v", n.Name(), err)
		}
	}

	newData := []byte("scenario-7d-post-loss-new-file-ring-still-works")
	cid2, err := remainingNodes[0].StoreFile(newData, group)
	if err != nil {
		t.Fatalf("store new file after loss: %v", err)
	}
	t.Logf("[cascade-7d] new CID2=%s stored from %s (ring still works)", cid2, remainingNodes[0].Name())

	for _, n := range remainingNodes {
		if err := n.FetchCID(cid2, group); err != nil {
			t.Errorf("[cascade-7d] %s: fetch CID2 failed: %v (ring should still work)", n.Name(), err)
		}
	}
	t.Log("[cascade-7d] ring remains functional for new data after irrecoverable loss of CID1")
}
