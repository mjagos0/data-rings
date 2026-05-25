//go:build system

package system_test

import (
	"testing"
	"time"

	"github.com/mjagos0/datarings/testrig"
)

func TestScenario_BlockTransfer_GracefulLeave(t *testing.T) {
	const k = 3

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	storer := nodes[0]

	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	data := []byte("scenario-3a-graceful-leave-block-transfer-payload")
	cidStr, err := storer.StoreFile(data, group)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	t.Logf("[block-3a] stored CID=%s from %s", cidStr, storer.Name())

	keyHex := testrig.CIDToRingKeyHex(cidStr)

	replicaNodes := c.ReplicaNodesForCID(cidStr, k)
	replicaSet := testrig.NameSet(replicaNodes)

	t.Logf("[block-3a] 8-node replicas: %v", testrig.NodeNamesFromSlice(replicaNodes))

	for _, n := range nodes {
		has, err := n.HasBlock(cidStr)
		if err != nil {
			t.Fatalf("%s: HasBlock: %v", n.Name(), err)
		}
		if replicaSet[n.Name()] {
			if !has {
				t.Errorf("[block-3a] replica %s: expected to hold root block, but does not", n.Name())
			}
		} else if n.Name() != storer.Name() {
			if has {
				t.Errorf("[block-3a] non-replica %s: holds root block but should not", n.Name())
			}
		}
	}

	leavingNode := replicaNodes[0]
	t.Logf("[block-3a] %s (primary replica) gracefully leaving", leavingNode.Name())
	out, err := leavingNode.Exec("ring", "leave", group)
	if err != nil {
		t.Fatalf("leave failed: %v\n%s", err, out)
	}

	var remainingNodes []*testrig.TestNode
	var remainingIdents []testrig.PoolIdentity
	for _, n := range nodes {
		if n.Name() != leavingNode.Name() {
			remainingNodes = append(remainingNodes, n)
			remainingIdents = append(remainingIdents, n.GetIdentity())
		}
	}

	t.Log("[block-3a] stabilizing ring (S-1 rounds)")
	c.StabilizePrivate(group, testrig.DefaultSuccListSize-1)

	newReplicaIDs := testrig.ReplicaNodes(keyHex, remainingIdents, k)
	newReplicaSet := make(map[string]bool)
	for _, id := range newReplicaIDs {
		newReplicaSet[id] = true
	}

	t.Logf("[block-3a] 7-node replicas: %v (primary=%s)",
		testrig.ShortIDs(newReplicaIDs), testrig.NodeIDHexToShort(newReplicaIDs[0]))

	for _, n := range remainingNodes {
		has, err := n.HasBlock(cidStr)
		if err != nil {
			t.Errorf("%s: HasBlock: %v", n.Name(), err)
			continue
		}
		isReplica := newReplicaSet[n.ID()]
		if isReplica {
			if !has {
				t.Errorf("[block-3a] new replica %s: expected to hold root block, but does not", n.Name())
			} else {
				t.Logf("  [block-3a] %s: holds root block (expected replica)", n.Name())
			}
		} else if n.Name() != storer.Name() {
			if has {
				t.Errorf("[block-3a] non-replica %s: holds root block but should not", n.Name())
			}
		}
	}

	t.Log("[block-3a] verifying fetch from all remaining nodes")
	for _, n := range remainingNodes {
		if err := n.FetchCID(cidStr, group); err != nil {
			t.Errorf("[block-3a] %s: fetch CID failed: %v", n.Name(), err)
		}
	}
}

func TestScenario_BlockTransfer_AbruptLeave(t *testing.T) {
	const k = 3

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	storer := nodes[0]

	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	data := []byte("scenario-3b-abrupt-leave-block-transfer-payload")
	cidStr, err := storer.StoreFile(data, group)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	t.Logf("[block-3b] stored CID=%s from %s", cidStr, storer.Name())

	keyHex := testrig.CIDToRingKeyHex(cidStr)

	replicaNodes := c.ReplicaNodesForCID(cidStr, k)
	replicaSet := testrig.NameSet(replicaNodes)

	t.Logf("[block-3b] 8-node replicas: %v", testrig.NodeNamesFromSlice(replicaNodes))

	for _, n := range nodes {
		has, err := n.HasBlock(cidStr)
		if err != nil {
			t.Fatalf("%s: HasBlock: %v", n.Name(), err)
		}
		if replicaSet[n.Name()] {
			if !has {
				t.Errorf("[block-3b] replica %s: expected to hold root block, but does not", n.Name())
			}
		} else if n.Name() != storer.Name() {
			if has {
				t.Errorf("[block-3b] non-replica %s: holds root block but should not", n.Name())
			}
		}
	}

	crashNode := replicaNodes[0]
	t.Logf("[block-3b] abruptly stopping %s (primary replica)", crashNode.Name())
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

	survivingReplicas := replicaNodes[1:]
	for _, n := range survivingReplicas {
		has, err := n.HasBlock(cidStr)
		if err != nil {
			t.Fatalf("%s: HasBlock: %v", n.Name(), err)
		}
		if !has {
			t.Errorf("[block-3b] %s: surviving replica lost root block after crash", n.Name())
		}
	}

	t.Log("[block-3b] stabilizing ring (S-1 rounds)")
	c.StabilizePrivate(group, testrig.DefaultSuccListSize-1)
	time.Sleep(2 * time.Second)

	newReplicaIDs := testrig.ReplicaNodes(keyHex, remainingIdents, k)
	newPrimary := newReplicaIDs[0]

	t.Logf("[block-3b] 7-node replicas: %v (primary=%s)",
		testrig.ShortIDs(newReplicaIDs), testrig.NodeIDHexToShort(newPrimary))

	primaryNode := c.NodeByID(newPrimary)
	if primaryNode == nil {
		t.Fatalf("[block-3b] cannot find new primary node %s", testrig.NodeIDHexToShort(newPrimary))
	}
	has, err := primaryNode.HasBlock(cidStr)
	if err != nil {
		t.Fatalf("HasBlock on new primary: %v", err)
	}
	if !has {
		t.Errorf("[block-3b] new primary %s: does not hold root block", primaryNode.Name())
	} else {
		t.Logf("[block-3b] new primary %s: holds root block (correct)", primaryNode.Name())
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
			t.Logf("  [block-3b] %s: holds root block", n.Name())
		}
	}
	if holdCount < k {
		t.Errorf("[block-3b] only %d of 7 remaining nodes hold root block, want >= %d (re-replication should restore k)", holdCount, k)
	}

	t.Log("[block-3b] verifying fetch from all remaining nodes")
	for _, n := range remainingNodes {
		if err := n.FetchCID(cidStr, group); err != nil {
			t.Errorf("[block-3b] %s: fetch CID failed: %v", n.Name(), err)
		}
	}
}
