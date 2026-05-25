//go:build system

package system_test

import (
	"testing"

	"github.com/mjagos0/datarings/testrig"
)

func TestScenario_Replication_ProviderRecord(t *testing.T) {
	const k = 3

	c := testrig.NewCluster(t, testrig.StateFreshPublic)
	nodes := c.Nodes8()
	c.Setup()

	topo := c.Topology()

	ring := topo.Nodes
	pair1Primary := ring[0]
	pair2Primary := ring[1]

	t.Logf("pair1 primary: %s (%s)", c.NodeByID(pair1Primary.NodeIDHex).Name(), testrig.NodeIDHexToShort(pair1Primary.NodeIDHex))
	t.Logf("pair2 primary: %s (%s)", c.NodeByID(pair2Primary.NodeIDHex).Name(), testrig.NodeIDHexToShort(pair2Primary.NodeIDHex))

	cid1Content := generateContent(pair1Primary.NodeIDHex)
	cid2Content := generateContent(pair2Primary.NodeIDHex)

	publisher := nodes[0]
	cidStr1, err := publisher.StoreFile([]byte(cid1Content), "")
	if err != nil {
		t.Fatalf("store file 1: %v", err)
	}
	cidStr2, err := publisher.StoreFile([]byte(cid2Content), "")
	if err != nil {
		t.Fatalf("store file 2: %v", err)
	}

	actualKey1 := testrig.CIDToRingKeyHex(cidStr1)
	replicas1 := c.ReplicaNodesForCID(cidStr1, k)
	replicaSet1 := testrig.NameSet(replicas1)
	t.Logf("CID1=%s, ring key=%s, replicas=%v", cidStr1, testrig.NodeIDHexToShort(actualKey1), testrig.NodeNamesFromSlice(replicas1))

	for _, n := range nodes {
		has, err := n.HasRecord(actualKey1)
		if err != nil {
			t.Errorf("%s: HasRecord(%s): %v", n.Name(), testrig.NodeIDHexToShort(actualKey1), err)
			continue
		}
		if replicaSet1[n.Name()] {
			if !has {
				t.Errorf("%s: MISSING ProviderRecord 1 (expected as replica)", n.Name())
			}
		} else {
			if has {
				t.Errorf("%s: has ProviderRecord 1 but should NOT (not in replica set)", n.Name())
			}
		}
	}

	actualKey2 := testrig.CIDToRingKeyHex(cidStr2)
	replicas2 := c.ReplicaNodesForCID(cidStr2, k)
	replicaSet2 := testrig.NameSet(replicas2)
	t.Logf("CID2=%s, ring key=%s, replicas=%v", cidStr2, testrig.NodeIDHexToShort(actualKey2), testrig.NodeNamesFromSlice(replicas2))

	for _, n := range nodes {
		has, err := n.HasRecord(actualKey2)
		if err != nil {
			t.Errorf("%s: HasRecord(%s): %v", n.Name(), testrig.NodeIDHexToShort(actualKey2), err)
			continue
		}
		if replicaSet2[n.Name()] {
			if !has {
				t.Errorf("%s: MISSING ProviderRecord 2 (expected as replica)", n.Name())
			}
		} else {
			if has {
				t.Errorf("%s: has ProviderRecord 2 but should NOT (not in replica set)", n.Name())
			}
		}
	}

	peerIDs := testrig.PeerIDSet(nodes)
	for _, n := range nodes {
		keys, err := n.RecordKeys()
		if err != nil {
			t.Errorf("%s: RecordKeys: %v", n.Name(), err)
			continue
		}
		unexpected := 0
		for _, k := range keys {
			if peerIDs[k] {
				continue
			}
			if k == actualKey1 && replicaSet1[n.Name()] {
				continue
			}
			if k == actualKey2 && replicaSet2[n.Name()] {
				continue
			}
			unexpected++
			t.Logf("  %s: unexpected record key: %s", n.Name(), testrig.NodeIDHexToShort(k))
		}
		if unexpected > 0 {
			t.Errorf("%s: %d unexpected records", n.Name(), unexpected)
		}
	}
}

func TestScenario_Replication_PeerIdentityRecord(t *testing.T) {
	const k = 3

	c := testrig.NewCluster(t, testrig.StateFreshPublic)
	nodes := c.Nodes8()
	c.Setup()

	for _, target := range nodes {
		peerIDHex := target.ID()
		expectedReplicas := c.ReplicaNodesForHash(peerIDHex, k)
		expectedSet := testrig.NameSet(expectedReplicas)

		for _, n := range nodes {
			has, err := n.HasRecord(peerIDHex)
			if err != nil {
				t.Errorf("%s: HasRecord(%s): %v", n.Name(), target.Name(), err)
				continue
			}
			if expectedSet[n.Name()] {
				if !has {
					t.Errorf("%s: MISSING PeerIdentityRecord for %s", n.Name(), target.Name())
				}
			} else {
				if has {
					t.Errorf("%s: has PeerIdentityRecord for %s but should NOT", n.Name(), target.Name())
				}
			}
		}
	}
}

func TestScenario_Replication_GroupIdentityRecord(t *testing.T) {
	const k = 3

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	entry, ok := nodes[0].PrivateState(c.GroupName())
	if !ok {
		t.Fatal("node1 not in private ring")
	}
	groupIDHex := entry.GroupID
	t.Logf("GroupID: %s", testrig.NodeIDHexToShort(groupIDHex))

	expectedReplicas := c.ReplicaNodesForHash(groupIDHex, k)
	expectedSet := testrig.NameSet(expectedReplicas)
	t.Logf("expected replicas: %v", testrig.NodeNamesFromSlice(expectedReplicas))

	for _, n := range nodes {
		has, err := n.HasRecord(groupIDHex)
		if err != nil {
			t.Errorf("%s: HasRecord(%s): %v", n.Name(), testrig.NodeIDHexToShort(groupIDHex), err)
			continue
		}
		if expectedSet[n.Name()] {
			if !has {
				t.Errorf("%s: MISSING GroupIdentityRecord", n.Name())
			}
		} else {
			if has {
				t.Errorf("%s: has GroupIdentityRecord but should NOT", n.Name())
			}
		}
	}
}

func TestScenario_Replication_PrivateBlock(t *testing.T) {
	const k = 3

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()

	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	data := []byte("scenario-1d-private-block-replication-test-payload!")
	cidStr, err := nodes[0].StoreFile(data, group)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	t.Logf("[repl-1d] stored CID=%s from %s", cidStr, nodes[0].Name())

	replicaNodes := c.ReplicaNodesForCID(cidStr, k)
	replicaSet := testrig.NameSet(replicaNodes)
	t.Logf("[repl-1d] expected replicas: %v", testrig.NodeNamesFromSlice(replicaNodes))

	for _, n := range replicaNodes {
		has, err := n.HasBlock(cidStr)
		if err != nil {
			t.Fatalf("%s: HasBlock: %v", n.Name(), err)
		}
		if !has {
			t.Errorf("[repl-1d] %s: expected replica does NOT hold root block", n.Name())
		} else {
			t.Logf("  [ok] %s: holds root block (expected replica)", n.Name())
		}
		ringCount, err := n.RingBlockCount(group)
		if err != nil {
			t.Fatalf("%s: RingBlockCount(%s): %v", n.Name(), group, err)
		}
		if ringCount == 0 {
			t.Errorf("[repl-1d] %s: ring %s shows 0 blocks but node is a replica", n.Name(), group)
		}
	}

	publisher := nodes[0].Name()
	for _, n := range nodes {
		if replicaSet[n.Name()] || n.Name() == publisher {
			continue
		}
		has, err := n.HasBlock(cidStr)
		if err != nil {
			t.Errorf("%s: HasBlock: %v", n.Name(), err)
			continue
		}
		if has {
			t.Errorf("[repl-1d] %s: holds root block but is neither replica nor publisher (over-replication)", n.Name())
		} else {
			t.Logf("  [ok] %s: does not hold root block (correct, not a replica)", n.Name())
		}
	}

	t.Log("[repl-1d] verifying fetch from all nodes")
	for _, n := range nodes {
		if err := n.FetchCID(cidStr, group); err != nil {
			t.Errorf("[repl-1d] %s: fetch failed: %v", n.Name(), err)
		}
	}
}
