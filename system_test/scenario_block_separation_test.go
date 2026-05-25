//go:build system

package system_test

import (
	"testing"
	"time"

	"github.com/mjagos0/datarings/testrig"
)

func TestScenario_NetworkRoots_SurviveRestart(t *testing.T) {
	const k = 3

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	data := []byte("network-roots-must-persist-across-restart!")
	cidStr, err := nodes[0].StoreFile(data, group)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	t.Logf("[persist-1] stored CID=%s from %s", cidStr, nodes[0].Name())

	replicaNodes := c.ReplicaNodesForCID(cidStr, k)
	var target *testrig.TestNode
	for _, n := range replicaNodes {
		if n.Name() != nodes[0].Name() {
			has, err := n.HasBlock(cidStr)
			if err == nil && has {
				target = n
				break
			}
		}
	}
	if target == nil {
		t.Fatal("[persist-2] could not find a non-publisher replica holding the block")
	}
	t.Logf("[persist-2] target replica: %s", target.Name())

	if err := target.Stop(); err != nil {
		t.Fatalf("stop %s: %v", target.Name(), err)
	}
	time.Sleep(500 * time.Millisecond)
	if err := target.Start(); err != nil {
		t.Fatalf("start %s: %v", target.Name(), err)
	}
	t.Logf("[persist-3] restarted %s", target.Name())

	out, err := target.Exec("gc")
	if err != nil {
		t.Fatalf("gc on %s: %v\n%s", target.Name(), err, out)
	}
	t.Logf("[persist-4] GC on %s: %s", target.Name(), out)

	has, err := target.HasBlock(cidStr)
	if err != nil {
		t.Fatalf("HasBlock on %s: %v", target.Name(), err)
	}
	if !has {
		t.Errorf("[persist-5] %s: block MISSING after restart+GC — network roots did not persist!", target.Name())
	} else {
		t.Logf("[persist-5] %s: block preserved after restart+GC (network roots persisted)", target.Name())
	}
}

func TestScenario_FetchedBlocks_StayLocal(t *testing.T) {
	const k = 3

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	data := []byte("fetched-blocks-must-stay-local-and-not-replicate!")
	cidStr, err := nodes[0].StoreFile(data, group)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	t.Logf("[fetch-1] stored CID=%s from %s", cidStr, nodes[0].Name())

	replicaNodes := c.ReplicaNodesForCID(cidStr, k)
	replicaSet := testrig.NameSet(replicaNodes)
	publisher := nodes[0].Name()

	var fetcher, bystander *testrig.TestNode
	for _, n := range nodes {
		if replicaSet[n.Name()] || n.Name() == publisher {
			continue
		}
		if fetcher == nil {
			fetcher = n
		} else if bystander == nil {
			bystander = n
			break
		}
	}
	if fetcher == nil || bystander == nil {
		t.Fatal("[fetch-2] could not find two non-replica, non-publisher nodes")
	}
	t.Logf("[fetch-2] fetcher=%s bystander=%s replicas=%v", fetcher.Name(), bystander.Name(),
		testrig.NodeNamesFromSlice(replicaNodes))

	if err := fetcher.FetchCID(cidStr, group); err != nil {
		t.Fatalf("fetch on %s: %v", fetcher.Name(), err)
	}
	has, _ := fetcher.HasBlock(cidStr)
	if !has {
		t.Fatalf("[fetch-3] %s: does not hold block after fetch", fetcher.Name())
	}
	t.Logf("[fetch-3] %s fetched CID and holds block", fetcher.Name())

	c.StabilizePrivate(group, 4)

	has, err = bystander.HasBlock(cidStr)
	if err != nil {
		t.Fatalf("%s: HasBlock: %v", bystander.Name(), err)
	}
	if has {
		t.Errorf("[fetch-4] %s: holds block — fetched blocks leaked via replication!", bystander.Name())
	} else {
		t.Logf("[fetch-4] %s: does not hold block (correct, fetched blocks stay local)", bystander.Name())
	}
}

func TestScenario_LocalGC_DoesNotAffectNetworkBlocks(t *testing.T) {
	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	dataA := []byte("file-A-pushed-to-ring-should-survive-local-gc!")
	cidA, err := nodes[0].StoreFile(dataA, group)
	if err != nil {
		t.Fatalf("store file A: %v", err)
	}
	t.Logf("[lgc-1] stored CID_A=%s (ring)", cidA)

	cidB := addFileLocally(t, nodes[0], []byte("file-B-local-only-will-be-gc-removed"))
	t.Logf("[lgc-2] added CID_B=%s (local only)", cidB)

	if _, err := nodes[0].Exec("rm", cidB); err != nil {
		t.Fatalf("rm CID_B: %v", err)
	}
	out, err := nodes[0].Exec("gc")
	if err != nil {
		t.Fatalf("gc on node1: %v\n%s", err, out)
	}
	t.Logf("[lgc-3] removed CID_B local root and ran GC: %s", out)

	if err := nodes[4].FetchCID(cidA, group); err != nil {
		t.Errorf("[lgc-4] file A not fetchable after local GC: %v", err)
	} else {
		t.Log("[lgc-4] file A still fetchable from ring (network blocks unaffected)")
	}

	has, _ := nodes[0].HasBlock(cidB)
	if has {
		t.Errorf("[lgc-5] CID_B still present on node1 after rm+GC — local GC failed")
	} else {
		t.Log("[lgc-5] CID_B correctly removed from node1 after rm+GC")
	}
}

func TestScenario_NetworkRoots_ExactPlacement(t *testing.T) {
	const k = 3

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	data := []byte("network-roots-exact-placement-test!")
	cidStr, err := nodes[0].StoreFile(data, group)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	t.Logf("[exact-1] stored CID=%s from %s", cidStr, nodes[0].Name())

	replicaNodes := c.ReplicaNodesForCID(cidStr, k)
	replicaSet := testrig.NameSet(replicaNodes)
	t.Logf("[exact-2] root block replicas: %v", testrig.NodeNamesFromSlice(replicaNodes))

	for _, n := range replicaNodes {
		hasBlock, err := n.HasBlock(cidStr)
		if err != nil {
			t.Fatalf("%s: HasBlock: %v", n.Name(), err)
		}
		if !hasBlock {
			t.Errorf("[exact-3] replica %s: does NOT hold root block", n.Name())
		}
	}

	publisher := nodes[0].Name()
	for _, n := range nodes {
		roots, err := n.NetworkRoots()
		if err != nil {
			t.Fatalf("%s: NetworkRoots: %v", n.Name(), err)
		}
		hasRoot := false
		for _, r := range roots {
			if r == cidStr {
				hasRoot = true
				break
			}
		}

		hasRootBlock, _ := n.HasBlock(cidStr)
		holdsAnyBlock := replicaSet[n.Name()] || hasRoot

		if n.Name() == publisher {
			continue
		}

		if hasRoot && !holdsAnyBlock {
			t.Errorf("[exact-4] %s: has network root but holds no blocks — phantom root", n.Name())
		}
		if !hasRoot && hasRootBlock {
			t.Errorf("[exact-4] %s: holds root block but has no network root", n.Name())
		}

		t.Logf("  %s: root_block=%v network_root=%v", n.Name(), hasRootBlock, hasRoot)
	}

	for _, n := range nodes {
		ringRoots, err := n.RingNetworkRoots(group)
		if err != nil {
			t.Fatalf("%s: RingNetworkRoots(%s): %v", n.Name(), group, err)
		}
		ringHasRoot := false
		for _, r := range ringRoots {
			if r == cidStr {
				ringHasRoot = true
				break
			}
		}
		aggRoots, _ := n.NetworkRoots()
		aggHasRoot := false
		for _, r := range aggRoots {
			if r == cidStr {
				aggHasRoot = true
				break
			}
		}
		if ringHasRoot != aggHasRoot {
			t.Errorf("[exact-5] %s: per-ring substore disagrees with aggregate (ring=%v aggregate=%v)",
				n.Name(), ringHasRoot, aggHasRoot)
		}
	}
}

func TestScenario_DeleteCID_CleansNetworkRoots(t *testing.T) {
	const k = 3

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	data := []byte("delete-cid-should-clean-network-roots-and-blocks!")
	cidStr, err := nodes[0].StoreFile(data, group)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	t.Logf("[delroot-1] stored CID=%s", cidStr)

	replicaNodes := c.ReplicaNodesForCID(cidStr, k)
	t.Logf("[delroot-2] root block replicas: %v", testrig.NodeNamesFromSlice(replicaNodes))

	for _, n := range replicaNodes {
		hasBlock, err := n.HasBlock(cidStr)
		if err != nil {
			t.Fatalf("%s: HasBlock: %v", n.Name(), err)
		}
		if !hasBlock {
			t.Errorf("[delroot-2] replica %s: does NOT hold root block", n.Name())
		}

		roots, err := n.NetworkRoots()
		if err != nil {
			t.Fatalf("%s: NetworkRoots: %v", n.Name(), err)
		}
		hasRoot := false
		for _, r := range roots {
			if r == cidStr {
				hasRoot = true
				break
			}
		}
		if !hasRoot {
			t.Errorf("[delroot-2] replica %s: does NOT have network root", n.Name())
		}
	}

	if err := nodes[0].DeleteCID(cidStr, group); err != nil {
		t.Fatalf("delete CID: %v", err)
	}
	t.Log("[delroot-3] deleted CID from network")

	for _, n := range nodes {
		n.Exec("rm", cidStr)
	}
	for _, n := range nodes {
		if _, err := n.Exec("gc"); err != nil {
			t.Logf("[delroot-4] gc on %s: %v", n.Name(), err)
		}
	}

	for _, n := range nodes {
		roots, err := n.NetworkRoots()
		if err != nil {
			continue
		}
		for _, r := range roots {
			if r == cidStr {
				t.Errorf("[delroot-5] %s: still has network root after delete+GC", n.Name())
			}
		}

		has, _ := n.HasBlock(cidStr)
		if has {
			t.Errorf("[delroot-5] %s: still holds block after delete+GC", n.Name())
		}

		ringRoots, err := n.RingNetworkRoots(group)
		if err == nil {
			for _, r := range ringRoots {
				if r == cidStr {
					t.Errorf("[delroot-5] %s: ring %s still has network root after delete+GC",
						n.Name(), group)
				}
			}
		}
	}

	for _, n := range nodes {
		ringRootCount, err := n.RingNetworkRootCount(group)
		if err != nil {
			continue
		}
		if ringRootCount != 0 {
			t.Errorf("[delroot-6] %s: ring %s has %d network root(s) after delete+GC, want 0",
				n.Name(), group, ringRootCount)
		}
	}
	t.Log("[delroot-5] all network roots and blocks cleaned up after delete+GC")
}
