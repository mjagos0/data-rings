//go:build system

package system_test

import (
	"testing"

	"github.com/mjagos0/datarings/testrig"
)

func TestScenario_DeleteCID_RemovesFromNetwork(t *testing.T) {
	const k = 3

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()

	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	data := []byte("scenario-5a-delete-cid-test-content-unique-data")
	cidStr, err := nodes[0].StoreFile(data, group)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	t.Logf("[delete-5a] stored CID=%s from %s", cidStr, nodes[0].Name())

	holdersBefore := 0
	for _, n := range nodes {
		has, err := n.HasBlock(cidStr)
		if err != nil {
			t.Fatalf("%s: HasBlock: %v", n.Name(), err)
		}
		if has {
			holdersBefore++
			t.Logf("[delete-5a] before delete: %s holds root block", n.Name())
		}
	}
	if holdersBefore < k {
		t.Fatalf("[delete-5a] expected >= %d holders, got %d", k, holdersBefore)
	}
	t.Logf("[delete-5a] holders before delete: %d", holdersBefore)

	for _, n := range nodes {
		if err := n.FetchCID(cidStr, group); err != nil {
			t.Fatalf("[delete-5a] %s: prefetch failed: %v", n.Name(), err)
		}
	}

	if err := nodes[0].DeleteCID(cidStr, group); err != nil {
		t.Fatalf("delete-cid: %v", err)
	}
	t.Logf("[delete-5a] deleted CID=%s from network", cidStr)

	for _, n := range nodes {
		out, err := n.Exec("rm", cidStr)
		if err != nil {

			t.Logf("[delete-5a] %s: rm: %v", n.Name(), err)
		} else {
			_ = out
		}
	}

	for _, n := range nodes {
		out, err := n.Exec("gc")
		if err != nil {
			t.Fatalf("gc on %s: %v\n%s", n.Name(), err, out)
		}
		t.Logf("[delete-5a] %s: %s", n.Name(), out)
	}

	holdersAfter := 0
	for _, n := range nodes {
		has, err := n.HasBlock(cidStr)
		if err != nil {
			continue
		}
		if has {
			holdersAfter++
			t.Logf("[delete-5a] UNEXPECTED: %s still holds root block after delete+GC", n.Name())
		}
	}
	if holdersAfter > 0 {
		t.Errorf("[delete-5a] %d nodes still hold blocks after delete+GC — expected 0", holdersAfter)
	} else {
		t.Logf("[delete-5a] all blocks removed from network after delete+GC")
	}
}

func TestScenario_DeleteCID_PreservesOtherFiles(t *testing.T) {
	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()

	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	dataA := []byte("scenario-5b-file-A-to-delete-unique-content")
	cidA, err := nodes[0].StoreFile(dataA, group)
	if err != nil {
		t.Fatalf("store file A: %v", err)
	}
	t.Logf("[delete-5b] stored CID_A=%s", cidA)

	dataB := []byte("scenario-5b-file-B-to-preserve-unique-content")
	cidB, err := nodes[0].StoreFile(dataB, group)
	if err != nil {
		t.Fatalf("store file B: %v", err)
	}
	t.Logf("[delete-5b] stored CID_B=%s", cidB)

	if err := nodes[0].DeleteCID(cidA, group); err != nil {
		t.Fatalf("delete-cid A: %v", err)
	}
	out, err := nodes[0].Exec("rm", cidA)
	if err != nil {
		t.Fatalf("rm A: %v\n%s", err, out)
	}
	t.Logf("[delete-5b] deleted CID_A from network")

	for _, n := range nodes {
		out, err := n.Exec("gc")
		if err != nil {
			t.Fatalf("gc on %s: %v\n%s", n.Name(), err, out)
		}
	}

	for _, n := range nodes {
		if err := n.FetchCID(cidB, group); err != nil {
			t.Errorf("[delete-5b] %s: fetch CID_B failed after deleting CID_A: %v", n.Name(), err)
		}
	}
	t.Logf("[delete-5b] CID_B is still fetchable from all nodes")

	holdersA := 0
	for _, n := range nodes {
		has, err := n.HasBlock(cidA)
		if err != nil {
			continue
		}
		if has {
			holdersA++
		}
	}
	if holdersA > 0 {
		t.Errorf("[delete-5b] %d nodes still hold CID_A blocks — expected 0", holdersA)
	} else {
		t.Logf("[delete-5b] CID_A blocks fully removed")
	}
}

func TestScenario_DeleteCID_NetworkRootsPropagated(t *testing.T) {
	const k = 3

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()

	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	data := []byte("scenario-5c-network-roots-propagation-test")
	cidStr, err := nodes[0].StoreFile(data, group)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	t.Logf("[delete-5c] stored CID=%s from %s", cidStr, nodes[0].Name())

	replicaNodes := c.ReplicaNodesForCID(cidStr, k)
	t.Logf("[delete-5c] root block replicas: %v", testrig.NodeNamesFromSlice(replicaNodes))

	for _, n := range replicaNodes {
		hasBlock, err := n.HasBlock(cidStr)
		if err != nil {
			t.Fatalf("%s: HasBlock: %v", n.Name(), err)
		}
		if !hasBlock {
			t.Errorf("[delete-5c] replica %s: does NOT hold root block", n.Name())
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
			t.Errorf("[delete-5c] replica %s: does NOT have network root", n.Name())
		}
	}

	for _, n := range nodes {
		out, err := n.Exec("gc")
		if err != nil {
			t.Fatalf("gc on %s: %v\n%s", n.Name(), err, out)
		}
	}

	for _, n := range replicaNodes {
		has, err := n.HasBlock(cidStr)
		if err != nil {
			t.Fatalf("%s: HasBlock after GC: %v", n.Name(), err)
		}
		if !has {
			t.Errorf("[delete-5c] replica %s: lost block after GC — network root failed to preserve", n.Name())
		}
	}

	for _, n := range nodes {
		if err := n.FetchCID(cidStr, group); err != nil {
			t.Errorf("[delete-5c] %s: fetch failed: %v", n.Name(), err)
		}
	}
}
