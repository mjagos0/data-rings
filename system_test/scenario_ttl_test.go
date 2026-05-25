//go:build system

package system_test

import (
	"testing"
	"time"

	"github.com/mjagos0/datarings/testrig"
)

func TestScenario_TTL_ExpiresAndBlocksRemoved(t *testing.T) {
	const k = 3

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	data := []byte("scenario-6a-ttl-expiry-test-content-unique")
	cidStr, err := nodes[0].StoreFileWithTTL(data, group, "3s")
	if err != nil {
		t.Fatalf("store file with TTL: %v", err)
	}
	t.Logf("[ttl-6a] stored CID=%s with TTL=3s from %s", cidStr, nodes[0].Name())

	holdersBefore := 0
	for _, n := range nodes {
		has, err := n.HasBlock(cidStr)
		if err != nil {
			t.Fatalf("%s: HasBlock: %v", n.Name(), err)
		}
		if has {
			holdersBefore++
		}
	}
	t.Logf("[ttl-6a] holders before expiry: %d", holdersBefore)
	if holdersBefore < k {
		t.Fatalf("[ttl-6a] expected >= %d holders, got %d", k, holdersBefore)
	}

	for _, n := range nodes {
		if err := n.FetchCID(cidStr, group); err != nil {
			t.Fatalf("[ttl-6a] %s: pre-expiry fetch failed: %v", n.Name(), err)
		}
	}
	t.Logf("[ttl-6a] all nodes can fetch before TTL expiry")

	t.Logf("[ttl-6a] waiting 4s for TTL to expire...")
	time.Sleep(4 * time.Second)

	for _, n := range nodes {
		out, err := n.Exec("gc")
		if err != nil {
			t.Fatalf("gc on %s: %v\n%s", n.Name(), err, out)
		}
		t.Logf("[ttl-6a] %s: %s", n.Name(), out)
	}

	for _, n := range nodes {
		if out, err := n.Exec("rm", cidStr); err != nil {
			t.Logf("[ttl-6a] %s: rm (may fail if no local root): %v\n%s", n.Name(), err, out)
		}
	}
	for _, n := range nodes {
		out, err := n.Exec("gc")
		if err != nil {
			t.Logf("[ttl-6a] %s: second gc: %v\n%s", n.Name(), err, out)
		} else {
			t.Logf("[ttl-6a] %s: %s", n.Name(), out)
		}
	}

	holdersAfter := 0
	for _, n := range nodes {
		has, err := n.HasBlock(cidStr)
		if err != nil {
			continue
		}
		if has {
			holdersAfter++
			t.Logf("[ttl-6a] UNEXPECTED: %s still holds root block after TTL+GC", n.Name())
		}
	}
	if holdersAfter > 0 {
		t.Errorf("[ttl-6a] %d nodes still hold blocks after TTL expiry+GC — expected 0", holdersAfter)
	} else {
		t.Logf("[ttl-6a] all blocks removed from all nodes after TTL expiry+GC")
	}
	for _, n := range nodes {
		ringRootCount, err := n.RingNetworkRootCount(group)
		if err != nil {
			continue
		}
		if ringRootCount != 0 {
			t.Errorf("[ttl-6a] %s: ring %s has %d network root(s) after TTL+GC, want 0",
				n.Name(), group, ringRootCount)
		}
	}
}

func TestScenario_TTL_DoesNotAffectPermanentFiles(t *testing.T) {
	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	dataA := []byte("scenario-6b-file-A-ephemeral-with-ttl")
	cidA, err := nodes[0].StoreFileWithTTL(dataA, group, "3s")
	if err != nil {
		t.Fatalf("store file A with TTL: %v", err)
	}
	t.Logf("[ttl-6b] stored CID_A=%s (TTL=3s)", cidA)

	dataB := []byte("scenario-6b-file-B-permanent-no-ttl")
	cidB, err := nodes[0].StoreFile(dataB, group)
	if err != nil {
		t.Fatalf("store file B: %v", err)
	}
	t.Logf("[ttl-6b] stored CID_B=%s (permanent)", cidB)

	t.Logf("[ttl-6b] waiting 4s for TTL to expire...")
	time.Sleep(4 * time.Second)

	for _, n := range nodes {
		out, err := n.Exec("gc")
		if err != nil {
			t.Fatalf("gc on %s: %v\n%s", n.Name(), err, out)
		}
	}

	for _, n := range nodes {
		if err := n.FetchCID(cidB, group); err != nil {
			t.Errorf("[ttl-6b] %s: fetch CID_B (permanent) failed: %v", n.Name(), err)
		}
	}
	t.Logf("[ttl-6b] CID_B (permanent) still fetchable from all nodes")

	if out, err := nodes[0].Exec("rm", cidA); err != nil {
		t.Logf("[ttl-6b] rm CID_A on publisher: %v\n%s", err, out)
	}
	if out, err := nodes[0].Exec("gc"); err != nil {
		t.Logf("[ttl-6b] second gc on publisher: %v\n%s", err, out)
	}

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
		t.Errorf("[ttl-6b] %d nodes still hold CID_A after TTL+GC — expected 0", holdersA)
	} else {
		t.Logf("[ttl-6b] CID_A blocks fully removed after TTL expiry")
	}
}

func TestScenario_TTL_PropagatedToReplicas(t *testing.T) {
	const k = 3

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	data := []byte("scenario-6c-ttl-replica-propagation-test")
	cidStr, err := nodes[0].StoreFileWithTTL(data, group, "3s")
	if err != nil {
		t.Fatalf("store file with TTL: %v", err)
	}
	publisher := nodes[0]
	t.Logf("[ttl-6c] stored CID=%s with TTL=3s from %s", cidStr, publisher.Name())

	var nonPublisherHolders []*testrig.TestNode
	for _, n := range nodes[1:] {
		has, err := n.HasBlock(cidStr)
		if err != nil {
			t.Fatalf("%s: HasBlock: %v", n.Name(), err)
		}
		if has {
			nonPublisherHolders = append(nonPublisherHolders, n)
		}
	}
	t.Logf("[ttl-6c] non-publisher holders before expiry: %d (%v)",
		len(nonPublisherHolders), testrig.NodeNamesFromSlice(nonPublisherHolders))

	if len(nonPublisherHolders) < 1 {
		t.Fatalf("[ttl-6c] need at least 1 non-publisher holder, got %d", len(nonPublisherHolders))
	}

	t.Logf("[ttl-6c] waiting 4s for TTL to expire...")
	time.Sleep(4 * time.Second)

	for _, n := range nonPublisherHolders {
		out, err := n.Exec("gc")
		if err != nil {
			t.Fatalf("gc on %s: %v\n%s", n.Name(), err, out)
		}
		t.Logf("[ttl-6c] %s: %s", n.Name(), out)
	}

	stillHolding := 0
	for _, n := range nonPublisherHolders {
		has, err := n.HasBlock(cidStr)
		if err != nil {
			continue
		}
		if has {
			stillHolding++
			t.Logf("[ttl-6c] UNEXPECTED: %s still holds block after TTL+GC", n.Name())
		}
	}
	if stillHolding > 0 {
		t.Errorf("[ttl-6c] %d non-publisher replicas still hold blocks — TTL was not propagated", stillHolding)
	} else {
		t.Logf("[ttl-6c] all non-publisher replicas dropped blocks after TTL expiry — TTL propagation confirmed")
	}

	for _, n := range nonPublisherHolders {
		ringRoots, err := n.RingNetworkRoots(group)
		if err != nil {
			continue
		}
		for _, r := range ringRoots {
			if r == cidStr {
				t.Errorf("[ttl-6c] %s: ring %s still has network root after TTL+GC", n.Name(), group)
			}
		}
	}
}
