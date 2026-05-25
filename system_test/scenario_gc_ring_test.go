//go:build system

package system_test

import (
	"testing"

	"github.com/mjagos0/datarings/testrig"
)

func TestScenario_GC_PreservesRingBlocks(t *testing.T) {
	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()

	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	data := []byte("scenario-4b-gc-preserves-ring-blocks-test-payload")
	cidStr, err := nodes[0].StoreFile(data, group)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	publisher := nodes[0]
	t.Logf("[gc-4b] stored CID=%s from %s (publisher)", cidStr, publisher.Name())

	var holdersBefore []*testrig.TestNode
	for _, n := range nodes {
		has, err := n.HasBlock(cidStr)
		if err != nil {
			t.Fatalf("%s: HasBlock: %v", n.Name(), err)
		}
		if has {
			holdersBefore = append(holdersBefore, n)
		}
	}
	t.Logf("[gc-4b] holders before GC (%d): %v",
		len(holdersBefore), testrig.NodeNamesFromSlice(holdersBefore))

	if len(holdersBefore) < 2 {
		t.Fatalf("[gc-4b] need at least 2 holders (publisher + replica), got %d", len(holdersBefore))
	}

	var target *testrig.TestNode
	for _, n := range holdersBefore {
		if n.Name() != publisher.Name() {
			target = n
			break
		}
	}
	if target == nil {
		t.Fatal("[gc-4b] no non-publisher holder found")
	}
	t.Logf("[gc-4b] target (non-publisher ring replica): %s", target.Name())

	has, err := target.HasBlock(cidStr)
	if err != nil {
		t.Fatalf("target HasBlock: %v", err)
	}
	if !has {
		t.Fatalf("[gc-4b] %s: does not hold root block before GC", target.Name())
	}

	out, err := target.Exec("gc")
	if err != nil {
		t.Fatalf("gc on %s: %v\n%s", target.Name(), err, out)
	}
	t.Logf("[gc-4b] %s GC output: %s", target.Name(), out)

	has, err = target.HasBlock(cidStr)
	if err != nil {
		t.Fatalf("target HasBlock after GC: %v", err)
	}
	if !has {
		t.Errorf("[gc-4b] %s: lost root block after GC — GC must preserve ring-responsible blocks", target.Name())
	} else {
		t.Logf("[gc-4b] %s: correctly preserved root block after GC", target.Name())
	}

	var holdersAfter []*testrig.TestNode
	for _, n := range nodes {
		h, err := n.HasBlock(cidStr)
		if err != nil {
			continue
		}
		if h {
			holdersAfter = append(holdersAfter, n)
		}
	}
	t.Logf("[gc-4b] holders after GC (%d): %v",
		len(holdersAfter), testrig.NodeNamesFromSlice(holdersAfter))

	if len(holdersAfter) != len(holdersBefore) {
		t.Errorf("[gc-4b] holder count changed: before=%d after=%d — GC degraded replication",
			len(holdersBefore), len(holdersAfter))
	}

	ringRoots, err := target.RingNetworkRoots(group)
	if err != nil {
		t.Fatalf("%s RingNetworkRoots: %v", target.Name(), err)
	}
	hasRingRoot := false
	for _, r := range ringRoots {
		if r == cidStr {
			hasRingRoot = true
			break
		}
	}
	if !hasRingRoot {
		t.Errorf("[gc-4b] %s: ring %s lost network root after GC — per-ring index pruned",
			target.Name(), group)
	}

	t.Log("[gc-4b] verifying fetch from all nodes")
	for _, n := range nodes {
		if err := n.FetchCID(cidStr, group); err != nil {
			t.Errorf("[gc-4b] %s: fetch failed: %v", n.Name(), err)
		}
	}
}

func TestScenario_GC_AllNodesPreserveRingBlocks(t *testing.T) {
	const k = 3

	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()

	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	data := []byte("scenario-4c-gc-all-nodes-preserve-ring-blocks")
	cidStr, err := nodes[0].StoreFile(data, group)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	t.Logf("[gc-4c] stored CID=%s from %s", cidStr, nodes[0].Name())

	holderCountBefore := 0
	for _, n := range nodes {
		has, err := n.HasBlock(cidStr)
		if err != nil {
			t.Fatalf("%s: HasBlock: %v", n.Name(), err)
		}
		if has {
			holderCountBefore++
			t.Logf("[gc-4c] before GC: %s holds root block", n.Name())
		}
	}
	t.Logf("[gc-4c] holders before GC: %d", holderCountBefore)

	if holderCountBefore < k {
		t.Fatalf("[gc-4c] expected >= %d holders, got %d", k, holderCountBefore)
	}

	for _, n := range nodes {
		out, err := n.Exec("gc")
		if err != nil {
			t.Fatalf("gc on %s: %v\n%s", n.Name(), err, out)
		}
		t.Logf("[gc-4c] %s: %s", n.Name(), out)
	}

	holderCountAfter := 0
	for _, n := range nodes {
		has, err := n.HasBlock(cidStr)
		if err != nil {
			continue
		}
		if has {
			holderCountAfter++
			t.Logf("[gc-4c] after GC: %s holds root block", n.Name())
		}
	}
	t.Logf("[gc-4c] holders after GC: %d (was %d)", holderCountAfter, holderCountBefore)

	if holderCountAfter != holderCountBefore {
		t.Errorf("[gc-4c] GC destroyed ring blocks: holders %d → %d", holderCountBefore, holderCountAfter)
	}

	for _, n := range nodes {
		ringRoots, err := n.RingNetworkRoots(group)
		if err != nil {
			t.Fatalf("%s RingNetworkRoots: %v", n.Name(), err)
		}
		hasInRing := false
		for _, r := range ringRoots {
			if r == cidStr {
				hasInRing = true
				break
			}
		}
		hasBytes, _ := n.HasBlock(cidStr)
		if hasBytes && !hasInRing {
			t.Errorf("[gc-4c] %s: holds bytes but ring %s lost network root after GC",
				n.Name(), group)
		}
	}

	t.Log("[gc-4c] verifying fetch from all nodes")
	for _, n := range nodes {
		if err := n.FetchCID(cidStr, group); err != nil {
			t.Errorf("[gc-4c] %s: fetch failed: %v", n.Name(), err)
		}
	}
}
