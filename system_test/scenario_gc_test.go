//go:build system

package system_test

import (
	"testing"

	"github.com/mjagos0/datarings/testrig"
)

func TestScenario_GarbageCollection(t *testing.T) {
	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()

	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	dataA := []byte("scenario-4a-gc-test-file-A-content-unique-aaa")
	cidA, err := nodes[0].StoreFile(dataA, group)
	if err != nil {
		t.Fatalf("store file A: %v", err)
	}
	t.Logf("[gc-4a] stored CID_A=%s from %s", cidA, nodes[0].Name())

	dataB := []byte("scenario-4a-gc-test-file-B-content-unique-bbb")
	cidB, err := nodes[0].StoreFile(dataB, group)
	if err != nil {
		t.Fatalf("store file B: %v", err)
	}
	t.Logf("[gc-4a] stored CID_B=%s from %s", cidB, nodes[0].Name())

	hasA, err := nodes[0].HasBlock(cidA)
	if err != nil {
		t.Fatalf("HasBlock CID_A: %v", err)
	}
	if !hasA {
		t.Fatalf("[gc-4a] %s does not hold CID_A root block before GC", nodes[0].Name())
	}

	hasB, err := nodes[0].HasBlock(cidB)
	if err != nil {
		t.Fatalf("HasBlock CID_B: %v", err)
	}
	if !hasB {
		t.Fatalf("[gc-4a] %s does not hold CID_B root block before GC", nodes[0].Name())
	}
	t.Logf("[gc-4a] %s holds both root blocks before GC", nodes[0].Name())

	out, err := nodes[0].Exec("rm", cidA)
	if err != nil {
		t.Fatalf("rm CID_A: %v\n%s", err, out)
	}
	t.Logf("[gc-4a] removed root for CID_A on %s", nodes[0].Name())

	out, err = nodes[0].Exec("gc")
	if err != nil {
		t.Fatalf("gc: %v\n%s", err, out)
	}
	t.Logf("[gc-4a] GC output: %s", out)

	hasA, err = nodes[0].HasBlock(cidA)
	if err != nil {
		t.Fatalf("HasBlock CID_A after GC: %v", err)
	}
	if hasA {
		t.Errorf("[gc-4a] %s still holds CID_A root block after GC — should have been collected", nodes[0].Name())
	} else {
		t.Logf("[gc-4a] %s: CID_A root block correctly removed by GC", nodes[0].Name())
	}

	hasB, err = nodes[0].HasBlock(cidB)
	if err != nil {
		t.Fatalf("HasBlock CID_B after GC: %v", err)
	}
	if !hasB {
		t.Errorf("[gc-4a] %s lost CID_B root block after GC — should have been preserved", nodes[0].Name())
	} else {
		t.Logf("[gc-4a] %s: CID_B root block correctly preserved by GC", nodes[0].Name())
	}

	if err := nodes[1].FetchCID(cidA, group); err != nil {
		t.Errorf("[gc-4a] %s: fetch CID_A after GC failed: %v — GC should only affect local node", nodes[1].Name(), err)
	} else {
		t.Logf("[gc-4a] CID_A still fetchable from network (GC is local-only)")
	}

	if err := nodes[1].FetchCID(cidB, group); err != nil {
		t.Errorf("[gc-4a] %s: fetch CID_B failed: %v", nodes[1].Name(), err)
	}
}
