//go:build system

package system_test

import (
	"testing"

	"github.com/mjagos0/datarings/testrig"
)

func TestScenario_Provider_PublishAndFetch(t *testing.T) {
	c := testrig.NewCluster(t, testrig.StateFreshPublic)
	nodes := c.Nodes8()
	c.Setup()

	data := []byte("scenario-5a-provider-record-test-content-unique")
	cidStr, err := nodes[0].StoreFile(data, "")
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	t.Logf("[provider-5a] %s stored and published CID=%s", nodes[0].Name(), cidStr)

	has, err := nodes[0].HasBlock(cidStr)
	if err != nil {
		t.Fatalf("HasBlock on publisher: %v", err)
	}
	if !has {
		t.Fatalf("[provider-5a] %s does not hold root block after store", nodes[0].Name())
	}

	fetcher := nodes[4]
	t.Logf("[provider-5a] %s fetching CID=%s via provider lookup", fetcher.Name(), cidStr)
	if err := fetcher.FetchCID(cidStr, ""); err != nil {
		t.Fatalf("[provider-5a] %s: fetch via provider failed: %v", fetcher.Name(), err)
	}
	t.Logf("[provider-5a] %s: fetch succeeded", fetcher.Name())

	has, err = fetcher.HasBlock(cidStr)
	if err != nil {
		t.Fatalf("HasBlock on fetcher: %v", err)
	}
	if !has {
		t.Errorf("[provider-5a] %s does not hold root block after fetch", fetcher.Name())
	} else {
		t.Logf("[provider-5a] %s: holds root block after fetch (correct)", fetcher.Name())
	}
}
