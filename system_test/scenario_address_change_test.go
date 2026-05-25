//go:build system

package system_test

import (
	"strings"
	"testing"
	"time"

	"github.com/mjagos0/datarings/testrig"
)

const (
	addrChangeNewDHTPort	= 17900
	addrChangeNewAPIPort	= 17800
)

func TestScenario_AddressChange_SingleNodeNewPort(t *testing.T) {
	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	payload := []byte("address-change-resilience-test-payload-bytes")
	cidStr, err := nodes[0].StoreFile(payload, group)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	t.Logf("[addr-change] stored CID=%s", cidStr)

	victim := c.NodeByName("node5")
	t.Logf("[addr-change] stopping %s and restarting on new port", victim.Name())

	if err := victim.RestartOnNewPort(addrChangeNewDHTPort, addrChangeNewAPIPort); err != nil {
		t.Skipf("backend does not support per-test port reassignment: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	c.Stabilize(len(nodes) + testrig.DefaultSuccListSize - 2)
	c.RepublishSelf()

	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	failures := 0
	for _, n := range nodes {
		if err := n.FetchCID(cidStr, group); err != nil {
			t.Errorf("[addr-change] %s: fetch failed after node5 changed port: %v", n.Name(), err)
			failures++
		}
	}
	if failures == 0 {
		t.Log("[addr-change] all 8 nodes successfully fetched after address change")
	} else {
		t.Logf("[addr-change] %d / 8 nodes failed to fetch — staleness recovery is incomplete",
			failures)
	}
}

func TestScenario_AddressChange_RepeatedPortShuffle(t *testing.T) {
	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	payload := []byte("address-change-port-shuffle-test-payload")
	cidStr, err := nodes[0].StoreFile(payload, group)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	t.Logf("[shuffle] stored CID=%s", cidStr)

	victim := c.NodeByName("node5")

	hops := []struct {
		dhtPort	int
		apiPort	int
	}{
		{17910, 17810},
		{17920, 17820},
		{17930, 17830},
		{17940, 17840},
	}

	for i, hop := range hops {
		t.Logf("[shuffle] hop %d/%d: %s -> dht=%d api=%d", i+1, len(hops), victim.Name(), hop.dhtPort, hop.apiPort)
		if err := victim.RestartOnNewPort(hop.dhtPort, hop.apiPort); err != nil {
			if i == 0 {
				t.Skipf("backend does not support per-test port reassignment: %v", err)
			}
			t.Fatalf("hop %d: %v", i+1, err)
		}
		time.Sleep(300 * time.Millisecond)
		c.Stabilize(testrig.DefaultSuccListSize)
		c.RepublishSelf()
		c.StabilizePrivate(group, testrig.DefaultSuccListSize)
	}

	c.Stabilize(len(nodes) + testrig.DefaultSuccListSize - 2)
	c.RepublishSelf()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	failures := 0
	for _, n := range nodes {
		if err := n.FetchCID(cidStr, group); err != nil {
			t.Errorf("[shuffle] %s: fetch failed after %d port hops: %v", n.Name(), len(hops), err)
			failures++
		}
	}
	if failures == 0 {
		t.Logf("[shuffle] all 8 nodes converged after %d port hops", len(hops))
	}
}

func TestScenario_AddressChange_WildcardAdvertise(t *testing.T) {

	c := testrig.NewCluster(t, testrig.StateFreshPublic)
	c.DaemonArgs = []string{"--advertise", "0.0.0.0"}
	nodes := c.Nodes8()
	c.Setup()

	bad := 0
	for _, n := range nodes {
		s, err := n.PublicState()
		if err != nil {
			t.Logf("%s: PublicState: %v", n.Name(), err)
			continue
		}

		if strings.Contains(s.Successor.Addr, "0.0.0.0") {
			t.Errorf("[wildcard] %s: successor.Addr = %q contains 0.0.0.0 — peers cannot dial wildcard",
				n.Name(), s.Successor.Addr)
			bad++
		}
		for i, sl := range s.SuccessorList {
			if strings.Contains(sl.Addr, "0.0.0.0") {
				t.Errorf("[wildcard] %s: successorList[%d].Addr = %q contains 0.0.0.0",
					n.Name(), i, sl.Addr)
				bad++
			}
		}
	}
	if bad == 0 {
		t.Log("[wildcard] no node advertised 0.0.0.0 — daemon either rejected the flag or sanitised it")
	}

	cidStr, err := nodes[0].StoreFile([]byte("wildcard-advertise-functional-check"), "")
	if err != nil {
		t.Errorf("[wildcard] StoreFile after wildcard sanitisation failed: %v", err)
		return
	}
	if err := nodes[4].FetchCID(cidStr, ""); err != nil {
		t.Errorf("[wildcard] FetchCID from a non-publisher failed: %v", err)
	}
}
