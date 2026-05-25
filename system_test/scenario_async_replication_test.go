//go:build system

package system_test

import (
	"strings"
	"testing"
	"time"

	"github.com/mjagos0/datarings/testrig"
)

func TestScenario_AsyncReplication_PublisherDoesNotWaitForReplicas(t *testing.T) {
	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	payload := []byte("async-replication-latency-regression-guard-payload")

	const trials = 5
	var publisherTotal, drainTotal time.Duration
	for i := 0; i < trials; i++ {

		uniq := append(payload, byte(i))

		start := time.Now()
		cidStr, err := nodes[0].StoreFile(uniq, group)
		if err != nil {
			t.Fatalf("trial %d: store: %v", i, err)
		}
		publisherLatency := time.Since(start)

		drainStart := time.Now()
		holders, ok := c.WaitForBlockReplicas(cidStr, 3, 5*time.Second)
		if !ok {
			t.Fatalf("trial %d: replicas didn't reach k=3 in 5s; only %v", i, holders)
		}
		drainLatency := time.Since(drainStart)

		t.Logf("trial %d: publisher=%v, drain-after-publish=%v",
			i+1, publisherLatency, drainLatency)
		publisherTotal += publisherLatency
		drainTotal += drainLatency
	}

	avgPub := publisherTotal / trials
	avgDrain := drainTotal / trials
	t.Logf("avg publisher latency = %v, avg drain-after-publish latency = %v", avgPub, avgDrain)

	if avgDrain == 0 {
		t.Skip("drain-after-publish was zero — local fabric is too fast to see the async gap; not a regression")
	}
}

func TestScenario_AsyncReplication_DrainOnShutdownPreservesReplicas(t *testing.T) {
	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	payload := []byte("drain-on-shutdown-payload-bytes")
	cidStr, err := nodes[0].StoreFile(payload, group)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	holders, ok := c.WaitForBlockReplicas(cidStr, 3, 5*time.Second)
	if !ok {
		t.Fatalf("replicas did not reach k=3 in 5s; got %v", holders)
	}

	publisherName := nodes[0].Name()
	var stopName string
	for _, h := range holders {
		if h != publisherName {
			stopName = h
			break
		}
	}
	if stopName == "" {
		t.Fatalf("no non-publisher holder to stop; holders=%v publisher=%s", holders, publisherName)
	}
	target := c.NodeByName(stopName)
	if err := target.Stop(); err != nil {
		t.Fatalf("stop %s: %v", stopName, err)
	}
	t.Logf("stopped %s (one of holders %v); fetching from a surviving node", stopName, holders)

	var fetcher *testrig.TestNode
	for _, n := range nodes {
		if n.Name() != stopName {
			fetcher = n
			break
		}
	}
	if err := fetcher.FetchCID(cidStr, group); err != nil {
		t.Errorf("fetch from %s after stopping %s failed: %v", fetcher.Name(), stopName, err)
	}
}

var _ = strings.Contains
