//go:build system

package system_test

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/mjagos0/datarings/testrig"
)

type chaosStabilizer struct {
	t	*testing.T
	rounds	atomic.Int64
	stopCh	chan struct{}
	doneCh	chan struct{}
}

func startChaosStabilizer(t *testing.T, c *testrig.Cluster, group string, interval time.Duration) *chaosStabilizer {
	t.Helper()
	cs := &chaosStabilizer{
		t:	t,
		stopCh:	make(chan struct{}),
		doneCh:	make(chan struct{}),
	}
	nodes := c.Nodes()
	go func() {
		defer close(cs.doneCh)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-cs.stopCh:
				return
			case <-ticker.C:
				var wg sync.WaitGroup
				for _, n := range nodes {
					wg.Add(1)
					go func(n *testrig.TestNode) {
						defer wg.Done()
						_ = n.ForceStabilizePrivate(group)
					}(n)
				}
				wg.Wait()
				cs.rounds.Add(1)
			}
		}
	}()
	return cs
}

func (cs *chaosStabilizer) stop() int64 {
	close(cs.stopCh)
	<-cs.doneCh
	r := cs.rounds.Load()
	cs.t.Logf("[chaos-stabilize] stopped after %d storm rounds", r)
	return r
}

func uniquePayload(i int, size int) []byte {
	buf := make([]byte, size)
	header := []byte(fmt.Sprintf("chaos-payload-%08d-", i))
	copy(buf, header)
	for j := len(header); j < size; j++ {
		buf[j] = byte(i*7 + j*13)
	}
	return buf
}

func expectedReplicaWindow(t *testing.T, c *testrig.Cluster, cidStr string, k int, dead map[string]bool) []string {
	t.Helper()
	decoded, err := cid.Decode(cidStr)
	if err != nil {
		t.Fatalf("expectedReplicaWindow: decode CID %q: %v", cidStr, err)
	}
	target := sha1.Sum(decoded.Hash())
	targetHex := hex.EncodeToString(target[:])

	sorted := append([]testrig.PoolIdentity(nil), c.Topology().Nodes...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].NodeIDHex < sorted[j].NodeIDHex })

	primaryIdx := 0
	found := false
	for i, n := range sorted {
		if n.NodeIDHex >= targetHex {
			primaryIdx = i
			found = true
			break
		}
	}
	if !found {

		primaryIdx = 0
	}

	out := make([]string, 0, k)
	n := len(sorted)
	for j := 0; j < n && len(out) < k; j++ {
		nodeID := sorted[(primaryIdx+j)%n].NodeIDHex
		name := c.Assignment().IDToNode[nodeID]
		if dead[name] {
			continue
		}
		out = append(out, name)
	}
	return out
}

type ringHoldersIndex map[string]map[string]struct{}

func buildRingHoldersIndex(t *testing.T, c *testrig.Cluster, group string, dead map[string]bool) ringHoldersIndex {
	t.Helper()
	out := make(ringHoldersIndex, len(c.Nodes()))
	for _, n := range c.Nodes() {
		if dead[n.Name()] {
			continue
		}
		cids, err := n.RingNetworkBlockCIDs(group)
		if err != nil {
			t.Errorf("buildRingHoldersIndex: %s: %v", n.Name(), err)
			continue
		}
		s := make(map[string]struct{}, len(cids))
		for _, cidStr := range cids {
			s[cidToMultihashHex(cidStr)] = struct{}{}
		}
		out[n.Name()] = s
	}
	return out
}

func ringHoldersOfCID(idx ringHoldersIndex, cidStr string) []string {
	mh := cidToMultihashHex(cidStr)
	var out []string
	for name, set := range idx {
		if _, ok := set[mh]; ok {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func assertReplicationInvariants(t *testing.T, c *testrig.Cluster, idx ringHoldersIndex, cidStr string, k, tolExtra int, dead map[string]bool, label string) {
	t.Helper()
	expected := expectedReplicaWindow(t, c, cidStr, k, dead)
	holders := ringHoldersOfCID(idx, cidStr)

	if len(holders) < len(expected) {
		t.Errorf("%s: CID=%s ring-holders=%d, want ≥ %d (under-replication; data loss). holders=%v expected_window=%v",
			label, cidStr, len(holders), len(expected), holders, expected)
	}
	if len(holders) > k+tolExtra {
		t.Errorf("%s: CID=%s ring-holders=%d, want ≤ %d (over-replication). holders=%v expected_window=%v",
			label, cidStr, len(holders), k+tolExtra, holders, expected)
	}

	expectedSet := make(map[string]bool, len(expected))
	for _, n := range expected {
		expectedSet[n] = true
	}
	var outsiders []string
	for _, h := range holders {
		if !expectedSet[h] {
			outsiders = append(outsiders, h)
		}
	}
	if len(outsiders) > 0 {
		t.Errorf("%s: CID=%s ring-held by outsiders %v (must be subset of live window %v). holders=%v",
			label, cidStr, outsiders, expected, holders)
	}
}

func assertAllDaemonsHealthy(t *testing.T, c *testrig.Cluster, dead map[string]bool, label string) {
	t.Helper()
	type result struct {
		name	string
		err	error
	}
	out := make(chan result, len(c.Nodes()))
	for _, n := range c.Nodes() {
		if dead[n.Name()] {
			continue
		}
		go func(n *testrig.TestNode) {
			done := make(chan error, 1)
			go func() {
				_, err := n.PublicState()
				done <- err
			}()
			select {
			case err := <-done:
				out <- result{name: n.Name(), err: err}
			case <-time.After(2 * time.Second):
				out <- result{name: n.Name(), err: fmt.Errorf("PublicState timeout (2s)")}
			}
		}(n)
	}
	expectedAlive := 0
	for _, n := range c.Nodes() {
		if !dead[n.Name()] {
			expectedAlive++
		}
	}
	for i := 0; i < expectedAlive; i++ {
		r := <-out
		if r.err != nil {
			t.Errorf("%s: daemon %s unhealthy after chaos: %v", label, r.name, r.err)
		}
	}
}

func assertTotalRingStorageWithinBounds(t *testing.T, c *testrig.Cluster, group string, expectedBytes int64, tolerance float64, dead map[string]bool, label string) {
	t.Helper()
	var total int64
	for _, n := range c.Nodes() {
		if dead[n.Name()] {
			continue
		}
		used, err := n.RingStorageUsedBytes(group)
		if err != nil {
			t.Errorf("%s: %s.RingStorageUsedBytes: %v", label, n.Name(), err)
			continue
		}
		total += used
	}
	lo := int64(float64(expectedBytes) * (1 - tolerance))
	hi := int64(float64(expectedBytes) * (1 + tolerance))
	if total < lo {
		t.Errorf("%s: total ring storage %s < expected %s (data loss; tolerance ±%.0f%%)",
			label, formatBytes(total), formatBytes(expectedBytes), tolerance*100)
	}
	if total > hi {
		t.Errorf("%s: total ring storage %s > expected %s (over-replication; tolerance ±%.0f%%)",
			label, formatBytes(total), formatBytes(expectedBytes), tolerance*100)
	}
	t.Logf("%s: total ring storage %s (expected %s ± %.0f%%)",
		label, formatBytes(total), formatBytes(expectedBytes), tolerance*100)
}

func TestScenario_Chaos_AsyncReplication_BurstyPublishUnderStorm(t *testing.T) {
	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	const (
		publishersPerBurst	= 4
		filesPerPublisher	= 12
		smallPayloadSize	= 96
		stabilizeInterval	= 50 * time.Millisecond
	)

	storm := startChaosStabilizer(t, c, group, stabilizeInterval)

	type publishResult struct {
		cid	string
		payload	[]byte
		err	error
		latency	time.Duration
	}
	results := make([][]publishResult, publishersPerBurst)
	for i := range results {
		results[i] = make([]publishResult, filesPerPublisher)
	}

	var wg sync.WaitGroup
	burstStart := time.Now()
	for p := 0; p < publishersPerBurst; p++ {
		wg.Add(1)
		go func(publisherIdx int) {
			defer wg.Done()
			pub := nodes[publisherIdx]
			for i := 0; i < filesPerPublisher; i++ {
				payload := uniquePayload(publisherIdx*1000+i, smallPayloadSize)
				start := time.Now()
				cidStr, err := pub.StoreFile(payload, group)
				results[publisherIdx][i] = publishResult{
					cid:		cidStr,
					payload:	payload,
					err:		err,
					latency:	time.Since(start),
				}
			}
		}(p)
	}
	wg.Wait()
	burstElapsed := time.Since(burstStart)
	stormRounds := storm.stop()

	t.Logf("[burst] %d publishers × %d files = %d publishes in %v (storm: %d rounds)",
		publishersPerBurst, filesPerPublisher, publishersPerBurst*filesPerPublisher,
		burstElapsed, stormRounds)

	c.StabilizePrivate(group, 6)

	var allCIDs []string
	var allPayloads [][]byte
	publishFailures := 0
	for p := 0; p < publishersPerBurst; p++ {
		for i, r := range results[p] {
			if r.err != nil {
				publishFailures++
				t.Errorf("[burst] publisher=%d file=%d: %v", p, i, r.err)
				continue
			}
			allCIDs = append(allCIDs, r.cid)
			allPayloads = append(allPayloads, r.payload)
		}
	}
	if publishFailures > 0 {
		t.Fatalf("[burst] %d/%d publishes failed under chaos — fire-and-forget pipeline did not survive",
			publishFailures, publishersPerBurst*filesPerPublisher)
	}

	belowK := 0
	for _, cid := range allCIDs {
		holders, ok := c.WaitForBlockReplicas(cid, 3, 10*time.Second)
		if !ok {
			belowK++
			t.Errorf("[burst] CID=%s did not reach k=3: holders=%v", cid, holders)
		}
	}
	if belowK > 0 {
		t.Fatalf("[burst] %d/%d CIDs below k=3 after settle", belowK, len(allCIDs))
	}

	fetchers := []*testrig.TestNode{nodes[0], nodes[3], nodes[7]}
	for _, f := range fetchers {
		for i, cid := range allCIDs {
			if err := f.FetchCID(cid, group); err != nil {
				t.Errorf("[burst] fetch %s from %s (idx %d): %v", cid, f.Name(), i, err)
			}
		}
	}

	assertAllDaemonsHealthy(t, c, nil, "[burst]")
	idx := buildRingHoldersIndex(t, c, group, nil)
	for _, cid := range allCIDs {
		assertReplicationInvariants(t, c, idx, cid, 3, 1, nil, "[burst]")
	}

	expectedRaw := int64(len(allCIDs) * smallPayloadSize * 3)
	assertTotalRingStorageWithinBounds(t, c, group, expectedRaw, 5.0, nil, "[burst]")
}

func TestScenario_Chaos_ShareDAGPipeline_LargeFileUnderStormAndChurn(t *testing.T) {
	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	const fileSize = 16 * mibibyte
	fileData := genRandomFile(t, fileSize)

	storm := startChaosStabilizer(t, c, group, 75*time.Millisecond)

	dieAfter := 600 * time.Millisecond
	publisher := nodes[0]
	victim := nodes[4]
	dieDoneCh := make(chan struct{})
	go func() {
		defer close(dieDoneCh)
		time.Sleep(dieAfter)
		t.Logf("[storm-share] killing %s mid-publish", victim.Name())
		if err := victim.Stop(); err != nil {
			t.Errorf("[storm-share] kill %s: %v", victim.Name(), err)
		}
	}()

	publishStart := time.Now()
	cidStr, err := publisher.StoreFile(fileData, group)
	publishElapsed := time.Since(publishStart)

	<-dieDoneCh
	stormRounds := storm.stop()

	if err != nil {
		t.Fatalf("[storm-share] StoreFile failed under chaos: %v", err)
	}
	t.Logf("[storm-share] published %s in %v under %d storm rounds (victim %s killed at +%v)",
		formatBytes(int64(fileSize)), publishElapsed, stormRounds, victim.Name(), dieAfter)

	c.StabilizePrivate(group, testrig.DefaultSuccListSize+2)

	fetcher := nodes[7]
	if fetcher.Name() == victim.Name() {
		t.Fatalf("[storm-share] fetcher == victim; pick a different node")
	}
	fetchStart := time.Now()
	if err := fetcher.FetchCID(cidStr, group); err != nil {
		t.Fatalf("[storm-share] fetch %s from %s after chaos: %v", cidStr, fetcher.Name(), err)
	}
	t.Logf("[storm-share] fetched from %s in %v", fetcher.Name(), time.Since(fetchStart))

	dead := map[string]bool{victim.Name(): true}
	assertAllDaemonsHealthy(t, c, dead, "[storm-share]")

	idx := buildRingHoldersIndex(t, c, group, dead)
	assertReplicationInvariants(t, c, idx, cidStr, 3, 2, dead, "[storm-share/root]")

	expectedRaw := int64(fileSize) * 3
	assertTotalRingStorageWithinBounds(t, c, group, expectedRaw, 0.25, dead, "[storm-share]")
}

func TestScenario_Chaos_FetchDAGPipeline_PrimaryLossWithStorm(t *testing.T) {
	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	const fileSize = 8 * mibibyte
	fileData := genRandomFile(t, fileSize)

	publisher := nodes[0]
	cidStr, err := publisher.StoreFile(fileData, group)
	if err != nil {
		t.Fatalf("[fetch-loss] publish: %v", err)
	}
	if _, ok := c.WaitForBlockReplicas(cidStr, 3, 10*time.Second); !ok {
		t.Fatalf("[fetch-loss] root CID did not reach k=3 holders pre-churn")
	}
	t.Logf("[fetch-loss] published %s, root=%s", formatBytes(int64(fileSize)), cidStr)

	doomed := []string{nodes[2].Name(), nodes[5].Name()}
	for _, name := range doomed {
		v := c.NodeByName(name)
		if err := v.Stop(); err != nil {
			t.Fatalf("[fetch-loss] stop %s: %v", name, err)
		}
		t.Logf("[fetch-loss] killed %s", name)
	}

	storm := startChaosStabilizer(t, c, group, 100*time.Millisecond)
	c.StabilizePrivate(group, testrig.DefaultSuccListSize-1)

	fetcher := nodes[7]
	for _, d := range doomed {
		if fetcher.Name() == d {
			t.Fatalf("[fetch-loss] fetcher == doomed; pick a different node")
		}
	}

	fetchStart := time.Now()
	fetchErr := fetcher.FetchCID(cidStr, group)
	fetchElapsed := time.Since(fetchStart)
	stormRounds := storm.stop()

	if fetchErr != nil {
		t.Fatalf("[fetch-loss] fetch %s from %s after killing %v under storm: %v",
			cidStr, fetcher.Name(), doomed, fetchErr)
	}
	t.Logf("[fetch-loss] fetched %s from %s in %v under %d storm rounds (killed %v)",
		formatBytes(int64(fileSize)), fetcher.Name(), fetchElapsed, stormRounds, doomed)

	if fetchElapsed > 10*time.Second {
		t.Errorf("[fetch-loss] fetch took %v (> 10s) — pipeline or fan-out likely degraded",
			fetchElapsed)
	}

	dead := map[string]bool{doomed[0]: true, doomed[1]: true}
	assertAllDaemonsHealthy(t, c, dead, "[fetch-loss]")

	idx := buildRingHoldersIndex(t, c, group, dead)
	assertReplicationInvariants(t, c, idx, cidStr, 3, 2, dead, "[fetch-loss/root]")

	expectedRaw := int64(fileSize) * 3
	assertTotalRingStorageWithinBounds(t, c, group, expectedRaw, 0.25, dead, "[fetch-loss]")
}

func TestScenario_Chaos_AllPipelinesContinuousChurn(t *testing.T) {
	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()
	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	const (
		duration	= 20 * time.Second
		publishEvery	= 250 * time.Millisecond
		fetchEvery	= 200 * time.Millisecond
		stormInterval	= 80 * time.Millisecond
		payloadSize	= 4 * 1024
	)

	storm := startChaosStabilizer(t, c, group, stormInterval)

	type entry struct {
		cid	string
		payload	[]byte
	}
	var (
		mu		sync.Mutex
		published	[]entry
	)
	addEntry := func(e entry) {
		mu.Lock()
		published = append(published, e)
		mu.Unlock()
	}
	pickEntry := func(rng *rand.Rand) (entry, bool) {
		mu.Lock()
		defer mu.Unlock()
		if len(published) == 0 {
			return entry{}, false
		}
		return published[rng.Intn(len(published))], true
	}

	stopCh := make(chan struct{})
	var loopWG sync.WaitGroup

	var publishCount, publishErrs atomic.Int64
	loopWG.Add(1)
	go func() {
		defer loopWG.Done()
		ticker := time.NewTicker(publishEvery)
		defer ticker.Stop()
		i := 0
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				pub := nodes[i%len(nodes)]
				payload := uniquePayload(i, payloadSize)
				cidStr, err := pub.StoreFile(payload, group)
				if err != nil {
					publishErrs.Add(1)
					t.Logf("[chaos-publish] %s: %v", pub.Name(), err)
				} else {
					addEntry(entry{cid: cidStr, payload: payload})
					publishCount.Add(1)
				}
				i++
			}
		}
	}()

	var fetchCount, fetchErrs atomic.Int64
	loopWG.Add(1)
	go func() {
		defer loopWG.Done()
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		ticker := time.NewTicker(fetchEvery)
		defer ticker.Stop()
		i := 0
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				e, ok := pickEntry(rng)
				if !ok {
					continue
				}
				fetcher := nodes[i%len(nodes)]
				if err := fetcher.FetchCID(e.cid, group); err != nil {
					fetchErrs.Add(1)
					t.Logf("[chaos-fetch] %s fetch %s: %v", fetcher.Name(), e.cid, err)
				} else {
					fetchCount.Add(1)
				}
				i++
			}
		}
	}()

	time.Sleep(duration)
	close(stopCh)
	loopWG.Wait()
	stormRounds := storm.stop()

	t.Logf("[chaos-summary] published=%d (errs=%d), fetched=%d (errs=%d), storm=%d rounds",
		publishCount.Load(), publishErrs.Load(),
		fetchCount.Load(), fetchErrs.Load(),
		stormRounds)

	if publishCount.Load() == 0 {
		t.Fatalf("[chaos-summary] no successful publishes under chaos — pipeline broken")
	}
	if fetchCount.Load() == 0 {
		t.Fatalf("[chaos-summary] no successful fetches under chaos — fetch path broken")
	}

	totalFetches := fetchCount.Load() + fetchErrs.Load()
	if totalFetches > 0 {
		errRate := float64(fetchErrs.Load()) / float64(totalFetches)
		if errRate > 0.25 {
			t.Errorf("[chaos-summary] fetch error rate %.2f > 0.25 (errors=%d/%d)",
				errRate, fetchErrs.Load(), totalFetches)
		}
	}

	c.StabilizePrivate(group, 6)

	mu.Lock()
	finalCIDs := make([]entry, len(published))
	copy(finalCIDs, published)
	mu.Unlock()

	verifier := nodes[3]
	finalErrs := 0
	for _, e := range finalCIDs {
		if err := verifier.FetchCID(e.cid, group); err != nil {
			finalErrs++
			t.Errorf("[chaos-final] %s fetch %s after settle: %v", verifier.Name(), e.cid, err)
		}
	}
	if finalErrs > 0 {
		t.Fatalf("[chaos-final] %d/%d CIDs unfetchable after settle — durability lost under chaos",
			finalErrs, len(finalCIDs))
	}
	t.Logf("[chaos-final] all %d published CIDs fetchable from %s after settle",
		len(finalCIDs), verifier.Name())

	assertAllDaemonsHealthy(t, c, nil, "[chaos-final]")

	idx := buildRingHoldersIndex(t, c, group, nil)
	overReplicated, outsiders := 0, 0

	for _, e := range finalCIDs {
		holders := ringHoldersOfCID(idx, e.cid)
		if len(holders) > 4 {
			overReplicated++
		}
		expected := expectedReplicaWindow(t, c, e.cid, 3, nil)
		expectedSet := make(map[string]bool, len(expected))
		for _, n := range expected {
			expectedSet[n] = true
		}
		for _, h := range holders {
			if !expectedSet[h] {
				outsiders++
				break
			}
		}
		assertReplicationInvariants(t, c, idx, e.cid, 3, 1, nil, "[chaos-final]")
	}
	t.Logf("[chaos-final] invariant summary: %d/%d over-replicated, %d/%d outsider-holders",
		overReplicated, len(finalCIDs), outsiders, len(finalCIDs))

	expectedRaw := int64(len(finalCIDs)) * payloadSize * 3
	assertTotalRingStorageWithinBounds(t, c, group, expectedRaw, 0.5, nil, "[chaos-final]")
}
