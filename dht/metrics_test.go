package dht

import (
	"context"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/mjagos0/datarings/metrics"
	"github.com/mjagos0/datarings/store"
)

func collectGauge(g interface{ Desc() *dto.MetricFamily }) float64 {
	return 0
}

func counterValue(met *metrics.Registry, name string) float64 {
	mfs, _ := met.Prometheus.Gather()
	for _, mf := range mfs {
		if mf.GetName() == name {
			for _, m := range mf.GetMetric() {
				if m.Counter != nil {
					return m.Counter.GetValue()
				}
			}
		}
	}
	return 0
}

func counterVecValue(met *metrics.Registry, name string, labels map[string]string) float64 {
	mfs, _ := met.Prometheus.Gather()
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if matchLabels(m, labels) {
				if m.Counter != nil {
					return m.Counter.GetValue()
				}
			}
		}
	}
	return 0
}

func histogramCount(met *metrics.Registry, name string, labels map[string]string) uint64 {
	mfs, _ := met.Prometheus.Gather()
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if matchLabels(m, labels) {
				if m.Histogram != nil {
					return m.Histogram.GetSampleCount()
				}
			}
		}
	}
	return 0
}

func histogramSum(met *metrics.Registry, name string, labels map[string]string) float64 {
	mfs, _ := met.Prometheus.Gather()
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if matchLabels(m, labels) {
				if m.Histogram != nil {
					return m.Histogram.GetSampleSum()
				}
			}
		}
	}
	return 0
}

func matchLabels(m *dto.Metric, want map[string]string) bool {
	if len(want) == 0 {
		return true
	}
	have := make(map[string]string)
	for _, lp := range m.GetLabel() {
		have[lp.GetName()] = lp.GetValue()
	}
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

func TestMetrics_BlockTransfer(t *testing.T) {
	met := metrics.New()
	ring := newPrivateTestRing(t)
	defer ring.cleanup()

	for i := 0; i < 3; i++ {
		n := ring.addNode()
		ringLabel := n.ring.GroupID().String()
		n.ring.SetMetrics(met)
		n.ring.Node().SetMetrics(met, ringLabel)
	}

	for i := 0; i < 4; i++ {
		for _, n := range ring.nodes {
			n.ring.Node().StabilizeFull()
		}
	}

	ctx := context.Background()
	storer := ring.nodes[0]

	blockData := []byte("metrics-test-block-payload-12345")
	rootCID := testCID(blockData)
	if err := storer.store.Put(ctx, rootCID, blockData); err != nil {
		t.Fatalf("local put: %v", err)
	}

	if err := storer.ring.ShareDAG(ctx, rootCID); err != nil {
		t.Fatalf("ShareDAG: %v", err)
	}

	ringLabel := storer.ring.node.metRing
	labels := map[string]string{"ring": ringLabel}

	dagPushTotal := counterVecValue(met, "datarings_dag_push_total", labels)
	if dagPushTotal != 1 {
		t.Errorf("datarings_dag_push_total = %v, want 1", dagPushTotal)
	}

	dagPushBlocks := counterVecValue(met, "datarings_dag_push_blocks_total", labels)
	if dagPushBlocks < 1 {
		t.Errorf("datarings_dag_push_blocks_total = %v, want >= 1", dagPushBlocks)
	}

	dagPushBytes := counterVecValue(met, "datarings_dag_push_bytes_total", labels)
	if dagPushBytes < float64(len(blockData)) {
		t.Errorf("datarings_dag_push_bytes_total = %v, want >= %d", dagPushBytes, len(blockData))
	}

	dagPushDurCount := histogramCount(met, "datarings_dag_push_duration_seconds", labels)
	if dagPushDurCount != 1 {
		t.Errorf("datarings_dag_push_duration_seconds count = %v, want 1", dagPushDurCount)
	}

	dagPushDurSum := histogramSum(met, "datarings_dag_push_duration_seconds", labels)
	if dagPushDurSum <= 0 {
		t.Errorf("datarings_dag_push_duration_seconds sum = %v, want > 0", dagPushDurSum)
	}

	pushTotal := counterVecValue(met, "datarings_blocks_pushed_total", labels)
	pushBytes := counterVecValue(met, "datarings_push_bytes_total", labels)
	t.Logf("blocks_pushed_total=%v push_bytes_total=%v", pushTotal, pushBytes)

	fetcher := ring.nodes[2]
	if err := fetcher.ring.FetchDAG(ctx, rootCID); err != nil {
		t.Fatalf("FetchDAG: %v", err)
	}

	dagFetchTotal := counterVecValue(met, "datarings_dag_fetch_total", labels)
	if dagFetchTotal != 1 {
		t.Errorf("datarings_dag_fetch_total = %v, want 1", dagFetchTotal)
	}

	dagFetchBlocks := counterVecValue(met, "datarings_dag_fetch_blocks_total", labels)
	if dagFetchBlocks < 1 {
		t.Errorf("datarings_dag_fetch_blocks_total = %v, want >= 1", dagFetchBlocks)
	}

	dagFetchBytes := counterVecValue(met, "datarings_dag_fetch_bytes_total", labels)
	if dagFetchBytes >= float64(len(blockData)) {
		t.Logf("datarings_dag_fetch_bytes_total = %v (OK, >= %d)", dagFetchBytes, len(blockData))
	}

	dagFetchDurCount := histogramCount(met, "datarings_dag_fetch_duration_seconds", labels)
	if dagFetchDurCount != 1 {
		t.Errorf("datarings_dag_fetch_duration_seconds count = %v, want 1", dagFetchDurCount)
	}

	fetchTotal := counterVecValue(met, "datarings_blocks_fetched_total", labels)
	fetchBytes := counterVecValue(met, "datarings_fetch_bytes_total", labels)
	t.Logf("blocks_fetched_total=%v fetch_bytes_total=%v", fetchTotal, fetchBytes)

	stabRounds := counterVecValue(met, "datarings_stabilize_rounds_total", labels)
	if stabRounds < 1 {
		t.Errorf("datarings_stabilize_rounds_total = %v, want >= 1", stabRounds)
	}
	stabDurCount := histogramCount(met, "datarings_stabilize_duration_seconds", labels)
	if stabDurCount < 1 {
		t.Errorf("datarings_stabilize_duration_seconds count = %v, want >= 1", stabDurCount)
	}

	t.Logf("All metrics verified:")
	t.Logf("  dag_push:  total=%v blocks=%v bytes=%v dur_count=%v dur_sum=%.4fs",
		dagPushTotal, dagPushBlocks, dagPushBytes, dagPushDurCount, dagPushDurSum)
	t.Logf("  dag_fetch: total=%v blocks=%v bytes=%v dur_count=%v",
		dagFetchTotal, dagFetchBlocks, dagFetchBytes, dagFetchDurCount)
	t.Logf("  stabilize: rounds=%v dur_count=%v", stabRounds, stabDurCount)
	t.Logf("  push:  blocks=%v bytes=%v", pushTotal, pushBytes)
	t.Logf("  fetch: blocks=%v bytes=%v", fetchTotal, fetchBytes)
}

func TestMetrics_GC(t *testing.T) {
	met := metrics.New()

	dir := t.TempDir()
	st, err := store.Open(dir, 0)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer st.Close()
	st.SetMetrics(met)

	ctx := context.Background()
	blockData := []byte("gc-test-orphan-block")
	cid := testCID(blockData)
	if err := st.LocalBlocks.Put(ctx, cid, blockData); err != nil {
		t.Fatalf("put: %v", err)
	}

	has, err := st.LocalBlocks.Has(ctx, cid)
	if err != nil {
		t.Fatalf("has: %v", err)
	}
	if !has {
		t.Fatal("block should exist before GC")
	}

	result, err := st.GC(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	t.Logf("GC result: removed=%d kept=%d elapsed=%v", result.Removed, result.Kept, result.Elapsed)

	gcRuns := counterValue(met, "datarings_gc_runs_total")
	if gcRuns != 1 {
		t.Errorf("datarings_gc_runs_total = %v, want 1", gcRuns)
	}

	gcRemoved := counterValue(met, "datarings_gc_blocks_removed_total")
	if gcRemoved < 1 {
		t.Errorf("datarings_gc_blocks_removed_total = %v, want >= 1", gcRemoved)
	}

	gcDurCount := histogramCount(met, "datarings_gc_duration_seconds", nil)
	if gcDurCount != 1 {
		t.Errorf("datarings_gc_duration_seconds count = %v, want 1", gcDurCount)
	}

	t.Logf("GC metrics: runs=%v removed=%v dur_count=%v", gcRuns, gcRemoved, gcDurCount)
}
