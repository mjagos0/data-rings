package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type Registry struct {
	Prometheus	*prometheus.Registry

	BlocksFetched	*prometheus.CounterVec

	BlocksPushed	*prometheus.CounterVec

	FetchBytes	*prometheus.CounterVec

	PushBytes	*prometheus.CounterVec

	FetchErrors	*prometheus.CounterVec

	PushErrors	*prometheus.CounterVec

	FetchDuration	*prometheus.HistogramVec

	PushDuration	*prometheus.HistogramVec

	DHTRPC	*prometheus.CounterVec

	RecordsStored	*prometheus.CounterVec

	BlockStoreSizeBytes	*prometheus.GaugeVec

	BlockStoreCount	*prometheus.GaugeVec

	PublicRecordCount	prometheus.Gauge

	PublicFingerCount	prometheus.Gauge

	ActivePrivateRings	*prometheus.GaugeVec

	PublicSuccessorCount	prometheus.Gauge

	GroupRecordVersion	*prometheus.GaugeVec

	GroupRecordMembers	*prometheus.GaugeVec

	PrivateSuccessorCount	*prometheus.GaugeVec

	PrivateVerifiedPeers	*prometheus.GaugeVec

	FixFingerErrors	*prometheus.CounterVec

	LocalRootCount	prometheus.Gauge

	NetworkRootCount	*prometheus.GaugeVec

	GCRunsTotal	prometheus.Counter

	GCBlocksRemovedTotal	prometheus.Counter

	GCBlocksKept	prometheus.Gauge

	GCDurationSeconds	prometheus.Histogram

	StabilizeDurationSeconds	*prometheus.HistogramVec

	StabilizeRoundsTotal	*prometheus.CounterVec

	DAGPushTotal	*prometheus.CounterVec

	DAGPushBlocks	*prometheus.CounterVec

	DAGPushBytes	*prometheus.CounterVec

	DAGPushDurationSeconds	*prometheus.HistogramVec

	DAGFetchTotal	*prometheus.CounterVec

	DAGFetchBlocks	*prometheus.CounterVec

	DAGFetchBytes	*prometheus.CounterVec

	DAGFetchDurationSeconds	*prometheus.HistogramVec

	StorageQuotaBytes	prometheus.Gauge

	StorageUsedRatio	prometheus.Gauge

	BlocksStoredByType	*prometheus.CounterVec

	BlockFetchSpeedBytesPerSec	*prometheus.HistogramVec

	BlockPushSpeedBytesPerSec	*prometheus.HistogramVec

	DAGFetchSpeedBytesPerSec	*prometheus.HistogramVec

	DAGPushSpeedBytesPerSec	*prometheus.HistogramVec

	QuotaRejectionsTotal	*prometheus.CounterVec

	NodeUptimeSeconds	prometheus.Gauge

	NodeInfo	*prometheus.GaugeVec

	CIDStorageBytes	*prometheus.GaugeVec

	RingStorageUsedBytes	*prometheus.GaugeVec

	RingStorageMaxBytes	*prometheus.GaugeVec

	RingBlockCount	*prometheus.GaugeVec

	RingNetworkRootCount	*prometheus.GaugeVec
}

func New() *Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	ringLabels := []string{"ring"}
	opLabels := []string{"op"}
	typeLabels := []string{"record_type"}

	durationBuckets := []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}

	r := &Registry{Prometheus: reg}

	r.BlocksFetched = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:	"datarings_blocks_fetched_total",
		Help:	"Total number of blocks successfully fetched from remote peers.",
	}, ringLabels)

	r.BlocksPushed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:	"datarings_blocks_pushed_total",
		Help:	"Total number of blocks successfully pushed to remote peers.",
	}, ringLabels)

	r.FetchBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:	"datarings_fetch_bytes_total",
		Help:	"Total bytes received when fetching blocks from remote peers.",
	}, ringLabels)

	r.PushBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:	"datarings_push_bytes_total",
		Help:	"Total bytes sent when pushing blocks to remote peers.",
	}, ringLabels)

	r.FetchErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:	"datarings_fetch_errors_total",
		Help:	"Total failed block fetch attempts.",
	}, ringLabels)

	r.PushErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:	"datarings_push_errors_total",
		Help:	"Total failed block push attempts.",
	}, ringLabels)

	r.FetchDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:		"datarings_block_fetch_duration_seconds",
		Help:		"Latency of individual block fetches from remote peers.",
		Buckets:	durationBuckets,
	}, ringLabels)

	r.PushDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:		"datarings_block_push_duration_seconds",
		Help:		"Latency of individual block pushes to remote peers.",
		Buckets:	durationBuckets,
	}, ringLabels)

	r.DHTRPC = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:	"datarings_dht_rpc_total",
		Help:	"Total outgoing Chord protocol RPCs by operation type.",
	}, opLabels)

	r.RecordsStored = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:	"datarings_records_stored_total",
		Help:	"Total DHT records published (peer, group, provider).",
	}, typeLabels)

	r.BlockStoreSizeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:	"datarings_block_store_size_bytes",
		Help:	"On-disk size of the block store directory in bytes (label: store=local|network).",
	}, []string{"store"})

	r.BlockStoreCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:	"datarings_block_store_count",
		Help:	"Number of distinct blocks in the store (label: store=local|network).",
	}, []string{"store"})

	r.PublicRecordCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:	"datarings_public_record_count",
		Help:	"Number of DHT records stored on this node of the public ring.",
	})

	r.PublicFingerCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:	"datarings_public_finger_count",
		Help:	"Number of unique peers in the public ring finger table.",
	})

	r.ActivePrivateRings = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:	"datarings_active_private_rings",
		Help:	"1 when this node is an active member of the given private ring (label: group_id).",
	}, []string{"group_id"})

	r.PublicSuccessorCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:	"datarings_public_successor_count",
		Help:	"Number of distinct peers in the public ring successor list. Value of 1 indicates a potentially split ring.",
	})

	r.GroupRecordVersion = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:	"datarings_group_record_version",
		Help:	"Current version of the GroupIdentityRecord as seen by this node.",
	}, []string{"group_id"})

	r.GroupRecordMembers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:	"datarings_group_record_members",
		Help:	"Number of peers listed in the GroupIdentityRecord.",
	}, []string{"group_id"})

	r.PrivateSuccessorCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:	"datarings_private_successor_count",
		Help:	"Number of distinct peers in each private ring's successor list. Value of 1 indicates an isolated private ring.",
	}, []string{"group_id"})

	r.PrivateVerifiedPeers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:	"datarings_private_verified_peers",
		Help:	"Number of peers that have completed PSK authentication on each private ring.",
	}, []string{"group_id"})

	r.FixFingerErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:	"datarings_fix_finger_errors_total",
		Help:	"Total times fixFinger failed to resolve a finger table entry. A persistently increasing value for a ring indicates unrecoverable stale routing.",
	}, ringLabels)

	r.LocalRootCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:	"datarings_local_root_count",
		Help:	"Number of CIDs explicitly pinned by the user.",
	})
	r.NetworkRootCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:	"datarings_network_root_count",
		Help:	"Number of non-expired network roots registered on this node.",
	}, []string{"group_id"})

	r.GCRunsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name:	"datarings_gc_runs_total",
		Help:	"Total completed garbage collection runs.",
	})
	r.GCBlocksRemovedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name:	"datarings_gc_blocks_removed_total",
		Help:	"Total blocks deleted across all GC runs.",
	})
	r.GCBlocksKept = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:	"datarings_gc_blocks_kept",
		Help:	"Number of blocks kept after the last GC run.",
	})
	r.GCDurationSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:		"datarings_gc_duration_seconds",
		Help:		"Duration of garbage collection runs.",
		Buckets:	[]float64{.1, .5, 1, 2.5, 5, 10, 30, 60},
	})

	r.StabilizeDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:		"datarings_stabilize_duration_seconds",
		Help:		"Duration of individual stabilization rounds.",
		Buckets:	[]float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, ringLabels)
	r.StabilizeRoundsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:	"datarings_stabilize_rounds_total",
		Help:	"Total stabilization rounds completed.",
	}, ringLabels)

	dagBuckets := []float64{.1, .5, 1, 2.5, 5, 10, 30, 60, 120}

	r.DAGPushTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:	"datarings_dag_push_total",
		Help:	"Total completed DAG push (ShareDAG) operations.",
	}, ringLabels)
	r.DAGPushBlocks = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:	"datarings_dag_push_blocks_total",
		Help:	"Total blocks distributed across all DAG pushes.",
	}, ringLabels)
	r.DAGPushBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:	"datarings_dag_push_bytes_total",
		Help:	"Total bytes distributed across all DAG pushes.",
	}, ringLabels)
	r.DAGPushDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:		"datarings_dag_push_duration_seconds",
		Help:		"Duration of full DAG push operations.",
		Buckets:	dagBuckets,
	}, ringLabels)

	r.DAGFetchTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:	"datarings_dag_fetch_total",
		Help:	"Total completed DAG fetch operations.",
	}, ringLabels)
	r.DAGFetchBlocks = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:	"datarings_dag_fetch_blocks_total",
		Help:	"Total blocks fetched across all DAG fetches.",
	}, ringLabels)
	r.DAGFetchBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:	"datarings_dag_fetch_bytes_total",
		Help:	"Total bytes fetched across all DAG fetches.",
	}, ringLabels)
	r.DAGFetchDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:		"datarings_dag_fetch_duration_seconds",
		Help:		"Duration of full DAG fetch operations.",
		Buckets:	dagBuckets,
	}, ringLabels)

	r.StorageQuotaBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:	"datarings_storage_quota_bytes",
		Help:	"Configured storage quota in bytes (0 = unlimited).",
	})
	r.StorageUsedRatio = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:	"datarings_storage_used_ratio",
		Help:	"Ratio of storage used to quota (0 when unlimited).",
	})

	r.BlocksStoredByType = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:	"datarings_blocks_stored_by_type_total",
		Help:	"Total block writes by operation type: primary (routed to responsible node), replica (pushed to successors), re-replication (topology change).",
	}, []string{"ring", "type"})

	speedBuckets := []float64{
		1024,
		10 * 1024,
		100 * 1024,
		512 * 1024,
		1024 * 1024,
		5 * 1024 * 1024,
		10 * 1024 * 1024,
		25 * 1024 * 1024,
		50 * 1024 * 1024,
		100 * 1024 * 1024,
	}
	r.BlockFetchSpeedBytesPerSec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:		"datarings_block_fetch_speed_bytes_per_sec",
		Help:		"Throughput of individual block fetches in bytes/sec.",
		Buckets:	speedBuckets,
	}, ringLabels)
	r.BlockPushSpeedBytesPerSec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:		"datarings_block_push_speed_bytes_per_sec",
		Help:		"Throughput of individual block pushes in bytes/sec.",
		Buckets:	speedBuckets,
	}, ringLabels)
	r.DAGFetchSpeedBytesPerSec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:		"datarings_dag_fetch_speed_bytes_per_sec",
		Help:		"Average throughput of entire DAG fetch operations in bytes/sec.",
		Buckets:	speedBuckets,
	}, ringLabels)
	r.DAGPushSpeedBytesPerSec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:		"datarings_dag_push_speed_bytes_per_sec",
		Help:		"Average throughput of entire DAG push operations in bytes/sec.",
		Buckets:	speedBuckets,
	}, ringLabels)

	r.QuotaRejectionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:	"datarings_quota_rejections_total",
		Help:	"Total block writes rejected due to storage quota exhaustion.",
	}, ringLabels)

	r.NodeUptimeSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:	"datarings_node_uptime_seconds",
		Help:	"Seconds since the daemon process started.",
	})

	r.NodeInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:	"datarings_node_info",
		Help:	"Constant value 1 with node identity labels for join queries.",
	}, []string{"node_id"})

	r.RingStorageUsedBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:	"datarings_ring_storage_used_bytes",
		Help:	"Bytes of blocks tracked by each ring (label: ring).",
	}, ringLabels)
	r.RingStorageMaxBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:	"datarings_ring_storage_max_bytes",
		Help:	"Per-ring storage quota in bytes (0 = unlimited; label: ring).",
	}, ringLabels)
	r.RingBlockCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:	"datarings_ring_block_count",
		Help:	"Number of distinct blocks tracked by each ring (label: ring).",
	}, ringLabels)
	r.RingNetworkRootCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:	"datarings_ring_network_root_count",
		Help:	"Network roots registered in each ring (label: ring).",
	}, ringLabels)

	r.CIDStorageBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:	"datarings_cid_storage_bytes",
		Help:	"Total bytes of blocks belonging to each network root CID on this node.",
	}, []string{"cid"})

	reg.MustRegister(
		r.BlocksFetched, r.BlocksPushed,
		r.FetchBytes, r.PushBytes,
		r.FetchErrors, r.PushErrors,
		r.FetchDuration, r.PushDuration,
		r.DHTRPC,
		r.RecordsStored,
		r.BlockStoreSizeBytes, r.BlockStoreCount,
		r.PublicRecordCount, r.PublicFingerCount,
		r.ActivePrivateRings,
		r.PublicSuccessorCount,
		r.GroupRecordVersion, r.GroupRecordMembers,
		r.PrivateSuccessorCount, r.PrivateVerifiedPeers,
		r.FixFingerErrors,

		r.LocalRootCount, r.NetworkRootCount,
		r.GCRunsTotal, r.GCBlocksRemovedTotal, r.GCBlocksKept, r.GCDurationSeconds,
		r.StabilizeDurationSeconds, r.StabilizeRoundsTotal,
		r.DAGPushTotal, r.DAGPushBlocks, r.DAGPushBytes, r.DAGPushDurationSeconds,
		r.DAGFetchTotal, r.DAGFetchBlocks, r.DAGFetchBytes, r.DAGFetchDurationSeconds,
		r.StorageQuotaBytes, r.StorageUsedRatio,
		r.BlocksStoredByType,
		r.BlockFetchSpeedBytesPerSec, r.BlockPushSpeedBytesPerSec,
		r.DAGFetchSpeedBytesPerSec, r.DAGPushSpeedBytesPerSec,
		r.QuotaRejectionsTotal,
		r.NodeUptimeSeconds,
		r.NodeInfo,
		r.CIDStorageBytes,
		r.RingStorageUsedBytes, r.RingStorageMaxBytes,
		r.RingBlockCount, r.RingNetworkRootCount,
	)

	return r
}
