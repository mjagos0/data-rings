package dht

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/mjagos0/datarings/store"
)

const defaultDAGShareConcurrency = 64

const (
	minDAGShareConcurrency	= 1
	maxDAGShareConcurrency	= 1024
)

var dagShareConcurrency atomic.Int32

func init() {
	dagShareConcurrency.Store(int32(defaultDAGShareConcurrency))
}

func DAGShareConcurrency() int {
	return int(dagShareConcurrency.Load())
}

func SetDAGShareConcurrency(n int) int {
	if n < minDAGShareConcurrency {
		n = minDAGShareConcurrency
	}
	if n > maxDAGShareConcurrency {
		n = maxDAGShareConcurrency
	}
	dagShareConcurrency.Store(int32(n))
	return n
}

func (n *Node) ShareDAG(ctx context.Context, root cid.Cid) error {
	return n.ShareDAGWithTTL(ctx, root, 0)
}

func (n *Node) ShareDAGWithTTL(ctx context.Context, root cid.Cid, ttl time.Duration) error {
	slog.Info("sharing DAG to ring", "node", n.id, "root", root, "ttl", ttl)
	start := time.Now()
	var rootExpiry int64
	if ttl > 0 {
		rootExpiry = time.Now().Add(ttl).UnixNano()
	}

	pipeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type job struct {
		c	cid.Cid
		data	[]byte
	}

	concurrency := DAGShareConcurrency()
	jobs := make(chan job, concurrency)

	var firstErr atomic.Pointer[error]
	setErr := func(err error) {
		if err == nil {
			return
		}
		wrapped := err
		if firstErr.CompareAndSwap(nil, &wrapped) {
			cancel()
		}
	}

	var totalBytes atomic.Int64
	var blockCount atomic.Int64

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if pipeCtx.Err() != nil {
					continue
				}
				if err := n.PutWithRoot(pipeCtx, j.c, j.data, root, rootExpiry); err != nil {
					setErr(fmt.Errorf("put to ring %s: %w", j.c, err))
				}
			}
		}()
	}

	visited := make(map[cid.Cid]struct{})
	var visitedMu sync.Mutex
	walkSem := make(chan struct{}, concurrency)
	var walkWg sync.WaitGroup

	var walk func(c cid.Cid)
	walk = func(c cid.Cid) {
		defer walkWg.Done()

		if pipeCtx.Err() != nil {
			return
		}

		visitedMu.Lock()
		if _, seen := visited[c]; seen {
			visitedMu.Unlock()
			return
		}
		visited[c] = struct{}{}
		visitedMu.Unlock()

		select {
		case walkSem <- struct{}{}:
		case <-pipeCtx.Done():
			return
		}
		defer func() { <-walkSem }()

		data, err := n.localOrRingBlocks().Get(ctx, c)
		if err != nil {
			setErr(fmt.Errorf("get local block %s: %w", c, err))
			return
		}
		totalBytes.Add(int64(len(data)))
		blockCount.Add(1)

		select {
		case jobs <- job{c: c, data: data}:
		case <-pipeCtx.Done():
			return
		}

		children, err := store.LinksOf(c, data)
		if err != nil {
			return
		}
		for _, child := range children {
			walkWg.Add(1)
			go walk(child)
		}
	}

	walkWg.Add(1)
	go walk(root)
	walkWg.Wait()
	close(jobs)
	wg.Wait()

	if errPtr := firstErr.Load(); errPtr != nil {
		return *errPtr
	}

	elapsed := time.Since(start)
	bytes := totalBytes.Load()
	blocks := blockCount.Load()
	speedMBps := float64(bytes) / (1024 * 1024) / elapsed.Seconds()
	slog.Info("DAG shared", "node", n.id, "root", root, "blocks", blocks,
		"bytes", bytes, "duration", elapsed.Round(time.Millisecond),
		"avg_speed_mbps", fmt.Sprintf("%.2f", speedMBps))
	if n.met != nil {
		n.met.DAGPushTotal.WithLabelValues(n.metRing).Inc()
		n.met.DAGPushBlocks.WithLabelValues(n.metRing).Add(float64(blocks))
		n.met.DAGPushBytes.WithLabelValues(n.metRing).Add(float64(bytes))
		n.met.DAGPushDurationSeconds.WithLabelValues(n.metRing).Observe(elapsed.Seconds())
		if elapsed.Seconds() > 0 && bytes > 0 {
			n.met.DAGPushSpeedBytesPerSec.WithLabelValues(n.metRing).Observe(float64(bytes) / elapsed.Seconds())
		}
	}
	return nil
}

const defaultDAGFetchConcurrency = 16

func (n *Node) FetchDAG(ctx context.Context, root cid.Cid) error {
	return n.fetchDAGPipeline(ctx, root, "", NodeAddr{}, n.Get)
}

func (n *Node) FetchDAGFromPeer(ctx context.Context, root cid.Cid, peer NodeAddr) error {
	fetcher := func(ctx context.Context, c cid.Cid) ([]byte, error) {
		return n.transport.FetchBlock(ctx, peer, c)
	}
	return n.fetchDAGPipeline(ctx, root, "peer", peer, fetcher)
}

func (n *Node) fetchDAGPipeline(ctx context.Context, root cid.Cid, mode string, peer NodeAddr, fetch func(context.Context, cid.Cid) ([]byte, error)) error {
	if mode == "peer" {
		slog.Info("fetching DAG directly from peer", "node", n.id, "root", root, "peer", peer.ID, "addr", peer.Addr)
	} else {
		slog.Info("fetching DAG from ring", "node", n.id, "root", root)
	}
	start := time.Now()

	pipeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var firstErr atomic.Pointer[error]
	setErr := func(err error) {
		if err == nil {
			return
		}
		wrapped := err
		if firstErr.CompareAndSwap(nil, &wrapped) {
			cancel()
		}
	}

	var totalBytes atomic.Int64
	var blockCount atomic.Int64

	visited := &sync.Map{}

	workQueue := make(chan cid.Cid, defaultDAGFetchConcurrency*4)
	var inflight sync.WaitGroup

	enqueue := func(c cid.Cid) {
		if _, loaded := visited.LoadOrStore(c, struct{}{}); loaded {
			return
		}
		inflight.Add(1)
		go func() {
			select {
			case workQueue <- c:
			case <-pipeCtx.Done():

				inflight.Done()
			}
		}()
	}

	var wg sync.WaitGroup
	for i := 0; i < defaultDAGFetchConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range workQueue {
				if pipeCtx.Err() != nil {
					inflight.Done()
					continue
				}
				if err := n.fetchOneDAGBlock(pipeCtx, c, &totalBytes, &blockCount, mode, peer, fetch, enqueue); err != nil {
					setErr(err)
				}
				inflight.Done()
			}
		}()
	}

	enqueue(root)

	go func() {
		inflight.Wait()
		close(workQueue)
	}()

	wg.Wait()

	if errPtr := firstErr.Load(); errPtr != nil {
		return *errPtr
	}

	elapsed := time.Since(start)
	bytes := totalBytes.Load()
	blocks := blockCount.Load()
	speedMBps := float64(bytes) / (1024 * 1024) / elapsed.Seconds()
	if mode == "peer" {
		slog.Info("DAG fetched from peer", "node", n.id, "root", root, "peer", peer.ID,
			"blocks", blocks, "bytes", bytes, "duration", elapsed.Round(time.Millisecond),
			"avg_speed_mbps", fmt.Sprintf("%.2f", speedMBps))
	} else {
		slog.Info("DAG fetched", "node", n.id, "root", root, "blocks", blocks,
			"bytes", bytes, "duration", elapsed.Round(time.Millisecond),
			"avg_speed_mbps", fmt.Sprintf("%.2f", speedMBps))
	}
	if n.met != nil {
		n.met.DAGFetchTotal.WithLabelValues(n.metRing).Inc()
		n.met.DAGFetchBlocks.WithLabelValues(n.metRing).Add(float64(blocks))
		n.met.DAGFetchBytes.WithLabelValues(n.metRing).Add(float64(bytes))
		n.met.DAGFetchDurationSeconds.WithLabelValues(n.metRing).Observe(elapsed.Seconds())
		if elapsed.Seconds() > 0 && bytes > 0 {
			n.met.DAGFetchSpeedBytesPerSec.WithLabelValues(n.metRing).Observe(float64(bytes) / elapsed.Seconds())
		}
	}
	return nil
}

func (n *Node) fetchOneDAGBlock(ctx context.Context, c cid.Cid, totalBytes *atomic.Int64, blockCount *atomic.Int64, mode string, peer NodeAddr, fetch func(context.Context, cid.Cid) ([]byte, error), enqueue func(cid.Cid)) error {
	lb := n.localOrRingBlocks()
	has, err := lb.Has(ctx, c)
	if err != nil {
		return fmt.Errorf("check local %s: %w", c, err)
	}

	var data []byte
	if has {
		data, err = lb.Get(ctx, c)
		if err != nil {
			return fmt.Errorf("local get %s: %w", c, err)
		}
		slog.Debug("chord: block cache hit", "cid", c)
	} else {
		data, err = fetch(ctx, c)
		if err != nil {
			if mode == "peer" {
				return fmt.Errorf("fetch %s from %s: %w", c, peer.Addr, err)
			}
			return fmt.Errorf("fetch %s: %w", c, err)
		}
		if err := lb.Put(ctx, c, data); err != nil {
			return fmt.Errorf("store %s: %w", c, err)
		}
		if mode == "peer" {
			slog.Info("chord: fetched block from peer", "cid", c, "peer", peer.ID, "addr", peer.Addr)
		} else {
			slog.Debug("chord: block fetched from ring", "node", n.id, "cid", c)
		}
	}
	totalBytes.Add(int64(len(data)))
	blockCount.Add(1)

	children, err := store.LinksOf(c, data)
	if err != nil {
		return nil
	}
	for _, child := range children {
		enqueue(child)
	}
	return nil
}
