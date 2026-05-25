package store

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	flatfs "github.com/ipfs/go-ds-flatfs"
)

var concurrencyLevels = []int{1, 2, 4, 8, 16}

const benchBlockSize = 4 * 1024

func enableContentionProfiles() {
	runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1)
}

func benchStoreDir(tb testing.TB) string {
	root := os.Getenv("DRINGS_BENCH_DIR")
	if root == "" {
		root = "/var/tmp/drings-bench"
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		tb.Fatalf("mkdir bench root: %v", err)
	}
	dir, err := os.MkdirTemp(root, "store-")
	if err != nil {
		tb.Fatalf("mkdtemp: %v", err)
	}
	tb.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func stampedBlock(buf []byte, workerID, iter int64) (cid.Cid, []byte) {
	binary.LittleEndian.PutUint64(buf[0:8], uint64(workerID))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(iter))
	return makeCID(buf), buf
}

func runWorkerPool(workers, total int, do func(key cid.Cid, data []byte)) {
	var idx int64 = -1
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		w := w
		go func() {
			defer wg.Done()
			buf := make([]byte, benchBlockSize)
			for {
				i := atomic.AddInt64(&idx, 1)
				if i >= int64(total) {
					return
				}
				key, data := stampedBlock(buf, int64(w), i)
				do(key, data)
			}
		}()
	}
	wg.Wait()
}

func BenchmarkPutWithRoot(b *testing.B) {
	enableContentionProfiles()
	for _, n := range concurrencyLevels {
		n := n
		b.Run(fmt.Sprintf("workers=%d", n), func(b *testing.B) {
			dir := benchStoreDir(b)
			nbs, err := OpenNetworkBlockStore(dir, 0)
			if err != nil {
				b.Fatalf("open store: %v", err)
			}
			b.Cleanup(func() { _ = nbs.Close() })

			ring := nbs.Ring("bench-ring")
			rootKey, _ := makeBlock(32, 0xAA)
			ctx := context.Background()

			b.SetBytes(int64(benchBlockSize))
			b.ResetTimer()

			runWorkerPool(n, b.N, func(key cid.Cid, data []byte) {
				if err := ring.PutWithRoot(ctx, key, data, rootKey, 0); err != nil {
					b.Errorf("PutWithRoot: %v", err)
				}
			})

			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
		})
	}
}

func BenchmarkRawFlatfsPut(b *testing.B) {
	enableContentionProfiles()
	for _, n := range concurrencyLevels {
		n := n
		b.Run(fmt.Sprintf("workers=%d", n), func(b *testing.B) {
			dir := benchStoreDir(b)
			fds, err := flatfs.CreateOrOpen(filepath.Join(dir, "blocks"), flatfs.IPFS_DEF_SHARD, false)
			if err != nil {
				b.Fatalf("flatfs open: %v", err)
			}
			b.Cleanup(func() { _ = fds.Close() })
			bs := blockstore.NewBlockstore(fds, blockstore.NoPrefix())
			ctx := context.Background()

			b.SetBytes(int64(benchBlockSize))
			b.ResetTimer()

			runWorkerPool(n, b.N, func(key cid.Cid, data []byte) {
				blk, err := blocks.NewBlockWithCid(data, key)
				if err != nil {
					b.Errorf("new block: %v", err)
					return
				}
				if err := bs.Put(ctx, blk); err != nil {
					b.Errorf("bs.Put: %v", err)
				}
			})

			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
		})
	}
}
