package store

import (
	"context"
	"io"
	"path/filepath"
	"sync"
	"time"

	blockservice "github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	flatfs "github.com/ipfs/go-ds-flatfs"
	leveldb "github.com/ipfs/go-ds-leveldb"
	ipld "github.com/ipfs/go-ipld-format"

	"github.com/mjagos0/datarings/metrics"
)

type Store struct {
	LocalBlocks	BlockStore
	NetworkBlocks	*NetworkBlockStore
	DAG		ipld.DAGService
	Roots		*RootRegistry
	localDB		io.Closer
	rootsDB		io.Closer

	gcMu	sync.Mutex
	met	*metrics.Registry
}

func (s *Store) SetMetrics(m *metrics.Registry) {
	s.met = m
}

func (s *Store) StorageStatus() *StorageStatus {
	return s.NetworkBlocks.StorageStatus()
}

func Open(dataDir string, storageMax int64) (*Store, error) {

	localFFS, err := flatfs.CreateOrOpen(
		filepath.Join(dataDir, "local-blocks"),
		flatfs.IPFS_DEF_SHARD,
		false,
	)
	if err != nil {
		return nil, err
	}

	rootsLDB, err := leveldb.NewDatastore(filepath.Join(dataDir, "roots"), nil)
	if err != nil {
		localFFS.Close()
		return nil, err
	}

	nbs, err := OpenNetworkBlockStore(filepath.Join(dataDir, "network-blocks"), storageMax)
	if err != nil {
		localFFS.Close()
		rootsLDB.Close()
		return nil, err
	}

	bs := blockstore.NewBlockstore(localFFS, blockstore.NoPrefix())
	cached, err := blockstore.CachedBlockstore(context.Background(), bs, blockstore.DefaultCacheOpts())
	if err != nil {
		localFFS.Close()
		rootsLDB.Close()
		nbs.Close()
		return nil, err
	}

	bsvc := blockservice.New(cached, nil)
	dag := merkledag.NewDAGService(bsvc)

	roots, err := openRootRegistry(rootsLDB)
	if err != nil {
		localFFS.Close()
		rootsLDB.Close()
		nbs.Close()
		return nil, err
	}

	localBlocks := newLocalBlockStore(cached)

	return &Store{
		LocalBlocks:	localBlocks,
		NetworkBlocks:	nbs,
		DAG:		dag,
		Roots:		roots,
		localDB:	localFFS,
		rootsDB:	rootsLDB,
	}, nil
}

func (s *Store) Close() error {
	err1 := s.localDB.Close()
	err2 := s.rootsDB.Close()
	err3 := s.NetworkBlocks.Close()
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return err3
}

func (s *Store) GC(ctx context.Context) (GCResult, error) {
	s.gcMu.Lock()
	defer s.gcMu.Unlock()

	start := time.Now()

	localResult, err := s.gcLocal(ctx)
	if err != nil {
		return GCResult{}, err
	}

	netResult, err := s.NetworkBlocks.GC(ctx)
	if err != nil {
		return GCResult{}, err
	}

	combined := GCResult{
		Removed:	localResult.Removed + netResult.Removed,
		Kept:		localResult.Kept + netResult.Kept,
		Elapsed:	time.Since(start),
	}

	if s.met != nil {
		s.met.GCRunsTotal.Inc()
		s.met.GCBlocksRemovedTotal.Add(float64(combined.Removed))
		s.met.GCBlocksKept.Set(float64(combined.Kept))
		s.met.GCDurationSeconds.Observe(combined.Elapsed.Seconds())
	}
	return combined, nil
}

func (s *Store) gcLocal(ctx context.Context) (GCResult, error) {
	reachable := make(map[string]struct{})
	for _, root := range s.Roots.List() {
		if err := gcWalkDAG(ctx, s.DAG, root.CID, reachable); err != nil {
			return GCResult{}, err
		}
	}

	ch, err := s.LocalBlocks.AllKeysChan(ctx)
	if err != nil {
		return GCResult{}, err
	}

	var removed, kept int
	for c := range ch {
		mhStr := c.Hash().String()
		if _, ok := reachable[mhStr]; ok {
			kept++
			continue
		}
		if err := s.LocalBlocks.Delete(ctx, c); err != nil {
			return GCResult{}, err
		}
		removed++
	}
	return GCResult{Removed: removed, Kept: kept}, nil
}
