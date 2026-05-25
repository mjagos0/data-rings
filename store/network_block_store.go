package store

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fxamacker/cbor/v2"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/ipfs/boxo/blockstore"
	flatfs "github.com/ipfs/go-ds-flatfs"
	leveldb "github.com/ipfs/go-ds-leveldb"

	datastore "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
)

const shardCount = 256

const RingPublic = "public"

const (
	nbKeyUsedBytes	= "/meta/used_bytes"
	nbKeyBlockCount	= "/meta/block_count"
	nbPrefixRings	= "/rings/"
	nbPrefixRing	= "/ring/"

	nbSubdirBlocks	= "blocks"
	nbSubdirMeta	= "meta"
)

type networkRootRecord struct {
	ExpiresAt int64 `cbor:"1,keyasint"`
}

type NetworkRootEntry struct {
	CID		string	`json:"cid"`
	ExpiresAt	int64	`json:"expires_at,omitempty"`
}

type NetworkBlockStore struct {
	ds		datastore.Batching
	bs		blockstore.Blockstore
	metaDS		*leveldb.Datastore
	blocksDS	*flatfs.Datastore
	rootsMu		sync.RWMutex
	maxBytes	int64

	lockShards	[shardCount]sync.Mutex

	usedBytesAtomic		atomic.Int64
	blockCountAtomic	atomic.Int64

	ringCountersMu	sync.Mutex
	ringCounters	map[string]*ringCounters

	knownRings	sync.Map
}

type ringCounters struct {
	used	atomic.Int64
	count	atomic.Int64
	quota	atomic.Int64
}

var _ BlockStore = (*NetworkBlockStore)(nil)

func (s *NetworkBlockStore) lockShard(key cid.Cid) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write(key.Hash())
	return &s.lockShards[h.Sum32()%shardCount]
}

func (s *NetworkBlockStore) lockAllShards() {
	for i := range s.lockShards {
		s.lockShards[i].Lock()
	}
}

func (s *NetworkBlockStore) unlockAllShards() {

	for i := len(s.lockShards) - 1; i >= 0; i-- {
		s.lockShards[i].Unlock()
	}
}

func (s *NetworkBlockStore) reserveAggBytes(size int64) bool {
	if s.maxBytes <= 0 {
		s.usedBytesAtomic.Add(size)
		return true
	}
	for {
		cur := s.usedBytesAtomic.Load()
		if cur+size > s.maxBytes {
			return false
		}
		if s.usedBytesAtomic.CompareAndSwap(cur, cur+size) {
			return true
		}
	}
}

func (s *NetworkBlockStore) addAggDelta(bytesDelta, blockDelta int64) {
	if bytesDelta != 0 {
		applySignedDelta(&s.usedBytesAtomic, bytesDelta)
	}
	if blockDelta != 0 {
		applySignedDelta(&s.blockCountAtomic, blockDelta)
	}
}

func applySignedDelta(a *atomic.Int64, delta int64) {
	for {
		cur := a.Load()
		next := cur + delta
		if next < 0 {
			next = 0
		}
		if a.CompareAndSwap(cur, next) {
			return
		}
	}
}

func (s *NetworkBlockStore) flushAggCountersLocked(ctx context.Context) {
	s.writeAggCounters(ctx, s.usedBytesAtomic.Load(), s.blockCountAtomic.Load())
}

func (s *NetworkBlockStore) getRingCounters(ringID string) *ringCounters {
	s.ringCountersMu.Lock()
	defer s.ringCountersMu.Unlock()
	if rc, ok := s.ringCounters[ringID]; ok {
		return rc
	}
	rc := &ringCounters{}
	ctx := context.Background()
	base := nbPrefixRing + ringID + "/"
	if data, err := s.ds.Get(ctx, datastore.NewKey(base+"usedbytes")); err == nil && len(data) == 8 {
		rc.used.Store(decodeInt64LE(data))
	}
	if data, err := s.ds.Get(ctx, datastore.NewKey(base+"blockcount")); err == nil && len(data) == 8 {
		rc.count.Store(decodeInt64LE(data))
	}
	if data, err := s.ds.Get(ctx, datastore.NewKey(base+"quota")); err == nil && len(data) == 8 {
		rc.quota.Store(decodeInt64LE(data))
	}
	if s.ringCounters == nil {
		s.ringCounters = make(map[string]*ringCounters)
	}
	s.ringCounters[ringID] = rc
	return rc
}

func (s *NetworkBlockStore) flushRingCountersLocked(ctx context.Context) {
	type snapshot struct {
		ringID	string
		used	int64
		count	int64
		quota	int64
	}
	s.ringCountersMu.Lock()
	snaps := make([]snapshot, 0, len(s.ringCounters))
	for id, rc := range s.ringCounters {
		snaps = append(snaps, snapshot{
			ringID:	id,
			used:	rc.used.Load(),
			count:	rc.count.Load(),
			quota:	rc.quota.Load(),
		})
	}
	s.ringCountersMu.Unlock()

	for _, snap := range snaps {
		base := nbPrefixRing + snap.ringID + "/"
		_ = s.ds.Put(ctx, datastore.NewKey(base+"usedbytes"), encodeInt64LE(snap.used))
		_ = s.ds.Put(ctx, datastore.NewKey(base+"blockcount"), encodeInt64LE(snap.count))
		_ = s.ds.Put(ctx, datastore.NewKey(base+"quota"), encodeInt64LE(snap.quota))
	}
}

func (rc *ringCounters) reserveBytes(size int64) bool {
	if q := rc.quota.Load(); q > 0 {
		for {
			cur := rc.used.Load()
			if cur+size > q {
				return false
			}
			if rc.used.CompareAndSwap(cur, cur+size) {
				return true
			}
		}
	}
	rc.used.Add(size)
	return true
}

func (rc *ringCounters) addUsageDelta(bytesDelta, blockDelta int64) {
	if bytesDelta != 0 {
		applySignedDelta(&rc.used, bytesDelta)
	}
	if blockDelta != 0 {
		applySignedDelta(&rc.count, blockDelta)
	}
}

func OpenNetworkBlockStore(dir string, storageMax int64) (*NetworkBlockStore, error) {
	metaDS, err := leveldb.NewDatastore(filepath.Join(dir, nbSubdirMeta), nil)
	if err != nil {
		return nil, fmt.Errorf("open metadata leveldb: %w", err)
	}

	blocksDS, err := flatfs.CreateOrOpen(
		filepath.Join(dir, nbSubdirBlocks),
		flatfs.IPFS_DEF_SHARD,
		false,
	)
	if err != nil {
		metaDS.Close()
		return nil, fmt.Errorf("open blocks flatfs: %w", err)
	}

	bs := blockstore.NewBlockstore(blocksDS, blockstore.NoPrefix())

	nbs := &NetworkBlockStore{
		ds:		metaDS,
		bs:		bs,
		metaDS:		metaDS,
		blocksDS:	blocksDS,
		maxBytes:	storageMax,
	}

	if err := nbs.loadCounters(); err != nil {
		blocksDS.Close()
		metaDS.Close()
		return nil, err
	}

	for _, ringID := range nbs.RingsKnown() {
		nbs.knownRings.Store(ringID, struct{}{})
	}

	return nbs, nil
}

func (s *NetworkBlockStore) Close() error {
	ctx := context.Background()
	s.flushAggCountersLocked(ctx)
	s.flushRingCountersLocked(ctx)
	err1 := s.metaDS.Close()
	err2 := s.blocksDS.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (s *NetworkBlockStore) loadCounters() error {
	ctx := context.Background()
	bytesData, bytesErr := s.ds.Get(ctx, datastore.NewKey(nbKeyUsedBytes))
	countData, countErr := s.ds.Get(ctx, datastore.NewKey(nbKeyBlockCount))
	if bytesErr == nil && countErr == nil && len(bytesData) == 8 && len(countData) == 8 {
		s.usedBytesAtomic.Store(decodeInt64LE(bytesData))
		s.blockCountAtomic.Store(decodeInt64LE(countData))
		slog.Info("network block store: loaded counters",
			"used_bytes", s.usedBytesAtomic.Load(),
			"block_count", s.blockCountAtomic.Load(),
			"max_bytes", s.maxBytes)
		return nil
	}

	ch, err := s.bs.AllKeysChan(ctx)
	if err != nil {
		return err
	}
	var totalBytes, totalBlocks int64
	for key := range ch {

		sz, err := s.bs.GetSize(ctx, key)
		if err != nil {
			continue
		}
		totalBytes += int64(sz)
		totalBlocks++
	}
	s.usedBytesAtomic.Store(totalBytes)
	s.blockCountAtomic.Store(totalBlocks)
	s.writeAggCounters(ctx, totalBytes, totalBlocks)
	if totalBlocks > 0 {
		slog.Info("network block store: computed initial counters",
			"used_bytes", totalBytes, "block_count", totalBlocks, "max_bytes", s.maxBytes)
	}
	return nil
}

func (s *NetworkBlockStore) writeAggCounters(ctx context.Context, bytes, count int64) {
	if bytes < 0 {
		bytes = 0
	}
	if count < 0 {
		count = 0
	}
	batch, err := s.ds.Batch(ctx)
	if err != nil {
		_ = s.ds.Put(ctx, datastore.NewKey(nbKeyUsedBytes), encodeInt64LE(bytes))
		_ = s.ds.Put(ctx, datastore.NewKey(nbKeyBlockCount), encodeInt64LE(count))
		return
	}
	_ = batch.Put(ctx, datastore.NewKey(nbKeyUsedBytes), encodeInt64LE(bytes))
	_ = batch.Put(ctx, datastore.NewKey(nbKeyBlockCount), encodeInt64LE(count))
	if err := batch.Commit(ctx); err != nil {
		slog.Warn("network block store: failed to commit aggregate counters", "error", err)
	}
}

func (s *NetworkBlockStore) UsedBytes() int64 {
	return s.usedBytesAtomic.Load()
}

func (s *NetworkBlockStore) BlockCount() int64 {
	return s.blockCountAtomic.Load()
}

func (s *NetworkBlockStore) MaxBytes() int64	{ return s.maxBytes }

func (s *NetworkBlockStore) StorageStatus() *StorageStatus {
	if s.maxBytes == 0 {
		return nil
	}
	return &StorageStatus{
		UsedBytes:	s.usedBytesAtomic.Load(),
		MaxBytes:	s.maxBytes,
		BlockCount:	s.blockCountAtomic.Load(),
	}
}

func (s *NetworkBlockStore) HasBlockStr(cidStr string) (bool, error) {
	c, err := cid.Parse(cidStr)
	if err != nil {
		return false, fmt.Errorf("invalid CID %q: %w", cidStr, err)
	}
	return s.bs.Has(context.Background(), c)
}

func (s *NetworkBlockStore) ListBlockCIDs() ([]string, error) {
	ctx := context.Background()
	ch, err := s.bs.AllKeysChan(ctx)
	if err != nil {
		return nil, err
	}
	var cids []string
	for c := range ch {
		cids = append(cids, c.String())
	}
	return cids, nil
}

func (s *NetworkBlockStore) Put(ctx context.Context, key cid.Cid, data []byte) error {
	mu := s.lockShard(key)
	mu.Lock()
	defer mu.Unlock()

	has, err := s.bs.Has(ctx, key)
	if err != nil {
		return err
	}
	if has {
		return nil
	}

	size := int64(len(data))
	if !s.reserveAggBytes(size) {
		return ErrStorageFull
	}

	blk, err := blocks.NewBlockWithCid(data, key)
	if err != nil {
		s.addAggDelta(-size, 0)
		return err
	}
	if err := s.bs.Put(ctx, blk); err != nil {
		s.addAggDelta(-size, 0)
		return err
	}
	s.blockCountAtomic.Add(1)
	return nil
}

func (s *NetworkBlockStore) Get(ctx context.Context, key cid.Cid) ([]byte, error) {
	blk, err := s.bs.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return blk.RawData(), nil
}

func (s *NetworkBlockStore) Delete(ctx context.Context, key cid.Cid) error {
	mu := s.lockShard(key)
	mu.Lock()
	defer mu.Unlock()
	return s.deleteLocked(ctx, key)
}

func (s *NetworkBlockStore) deleteLocked(ctx context.Context, key cid.Cid) error {
	if sz, err := s.bs.GetSize(ctx, key); err == nil {
		size := int64(sz)
		if err := s.bs.DeleteBlock(ctx, key); err != nil {
			return err
		}
		s.addAggDelta(-size, -1)
		return nil
	}
	return s.bs.DeleteBlock(ctx, key)
}

func (s *NetworkBlockStore) Has(ctx context.Context, key cid.Cid) (bool, error) {
	return s.bs.Has(ctx, key)
}

func (s *NetworkBlockStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return s.bs.AllKeysChan(ctx)
}

func (s *NetworkBlockStore) blockSize(ctx context.Context, key cid.Cid) (int, error) {
	return s.bs.GetSize(ctx, key)
}

func ringPresenceKey(ringID string) datastore.Key {
	return datastore.NewKey(nbPrefixRings + ringID)
}

func (s *NetworkBlockStore) markRingKnown(ringID string) {
	if _, loaded := s.knownRings.LoadOrStore(ringID, struct{}{}); loaded {
		return
	}
	_ = s.ds.Put(context.Background(), ringPresenceKey(ringID), []byte{})
}

func (s *NetworkBlockStore) MarkRingKnown(ringID string) {
	if ringID == "" {
		return
	}
	s.markRingKnown(ringID)
}

func (s *NetworkBlockStore) RingsKnown() []string {
	ctx := context.Background()
	results, err := s.ds.Query(ctx, query.Query{Prefix: nbPrefixRings, KeysOnly: true})
	if err != nil {
		return nil
	}
	defer results.Close()
	var out []string
	for r := range results.Next() {
		if r.Error != nil {
			continue
		}
		out = append(out, r.Key[len(nbPrefixRings):])
	}
	return out
}

func (s *NetworkBlockStore) ForgetRing(ringID string) {
	ctx := context.Background()
	prefix := nbPrefixRing + ringID + "/"
	results, err := s.ds.Query(ctx, query.Query{Prefix: prefix, KeysOnly: true})
	if err == nil {
		for r := range results.Next() {
			if r.Error == nil {
				_ = s.ds.Delete(ctx, datastore.NewKey(r.Key))
			}
		}
		results.Close()
	}
	_ = s.ds.Delete(ctx, ringPresenceKey(ringID))
	s.knownRings.Delete(ringID)
}

type RingView struct {
	nbs	*NetworkBlockStore
	ringID	string

	prefixRoots		string
	prefixBlockRoots	string
	prefixCIDUsage		string
	keyUsedBytes		datastore.Key
	keyBlockCount		datastore.Key
	keyQuota		datastore.Key
}

func (s *NetworkBlockStore) Ring(ringID string) *RingView {
	if ringID == "" {
		panic("network block store: empty ringID — pass store.RingPublic or a GroupID hex")
	}
	base := nbPrefixRing + ringID + "/"
	return &RingView{
		nbs:			s,
		ringID:			ringID,
		prefixRoots:		base + "netroots/",
		prefixBlockRoots:	base + "blockroots/",
		prefixCIDUsage:		base + "cidusage/",
		keyUsedBytes:		datastore.NewKey(base + "usedbytes"),
		keyBlockCount:		datastore.NewKey(base + "blockcount"),
		keyQuota:		datastore.NewKey(base + "quota"),
	}
}

func (r *RingView) RingID() string	{ return r.ringID }

func (r *RingView) rootKey(cidStr string) datastore.Key {
	return datastore.NewKey(r.prefixRoots + cidStr)
}

func (r *RingView) blockRootKey(mhHex, cidStr string) datastore.Key {
	return datastore.NewKey(r.prefixBlockRoots + mhHex + "/" + cidStr)
}

func (r *RingView) cidUsageKey(cidStr string) datastore.Key {
	return datastore.NewKey(r.prefixCIDUsage + cidStr)
}

func (r *RingView) AddRoot(c cid.Cid) {
	r.AddRootWithExpiry(c.String(), 0)
}

func (r *RingView) AddRootWithTTL(c cid.Cid, ttl time.Duration) {
	var expNanos int64
	if ttl > 0 {
		expNanos = time.Now().Add(ttl).UnixNano()
	}
	r.AddRootWithExpiry(c.String(), expNanos)
}

func (r *RingView) AddRootStr(cidStr string) {
	r.AddRootWithExpiry(cidStr, 0)
}

func (r *RingView) AddRootWithExpiry(cidStr string, expiresAtNanos int64) {
	rec := networkRootRecord{ExpiresAt: expiresAtNanos}
	data, err := cbor.Marshal(rec)
	if err != nil {
		slog.Warn("network roots: marshal error", "ring", r.ringID, "cid", cidStr, "error", err)
		return
	}
	ctx := context.Background()
	if err := r.nbs.ds.Put(ctx, r.rootKey(cidStr), data); err != nil {
		slog.Warn("network roots: put error", "ring", r.ringID, "cid", cidStr, "error", err)
		return
	}
	r.nbs.markRingKnown(r.ringID)
}

func (r *RingView) AddAllRoots(cids []string) {
	for _, c := range cids {
		r.AddRootWithExpiry(c, 0)
	}
}

func (r *RingView) AddAllRootEntries(entries []NetworkRootEntry) {
	for _, e := range entries {
		r.AddRootWithExpiry(e.CID, e.ExpiresAt)
	}
}

func (r *RingView) RemoveRoot(c cid.Cid) {
	r.RemoveRootStr(c.String())
}

func (r *RingView) RemoveRootStr(cidStr string) {
	ctx := context.Background()
	_ = r.nbs.ds.Delete(ctx, r.rootKey(cidStr))
	_ = r.nbs.ds.Delete(ctx, r.cidUsageKey(cidStr))
	r.removeBlockRootEntries(ctx, cidStr)
}

func (r *RingView) HasRoot(c cid.Cid) bool {
	return r.HasRootStr(c.String())
}

func (r *RingView) HasRootStr(cidStr string) bool {
	ctx := context.Background()
	data, err := r.nbs.ds.Get(ctx, r.rootKey(cidStr))
	if err != nil {
		return false
	}
	var rec networkRootRecord
	if err := cbor.Unmarshal(data, &rec); err != nil {
		return false
	}
	if rec.ExpiresAt > 0 && time.Now().UnixNano() > rec.ExpiresAt {
		return false
	}
	return true
}

func (r *RingView) ListRoots() []string {
	return r.listRootsFiltered(true)
}

func (r *RingView) listRootsFiltered(filterExpired bool) []string {
	ctx := context.Background()
	results, err := r.nbs.ds.Query(ctx, query.Query{Prefix: r.prefixRoots})
	if err != nil {
		return nil
	}
	defer results.Close()

	now := time.Now().UnixNano()
	var out []string
	for result := range results.Next() {
		if result.Error != nil {
			continue
		}
		cidStr := result.Key[len(r.prefixRoots):]
		if filterExpired {
			var rec networkRootRecord
			if err := cbor.Unmarshal(result.Value, &rec); err != nil {
				continue
			}
			if rec.ExpiresAt > 0 && now > rec.ExpiresAt {
				continue
			}
		}
		out = append(out, cidStr)
	}
	return out
}

func (r *RingView) ListRootEntries() []NetworkRootEntry {
	ctx := context.Background()
	results, err := r.nbs.ds.Query(ctx, query.Query{Prefix: r.prefixRoots})
	if err != nil {
		return nil
	}
	defer results.Close()

	now := time.Now().UnixNano()
	var out []NetworkRootEntry
	for result := range results.Next() {
		if result.Error != nil {
			continue
		}
		cidStr := result.Key[len(r.prefixRoots):]
		var rec networkRootRecord
		if err := cbor.Unmarshal(result.Value, &rec); err != nil {
			continue
		}
		if rec.ExpiresAt > 0 && now > rec.ExpiresAt {
			continue
		}
		out = append(out, NetworkRootEntry{CID: cidStr, ExpiresAt: rec.ExpiresAt})
	}
	return out
}

func (r *RingView) RootCount() int {
	return len(r.ListRoots())
}

func (r *RingView) PruneExpiredRoots() int {
	ctx := context.Background()
	results, err := r.nbs.ds.Query(ctx, query.Query{Prefix: r.prefixRoots})
	if err != nil {
		return 0
	}
	defer results.Close()

	now := time.Now().UnixNano()
	var pruned int
	for result := range results.Next() {
		if result.Error != nil {
			continue
		}
		var rec networkRootRecord
		if err := cbor.Unmarshal(result.Value, &rec); err != nil {
			continue
		}
		if rec.ExpiresAt > 0 && now > rec.ExpiresAt {
			cidStr := result.Key[len(r.prefixRoots):]
			r.RemoveRootStr(cidStr)
			pruned++
		}
	}
	return pruned
}

func (r *RingView) PutWithRoot(ctx context.Context, key cid.Cid, data []byte, rootCID cid.Cid, expiresAtNanos int64) error {
	mu := r.nbs.lockShard(key)
	mu.Lock()
	defer mu.Unlock()

	hasBlock, err := r.nbs.bs.Has(ctx, key)
	if err != nil {
		return err
	}
	size := int64(len(data))

	aggReserved := false
	if !hasBlock {
		if !r.nbs.reserveAggBytes(size) {
			return ErrStorageFull
		}
		aggReserved = true
	}

	rc := r.nbs.getRingCounters(r.ringID)
	addingToRing := false
	if rootCID.Defined() {
		mhHex := key.Hash().HexString()
		if !r.ringHasBlock(ctx, mhHex) {
			addingToRing = true
			if !rc.reserveBytes(size) {
				if aggReserved {
					r.nbs.addAggDelta(-size, 0)
				}
				return ErrStorageFull
			}
		}
	}

	if !hasBlock {
		blk, err := blocks.NewBlockWithCid(data, key)
		if err != nil {
			if aggReserved {
				r.nbs.addAggDelta(-size, 0)
			}
			if addingToRing {
				rc.addUsageDelta(-size, 0)
			}
			return err
		}
		if err := r.nbs.bs.Put(ctx, blk); err != nil {
			if aggReserved {
				r.nbs.addAggDelta(-size, 0)
			}
			if addingToRing {
				rc.addUsageDelta(-size, 0)
			}
			return err
		}
		r.nbs.blockCountAtomic.Add(1)
	}

	if rootCID.Defined() {
		mhHex := key.Hash().HexString()
		if addingToRing {
			rc.count.Add(1)
		}
		brKey := r.blockRootKey(mhHex, rootCID.String())
		if has, _ := r.nbs.ds.Has(ctx, brKey); !has {
			r.addCIDUsageLocked(ctx, rootCID.String(), size)
		}
		_ = r.nbs.ds.Put(ctx, brKey, []byte{})
		if !r.HasRootStr(rootCID.String()) {
			r.AddRootWithExpiry(rootCID.String(), expiresAtNanos)
		}
		r.nbs.markRingKnown(r.ringID)
	}
	return nil
}

func (r *RingView) DropBlock(key cid.Cid) bool {
	mhHex := key.Hash().HexString()
	prefix := r.prefixBlockRoots + mhHex + "/"
	ctx := context.Background()
	results, err := r.nbs.ds.Query(ctx, query.Query{Prefix: prefix, KeysOnly: true})
	if err != nil {
		return false
	}
	defer results.Close()

	mu := r.nbs.lockShard(key)
	mu.Lock()
	defer mu.Unlock()

	removed := false
	for result := range results.Next() {
		if result.Error != nil {
			continue
		}
		k := result.Key
		rootCID := k[len(prefix):]
		_ = r.nbs.ds.Delete(ctx, datastore.NewKey(k))
		if size, err := r.nbs.blockSize(ctx, key); err == nil && size > 0 {
			r.addCIDUsageLocked(ctx, rootCID, -int64(size))
		}
		removed = true
	}
	if removed {
		if size, err := r.nbs.blockSize(ctx, key); err == nil && size > 0 {
			rc := r.nbs.getRingCounters(r.ringID)
			rc.addUsageDelta(-int64(size), -1)
		}
	}
	return removed
}

func (r *RingView) AddBlockRootIndex(key cid.Cid, rootCIDStr string) {
	ctx := context.Background()
	mhHex := key.Hash().HexString()
	brKey := r.blockRootKey(mhHex, rootCIDStr)

	mu := r.nbs.lockShard(key)
	mu.Lock()
	defer mu.Unlock()

	has, _ := r.nbs.ds.Has(ctx, brKey)
	if has {
		return
	}

	size, err := r.nbs.blockSize(ctx, key)
	if err == nil && size > 0 {

		if !r.ringHasBlock(ctx, mhHex) {
			rc := r.nbs.getRingCounters(r.ringID)
			rc.addUsageDelta(int64(size), 1)
		}
		r.addCIDUsageLocked(ctx, rootCIDStr, int64(size))
	}
	_ = r.nbs.ds.Put(ctx, brKey, []byte{})
	r.nbs.markRingKnown(r.ringID)
}

func (r *RingView) ringHasBlock(ctx context.Context, mhHex string) bool {
	prefix := r.prefixBlockRoots + mhHex + "/"
	results, err := r.nbs.ds.Query(ctx, query.Query{Prefix: prefix, KeysOnly: true, Limit: 1})
	if err != nil {
		return false
	}
	defer results.Close()
	for r := range results.Next() {
		if r.Error == nil {
			return true
		}
	}
	return false
}

func (r *RingView) RootsForBlock(key cid.Cid) []string {
	mhHex := key.Hash().HexString()
	prefix := r.prefixBlockRoots + mhHex + "/"
	ctx := context.Background()
	results, err := r.nbs.ds.Query(ctx, query.Query{Prefix: prefix, KeysOnly: true})
	if err != nil {
		return nil
	}
	defer results.Close()

	var roots []string
	for result := range results.Next() {
		if result.Error != nil {
			continue
		}
		cidStr := result.Key[len(prefix):]
		roots = append(roots, cidStr)
	}
	return roots
}

func (r *RingView) RootsForBlockEntries(key cid.Cid) []NetworkRootEntry {
	rootCIDs := r.RootsForBlock(key)
	if len(rootCIDs) == 0 {
		return nil
	}
	ctx := context.Background()
	entries := make([]NetworkRootEntry, 0, len(rootCIDs))
	for _, cidStr := range rootCIDs {
		data, err := r.nbs.ds.Get(ctx, r.rootKey(cidStr))
		if err != nil {
			continue
		}
		var rec networkRootRecord
		if err := cbor.Unmarshal(data, &rec); err != nil {
			continue
		}
		entries = append(entries, NetworkRootEntry{CID: cidStr, ExpiresAt: rec.ExpiresAt})
	}
	return entries
}

func (r *RingView) HasBlock(key cid.Cid) bool {
	return r.ringHasBlock(context.Background(), key.Hash().HexString())
}

func (r *RingView) BlockHasLiveRoot(key cid.Cid) bool {
	rootCIDs := r.RootsForBlock(key)
	if len(rootCIDs) == 0 {
		return false
	}
	now := time.Now().UnixNano()
	ctx := context.Background()
	for _, cidStr := range rootCIDs {
		data, err := r.nbs.ds.Get(ctx, r.rootKey(cidStr))
		if err != nil {
			continue
		}
		var rec networkRootRecord
		if err := cbor.Unmarshal(data, &rec); err != nil {
			continue
		}
		if rec.ExpiresAt == 0 || now <= rec.ExpiresAt {
			return true
		}
	}
	return false
}

func (r *RingView) Blocks(ctx context.Context) ([]cid.Cid, error) {
	results, err := r.nbs.ds.Query(ctx, query.Query{Prefix: r.prefixBlockRoots, KeysOnly: true})
	if err != nil {
		return nil, err
	}
	defer results.Close()

	seen := make(map[string]cid.Cid)
	for result := range results.Next() {
		if result.Error != nil {
			continue
		}

		rest := result.Key[len(r.prefixBlockRoots):]
		slash := strings.IndexByte(rest, '/')
		if slash < 0 {
			continue
		}
		mhHex := rest[:slash]
		if _, ok := seen[mhHex]; ok {
			continue
		}

		c, ok := r.lookupBlockCID(ctx, mhHex)
		if !ok {
			continue
		}
		seen[mhHex] = c
	}

	out := make([]cid.Cid, 0, len(seen))
	for _, c := range seen {
		out = append(out, c)
	}
	return out, nil
}

func (r *RingView) lookupBlockCID(ctx context.Context, mhHex string) (cid.Cid, bool) {
	mhBytes, err := decodeHex(mhHex)
	if err != nil {
		return cid.Undef, false
	}

	c := cid.NewCidV1(cid.Raw, mhBytes)
	if has, err := r.nbs.bs.Has(ctx, c); err == nil && has {
		return c, true
	}

	ch, err := r.nbs.bs.AllKeysChan(ctx)
	if err != nil {
		return cid.Undef, false
	}
	for k := range ch {
		if k.Hash().HexString() == mhHex {
			return k, true
		}
	}
	return cid.Undef, false
}

func (r *RingView) removeBlockRootEntries(ctx context.Context, rootCIDStr string) {
	results, err := r.nbs.ds.Query(ctx, query.Query{Prefix: r.prefixBlockRoots, KeysOnly: true})
	if err != nil {
		return
	}
	defer results.Close()

	suffix := "/" + rootCIDStr
	r.nbs.lockAllShards()
	defer r.nbs.unlockAllShards()
	rc := r.nbs.getRingCounters(r.ringID)

	for result := range results.Next() {
		if result.Error != nil {
			continue
		}
		k := result.Key
		if len(k) <= len(suffix) || !strings.HasSuffix(k, suffix) {
			continue
		}

		mhHex := k[len(r.prefixBlockRoots) : len(k)-len(suffix)]

		_ = r.nbs.ds.Delete(ctx, datastore.NewKey(k))

		if !r.ringHasBlock(ctx, mhHex) {
			c, ok := r.lookupBlockCID(ctx, mhHex)
			if ok {
				if size, err := r.nbs.blockSize(ctx, c); err == nil {
					rc.addUsageDelta(-int64(size), -1)
				}
			}
		}
	}
}

func (r *RingView) UsedBytes() int64 {
	return r.nbs.getRingCounters(r.ringID).used.Load()
}

func (r *RingView) BlockCount() int64 {
	return r.nbs.getRingCounters(r.ringID).count.Load()
}

func (r *RingView) Quota() int64 {
	return r.nbs.getRingCounters(r.ringID).quota.Load()
}

func (r *RingView) SetQuota(max int64) error {
	if max < 0 {
		return fmt.Errorf("quota must be non-negative")
	}
	rc := r.nbs.getRingCounters(r.ringID)
	if max > 0 {
		if used := rc.used.Load(); max < used {
			return fmt.Errorf("quota %d below current ring usage %d", max, used)
		}
	}
	rc.quota.Store(max)
	if err := r.nbs.ds.Put(context.Background(), r.keyQuota, encodeInt64LE(max)); err != nil {
		return err
	}
	r.nbs.markRingKnown(r.ringID)
	return nil
}

func (r *RingView) addCIDUsageLocked(ctx context.Context, rootCID string, delta int64) {
	if delta == 0 {
		return
	}
	key := r.cidUsageKey(rootCID)
	var current int64
	if data, err := r.nbs.ds.Get(ctx, key); err == nil && len(data) == 8 {
		current = decodeInt64LE(data)
	}
	current += delta
	if current < 0 {
		current = 0
	}
	_ = r.nbs.ds.Put(ctx, key, encodeInt64LE(current))
}

func (r *RingView) CIDUsage() map[string]int64 {
	ctx := context.Background()
	results, err := r.nbs.ds.Query(ctx, query.Query{Prefix: r.prefixCIDUsage})
	if err != nil {
		return nil
	}
	defer results.Close()

	out := make(map[string]int64)
	for result := range results.Next() {
		if result.Error != nil {
			continue
		}
		cidStr := result.Key[len(r.prefixCIDUsage):]
		if len(result.Value) == 8 {
			if v := decodeInt64LE(result.Value); v > 0 {
				out[cidStr] = v
			}
		}
	}
	return out
}

func (s *NetworkBlockStore) BlockHasLiveRootAnyRing(key cid.Cid) bool {
	for _, ringID := range s.RingsKnown() {
		if s.Ring(ringID).BlockHasLiveRoot(key) {
			return true
		}
	}
	return false
}

func (s *NetworkBlockStore) PruneExpiredRoots() int {
	total := 0
	for _, ringID := range s.RingsKnown() {
		total += s.Ring(ringID).PruneExpiredRoots()
	}
	return total
}

func (s *NetworkBlockStore) AggregateUsedBytes() int64	{ return s.UsedBytes() }

func (s *NetworkBlockStore) ListRoots() []string {
	seen := make(map[string]struct{})
	for _, ringID := range s.RingsKnown() {
		for _, c := range s.Ring(ringID).ListRoots() {
			seen[c] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}
	return out
}

func (s *NetworkBlockStore) RootCount() int {
	return len(s.ListRoots())
}

func (s *NetworkBlockStore) CIDUsage() map[string]int64 {
	out := make(map[string]int64)
	for _, ringID := range s.RingsKnown() {
		for cidStr, b := range s.Ring(ringID).CIDUsage() {
			out[cidStr] += b
		}
	}
	return out
}

func (s *NetworkBlockStore) AddRoot(c cid.Cid)	{ s.Ring(RingPublic).AddRoot(c) }

func (s *NetworkBlockStore) AddRootStr(cidStr string)	{ s.Ring(RingPublic).AddRootStr(cidStr) }

func (s *NetworkBlockStore) AddRootWithExpiry(cidStr string, exp int64) {
	s.Ring(RingPublic).AddRootWithExpiry(cidStr, exp)
}

func (s *NetworkBlockStore) AddRootWithTTL(c cid.Cid, ttl time.Duration) {
	s.Ring(RingPublic).AddRootWithTTL(c, ttl)
}

func (s *NetworkBlockStore) RemoveRoot(c cid.Cid)	{ s.Ring(RingPublic).RemoveRoot(c) }

func (s *NetworkBlockStore) RemoveRootStr(cidStr string)	{ s.Ring(RingPublic).RemoveRootStr(cidStr) }

func (s *NetworkBlockStore) HasRoot(c cid.Cid) bool	{ return s.Ring(RingPublic).HasRoot(c) }

func (s *NetworkBlockStore) HasRootStr(cidStr string) bool {
	return s.Ring(RingPublic).HasRootStr(cidStr)
}

func (s *NetworkBlockStore) AddAllRoots(cids []string)	{ s.Ring(RingPublic).AddAllRoots(cids) }

func (s *NetworkBlockStore) AddAllRootEntries(entries []NetworkRootEntry) {
	s.Ring(RingPublic).AddAllRootEntries(entries)
}

func (s *NetworkBlockStore) RootsForBlock(key cid.Cid) []string {
	return s.Ring(RingPublic).RootsForBlock(key)
}

func (s *NetworkBlockStore) RootsForBlockEntries(key cid.Cid) []NetworkRootEntry {
	return s.Ring(RingPublic).RootsForBlockEntries(key)
}

func (s *NetworkBlockStore) AddBlockRootIndex(key cid.Cid, rootCIDStr string) {
	s.Ring(RingPublic).AddBlockRootIndex(key, rootCIDStr)
}

func (s *NetworkBlockStore) PutWithRoot(ctx context.Context, key cid.Cid, data []byte, rootCID cid.Cid, exp int64) error {
	return s.Ring(RingPublic).PutWithRoot(ctx, key, data, rootCID, exp)
}

func (s *NetworkBlockStore) ListRootEntries() []NetworkRootEntry {
	seen := make(map[string]NetworkRootEntry)
	for _, ringID := range s.RingsKnown() {
		for _, e := range s.Ring(ringID).ListRootEntries() {

			if cur, ok := seen[e.CID]; !ok || e.ExpiresAt > cur.ExpiresAt {
				seen[e.CID] = e
			}
		}
	}
	out := make([]NetworkRootEntry, 0, len(seen))
	for _, e := range seen {
		out = append(out, e)
	}
	return out
}

func (s *NetworkBlockStore) BlockHasLiveRoot(key cid.Cid) bool {
	return s.BlockHasLiveRootAnyRing(key)
}

func (s *NetworkBlockStore) GC(ctx context.Context) (GCResult, error) {
	start := time.Now()

	if pruned := s.PruneExpiredRoots(); pruned > 0 {
		slog.Info("network gc: pruned expired roots", "count", pruned)
	}

	rings := s.RingsKnown()
	live := make(map[string]struct{})
	for _, ringID := range rings {
		for _, cidStr := range s.Ring(ringID).ListRoots() {
			live[cidStr] = struct{}{}
		}
	}
	slog.Debug("network gc: live roots", "count", len(live), "rings", len(rings))

	ch, err := s.AllKeysChan(ctx)
	if err != nil {
		return GCResult{}, fmt.Errorf("enumerate blocks: %w", err)
	}

	var removed, kept int
	var totalBytesFreed, totalBlocksFreed int64
	for c := range ch {
		if s.blockHasLiveRootInSet(c, rings, live) {
			kept++
			continue
		}

		var blockSize int64
		if size, err := s.blockSize(ctx, c); err == nil {
			blockSize = int64(size)
		}

		mhHex := c.Hash().HexString()
		for _, ringID := range rings {
			rv := s.Ring(ringID)
			s.cleanupRingBlockIndex(ctx, rv, mhHex, blockSize)
		}

		if err := s.bs.DeleteBlock(ctx, c); err != nil {
			return GCResult{}, fmt.Errorf("delete block %s: %w", c, err)
		}
		totalBytesFreed += blockSize
		totalBlocksFreed++
		removed++
	}

	if totalBlocksFreed > 0 {
		s.addAggDelta(-totalBytesFreed, -totalBlocksFreed)
	}

	return GCResult{
		Removed:	removed,
		Kept:		kept,
		Elapsed:	time.Since(start),
	}, nil
}

func (s *NetworkBlockStore) cleanupRingBlockIndex(ctx context.Context, r *RingView, mhHex string, blockSize int64) {
	prefix := r.prefixBlockRoots + mhHex + "/"
	results, err := s.ds.Query(ctx, query.Query{Prefix: prefix, KeysOnly: true})
	if err != nil {
		return
	}
	defer results.Close()
	removedAny := false
	for result := range results.Next() {
		if result.Error != nil {
			continue
		}

		k := result.Key
		rootCID := k[len(prefix):]
		_ = s.ds.Delete(ctx, datastore.NewKey(k))
		r.addCIDUsageLocked(ctx, rootCID, -blockSize)
		removedAny = true
	}
	if removedAny {
		rc := s.getRingCounters(r.ringID)
		rc.addUsageDelta(-blockSize, -1)
	}
}

func (s *NetworkBlockStore) blockHasLiveRootInSet(key cid.Cid, rings []string, live map[string]struct{}) bool {
	mhHex := key.Hash().HexString()
	for _, ringID := range rings {
		r := s.Ring(ringID)
		prefix := r.prefixBlockRoots + mhHex + "/"
		ctx := context.Background()
		results, err := s.ds.Query(ctx, query.Query{Prefix: prefix, KeysOnly: true})
		if err != nil {
			continue
		}
		hit := false
		for result := range results.Next() {
			if result.Error != nil {
				continue
			}
			rootCID := result.Key[len(prefix):]
			if _, ok := live[rootCID]; ok {
				hit = true
				break
			}
		}
		results.Close()
		if hit {
			return true
		}
	}
	return false
}

func encodeInt64LE(v int64) []byte {
	data := make([]byte, 8)
	for i := 0; i < 8; i++ {
		data[i] = byte(v >> (i * 8))
	}
	return data
}

func decodeInt64LE(data []byte) int64 {
	var v int64
	for i := 0; i < 8; i++ {
		v |= int64(data[i]) << (i * 8)
	}
	return v
}

func decodeHex(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		return nil, fmt.Errorf("odd-length hex string")
	}
	out := make([]byte, len(s)/2)
	for i := 0; i < len(out); i++ {
		hi, err := hexNibble(s[i*2])
		if err != nil {
			return nil, err
		}
		lo, err := hexNibble(s[i*2+1])
		if err != nil {
			return nil, err
		}
		out[i] = (hi << 4) | lo
	}
	return out, nil
}

func hexNibble(b byte) (byte, error) {
	switch {
	case b >= '0' && b <= '9':
		return b - '0', nil
	case b >= 'a' && b <= 'f':
		return b - 'a' + 10, nil
	case b >= 'A' && b <= 'F':
		return b - 'A' + 10, nil
	}
	return 0, fmt.Errorf("invalid hex character %q", b)
}
