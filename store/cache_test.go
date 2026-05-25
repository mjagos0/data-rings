package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

func makeCID(data []byte) cid.Cid {
	h := sha256.Sum256(data)
	hash, _ := mh.Encode(h[:], mh.SHA2_256)
	return cid.NewCidV1(cid.Raw, hash)
}

func makeBlock(n int, seed byte) (cid.Cid, []byte) {
	data := make([]byte, n)
	for i := range data {
		data[i] = seed ^ byte(i)
	}
	return makeCID(data), data
}

type memBlockStore struct {
	mu	sync.Mutex
	blocks	map[cid.Cid][]byte
	gets	int
}

func newMemBlockStore() *memBlockStore {
	return &memBlockStore{blocks: make(map[cid.Cid][]byte)}
}

func (m *memBlockStore) Put(_ context.Context, key cid.Cid, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.blocks[key] = cp
	return nil
}

func (m *memBlockStore) Get(_ context.Context, key cid.Cid) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gets++
	data, ok := m.blocks[key]
	if !ok {
		return nil, fmt.Errorf("block not found: %s", key)
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, nil
}

func (m *memBlockStore) Delete(_ context.Context, key cid.Cid) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.blocks, key)
	return nil
}

func (m *memBlockStore) Has(_ context.Context, key cid.Cid) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.blocks[key]
	return ok, nil
}

func (m *memBlockStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	m.mu.Lock()
	keys := make([]cid.Cid, 0, len(m.blocks))
	for k := range m.blocks {
		keys = append(keys, k)
	}
	m.mu.Unlock()

	ch := make(chan cid.Cid)
	go func() {
		defer close(ch)
		for _, k := range keys {
			select {
			case ch <- k:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

func (m *memBlockStore) getCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gets
}

func (m *memBlockStore) resetGetCount() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gets = 0
}

func TestCacheHitAvoidsInnerGet(t *testing.T) {
	inner := newMemBlockStore()
	cache := newCachedBlockStore(inner, CacheOpts{MaxBytes: 1 << 20})
	ctx := context.Background()

	key, data := makeBlock(100, 0x01)
	if err := cache.Put(ctx, key, data); err != nil {
		t.Fatal(err)
	}
	inner.resetGetCount()

	got, err := cache.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatal("data mismatch")
	}
	if inner.getCount() != 0 {
		t.Fatalf("expected 0 inner gets after Put+Get, got %d", inner.getCount())
	}

	inner.resetGetCount()
	got2, err := cache.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if string(got2) != string(data) {
		t.Fatal("data mismatch on second get")
	}
	if inner.getCount() != 0 {
		t.Fatalf("expected 0 inner gets on second read, got %d", inner.getCount())
	}
}

func TestCacheMissDelegatesToInner(t *testing.T) {
	inner := newMemBlockStore()
	cache := newCachedBlockStore(inner, CacheOpts{MaxBytes: 1 << 20})
	ctx := context.Background()

	key, data := makeBlock(200, 0x02)

	if err := inner.Put(ctx, key, data); err != nil {
		t.Fatal(err)
	}
	inner.resetGetCount()

	got, err := cache.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatal("data mismatch")
	}
	if inner.getCount() != 1 {
		t.Fatalf("expected 1 inner get on cache miss, got %d", inner.getCount())
	}

	inner.resetGetCount()
	_, err = cache.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if inner.getCount() != 0 {
		t.Fatalf("expected 0 inner gets after cache was populated, got %d", inner.getCount())
	}
}

func TestCacheEvictsLRU(t *testing.T) {

	cache := newCachedBlockStore(newMemBlockStore(), CacheOpts{MaxBytes: 300})
	ctx := context.Background()

	keys := make([]cid.Cid, 4)
	datas := make([][]byte, 4)
	for i := 0; i < 4; i++ {
		keys[i], datas[i] = makeBlock(100, byte(i))
		if err := cache.Put(ctx, keys[i], datas[i]); err != nil {
			t.Fatal(err)
		}
	}

	cache.mu.Lock()
	_, firstPresent := cache.items[keys[0].Hash().String()]
	_, lastPresent := cache.items[keys[3].Hash().String()]
	cur := cache.curBytes
	cache.mu.Unlock()

	if firstPresent {
		t.Fatal("expected first block to be evicted")
	}
	if !lastPresent {
		t.Fatal("expected last block to remain cached")
	}
	if cur > 300 {
		t.Fatalf("cache exceeded budget: %d > 300", cur)
	}
}

func TestCacheLRUOrderRespected(t *testing.T) {

	cache := newCachedBlockStore(newMemBlockStore(), CacheOpts{MaxBytes: 300})
	ctx := context.Background()

	keyA, dataA := makeBlock(100, 0x0A)
	keyB, dataB := makeBlock(100, 0x0B)
	keyC, dataC := makeBlock(100, 0x0C)
	keyD, dataD := makeBlock(100, 0x0D)

	for _, pair := range []struct {
		k	cid.Cid
		d	[]byte
	}{{keyA, dataA}, {keyB, dataB}, {keyC, dataC}} {
		if err := cache.Put(ctx, pair.k, pair.d); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := cache.Get(ctx, keyA); err != nil {
		t.Fatal(err)
	}

	if err := cache.Put(ctx, keyD, dataD); err != nil {
		t.Fatal(err)
	}

	cache.mu.Lock()
	_, hasA := cache.items[keyA.Hash().String()]
	_, hasB := cache.items[keyB.Hash().String()]
	_, hasD := cache.items[keyD.Hash().String()]
	cache.mu.Unlock()

	if !hasA {
		t.Fatal("A was touched and should not be evicted")
	}
	if hasB {
		t.Fatal("B should have been evicted as LRU")
	}
	if !hasD {
		t.Fatal("D was just inserted and should be present")
	}
}

func TestCacheSkipsOversizedBlocks(t *testing.T) {
	cache := newCachedBlockStore(newMemBlockStore(), CacheOpts{MaxBytes: 50})
	ctx := context.Background()

	key, data := makeBlock(100, 0xFF)
	if err := cache.Put(ctx, key, data); err != nil {
		t.Fatal(err)
	}

	cache.mu.Lock()
	_, present := cache.items[key.Hash().String()]
	cache.mu.Unlock()

	if present {
		t.Fatal("block larger than budget should not be cached")
	}
}

func TestDeleteInvalidatesCache(t *testing.T) {
	inner := newMemBlockStore()
	cache := newCachedBlockStore(inner, CacheOpts{MaxBytes: 1 << 20})
	ctx := context.Background()

	key, data := makeBlock(100, 0x03)
	if err := cache.Put(ctx, key, data); err != nil {
		t.Fatal(err)
	}

	if err := cache.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}

	cache.mu.Lock()
	_, present := cache.items[key.Hash().String()]
	cache.mu.Unlock()
	if present {
		t.Fatal("deleted key should not remain in cache")
	}

	has, _ := inner.Has(ctx, key)
	if has {
		t.Fatal("deleted key should not remain in inner store")
	}
}

func TestHasReturnsTrueFromCache(t *testing.T) {
	inner := newMemBlockStore()
	cache := newCachedBlockStore(inner, CacheOpts{MaxBytes: 1 << 20})
	ctx := context.Background()

	key, data := makeBlock(100, 0x04)
	if err := cache.Put(ctx, key, data); err != nil {
		t.Fatal(err)
	}

	inner.mu.Lock()
	delete(inner.blocks, key)
	inner.mu.Unlock()

	has, err := cache.Has(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Fatal("Has should return true for cached key")
	}
}

func TestHasDelegatesToInnerOnMiss(t *testing.T) {
	inner := newMemBlockStore()
	cache := newCachedBlockStore(inner, CacheOpts{MaxBytes: 1 << 20})
	ctx := context.Background()

	key, data := makeBlock(100, 0x06)

	if err := inner.Put(ctx, key, data); err != nil {
		t.Fatal(err)
	}

	has, err := cache.Has(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Fatal("Has should delegate to inner and find the key")
	}
}

func TestCacheReturnsCopies(t *testing.T) {
	cache := newCachedBlockStore(newMemBlockStore(), CacheOpts{MaxBytes: 1 << 20})
	ctx := context.Background()

	key, data := makeBlock(100, 0x05)
	if err := cache.Put(ctx, key, data); err != nil {
		t.Fatal(err)
	}

	got1, _ := cache.Get(ctx, key)
	got2, _ := cache.Get(ctx, key)

	got1[0] = 0xFF

	if got2[0] == 0xFF {
		t.Fatal("cache returned aliased slices — mutations bleed across callers")
	}
}

func TestPutDoesNotAliasCaller(t *testing.T) {
	cache := newCachedBlockStore(newMemBlockStore(), CacheOpts{MaxBytes: 1 << 20})
	ctx := context.Background()

	key, data := makeBlock(100, 0x07)
	if err := cache.Put(ctx, key, data); err != nil {
		t.Fatal(err)
	}

	data[0] = 0xFF

	got, _ := cache.Get(ctx, key)
	if got[0] == 0xFF {
		t.Fatal("cache aliased the caller's slice — Put must copy")
	}
}

func TestAllKeysChanDelegatesToInner(t *testing.T) {
	inner := newMemBlockStore()
	cache := newCachedBlockStore(inner, CacheOpts{MaxBytes: 1 << 20})
	ctx := context.Background()

	var inserted []cid.Cid
	for i := 0; i < 5; i++ {
		key, data := makeBlock(50, byte(i+10))
		if err := cache.Put(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		inserted = append(inserted, key)
	}

	ch, err := cache.AllKeysChan(ctx)
	if err != nil {
		t.Fatal(err)
	}

	got := make(map[cid.Cid]bool)
	for k := range ch {
		got[k] = true
	}

	for _, k := range inserted {
		if !got[k] {
			t.Fatalf("AllKeysChan missing key %s", k)
		}
	}
}

func TestConcurrentAccess(t *testing.T) {
	cache := newCachedBlockStore(newMemBlockStore(), CacheOpts{MaxBytes: 1 << 20})
	ctx := context.Background()

	const goroutines = 16
	const blocksPerRoutine = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < blocksPerRoutine; i++ {
				data := make([]byte, 64)
				rand.Read(data)
				key := makeCID(data)

				if err := cache.Put(ctx, key, data); err != nil {
					t.Errorf("goroutine %d: put: %v", id, err)
					return
				}
				got, err := cache.Get(ctx, key)
				if err != nil {
					t.Errorf("goroutine %d: get: %v", id, err)
					return
				}
				if string(got) != string(data) {
					t.Errorf("goroutine %d: data mismatch", id)
					return
				}
				cache.Has(ctx, key)
			}
		}(g)
	}
	wg.Wait()
}

func TestCurBytesNeverNegative(t *testing.T) {
	cache := newCachedBlockStore(newMemBlockStore(), CacheOpts{MaxBytes: 200})
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		key, data := makeBlock(80, byte(i))
		cache.Put(ctx, key, data)
		cache.Delete(ctx, key)
	}

	cache.mu.Lock()
	cur := cache.curBytes
	cache.mu.Unlock()

	if cur != 0 {
		t.Fatalf("expected 0 curBytes after deleting everything, got %d", cur)
	}
}

func TestUpdateExistingEntry(t *testing.T) {
	cache := newCachedBlockStore(newMemBlockStore(), CacheOpts{MaxBytes: 1 << 20})
	ctx := context.Background()

	key, data1 := makeBlock(100, 0x10)
	if err := cache.Put(ctx, key, data1); err != nil {
		t.Fatal(err)
	}

	data2 := make([]byte, 200)
	for i := range data2 {
		data2[i] = 0xAB
	}
	cache.put(key, data2)

	cache.mu.Lock()
	cur := cache.curBytes
	cache.mu.Unlock()

	if cur != 200 {
		t.Fatalf("expected curBytes=200 after update, got %d", cur)
	}

	got, _ := cache.Get(ctx, key)
	if string(got) != string(data2) {
		t.Fatal("cache should return updated data")
	}
}
