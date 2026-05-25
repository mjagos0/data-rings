package store

import (
	"container/list"
	"context"
	"sync"

	"github.com/ipfs/go-cid"
)

type CacheOpts struct {
	MaxBytes int
}

func DefaultCacheOpts() CacheOpts {
	return CacheOpts{
		MaxBytes: 64 << 20,
	}
}

type cacheEntry struct {
	key	cid.Cid
	data	[]byte
}

type cachedBlockStore struct {
	inner	BlockStore

	mu		sync.Mutex
	items		map[string]*list.Element
	order		*list.List
	curBytes	int
	maxBytes	int
}

var _ BlockStore = (*cachedBlockStore)(nil)

func newCachedBlockStore(inner BlockStore, opts CacheOpts) *cachedBlockStore {
	if opts.MaxBytes <= 0 {
		opts = DefaultCacheOpts()
	}
	return &cachedBlockStore{
		inner:		inner,
		items:		make(map[string]*list.Element),
		order:		list.New(),
		maxBytes:	opts.MaxBytes,
	}
}

func (c *cachedBlockStore) Get(ctx context.Context, key cid.Cid) ([]byte, error) {
	mh := key.Hash().String()
	c.mu.Lock()
	if el, ok := c.items[mh]; ok {
		c.order.MoveToFront(el)
		data := el.Value.(*cacheEntry).data
		c.mu.Unlock()

		out := make([]byte, len(data))
		copy(out, data)
		return out, nil
	}
	c.mu.Unlock()

	data, err := c.inner.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	c.put(key, data)
	return data, nil
}

func (c *cachedBlockStore) Put(ctx context.Context, key cid.Cid, data []byte) error {
	if err := c.inner.Put(ctx, key, data); err != nil {
		return err
	}
	c.put(key, data)
	return nil
}

func (c *cachedBlockStore) Delete(ctx context.Context, key cid.Cid) error {
	c.evict(key)
	return c.inner.Delete(ctx, key)
}

func (c *cachedBlockStore) Has(ctx context.Context, key cid.Cid) (bool, error) {
	mh := key.Hash().String()
	c.mu.Lock()
	_, ok := c.items[mh]
	c.mu.Unlock()
	if ok {
		return true, nil
	}
	return c.inner.Has(ctx, key)
}

func (c *cachedBlockStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return c.inner.AllKeysChan(ctx)
}

func (c *cachedBlockStore) put(key cid.Cid, data []byte) {
	size := len(data)
	if size > c.maxBytes {
		return
	}

	owned := make([]byte, size)
	copy(owned, data)

	mh := key.Hash().String()

	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[mh]; ok {
		old := el.Value.(*cacheEntry)
		c.curBytes += size - len(old.data)
		old.data = owned
		c.order.MoveToFront(el)
		c.evictLocked()
		return
	}

	c.curBytes += size
	entry := &cacheEntry{key: key, data: owned}
	el := c.order.PushFront(entry)
	c.items[mh] = el
	c.evictLocked()
}

func (c *cachedBlockStore) evict(key cid.Cid) {
	mh := key.Hash().String()
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[mh]; ok {
		c.removeLocked(el)
	}
}

func (c *cachedBlockStore) evictLocked() {
	for c.curBytes > c.maxBytes {
		tail := c.order.Back()
		if tail == nil {
			break
		}
		c.removeLocked(tail)
	}
}

func (c *cachedBlockStore) removeLocked(el *list.Element) {
	entry := c.order.Remove(el).(*cacheEntry)
	delete(c.items, entry.key.Hash().String())
	c.curBytes -= len(entry.data)
}
