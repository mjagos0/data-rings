package store

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ipfs/go-cid"
)

var ErrStorageFull = errors.New("storage full: block store quota exceeded")

const storageFullPrefix = "storage full"

func IsStorageFull(err error) bool {
	return err != nil && (errors.Is(err, ErrStorageFull) ||
		strings.Contains(err.Error(), storageFullPrefix))
}

type StorageStatus struct {
	UsedBytes	int64	`json:"used_bytes"`
	MaxBytes	int64	`json:"max_bytes"`
	BlockCount	int64	`json:"block_count"`
}

type quotaBlockStore struct {
	inner		BlockStore
	maxBytes	int64
	usedBytes	atomic.Int64
	mu		sync.Mutex
}

var _ BlockStore = (*quotaBlockStore)(nil)

func newQuotaBlockStore(inner BlockStore, maxBytes int64) (*quotaBlockStore, error) {
	q := &quotaBlockStore{
		inner:		inner,
		maxBytes:	maxBytes,
	}
	if err := q.computeUsage(); err != nil {
		return nil, err
	}
	return q, nil
}

func (q *quotaBlockStore) computeUsage() error {
	ctx := context.Background()
	ch, err := q.inner.AllKeysChan(ctx)
	if err != nil {
		return err
	}
	var total int64
	var count int
	for key := range ch {
		data, err := q.inner.Get(ctx, key)
		if err != nil {
			continue
		}
		total += int64(len(data))
		count++
	}
	q.usedBytes.Store(total)
	if count > 0 {
		slog.Info("storage quota: computed initial usage", "blocks", count, "used_bytes", total, "max_bytes", q.maxBytes)
	}
	return nil
}

func (q *quotaBlockStore) UsedBytes() int64	{ return q.usedBytes.Load() }
func (q *quotaBlockStore) MaxBytes() int64	{ return q.maxBytes }

func (q *quotaBlockStore) Put(ctx context.Context, key cid.Cid, data []byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	has, err := q.inner.Has(ctx, key)
	if err != nil {
		return err
	}
	if has {
		return nil
	}

	size := int64(len(data))
	if q.maxBytes > 0 && q.usedBytes.Load()+size > q.maxBytes {
		return ErrStorageFull
	}

	if err := q.inner.Put(ctx, key, data); err != nil {
		return err
	}
	q.usedBytes.Add(size)
	return nil
}

func (q *quotaBlockStore) Get(ctx context.Context, key cid.Cid) ([]byte, error) {
	return q.inner.Get(ctx, key)
}

func (q *quotaBlockStore) Delete(ctx context.Context, key cid.Cid) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	data, err := q.inner.Get(ctx, key)
	if err != nil {
		return q.inner.Delete(ctx, key)
	}

	if err := q.inner.Delete(ctx, key); err != nil {
		return err
	}
	newUsed := q.usedBytes.Add(-int64(len(data)))
	if newUsed < 0 {
		q.usedBytes.Store(0)
	}
	return nil
}

func (q *quotaBlockStore) Has(ctx context.Context, key cid.Cid) (bool, error) {
	return q.inner.Has(ctx, key)
}

func (q *quotaBlockStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return q.inner.AllKeysChan(ctx)
}
