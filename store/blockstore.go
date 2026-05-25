package store

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/ipfs/boxo/blockstore"
)

type BlockStore interface {
	Put(ctx context.Context, key cid.Cid, data []byte) error

	Get(ctx context.Context, key cid.Cid) ([]byte, error)

	Delete(ctx context.Context, key cid.Cid) error

	Has(ctx context.Context, key cid.Cid) (bool, error)

	AllKeysChan(ctx context.Context) (<-chan cid.Cid, error)
}

type localBlockStore struct {
	bs blockstore.Blockstore
}

var _ BlockStore = (*localBlockStore)(nil)

func newLocalBlockStore(bs blockstore.Blockstore) *localBlockStore {
	return &localBlockStore{bs: bs}
}

func (s *localBlockStore) Put(ctx context.Context, key cid.Cid, data []byte) error {
	blk, err := blocks.NewBlockWithCid(data, key)
	if err != nil {
		return err
	}
	return s.bs.Put(ctx, blk)
}

func (s *localBlockStore) Get(ctx context.Context, key cid.Cid) ([]byte, error) {
	blk, err := s.bs.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return blk.RawData(), nil
}

func (s *localBlockStore) Delete(ctx context.Context, key cid.Cid) error {
	return s.bs.DeleteBlock(ctx, key)
}

func (s *localBlockStore) Has(ctx context.Context, key cid.Cid) (bool, error) {
	return s.bs.Has(ctx, key)
}

func (s *localBlockStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return s.bs.AllKeysChan(ctx)
}
