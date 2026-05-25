package store

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
)

var ErrAlreadyTracked = errors.New("CID already tracked")

var ErrNotFound = errors.New("root not found")

type Root struct {
	ID	string
	Name	string
	CID	cid.Cid
	Path	string
	AddedAt	time.Time
}

type rootRecord struct {
	ID	string	`cbor:"1,keyasint"`
	Name	string	`cbor:"2,keyasint"`
	CID	[]byte	`cbor:"3,keyasint"`
	Path	string	`cbor:"4,keyasint"`
	AddedAt	int64	`cbor:"5,keyasint"`
}

const (
	prefixRec	= "/roots/rec/"
	prefixIdxCID	= "/roots/idx/cid/"
	prefixIdxName	= "/roots/idx/name/"
)

type RootRegistry struct {
	ds datastore.Batching
}

func openRootRegistry(ds datastore.Batching) (*RootRegistry, error) {
	return &RootRegistry{ds: ds}, nil
}

func recKey(id string) datastore.Key {
	return datastore.NewKey(prefixRec + id)
}

func cidIdxKey(cidStr, id string) datastore.Key {
	return datastore.NewKey(prefixIdxCID + cidStr + "/" + id)
}

func nameIdxKey(name, id string) datastore.Key {
	return datastore.NewKey(prefixIdxName + url.PathEscape(name) + "/" + id)
}

func encodeRecord(root Root) ([]byte, error) {
	rec := rootRecord{
		ID:		root.ID,
		Name:		root.Name,
		CID:		root.CID.Bytes(),
		Path:		root.Path,
		AddedAt:	root.AddedAt.UnixNano(),
	}
	return cbor.Marshal(rec)
}

func decodeRecord(data []byte) (Root, error) {
	var rec rootRecord
	if err := cbor.Unmarshal(data, &rec); err != nil {
		return Root{}, err
	}
	c, err := cid.Cast(rec.CID)
	if err != nil {
		return Root{}, fmt.Errorf("invalid CID bytes: %w", err)
	}
	return Root{
		ID:		rec.ID,
		Name:		rec.Name,
		CID:		c,
		Path:		rec.Path,
		AddedAt:	time.Unix(0, rec.AddedAt),
	}, nil
}

func (r *RootRegistry) Add(root Root) (Root, error) {
	ctx := context.Background()

	existing, err := r.GetByCID(root.CID)
	if err != nil {
		return Root{}, err
	}
	if len(existing) > 0 {
		return existing[0], ErrAlreadyTracked
	}

	root.ID = uuid.New().String()
	if root.AddedAt.IsZero() {
		root.AddedAt = time.Now()
	}

	data, err := encodeRecord(root)
	if err != nil {
		return Root{}, err
	}

	batch, err := r.ds.Batch(ctx)
	if err != nil {
		return Root{}, err
	}
	if err := batch.Put(ctx, recKey(root.ID), data); err != nil {
		return Root{}, err
	}
	if err := batch.Put(ctx, cidIdxKey(root.CID.String(), root.ID), []byte{}); err != nil {
		return Root{}, err
	}
	if err := batch.Put(ctx, nameIdxKey(root.Name, root.ID), []byte{}); err != nil {
		return Root{}, err
	}
	if err := batch.Commit(ctx); err != nil {
		return Root{}, err
	}
	return root, nil
}

func (r *RootRegistry) Remove(id string) error {
	ctx := context.Background()

	root, ok, err := r.GetByID(id)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotFound
	}

	batch, err := r.ds.Batch(ctx)
	if err != nil {
		return err
	}
	if err := batch.Delete(ctx, recKey(id)); err != nil {
		return err
	}
	if err := batch.Delete(ctx, cidIdxKey(root.CID.String(), id)); err != nil {
		return err
	}
	if err := batch.Delete(ctx, nameIdxKey(root.Name, id)); err != nil {
		return err
	}
	return batch.Commit(ctx)
}

func (r *RootRegistry) Rename(id, newName string) error {
	if newName == "" {
		return errors.New("name must not be empty")
	}
	ctx := context.Background()

	root, ok, err := r.GetByID(id)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotFound
	}

	oldName := root.Name
	root.Name = newName

	data, err := encodeRecord(root)
	if err != nil {
		return err
	}

	batch, err := r.ds.Batch(ctx)
	if err != nil {
		return err
	}
	if err := batch.Put(ctx, recKey(id), data); err != nil {
		return err
	}
	if err := batch.Delete(ctx, nameIdxKey(oldName, id)); err != nil {
		return err
	}
	if err := batch.Put(ctx, nameIdxKey(newName, id), []byte{}); err != nil {
		return err
	}
	return batch.Commit(ctx)
}

func (r *RootRegistry) GetByID(id string) (Root, bool, error) {
	data, err := r.ds.Get(context.Background(), recKey(id))
	if errors.Is(err, datastore.ErrNotFound) {
		return Root{}, false, nil
	}
	if err != nil {
		return Root{}, false, err
	}
	root, err := decodeRecord(data)
	if err != nil {
		return Root{}, false, err
	}
	return root, true, nil
}

func (r *RootRegistry) GetByCID(c cid.Cid) ([]Root, error) {
	return r.queryByIndexPrefix(prefixIdxCID + c.String() + "/")
}

func (r *RootRegistry) GetByName(name string) ([]Root, error) {
	return r.queryByIndexPrefix(prefixIdxName + url.PathEscape(name) + "/")
}

func (r *RootRegistry) List() []Root {
	ctx := context.Background()
	results, err := r.ds.Query(ctx, query.Query{Prefix: prefixRec})
	if err != nil {
		return nil
	}
	defer results.Close()

	var roots []Root
	for result := range results.Next() {
		if result.Error != nil {
			continue
		}
		root, err := decodeRecord(result.Value)
		if err != nil {
			continue
		}
		roots = append(roots, root)
	}
	return roots
}

func (r *RootRegistry) queryByIndexPrefix(prefix string) ([]Root, error) {
	ctx := context.Background()
	results, err := r.ds.Query(ctx, query.Query{Prefix: prefix, KeysOnly: true})
	if err != nil {
		return nil, err
	}
	defer results.Close()

	var roots []Root
	for result := range results.Next() {
		if result.Error != nil {
			return nil, result.Error
		}

		parts := strings.Split(strings.TrimRight(result.Key, "/"), "/")
		id := parts[len(parts)-1]
		root, ok, err := r.GetByID(id)
		if err != nil {
			return nil, err
		}
		if ok {
			roots = append(roots, root)
		}
	}
	return roots, nil
}

func NameIno(name string) uint64 {
	const offset64 = 14695981039346656037
	const prime64 = 1099511628211
	h := uint64(offset64)
	for i := 0; i < len(name); i++ {
		h ^= uint64(name[i])
		h *= prime64
	}
	return h
}
