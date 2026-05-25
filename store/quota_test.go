package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

func testCID(t *testing.T, seed byte) cid.Cid {
	t.Helper()
	data := []byte{seed, seed, seed, seed}
	hash, err := mh.Sum(data, mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	return cid.NewCidV1(cid.Raw, hash)
}

func TestQuotaBlockStore_PutWithinLimit(t *testing.T) {
	mem := newMemBlockStore()
	q, err := newQuotaBlockStore(mem, 1024)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	c := testCID(t, 1)
	data := make([]byte, 500)

	if err := q.Put(ctx, c, data); err != nil {
		t.Fatalf("put within limit: %v", err)
	}
	if q.UsedBytes() != 500 {
		t.Fatalf("used bytes = %d, want 500", q.UsedBytes())
	}
}

func TestQuotaBlockStore_PutExceedsLimit(t *testing.T) {
	mem := newMemBlockStore()
	q, err := newQuotaBlockStore(mem, 1024)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	c1 := testCID(t, 1)
	if err := q.Put(ctx, c1, make([]byte, 800)); err != nil {
		t.Fatal(err)
	}

	c2 := testCID(t, 2)
	err = q.Put(ctx, c2, make([]byte, 300))
	if !IsStorageFull(err) {
		t.Fatalf("expected ErrStorageFull, got %v", err)
	}

	has, _ := q.Has(ctx, c2)
	if has {
		t.Fatal("block should not be stored after quota rejection")
	}

	if q.UsedBytes() != 800 {
		t.Fatalf("used bytes = %d, want 800", q.UsedBytes())
	}
}

func TestQuotaBlockStore_IdempotentPut(t *testing.T) {
	mem := newMemBlockStore()
	q, err := newQuotaBlockStore(mem, 1024)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	c := testCID(t, 1)
	data := make([]byte, 500)

	if err := q.Put(ctx, c, data); err != nil {
		t.Fatal(err)
	}

	if err := q.Put(ctx, c, data); err != nil {
		t.Fatal(err)
	}
	if q.UsedBytes() != 500 {
		t.Fatalf("used bytes = %d after idempotent put, want 500", q.UsedBytes())
	}
}

func TestQuotaBlockStore_DeleteFreesSpace(t *testing.T) {
	mem := newMemBlockStore()
	q, err := newQuotaBlockStore(mem, 1024)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	c1 := testCID(t, 1)
	c2 := testCID(t, 2)

	if err := q.Put(ctx, c1, make([]byte, 800)); err != nil {
		t.Fatal(err)
	}

	if err := q.Put(ctx, c2, make([]byte, 300)); !IsStorageFull(err) {
		t.Fatal("expected storage full")
	}

	if err := q.Delete(ctx, c1); err != nil {
		t.Fatal(err)
	}
	if q.UsedBytes() != 0 {
		t.Fatalf("used bytes after delete = %d, want 0", q.UsedBytes())
	}

	if err := q.Put(ctx, c2, make([]byte, 300)); err != nil {
		t.Fatalf("put after delete: %v", err)
	}
	if q.UsedBytes() != 300 {
		t.Fatalf("used bytes = %d, want 300", q.UsedBytes())
	}
}

func TestQuotaBlockStore_ZeroMaxIsUnlimited(t *testing.T) {
	mem := newMemBlockStore()
	q, err := newQuotaBlockStore(mem, 0)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	c := testCID(t, 1)

	if err := q.Put(ctx, c, make([]byte, 1<<20)); err != nil {
		t.Fatalf("put with unlimited quota: %v", err)
	}
}

func TestQuotaBlockStore_InitialUsageFromExistingBlocks(t *testing.T) {
	mem := newMemBlockStore()
	ctx := context.Background()

	c1 := testCID(t, 1)
	c2 := testCID(t, 2)
	mem.Put(ctx, c1, make([]byte, 100))
	mem.Put(ctx, c2, make([]byte, 200))

	q, err := newQuotaBlockStore(mem, 1024)
	if err != nil {
		t.Fatal(err)
	}

	if q.UsedBytes() != 300 {
		t.Fatalf("initial used bytes = %d, want 300", q.UsedBytes())
	}
}

func TestIsStorageFull_StringDetection(t *testing.T) {

	err := fmt.Errorf("rpc error: %s", ErrStorageFull.Error())
	if !IsStorageFull(err) {
		t.Fatal("IsStorageFull should detect string-serialised error")
	}
}
