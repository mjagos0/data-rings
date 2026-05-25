package store

import (
	"context"
	"testing"

	datastore "github.com/ipfs/go-datastore"
)

func TestNetworkBlockStore_CountersPersistAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	nbs, err := OpenNetworkBlockStore(dir, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	ctx := context.Background()
	var totalBytes int64
	for i, seed := range []byte{0xA1, 0xB2, 0xC3} {
		key, data := makeBlock(64+i*8, seed)
		if err := nbs.Put(ctx, key, data); err != nil {
			t.Fatalf("put seed=%x: %v", seed, err)
		}
		totalBytes += int64(len(data))
	}
	if got := nbs.UsedBytes(); got != totalBytes {
		t.Fatalf("UsedBytes after Puts = %d, want %d", got, totalBytes)
	}
	if got := nbs.BlockCount(); got != 3 {
		t.Fatalf("BlockCount after Puts = %d, want 3", got)
	}

	if err := nbs.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	nbs2, err := OpenNetworkBlockStore(dir, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer nbs2.Close()

	if got := nbs2.UsedBytes(); got != totalBytes {
		t.Errorf("UsedBytes after reopen = %d, want %d", got, totalBytes)
	}
	if got := nbs2.BlockCount(); got != 3 {
		t.Errorf("BlockCount after reopen = %d, want 3", got)
	}
}

func TestNetworkBlockStore_CountersTrackPutDelete(t *testing.T) {
	nbs := openTestNBS(t)
	ctx := context.Background()

	key1, data1 := makeBlock(128, 0x11)
	key2, data2 := makeBlock(256, 0x22)

	if err := nbs.Put(ctx, key1, data1); err != nil {
		t.Fatalf("put1: %v", err)
	}
	if err := nbs.Put(ctx, key2, data2); err != nil {
		t.Fatalf("put2: %v", err)
	}
	if got, want := nbs.BlockCount(), int64(2); got != want {
		t.Errorf("BlockCount after 2 puts = %d, want %d", got, want)
	}
	if got, want := nbs.UsedBytes(), int64(len(data1)+len(data2)); got != want {
		t.Errorf("UsedBytes after 2 puts = %d, want %d", got, want)
	}

	if err := nbs.Put(ctx, key1, data1); err != nil {
		t.Fatalf("put1 repeat: %v", err)
	}
	if got, want := nbs.BlockCount(), int64(2); got != want {
		t.Errorf("BlockCount after idempotent Put = %d, want %d", got, want)
	}

	if err := nbs.Delete(ctx, key1); err != nil {
		t.Fatalf("delete1: %v", err)
	}
	if got, want := nbs.BlockCount(), int64(1); got != want {
		t.Errorf("BlockCount after Delete = %d, want %d", got, want)
	}
	if got, want := nbs.UsedBytes(), int64(len(data2)); got != want {
		t.Errorf("UsedBytes after Delete = %d, want %d", got, want)
	}
}

func TestNetworkBlockStore_PerRingCountersMatchPutWithRoot(t *testing.T) {
	nbs := openTestNBS(t)
	ctx := context.Background()

	root1, _ := makeBlock(16, 0x01)
	root2, _ := makeBlock(16, 0x02)
	blockKey, blockData := makeBlock(512, 0xFE)

	ringA := nbs.Ring("ring-a")
	ringB := nbs.Ring("ring-b")

	if err := ringA.PutWithRoot(ctx, blockKey, blockData, root1, 0); err != nil {
		t.Fatalf("ringA put: %v", err)
	}
	if err := ringB.PutWithRoot(ctx, blockKey, blockData, root2, 0); err != nil {
		t.Fatalf("ringB put: %v", err)
	}

	if got := nbs.BlockCount(); got != 1 {
		t.Errorf("aggregate BlockCount = %d, want 1 (shared block)", got)
	}
	if got := nbs.UsedBytes(); got != int64(len(blockData)) {
		t.Errorf("aggregate UsedBytes = %d, want %d", got, len(blockData))
	}
	if got := ringA.BlockCount(); got != 1 {
		t.Errorf("ringA BlockCount = %d, want 1", got)
	}
	if got := ringB.BlockCount(); got != 1 {
		t.Errorf("ringB BlockCount = %d, want 1", got)
	}
	if got := ringA.UsedBytes(); got != int64(len(blockData)) {
		t.Errorf("ringA UsedBytes = %d, want %d", got, len(blockData))
	}
	if got := ringB.UsedBytes(); got != int64(len(blockData)) {
		t.Errorf("ringB UsedBytes = %d, want %d", got, len(blockData))
	}

	if !ringA.DropBlock(blockKey) {
		t.Fatalf("ringA DropBlock returned false")
	}
	if got := ringA.BlockCount(); got != 0 {
		t.Errorf("ringA BlockCount after drop = %d, want 0", got)
	}
	if got := ringB.BlockCount(); got != 1 {
		t.Errorf("ringB BlockCount after other-ring drop = %d, want 1", got)
	}
	if got := nbs.BlockCount(); got != 1 {
		t.Errorf("aggregate BlockCount after per-ring drop = %d, want 1", got)
	}
}

func TestNetworkBlockStore_LoadCountersRescansOnMissingKey(t *testing.T) {
	dir := t.TempDir()
	nbs, err := OpenNetworkBlockStore(dir, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	key, data := makeBlock(1024, 0xDE)
	if err := nbs.Put(ctx, key, data); err != nil {
		t.Fatalf("put: %v", err)
	}

	_ = nbs.ds.Delete(ctx, datastore.NewKey(nbKeyUsedBytes))
	_ = nbs.ds.Delete(ctx, datastore.NewKey(nbKeyBlockCount))
	if err := nbs.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	nbs2, err := OpenNetworkBlockStore(dir, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer nbs2.Close()

	if got := nbs2.BlockCount(); got != 1 {
		t.Errorf("BlockCount after rescan = %d, want 1", got)
	}
	if got := nbs2.UsedBytes(); got != int64(len(data)) {
		t.Errorf("UsedBytes after rescan = %d, want %d", got, len(data))
	}
}
