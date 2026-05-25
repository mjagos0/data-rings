package store

import (
	"context"
	"testing"
)

func TestMarkRingKnown_ShortCircuitsAfterFirstCall(t *testing.T) {
	nbs := openTestNBS(t)
	ctx := context.Background()
	const ringID = "test-ring-presence"
	key := ringPresenceKey(ringID)

	nbs.MarkRingKnown(ringID)
	if has, _ := nbs.ds.Has(ctx, key); !has {
		t.Fatal("first MarkRingKnown should persist the ring-presence key")
	}

	if err := nbs.ds.Delete(ctx, key); err != nil {
		t.Fatalf("out-of-band delete: %v", err)
	}

	nbs.MarkRingKnown(ringID)
	if has, _ := nbs.ds.Has(ctx, key); has {
		t.Error("second MarkRingKnown re-persisted the key; cache failed to short-circuit")
	}
}

func TestForgetRing_InvalidatesCache(t *testing.T) {
	nbs := openTestNBS(t)
	ctx := context.Background()
	const ringID = "ephemeral-ring"
	key := ringPresenceKey(ringID)

	nbs.MarkRingKnown(ringID)
	nbs.ForgetRing(ringID)

	if has, _ := nbs.ds.Has(ctx, key); has {
		t.Fatal("ForgetRing should remove the persisted ring-presence key")
	}

	nbs.MarkRingKnown(ringID)
	if has, _ := nbs.ds.Has(ctx, key); !has {
		t.Error("MarkRingKnown after ForgetRing should re-persist; cache was not invalidated")
	}
}

func TestOpen_SeedsKnownRingsFromDisk(t *testing.T) {
	dir := t.TempDir()
	const ringID = "persisted-ring"
	keyDS := ringPresenceKey(ringID)

	nbs, err := OpenNetworkBlockStore(dir, 0)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	nbs.MarkRingKnown(ringID)
	if err := nbs.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	nbs2, err := OpenNetworkBlockStore(dir, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { nbs2.Close() })

	ctx := context.Background()
	if has, _ := nbs2.ds.Has(ctx, keyDS); !has {
		t.Fatal("reopen should preserve the ring-presence key on disk")
	}
	if err := nbs2.ds.Delete(ctx, keyDS); err != nil {
		t.Fatalf("out-of-band delete: %v", err)
	}
	nbs2.MarkRingKnown(ringID)
	if has, _ := nbs2.ds.Has(ctx, keyDS); has {
		t.Error("MarkRingKnown after reopen should short-circuit; cache was not seeded from disk")
	}

	if _, ok := nbs2.knownRings.Load(ringID); !ok {
		t.Error("knownRings.Load returned !ok after reopen; seeding failed")
	}
}
