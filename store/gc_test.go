package store

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
)

func blockCount(t *testing.T, st *Store) int {
	t.Helper()
	ctx := context.Background()
	ch, err := st.LocalBlocks.AllKeysChan(ctx)
	if err != nil {
		t.Fatalf("AllKeysChan: %v", err)
	}
	n := 0
	for range ch {
		n++
	}
	return n
}

func blockPresent(t *testing.T, st *Store, c cid.Cid) bool {
	t.Helper()
	has, err := st.LocalBlocks.Has(context.Background(), c)
	if err != nil {
		t.Fatalf("Blocks.Has(%s): %v", c, err)
	}
	return has
}

func TestGC_RemovesOrphanedRawBlocks(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	dir := buildDir(t, map[string]string{"f.txt": "root content"})
	rootNode, err := IngestPath(ctx, dir, st.DAG)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Roots.Add(Root{Name: "kept", CID: rootNode.Cid()}); err != nil {
		t.Fatal(err)
	}

	orphanKey1, orphanData1 := makeBlock(64, 0xDE)
	orphanKey2, orphanData2 := makeBlock(64, 0xAD)
	if err := st.LocalBlocks.Put(ctx, orphanKey1, orphanData1); err != nil {
		t.Fatal(err)
	}
	if err := st.LocalBlocks.Put(ctx, orphanKey2, orphanData2); err != nil {
		t.Fatal(err)
	}

	if !blockPresent(t, st, orphanKey1) {
		t.Fatal("orphan block 1 not in store before GC")
	}
	if !blockPresent(t, st, orphanKey2) {
		t.Fatal("orphan block 2 not in store before GC")
	}

	result, err := st.GC(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}

	if result.Removed != 2 {
		t.Errorf("expected 2 removed, got %d", result.Removed)
	}

	if blockPresent(t, st, orphanKey1) {
		t.Error("orphan block 1 still present after GC")
	}
	if blockPresent(t, st, orphanKey2) {
		t.Error("orphan block 2 still present after GC")
	}

	if !blockPresent(t, st, rootNode.Cid()) {
		t.Error("root node was deleted by GC")
	}
}

func TestGC_PreservesAllRootBlocks(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	dir := buildDir(t, map[string]string{
		"a.txt":	"file A content",
		"b.txt":	"file B content",
		"c.txt":	"file C content",
	})
	rootNode, err := IngestPath(ctx, dir, st.DAG)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Roots.Add(Root{Name: "multi", CID: rootNode.Cid()}); err != nil {
		t.Fatal(err)
	}

	before := blockCount(t, st)
	if before == 0 {
		t.Fatal("no blocks in store before GC")
	}

	result, err := st.GC(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}

	if result.Removed != 0 {
		t.Errorf("expected 0 removed (all blocks reachable), got %d", result.Removed)
	}
	if result.Kept != before {
		t.Errorf("kept %d blocks, want %d", result.Kept, before)
	}

	after := blockCount(t, st)
	if after != before {
		t.Errorf("block count changed after GC with no orphans: %d → %d", before, after)
	}
}

func TestGC_IngestThenOrphanRoot(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	permanentDir := buildDir(t, map[string]string{"keep.txt": "keep me"})
	permanentNode, err := IngestPath(ctx, permanentDir, st.DAG)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Roots.Add(Root{Name: "permanent", CID: permanentNode.Cid()}); err != nil {
		t.Fatal(err)
	}

	orphanDir := buildDir(t, map[string]string{"orphan.txt": "delete me"})
	orphanNode, err := IngestPath(ctx, orphanDir, st.DAG)
	if err != nil {
		t.Fatal(err)
	}
	orphanRoot, err := st.Roots.Add(Root{Name: "orphan", CID: orphanNode.Cid()})
	if err != nil {
		t.Fatal(err)
	}

	if err := st.Roots.Remove(orphanRoot.ID); err != nil {
		t.Fatal(err)
	}

	result, err := st.GC(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}

	if result.Removed == 0 {
		t.Error("expected at least one orphaned block to be removed")
	}

	if blockPresent(t, st, orphanNode.Cid()) {
		t.Error("orphaned root node still present after GC")
	}

	if !blockPresent(t, st, permanentNode.Cid()) {
		t.Error("permanent root node was deleted by GC")
	}
}

func TestGC_SharedBlocksBetweenRoots(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	dir1 := buildDir(t, map[string]string{"shared.txt": "shared content"})
	dir2 := buildDir(t, map[string]string{
		"shared.txt":	"shared content",
		"extra.txt":	"extra content only here",
	})

	node1, err := IngestPath(ctx, dir1, st.DAG)
	if err != nil {
		t.Fatal(err)
	}
	node2, err := IngestPath(ctx, dir2, st.DAG)
	if err != nil {
		t.Fatal(err)
	}

	root1, err := st.Roots.Add(Root{Name: "r1", CID: node1.Cid()})
	if err != nil {
		t.Fatal(err)
	}
	root2, err := st.Roots.Add(Root{Name: "r2", CID: node2.Cid()})
	if err != nil {
		t.Fatal(err)
	}

	if err := st.Roots.Remove(root1.ID); err != nil {
		t.Fatal(err)
	}

	_, err = st.GC(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}

	if !blockPresent(t, st, root2.CID) {
		t.Error("root2 node was deleted by GC even though it is still registered")
	}

	if blockPresent(t, st, root1.CID) {
		t.Error("root1 node should have been deleted (it was unregistered and unique)")
	}
}

func TestGC_EmptyStore(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	result, err := st.GC(ctx)
	if err != nil {
		t.Fatalf("GC on empty store: %v", err)
	}
	if result.Removed != 0 {
		t.Errorf("expected 0 removed on empty store, got %d", result.Removed)
	}
	if result.Kept != 0 {
		t.Errorf("expected 0 kept on empty store, got %d", result.Kept)
	}
}

func TestGC_NoRootsDeletesEverything(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		key, data := makeBlock(32, byte(i))
		if err := st.LocalBlocks.Put(ctx, key, data); err != nil {
			t.Fatal(err)
		}
	}

	result, err := st.GC(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}

	if result.Removed != 5 {
		t.Errorf("expected 5 removed, got %d", result.Removed)
	}
	if result.Kept != 0 {
		t.Errorf("expected 0 kept, got %d", result.Kept)
	}

	if n := blockCount(t, st); n != 0 {
		t.Errorf("expected empty store after GC with no roots, got %d blocks", n)
	}
}

func TestGC_MultipleRuns(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	dir := buildDir(t, map[string]string{"f.txt": "idempotent"})
	rootNode, err := IngestPath(ctx, dir, st.DAG)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Roots.Add(Root{Name: "idem", CID: rootNode.Cid()}); err != nil {
		t.Fatal(err)
	}

	r1, err := st.GC(ctx)
	if err != nil {
		t.Fatalf("first GC: %v", err)
	}

	r2, err := st.GC(ctx)
	if err != nil {
		t.Fatalf("second GC: %v", err)
	}
	if r2.Removed != 0 {
		t.Errorf("second GC removed %d blocks; expected 0 (idempotent)", r2.Removed)
	}
	if r2.Kept != r1.Kept {
		t.Errorf("second GC kept %d; first kept %d (should be equal)", r2.Kept, r1.Kept)
	}
}

func TestGC_LargeFileOrphaned(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	largeContent := make([]byte, 512*1024)
	for i := range largeContent {
		largeContent[i] = byte(i)
	}
	dir := buildDir(t, map[string]string{"big.bin": string(largeContent)})
	rootNode, err := IngestPath(ctx, dir, st.DAG)
	if err != nil {
		t.Fatal(err)
	}
	added, err := st.Roots.Add(Root{Name: "large", CID: rootNode.Cid()})
	if err != nil {
		t.Fatal(err)
	}

	before := blockCount(t, st)
	if before <= 2 {
		t.Fatalf("expected multiple blocks for 512 KiB file, got %d", before)
	}

	if err := st.Roots.Remove(added.ID); err != nil {
		t.Fatal(err)
	}

	result, err := st.GC(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}

	if result.Removed != before {
		t.Errorf("expected all %d blocks removed, got %d", before, result.Removed)
	}

	if n := blockCount(t, st); n != 0 {
		t.Errorf("expected empty store after GC, got %d blocks", n)
	}
}

func TestGC_ResultIncludesElapsedTime(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		key, data := makeBlock(64, byte(i))
		if err := st.LocalBlocks.Put(ctx, key, data); err != nil {
			t.Fatal(err)
		}
	}

	result, err := st.GC(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}

	if result.Elapsed < 0 {
		t.Errorf("GCResult.Elapsed must be non-negative, got %v", result.Elapsed)
	}

	if result.Removed > 0 && result.Elapsed == time.Duration(0) {
		t.Errorf("GCResult.Elapsed is zero for a run that removed %d blocks — field may not be set", result.Removed)
	}
}
