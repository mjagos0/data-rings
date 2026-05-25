package store

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
)

func openTestNBS(t *testing.T) *NetworkBlockStore {
	t.Helper()
	nbs, err := OpenNetworkBlockStore(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("OpenNetworkBlockStore: %v", err)
	}
	t.Cleanup(func() { nbs.Close() })
	return nbs
}

func TestNetworkBlockStore_AddAndHasRoot(t *testing.T) {
	nbs := openTestNBS(t)
	key, _ := makeBlock(32, 0xAA)

	if nbs.HasRoot(key) {
		t.Fatal("empty registry should not report HasRoot")
	}
	if nbs.RootCount() != 0 {
		t.Fatalf("expected count 0, got %d", nbs.RootCount())
	}

	nbs.AddRoot(key)
	if !nbs.HasRoot(key) {
		t.Error("HasRoot should return true after AddRoot")
	}
	if nbs.RootCount() != 1 {
		t.Errorf("expected count 1, got %d", nbs.RootCount())
	}

	nbs.AddRoot(key)
	if nbs.RootCount() != 1 {
		t.Errorf("AddRoot should be idempotent, count=%d", nbs.RootCount())
	}
}

func TestNetworkBlockStore_RemoveRoot(t *testing.T) {
	nbs := openTestNBS(t)
	key, _ := makeBlock(32, 0xBB)

	nbs.AddRoot(key)
	nbs.RemoveRoot(key)
	if nbs.HasRoot(key) {
		t.Error("HasRoot should return false after RemoveRoot")
	}
	if nbs.RootCount() != 0 {
		t.Errorf("expected count 0 after remove, got %d", nbs.RootCount())
	}

	nbs.RemoveRoot(key)
}

func TestNetworkBlockStore_AddAllAndList(t *testing.T) {
	nbs := openTestNBS(t)
	key1, _ := makeBlock(32, 0x01)
	key2, _ := makeBlock(32, 0x02)

	nbs.AddAllRoots([]string{key1.String(), key2.String()})
	if nbs.RootCount() != 2 {
		t.Errorf("expected count 2, got %d", nbs.RootCount())
	}

	listed := nbs.ListRoots()
	if len(listed) != 2 {
		t.Errorf("expected 2 listed, got %d", len(listed))
	}
}

func TestNetworkBlockStore_AddStrRemoveStr(t *testing.T) {
	nbs := openTestNBS(t)
	key, _ := makeBlock(32, 0xCC)
	cidStr := key.String()

	nbs.AddRootStr(cidStr)
	if !nbs.HasRoot(key) {
		t.Error("AddRootStr should make HasRoot return true")
	}

	nbs.RemoveRootStr(cidStr)
	if nbs.HasRoot(key) {
		t.Error("RemoveRootStr should make HasRoot return false")
	}
}

func TestNetworkGC_KeepsRegisteredRootBlocks(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	nbs := st.NetworkBlocks

	dir := buildDir(t, map[string]string{"ring.txt": "ring content"})
	rootNode, err := IngestPath(ctx, dir, st.DAG)
	if err != nil {
		t.Fatal(err)
	}

	copyDAGToNetworkBlocks(t, st, rootNode.Cid())
	nbs.AddRoot(rootNode.Cid())

	before := nbsBlockCount(t, nbs)
	if before == 0 {
		t.Fatal("no blocks in network store before GC")
	}

	result, err := nbs.GC(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}

	if result.Removed != 0 {
		t.Errorf("expected 0 removed (blocks reachable from network root), got %d", result.Removed)
	}
	if result.Kept != before {
		t.Errorf("expected %d kept, got %d", before, result.Kept)
	}
}

func TestNetworkGC_DeletesUnregisteredBlocks(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	nbs := st.NetworkBlocks

	dir := buildDir(t, map[string]string{"orphan-ring.txt": "will be deleted"})
	rootNode, err := IngestPath(ctx, dir, st.DAG)
	if err != nil {
		t.Fatal(err)
	}
	copyDAGToNetworkBlocks(t, st, rootNode.Cid())

	before := nbsBlockCount(t, nbs)
	if before == 0 {
		t.Fatal("no blocks in network store before GC")
	}

	result, err := nbs.GC(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}

	if result.Removed != before {
		t.Errorf("expected all %d blocks removed (not in network roots), got %d removed, %d kept", before, result.Removed, result.Kept)
	}
}

func TestNetworkGC_DeleteAfterRemove(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	nbs := st.NetworkBlocks

	dir := buildDir(t, map[string]string{"deletable.txt": "I will be deleted"})
	rootNode, err := IngestPath(ctx, dir, st.DAG)
	if err != nil {
		t.Fatal(err)
	}
	copyDAGToNetworkBlocks(t, st, rootNode.Cid())
	nbs.AddRoot(rootNode.Cid())

	r1, err := nbs.GC(ctx)
	if err != nil {
		t.Fatalf("first GC: %v", err)
	}
	if r1.Removed != 0 {
		t.Fatalf("first GC removed %d blocks, expected 0", r1.Removed)
	}

	nbs.RemoveRoot(rootNode.Cid())

	r2, err := nbs.GC(ctx)
	if err != nil {
		t.Fatalf("second GC: %v", err)
	}
	if r2.Removed == 0 {
		t.Error("second GC should have removed blocks after network root removal")
	}
	has, _ := nbs.Has(ctx, rootNode.Cid())
	if has {
		t.Error("root block should be gone after network root removal + GC")
	}
}

func TestNetworkGC_EmptyRootsSweepsAll(t *testing.T) {
	nbs := openTestNBS(t)
	ctx := context.Background()

	key1, data1 := makeBlock(64, 0xDE)
	if err := nbs.Put(ctx, key1, data1); err != nil {
		t.Fatal(err)
	}

	result, err := nbs.GC(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}

	if result.Removed != 1 {
		t.Errorf("expected 1 removed (orphaned block), got %d", result.Removed)
	}
	if result.Kept != 0 {
		t.Errorf("expected 0 kept, got %d", result.Kept)
	}
}

func TestNetworkBlockStore_TTL_AddWithTTL(t *testing.T) {
	nbs := openTestNBS(t)
	key, _ := makeBlock(32, 0xDD)

	nbs.AddRootWithTTL(key, 1*time.Hour)
	if !nbs.HasRoot(key) {
		t.Error("HasRoot should return true for non-expired TTL root")
	}
	if nbs.RootCount() != 1 {
		t.Errorf("expected count 1, got %d", nbs.RootCount())
	}
}

func TestNetworkBlockStore_TTL_Expired(t *testing.T) {
	nbs := openTestNBS(t)
	key, _ := makeBlock(32, 0xEE)

	nbs.AddRootWithExpiry(key.String(), time.Now().Add(-1*time.Second).UnixNano())

	if nbs.HasRoot(key) {
		t.Error("HasRoot should return false for expired root")
	}
	if nbs.RootCount() != 0 {
		t.Errorf("RootCount should not include expired roots, got %d", nbs.RootCount())
	}
	if len(nbs.ListRoots()) != 0 {
		t.Errorf("ListRoots should not include expired roots, got %d", len(nbs.ListRoots()))
	}
	if len(nbs.ListRootEntries()) != 0 {
		t.Errorf("ListRootEntries should not include expired roots, got %d", len(nbs.ListRootEntries()))
	}
}

func TestNetworkBlockStore_TTL_PruneExpired(t *testing.T) {
	nbs := openTestNBS(t)
	key1, _ := makeBlock(32, 0x01)
	key2, _ := makeBlock(32, 0x02)
	key3, _ := makeBlock(32, 0x03)

	nbs.AddRootWithExpiry(key1.String(), time.Now().Add(-1*time.Second).UnixNano())

	nbs.AddRootWithTTL(key2, 1*time.Hour)

	nbs.AddRoot(key3)

	pruned := nbs.PruneExpiredRoots()
	if pruned != 1 {
		t.Errorf("expected 1 pruned, got %d", pruned)
	}
	if nbs.HasRootStr(key1.String()) {
		t.Error("expired root should be pruned")
	}
	if !nbs.HasRoot(key2) {
		t.Error("non-expired root should survive pruning")
	}
	if !nbs.HasRoot(key3) {
		t.Error("no-TTL root should survive pruning")
	}
}

func TestNetworkBlockStore_TTL_AddAllEntries(t *testing.T) {
	nbs := openTestNBS(t)
	key1, _ := makeBlock(32, 0x10)
	key2, _ := makeBlock(32, 0x20)

	entries := []NetworkRootEntry{
		{CID: key1.String(), ExpiresAt: time.Now().Add(1 * time.Hour).UnixNano()},
		{CID: key2.String(), ExpiresAt: 0},
	}
	nbs.AddAllRootEntries(entries)

	if !nbs.HasRoot(key1) {
		t.Error("key1 should be present")
	}
	if !nbs.HasRoot(key2) {
		t.Error("key2 should be present")
	}
	if nbs.RootCount() != 2 {
		t.Errorf("expected count 2, got %d", nbs.RootCount())
	}

	listed := nbs.ListRootEntries()
	foundWithTTL := false
	for _, e := range listed {
		if e.CID == key1.String() && e.ExpiresAt > 0 {
			foundWithTTL = true
		}
	}
	if !foundWithTTL {
		t.Error("ListRootEntries should preserve TTL info for key1")
	}
}

func TestNetworkGC_TTL_ExpiredRootBlocksDeleted(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	nbs := st.NetworkBlocks

	dir := buildDir(t, map[string]string{"ttl-expired.txt": "will expire"})
	rootNode, err := IngestPath(ctx, dir, st.DAG)
	if err != nil {
		t.Fatal(err)
	}
	copyDAGToNetworkBlocks(t, st, rootNode.Cid())
	nbs.AddRootWithExpiry(rootNode.Cid().String(), time.Now().Add(-1*time.Second).UnixNano())

	before := nbsBlockCount(t, nbs)
	if before == 0 {
		t.Fatal("no blocks in store before GC")
	}

	result, err := nbs.GC(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}

	if result.Removed != before {
		t.Errorf("expected all %d blocks removed (TTL expired), got %d removed, %d kept",
			before, result.Removed, result.Kept)
	}
	has, _ := nbs.Has(ctx, rootNode.Cid())
	if has {
		t.Error("root block still present after GC — TTL should have expired it")
	}
}

func TestNetworkGC_TTL_NonExpiredPreserved(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	nbs := st.NetworkBlocks

	dir := buildDir(t, map[string]string{"ttl-alive.txt": "still alive"})
	rootNode, err := IngestPath(ctx, dir, st.DAG)
	if err != nil {
		t.Fatal(err)
	}
	copyDAGToNetworkBlocks(t, st, rootNode.Cid())
	nbs.AddRootWithTTL(rootNode.Cid(), 1*time.Hour)

	before := nbsBlockCount(t, nbs)
	result, err := nbs.GC(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}

	if result.Removed != 0 {
		t.Errorf("expected 0 removed (TTL not expired), got %d", result.Removed)
	}
	if result.Kept != before {
		t.Errorf("expected %d kept, got %d", before, result.Kept)
	}
}

func TestNetworkGC_TTL_MixedExpiry(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	nbs := st.NetworkBlocks

	dirA := buildDir(t, map[string]string{"expired.txt": "expire me"})
	nodeA, err := IngestPath(ctx, dirA, st.DAG)
	if err != nil {
		t.Fatal(err)
	}
	copyDAGToNetworkBlocks(t, st, nodeA.Cid())
	nbs.AddRootWithExpiry(nodeA.Cid().String(), time.Now().Add(-1*time.Second).UnixNano())

	dirB := buildDir(t, map[string]string{"alive.txt": "keep me alive"})
	nodeB, err := IngestPath(ctx, dirB, st.DAG)
	if err != nil {
		t.Fatal(err)
	}
	copyDAGToNetworkBlocks(t, st, nodeB.Cid())
	nbs.AddRootWithTTL(nodeB.Cid(), 1*time.Hour)

	result, err := nbs.GC(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}

	hasA, _ := nbs.Has(ctx, nodeA.Cid())
	if hasA {
		t.Error("expired root A's blocks still present after GC")
	}
	hasB, _ := nbs.Has(ctx, nodeB.Cid())
	if !hasB {
		t.Error("live root B's blocks deleted by GC — should be preserved")
	}
	if result.Removed == 0 {
		t.Error("expected some blocks removed (expired root A)")
	}
}

func TestNetworkBlockStore_ReverseIndex(t *testing.T) {
	nbs := openTestNBS(t)
	ctx := context.Background()

	key, data := makeBlock(64, 0xAA)
	rootCID, _ := makeBlock(32, 0xBB)

	nbs.AddRoot(rootCID)
	if err := nbs.PutWithRoot(ctx, key, data, rootCID, 0); err != nil {
		t.Fatal(err)
	}

	roots := nbs.RootsForBlock(key)
	if len(roots) != 1 || roots[0] != rootCID.String() {
		t.Errorf("RootsForBlock: expected [%s], got %v", rootCID, roots)
	}
}

func TestNetworkBlockStore_ReverseIndex_MultipleRoots(t *testing.T) {
	nbs := openTestNBS(t)
	ctx := context.Background()

	key, data := makeBlock(64, 0xAA)
	root1, _ := makeBlock(32, 0xBB)
	root2, _ := makeBlock(32, 0xCC)

	if err := nbs.PutWithRoot(ctx, key, data, root1, 0); err != nil {
		t.Fatal(err)
	}
	nbs.AddBlockRootIndex(key, root2.String())

	roots := nbs.RootsForBlock(key)
	if len(roots) != 2 {
		t.Errorf("expected 2 roots, got %d", len(roots))
	}
}

func nbsBlockCount(t *testing.T, nbs *NetworkBlockStore) int {
	t.Helper()
	ch, err := nbs.AllKeysChan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for range ch {
		n++
	}
	return n
}

func copyDAGToNetworkBlocks(t *testing.T, st *Store, root cid.Cid) {
	t.Helper()
	ctx := context.Background()
	copyDAGNode(t, ctx, st, root, root, make(map[string]bool))
}

func copyDAGNode(t *testing.T, ctx context.Context, st *Store, root cid.Cid, c cid.Cid, visited map[string]bool) {
	key := c.String()
	if visited[key] {
		return
	}
	visited[key] = true

	node, err := st.DAG.Get(ctx, c)
	if err != nil {
		return
	}
	data := node.RawData()
	if err := st.NetworkBlocks.Put(ctx, c, data); err != nil {
		t.Fatalf("copy to network blocks: %v", err)
	}

	st.NetworkBlocks.AddBlockRootIndex(c, root.String())
	for _, link := range node.Links() {
		copyDAGNode(t, ctx, st, root, link.Cid, visited)
	}
}
