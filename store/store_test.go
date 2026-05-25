package store

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	ufsio "github.com/ipfs/boxo/ipld/unixfs/io"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Logf("Close: %v", err)
		}
	})
	return st
}

func writeFile(t *testing.T, dir, relPath, content string) {
	t.Helper()
	full := filepath.Join(dir, filepath.FromSlash(relPath))
	if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
		t.Fatalf("writeFile MkdirAll: %v", err)
	}
	if err := os.WriteFile(full, []byte(content), 0644); err != nil {
		t.Fatalf("writeFile Write: %v", err)
	}
}

func buildDir(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for path, content := range files {
		writeFile(t, dir, path, content)
	}
	return dir
}

func TestStore_Open(t *testing.T) {
	st := openTestStore(t)
	if st.LocalBlocks == nil {
		t.Error("Store.Blocks is nil")
	}
	if st.DAG == nil {
		t.Error("Store.DAG is nil")
	}
	if st.Roots == nil {
		t.Error("Store.Roots is nil")
	}
}

func TestStore_Close_IsIdempotentForCaller(t *testing.T) {

	st, err := Open(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
}

func TestBlockStore_PutAndGet(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	key, data := makeBlock(128, 0xAB)
	if err := st.LocalBlocks.Put(ctx, key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := st.LocalBlocks.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("Get returned different data than Put")
	}
}

func TestBlockStore_GetMissing(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	key := makeCID([]byte("not stored"))
	_, err := st.LocalBlocks.Get(ctx, key)
	if err == nil {
		t.Fatal("expected error for missing block")
	}
}

func TestBlockStore_Has_TrueForExisting(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	key, data := makeBlock(64, 0x01)
	if err := st.LocalBlocks.Put(ctx, key, data); err != nil {
		t.Fatal(err)
	}
	has, err := st.LocalBlocks.Has(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Error("Has returned false for a stored block")
	}
}

func TestBlockStore_Has_FalseForMissing(t *testing.T) {
	st := openTestStore(t)
	key := makeCID([]byte("absent"))
	has, err := st.LocalBlocks.Has(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if has {
		t.Error("Has returned true for a block that was never stored")
	}
}

func TestBlockStore_Delete(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	key, data := makeBlock(64, 0x02)
	if err := st.LocalBlocks.Put(ctx, key, data); err != nil {
		t.Fatal(err)
	}
	if err := st.LocalBlocks.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	has, err := st.LocalBlocks.Has(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if has {
		t.Error("block still present after Delete")
	}
}

func TestBlockStore_AllKeysChan(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	want := make(map[string]bool)
	for i := 0; i < 5; i++ {
		key, data := makeBlock(32, byte(i))
		if err := st.LocalBlocks.Put(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		want[key.String()] = true
	}

	ch, err := st.LocalBlocks.AllKeysChan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for k := range ch {
		delete(want, k.String())
	}
	if len(want) != 0 {
		t.Errorf("AllKeysChan missed %d keys", len(want))
	}
}

func TestBlockStore_PutReturnsCopy(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	key, data := makeBlock(64, 0x03)
	if err := st.LocalBlocks.Put(ctx, key, data); err != nil {
		t.Fatal(err)
	}

	data[0] ^= 0xFF

	got, err := st.LocalBlocks.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] == data[0] {
		t.Error("Put aliased the caller's slice — mutations visible in Get")
	}
}

func TestDAGService_AddAndGet(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	dir := buildDir(t, map[string]string{"f.txt": "dag test"})
	rootNode, err := IngestPath(ctx, dir, st.DAG)
	if err != nil {
		t.Fatalf("IngestPath: %v", err)
	}

	retrieved, err := st.DAG.Get(ctx, rootNode.Cid())
	if err != nil {
		t.Fatalf("DAG.Get: %v", err)
	}
	if retrieved.Cid() != rootNode.Cid() {
		t.Error("retrieved node has wrong CID")
	}
}

func TestDAGService_GetMissing(t *testing.T) {
	st := openTestStore(t)
	absent := makeCID([]byte("not in dag"))
	_, err := st.DAG.Get(context.Background(), absent)
	if err == nil {
		t.Fatal("expected error for missing DAG node")
	}
}

func TestDAGService_LinksTraversable(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	dir := buildDir(t, map[string]string{
		"a.txt":	"first",
		"b.txt":	"second",
	})
	rootNode, err := IngestPath(ctx, dir, st.DAG)
	if err != nil {
		t.Fatal(err)
	}

	for _, link := range rootNode.Links() {
		child, err := st.DAG.Get(ctx, link.Cid)
		if err != nil {
			t.Errorf("link %q: DAG.Get: %v", link.Name, err)
		}
		if child == nil {
			t.Errorf("link %q: got nil node", link.Name)
		}
	}
}

func TestRootRegistry_AddAndList(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	dir := buildDir(t, map[string]string{"f.txt": "data"})
	rootNode, err := IngestPath(ctx, dir, st.DAG)
	if err != nil {
		t.Fatal(err)
	}

	root := Root{Name: "myroot", CID: rootNode.Cid(), Path: dir}
	added, err := st.Roots.Add(root)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if added.ID == "" {
		t.Error("Add returned root without ID")
	}
	if added.CID != rootNode.Cid() {
		t.Error("Add returned wrong CID")
	}

	list := st.Roots.List()
	if len(list) != 1 {
		t.Fatalf("expected 1 root, got %d", len(list))
	}
	if list[0].ID != added.ID {
		t.Errorf("List returned wrong ID: want %q, got %q", added.ID, list[0].ID)
	}
}

func TestRootRegistry_AddDuplicate_ReturnsErrAlreadyTracked(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	dir := buildDir(t, map[string]string{"f.txt": "data"})
	rootNode, err := IngestPath(ctx, dir, st.DAG)
	if err != nil {
		t.Fatal(err)
	}

	root := Root{Name: "first", CID: rootNode.Cid(), Path: dir}
	first, err := st.Roots.Add(root)
	if err != nil {
		t.Fatalf("first Add: %v", err)
	}

	root2 := Root{Name: "second", CID: rootNode.Cid(), Path: dir}
	existing, err := st.Roots.Add(root2)
	if !isAlreadyTracked(err) {
		t.Fatalf("expected ErrAlreadyTracked, got %v", err)
	}
	if existing.ID != first.ID {
		t.Errorf("duplicate Add returned wrong existing ID: want %q, got %q", first.ID, existing.ID)
	}
}

func TestRootRegistry_Remove(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	dir := buildDir(t, map[string]string{"f.txt": "data"})
	rootNode, err := IngestPath(ctx, dir, st.DAG)
	if err != nil {
		t.Fatal(err)
	}

	added, _ := st.Roots.Add(Root{Name: "r", CID: rootNode.Cid(), Path: dir})

	if err := st.Roots.Remove(added.ID); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	list := st.Roots.List()
	for _, r := range list {
		if r.ID == added.ID {
			t.Error("removed root still appears in List")
		}
	}
}

func TestRootRegistry_RemoveNonExistent(t *testing.T) {
	st := openTestStore(t)
	if err := st.Roots.Remove("no-such-uuid"); err == nil {
		t.Fatal("expected error removing nonexistent root")
	}
}

func TestRootRegistry_Rename(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	dir := buildDir(t, map[string]string{"f.txt": "data"})
	rootNode, err := IngestPath(ctx, dir, st.DAG)
	if err != nil {
		t.Fatal(err)
	}

	added, _ := st.Roots.Add(Root{Name: "original", CID: rootNode.Cid(), Path: dir})

	if err := st.Roots.Rename(added.ID, "renamed"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	fetched, ok, err := st.Roots.GetByID(added.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("root not found after rename")
	}
	if fetched.Name != "renamed" {
		t.Errorf("expected name %q, got %q", "renamed", fetched.Name)
	}

	byOld, err := st.Roots.GetByName("original")
	if err != nil {
		t.Fatal(err)
	}
	if len(byOld) != 0 {
		t.Error("old name index entry persisted after rename")
	}
	byNew, err := st.Roots.GetByName("renamed")
	if err != nil {
		t.Fatal(err)
	}
	if len(byNew) != 1 || byNew[0].ID != added.ID {
		t.Error("new name index not found after rename")
	}
}

func TestRootRegistry_GetByID(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	dir := buildDir(t, map[string]string{"f.txt": "data"})
	rootNode, err := IngestPath(ctx, dir, st.DAG)
	if err != nil {
		t.Fatal(err)
	}
	added, _ := st.Roots.Add(Root{Name: "lookup-test", CID: rootNode.Cid(), Path: dir})

	got, ok, err := st.Roots.GetByID(added.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("GetByID returned not-found for existing ID")
	}
	if got.CID != added.CID {
		t.Errorf("GetByID returned wrong CID")
	}

	_, ok, err = st.Roots.GetByID("nonexistent-uuid")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("GetByID returned true for nonexistent ID")
	}
}

func TestRootRegistry_GetByCID(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	dir := buildDir(t, map[string]string{"f.txt": "data"})
	rootNode, err := IngestPath(ctx, dir, st.DAG)
	if err != nil {
		t.Fatal(err)
	}
	added, _ := st.Roots.Add(Root{Name: "cid-lookup", CID: rootNode.Cid(), Path: dir})

	results, err := st.Roots.GetByCID(rootNode.Cid())
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].ID != added.ID {
		t.Error("GetByCID returned wrong record")
	}

	absent := makeCID([]byte("absent"))
	results2, err := st.Roots.GetByCID(absent)
	if err != nil {
		t.Fatal(err)
	}
	if len(results2) != 0 {
		t.Error("GetByCID for absent CID returned non-empty slice")
	}
}

func TestRootRegistry_GetByName(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	dirs := []string{
		buildDir(t, map[string]string{"a.txt": "aaa"}),
		buildDir(t, map[string]string{"b.txt": "bbb"}),
	}
	var ids []string
	for i, dir := range dirs {
		rootNode, err := IngestPath(ctx, dir, st.DAG)
		if err != nil {
			t.Fatal(err)
		}
		added, _ := st.Roots.Add(Root{Name: "shared-name", CID: rootNode.Cid(), Path: dir})
		_ = i
		ids = append(ids, added.ID)
	}

	results, err := st.Roots.GetByName("shared-name")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 roots for name %q, got %d", "shared-name", len(results))
	}
}

func TestRootRegistry_PersistsAcrossReopens(t *testing.T) {
	dir := t.TempDir()

	st, err := Open(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	fsDir := buildDir(t, map[string]string{"f.txt": "persist test"})
	rootNode, err := IngestPath(ctx, fsDir, st.DAG)
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	added, err := st.Roots.Add(Root{Name: "persistent", CID: rootNode.Cid(), Path: fsDir})
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := Open(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()

	list := st2.Roots.List()
	found := false
	for _, r := range list {
		if r.ID == added.ID {
			found = true
			if r.CID != added.CID {
				t.Error("CID changed after reopen")
			}
			if r.Name != "persistent" {
				t.Errorf("Name changed after reopen: got %q", r.Name)
			}
		}
	}
	if !found {
		t.Error("root not found after store reopen")
	}
}

func TestIngestionPipeline_BlocksStoredForSingleFile(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	dir := buildDir(t, map[string]string{"data.txt": "block storage test"})
	rootNode, err := IngestPath(ctx, dir, st.DAG)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := st.DAG.Get(ctx, rootNode.Cid()); err != nil {
		t.Fatalf("root node not found in DAG: %v", err)
	}

	for _, link := range rootNode.Links() {
		if _, err := st.DAG.Get(ctx, link.Cid); err != nil {
			t.Errorf("child node %q (%s) not found in DAG: %v", link.Name, link.Cid, err)
		}
	}
}

func TestIngestionPipeline_CorrectDAGStructureForDirectory(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	dir := buildDir(t, map[string]string{
		"readme.txt":		"hello",
		"src/main.go":		"package main",
		"src/lib/util.go":	"package lib",
	})
	rootNode, err := IngestPath(ctx, dir, st.DAG)
	if err != nil {
		t.Fatal(err)
	}

	ufsRoot, err := ufsio.NewDirectoryFromNode(st.DAG, rootNode)
	if err != nil {
		t.Fatalf("root is not a UnixFS directory: %v", err)
	}

	readmeNode, err := ufsRoot.Find(ctx, "readme.txt")
	if err != nil {
		t.Fatalf("readme.txt not found: %v", err)
	}
	dr, err := ufsio.NewDagReader(ctx, readmeNode, st.DAG)
	if err != nil {
		t.Fatalf("NewDagReader for readme.txt: %v", err)
	}
	defer dr.Close()
	buf := make([]byte, 5)
	n, _ := dr.Read(buf)
	if string(buf[:n]) != "hello" {
		t.Errorf("readme.txt content: want %q, got %q", "hello", buf[:n])
	}

	srcNode, err := ufsRoot.Find(ctx, "src")
	if err != nil {
		t.Fatalf("src/ not found: %v", err)
	}
	ufsrSrc, err := ufsio.NewDirectoryFromNode(st.DAG, srcNode)
	if err != nil {
		t.Fatalf("src is not a UnixFS directory: %v", err)
	}

	libNode, err := ufsrSrc.Find(ctx, "lib")
	if err != nil {
		t.Fatalf("src/lib/ not found: %v", err)
	}
	if _, err := ufsio.NewDirectoryFromNode(st.DAG, libNode); err != nil {
		t.Fatalf("src/lib is not a UnixFS directory: %v", err)
	}
}

func TestIngestionPipeline_LargeFileMultiBlock(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	large := bytes.Repeat([]byte{0xCA, 0xFE}, 256*1024)
	dir := buildDir(t, map[string]string{"big.bin": string(large)})

	rootNode, err := IngestPath(ctx, dir, st.DAG)
	if err != nil {
		t.Fatal(err)
	}

	ufsRoot, err := ufsio.NewDirectoryFromNode(st.DAG, rootNode)
	if err != nil {
		t.Fatal(err)
	}
	fileNode, err := ufsRoot.Find(ctx, "big.bin")
	if err != nil {
		t.Fatal(err)
	}

	links := fileNode.Links()
	if len(links) <= 1 {
		t.Errorf("expected multiple blocks for 512 KiB file, got %d links", len(links))
	}

	dr, err := ufsio.NewDagReader(ctx, fileNode, st.DAG)
	if err != nil {
		t.Fatal(err)
	}
	defer dr.Close()
	if uint64(len(large)) != dr.Size() {
		t.Errorf("DagReader.Size: want %d, got %d", len(large), dr.Size())
	}
}

func TestIngestionPipeline_ContentAddressedIsDeterministic(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	spec := map[string]string{"f.txt": "deterministic content"}
	dir1 := buildDir(t, spec)
	dir2 := buildDir(t, spec)

	node1, err := IngestPath(ctx, dir1, st.DAG)
	if err != nil {
		t.Fatal(err)
	}
	node2, err := IngestPath(ctx, dir2, st.DAG)
	if err != nil {
		t.Fatal(err)
	}
	if node1.Cid() != node2.Cid() {
		t.Errorf("same content produced different CIDs: %s vs %s", node1.Cid(), node2.Cid())
	}
}

func TestIngestionPipeline_FullRoundtrip(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	files := map[string]string{
		"doc.txt":	"round-trip document",
		"sub/data.bin":	"\x00\x01\x02\x03\x04",
		"sub/note.txt":	"sub note",
	}
	dir := buildDir(t, files)

	rootNode, err := IngestPath(ctx, dir, st.DAG)
	if err != nil {
		t.Fatalf("IngestPath: %v", err)
	}

	added, err := st.Roots.Add(Root{Name: "roundtrip", CID: rootNode.Cid(), Path: dir})
	if err != nil {
		t.Fatalf("Roots.Add: %v", err)
	}
	if added.CID != rootNode.Cid() {
		t.Error("registered CID does not match ingested node")
	}

	ufsRoot, err := ufsio.NewDirectoryFromNode(st.DAG, rootNode)
	if err != nil {
		t.Fatal(err)
	}
	verifyFile := func(dir ufsio.Directory, name, want string) {
		t.Helper()
		node, err := dir.Find(ctx, name)
		if err != nil {
			t.Errorf("Find(%q): %v", name, err)
			return
		}
		dr, err := ufsio.NewDagReader(ctx, node, st.DAG)
		if err != nil {
			t.Errorf("DagReader(%q): %v", name, err)
			return
		}
		defer dr.Close()
		buf := make([]byte, int(dr.Size()))
		dr.Read(buf)
		if string(buf) != want {
			t.Errorf("%q: want %q, got %q", name, want, buf)
		}
	}

	verifyFile(ufsRoot, "doc.txt", "round-trip document")

	subNode, err := ufsRoot.Find(ctx, "sub")
	if err != nil {
		t.Fatal(err)
	}
	subDir, err := ufsio.NewDirectoryFromNode(st.DAG, subNode)
	if err != nil {
		t.Fatal(err)
	}
	verifyFile(subDir, "data.bin", "\x00\x01\x02\x03\x04")
	verifyFile(subDir, "note.txt", "sub note")
}

func TestIngestionPipeline_EmptyDirectory(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	emptyDir := t.TempDir()
	rootNode, err := IngestPath(ctx, emptyDir, st.DAG)
	if err != nil {
		t.Fatalf("IngestPath on empty dir: %v", err)
	}

	ufsDir, err := ufsio.NewDirectoryFromNode(st.DAG, rootNode)
	if err != nil {
		t.Fatalf("root is not a directory: %v", err)
	}
	links, err := ufsDir.Links(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(links) != 0 {
		t.Errorf("empty directory: expected 0 links, got %d", len(links))
	}
}

func TestIngestionPipeline_RegisterIngestRoundtrip_MultipleRoots(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	dirs := []struct {
		files	map[string]string
		name	string
	}{
		{map[string]string{"a.txt": "aaa"}, "root-a"},
		{map[string]string{"b.txt": "bbb"}, "root-b"},
		{map[string]string{"c.txt": "ccc"}, "root-c"},
	}

	added := make([]Root, 0, len(dirs))
	for _, d := range dirs {
		dir := buildDir(t, d.files)
		rootNode, err := IngestPath(ctx, dir, st.DAG)
		if err != nil {
			t.Fatalf("%s: IngestPath: %v", d.name, err)
		}
		r, err := st.Roots.Add(Root{Name: d.name, CID: rootNode.Cid(), Path: dir})
		if err != nil {
			t.Fatalf("%s: Roots.Add: %v", d.name, err)
		}
		added = append(added, r)
	}

	list := st.Roots.List()
	if len(list) != len(dirs) {
		t.Errorf("expected %d roots, got %d", len(dirs), len(list))
	}

	for _, r := range added {
		if _, err := st.DAG.Get(ctx, r.CID); err != nil {
			t.Errorf("root %q: DAG.Get: %v", r.Name, err)
		}
	}
}

func isAlreadyTracked(err error) bool {
	return err != nil && err.Error() == ErrAlreadyTracked.Error()
}
