package fuse

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	blockservice "github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	ufsio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	ipld "github.com/ipfs/go-ipld-format"
	mh "github.com/multiformats/go-multihash"
	"github.com/mjagos0/datarings/store"
)

type fsSpec map[string]string

type staticRootLister struct {
	mu	sync.RWMutex
	roots	[]store.Root
}

func (s *staticRootLister) List() []store.Root {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]store.Root, len(s.roots))
	copy(out, s.roots)
	return out
}

func (s *staticRootLister) add(r store.Root) {
	s.mu.Lock()
	s.roots = append(s.roots, r)
	s.mu.Unlock()
}

func newTestDAGService(t *testing.T) ipld.DAGService {
	t.Helper()
	ds := datastore.NewMapDatastore()
	bs := blockstore.NewBlockstore(ds)
	bsvc := blockservice.New(bs, nil)
	return merkledag.NewDAGService(bsvc)
}

func ingestSpec(t *testing.T, dagSvc ipld.DAGService, spec fsSpec) ipld.Node {
	t.Helper()
	dir := t.TempDir()
	for path, content := range spec {
		full := filepath.Join(dir, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
			t.Fatalf("ingestSpec mkdir: %v", err)
		}
		if err := os.WriteFile(full, []byte(content), 0644); err != nil {
			t.Fatalf("ingestSpec write: %v", err)
		}
	}
	rootNode, err := store.IngestPath(context.Background(), dir, dagSvc)
	if err != nil {
		t.Fatalf("IngestPath: %v", err)
	}
	return rootNode
}

func findFileNode(t *testing.T, dagSvc ipld.DAGService, dirNode ipld.Node, name string) ipld.Node {
	t.Helper()
	dir, err := ufsio.NewDirectoryFromNode(dagSvc, dirNode)
	if err != nil {
		t.Fatalf("findFileNode: NewDirectoryFromNode: %v", err)
	}
	node, err := dir.Find(context.Background(), name)
	if err != nil {
		t.Fatalf("findFileNode: Find(%q): %v", name, err)
	}
	return node
}

func makeDummyCID(seed []byte) cid.Cid {
	h := sha256.Sum256(seed)
	hash, _ := mh.Encode(h[:], mh.SHA2_256)
	return cid.NewCidV1(cid.Raw, hash)
}

func fuseAvailable() bool {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		return false
	}
	_, err := exec.LookPath("fusermount3")
	return err == nil
}

func mountTest(t *testing.T, lister RootLister, dagSvc ipld.DAGService) string {
	t.Helper()
	if !fuseAvailable() {
		t.Skip("FUSE not available on this host (missing /dev/fuse or fusermount3)")
	}
	mountpoint := t.TempDir()
	mp, err := Mount(MountOptions{Mountpoint: mountpoint}, lister, dagSvc)
	if err != nil {
		t.Fatalf("Mount: %v", err)
	}
	t.Cleanup(func() {
		if err := mp.Unmount(); err != nil {
			t.Logf("Unmount: %v (may be benign)", err)
		}
	})
	return mountpoint
}

func readFS(t *testing.T, root string) fsSpec {
	t.Helper()
	spec := make(fsSpec)
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		if rel == "." {
			return nil
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			spec[rel+"/"] = ""
		} else {
			data, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			spec[rel] = string(data)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("readFS: %v", err)
	}
	return spec
}

func assertFilesMatch(t *testing.T, want, got fsSpec) {
	t.Helper()
	for path, content := range want {
		if strings.HasSuffix(path, "/") {
			continue
		}
		gotContent, ok := got[path]
		if !ok {
			t.Errorf("mounted FS missing %q", path)
			continue
		}
		if gotContent != content {
			t.Errorf("%q: want %q, got %q", path, content, gotContent)
		}
	}
	for path := range got {
		if strings.HasSuffix(path, "/") {
			continue
		}
		if _, ok := want[path]; !ok {
			t.Errorf("unexpected file in mounted FS: %q", path)
		}
	}
}

func TestCidIno_StableFromSameCID(t *testing.T) {
	dagSvc := newTestDAGService(t)
	rootNode := ingestSpec(t, dagSvc, fsSpec{"f.txt": "hello"})
	c := rootNode.Cid()

	ino1 := cidIno(c)
	ino2 := cidIno(c)
	if ino1 != ino2 {
		t.Errorf("cidIno not stable: %d vs %d", ino1, ino2)
	}
	if ino1 == 0 {
		t.Error("cidIno returned 0 for a valid CID")
	}
}

func TestCidIno_DistinctForDifferentCIDs(t *testing.T) {
	dagSvc := newTestDAGService(t)
	node1 := ingestSpec(t, dagSvc, fsSpec{"a.txt": "aaa"})
	node2 := ingestSpec(t, dagSvc, fsSpec{"b.txt": "bbb"})

	if cidIno(node1.Cid()) == cidIno(node2.Cid()) {
		t.Error("cidIno collision on distinct CIDs (extremely unlikely)")
	}
}

func TestIsDir_ReturnsTrueForDirectory(t *testing.T) {
	dagSvc := newTestDAGService(t)
	dirNode := ingestSpec(t, dagSvc, fsSpec{"f.txt": "hi"})
	if !isDir(dirNode, dagSvc) {
		t.Error("isDir returned false for a UnixFS directory node")
	}
}

func TestIsDir_ReturnsFalseForFile(t *testing.T) {
	dagSvc := newTestDAGService(t)
	dirNode := ingestSpec(t, dagSvc, fsSpec{"f.txt": "hello"})
	fileNode := findFileNode(t, dagSvc, dirNode, "f.txt")
	if isDir(fileNode, dagSvc) {
		t.Error("isDir returned true for a UnixFS file node")
	}
}

func TestVirtualRootGetattr(t *testing.T) {
	dagSvc := newTestDAGService(t)
	root := newVirtualRootNode(&staticRootLister{}, dagSvc)

	var out fuse.AttrOut
	errno := root.Getattr(context.Background(), nil, &out)
	if errno != 0 {
		t.Fatalf("Getattr errno: %v", errno)
	}
	if out.Mode&syscall.S_IFDIR == 0 {
		t.Error("expected directory mode")
	}
	if out.Mode&0555 != 0555 {
		t.Errorf("expected mode 0555, got %o", out.Mode&0777)
	}
}

func TestVirtualRootReaddir_Empty(t *testing.T) {
	dagSvc := newTestDAGService(t)
	root := newVirtualRootNode(&staticRootLister{}, dagSvc)

	stream, errno := root.Readdir(context.Background())
	if errno != 0 {
		t.Fatalf("Readdir: %v", errno)
	}
	if stream.HasNext() {
		t.Error("expected empty listing for empty registry")
	}
}

func TestVirtualRootReaddir_WithRoots(t *testing.T) {
	dagSvc := newTestDAGService(t)

	node1 := ingestSpec(t, dagSvc, fsSpec{"a.txt": "aaa"})
	node2 := ingestSpec(t, dagSvc, fsSpec{"b.txt": "bbb"})
	node3 := ingestSpec(t, dagSvc, fsSpec{"c.txt": "ccc"})

	lister := &staticRootLister{}
	lister.add(store.Root{CID: node1.Cid()})
	lister.add(store.Root{CID: node2.Cid()})
	lister.add(store.Root{CID: node3.Cid()})

	root := newVirtualRootNode(lister, dagSvc)
	stream, errno := root.Readdir(context.Background())
	if errno != 0 {
		t.Fatalf("Readdir: %v", errno)
	}

	seen := make(map[string]bool)
	for stream.HasNext() {
		entry, errno := stream.Next()
		if errno != 0 {
			t.Fatalf("Next: %v", errno)
		}
		seen[entry.Name] = true
		if entry.Mode&syscall.S_IFDIR == 0 {
			t.Errorf("entry %q should be a directory", entry.Name)
		}
	}

	for _, n := range []ipld.Node{node1, node2, node3} {
		cidStr := n.Cid().String()
		if !seen[cidStr] {
			t.Errorf("CID %q not found in listing", cidStr)
		}
	}
	if len(seen) != 3 {
		t.Errorf("expected 3 entries, got %d", len(seen))
	}
}

func TestVirtualRootLookup_NonCIDName(t *testing.T) {
	dagSvc := newTestDAGService(t)
	root := newVirtualRootNode(&staticRootLister{}, dagSvc)

	var out fuse.EntryOut
	_, errno := root.Lookup(context.Background(), "not-a-valid-cid", &out)
	if errno != syscall.ENOENT {
		t.Errorf("expected ENOENT, got %v", errno)
	}
}

func TestVirtualRootLookup_UnknownCID(t *testing.T) {
	dagSvc := newTestDAGService(t)
	root := newVirtualRootNode(&staticRootLister{}, dagSvc)

	unknown := makeDummyCID([]byte("unknown content"))
	var out fuse.EntryOut
	_, errno := root.Lookup(context.Background(), unknown.String(), &out)
	if errno != syscall.ENOENT {
		t.Errorf("expected ENOENT for unknown CID, got %v", errno)
	}
}

func TestDagDirGetattr(t *testing.T) {
	dagSvc := newTestDAGService(t)
	dirNode := ingestSpec(t, dagSvc, fsSpec{"f.txt": "hi"})
	node := newDagDirNode(dirNode, dagSvc)

	var out fuse.AttrOut
	errno := node.Getattr(context.Background(), nil, &out)
	if errno != 0 {
		t.Fatalf("Getattr: %v", errno)
	}
	if out.Mode&syscall.S_IFDIR == 0 {
		t.Error("expected directory mode")
	}
	if out.Mode&0555 != 0555 {
		t.Errorf("expected mode 0555, got %o", out.Mode&0777)
	}
}

func TestDagDirReaddir_FilesOnlyDirectory(t *testing.T) {
	dagSvc := newTestDAGService(t)
	rootNode := ingestSpec(t, dagSvc, fsSpec{
		"alpha.txt":	"alpha",
		"beta.txt":	"beta",
		"gamma.txt":	"gamma",
	})
	node := newDagDirNode(rootNode, dagSvc)

	stream, errno := node.Readdir(context.Background())
	if errno != 0 {
		t.Fatalf("Readdir: %v", errno)
	}

	seen := make(map[string]uint32)
	for stream.HasNext() {
		entry, errno := stream.Next()
		if errno != 0 {
			t.Fatalf("Next: %v", errno)
		}
		seen[entry.Name] = entry.Mode
	}

	for _, name := range []string{"alpha.txt", "beta.txt", "gamma.txt"} {
		mode, ok := seen[name]
		if !ok {
			t.Errorf("missing entry %q", name)
			continue
		}
		if mode&syscall.S_IFREG == 0 {
			t.Errorf("%q: expected S_IFREG, got mode %o", name, mode)
		}
	}
}

func TestDagDirReaddir_MixedFilesAndSubdirs(t *testing.T) {
	dagSvc := newTestDAGService(t)
	rootNode := ingestSpec(t, dagSvc, fsSpec{
		"file.txt":		"content",
		"subdir/child.txt":	"nested",
	})
	node := newDagDirNode(rootNode, dagSvc)

	stream, errno := node.Readdir(context.Background())
	if errno != 0 {
		t.Fatalf("Readdir: %v", errno)
	}

	types := make(map[string]uint32)
	for stream.HasNext() {
		entry, _ := stream.Next()
		types[entry.Name] = entry.Mode
	}

	if types["file.txt"]&syscall.S_IFREG == 0 {
		t.Errorf("file.txt should be S_IFREG, got mode %o", types["file.txt"])
	}
	if types["subdir"]&syscall.S_IFDIR == 0 {
		t.Errorf("subdir should be S_IFDIR, got mode %o", types["subdir"])
	}
}

func TestDagDirReaddir_EmptyDirectory(t *testing.T) {
	dagSvc := newTestDAGService(t)
	rootNode := ingestSpec(t, dagSvc, fsSpec{})
	node := newDagDirNode(rootNode, dagSvc)

	stream, errno := node.Readdir(context.Background())
	if errno != 0 {
		t.Fatalf("Readdir: %v", errno)
	}
	if stream.HasNext() {
		t.Error("expected no entries in empty directory")
	}
}

func TestDagDirReaddir_InodesDerivedFromCIDs(t *testing.T) {
	dagSvc := newTestDAGService(t)
	rootNode := ingestSpec(t, dagSvc, fsSpec{"file.txt": "data"})
	node := newDagDirNode(rootNode, dagSvc)

	stream, errno := node.Readdir(context.Background())
	if errno != 0 {
		t.Fatalf("Readdir: %v", errno)
	}
	for stream.HasNext() {
		entry, _ := stream.Next()
		if entry.Ino == 0 {
			t.Errorf("entry %q has inode 0", entry.Name)
		}
	}
}

func TestDagFileGetattr_SizeAndMode(t *testing.T) {
	dagSvc := newTestDAGService(t)
	content := "hello, FUSE world!"
	rootNode := ingestSpec(t, dagSvc, fsSpec{"f.txt": content})
	fileNode := findFileNode(t, dagSvc, rootNode, "f.txt")

	dr, err := ufsio.NewDagReader(context.Background(), fileNode, dagSvc)
	if err != nil {
		t.Fatal(err)
	}
	size := dr.Size()
	dr.Close()

	node := &dagFileNode{node: fileNode, dagSvc: dagSvc, size: size}
	var out fuse.AttrOut
	errno := node.Getattr(context.Background(), nil, &out)
	if errno != 0 {
		t.Fatalf("Getattr: %v", errno)
	}
	if out.Mode&syscall.S_IFREG == 0 {
		t.Error("expected S_IFREG mode")
	}
	if out.Mode&0444 != 0444 {
		t.Errorf("expected mode 0444, got %o", out.Mode&0777)
	}
	if out.Size != uint64(len(content)) {
		t.Errorf("expected size %d, got %d", len(content), out.Size)
	}
}

func TestDagFileOpen_ReturnsHandle(t *testing.T) {
	dagSvc := newTestDAGService(t)
	rootNode := ingestSpec(t, dagSvc, fsSpec{"f.txt": "data"})
	fileNode := findFileNode(t, dagSvc, rootNode, "f.txt")

	fnode := &dagFileNode{node: fileNode, dagSvc: dagSvc, size: 4}
	fh, _, errno := fnode.Open(context.Background(), 0)
	if errno != 0 {
		t.Fatalf("Open: %v", errno)
	}
	if fh == nil {
		t.Fatal("Open returned nil file handle")
	}
	handle := fh.(*dagFileHandle)
	handle.Release(context.Background())
}

func TestDagFileHandle_ReadFullContent(t *testing.T) {
	dagSvc := newTestDAGService(t)
	content := "the quick brown fox jumps over the lazy dog"
	rootNode := ingestSpec(t, dagSvc, fsSpec{"f.txt": content})
	fileNode := findFileNode(t, dagSvc, rootNode, "f.txt")

	dr, err := ufsio.NewDagReader(context.Background(), fileNode, dagSvc)
	if err != nil {
		t.Fatal(err)
	}
	handle := &dagFileHandle{reader: dr}
	defer handle.Release(context.Background())

	dest := make([]byte, len(content))
	result, errno := handle.Read(context.Background(), dest, 0)
	if errno != 0 {
		t.Fatalf("Read: %v", errno)
	}
	data, st := result.Bytes(nil)
	if st != fuse.OK {
		t.Fatalf("Bytes status: %v", st)
	}
	if string(data) != content {
		t.Errorf("want %q, got %q", content, data)
	}
}

func TestDagFileHandle_ReadAtOffset(t *testing.T) {
	dagSvc := newTestDAGService(t)
	content := "0123456789abcdef"
	rootNode := ingestSpec(t, dagSvc, fsSpec{"f.txt": content})
	fileNode := findFileNode(t, dagSvc, rootNode, "f.txt")

	dr, err := ufsio.NewDagReader(context.Background(), fileNode, dagSvc)
	if err != nil {
		t.Fatal(err)
	}
	handle := &dagFileHandle{reader: dr}
	defer handle.Release(context.Background())

	tests := []struct {
		offset	int64
		length	int
		want	string
	}{
		{0, 4, "0123"},
		{4, 4, "4567"},
		{10, 6, "abcdef"},
		{8, 4, "89ab"},
	}
	for _, tc := range tests {
		dest := make([]byte, tc.length)
		result, errno := handle.Read(context.Background(), dest, tc.offset)
		if errno != 0 {
			t.Errorf("offset=%d: Read: %v", tc.offset, errno)
			continue
		}
		data, _ := result.Bytes(nil)
		if string(data) != tc.want {
			t.Errorf("offset=%d len=%d: want %q, got %q", tc.offset, tc.length, tc.want, data)
		}
	}
}

func TestDagFileHandle_ReadPastEnd(t *testing.T) {
	dagSvc := newTestDAGService(t)
	content := "short"
	rootNode := ingestSpec(t, dagSvc, fsSpec{"f.txt": content})
	fileNode := findFileNode(t, dagSvc, rootNode, "f.txt")

	dr, err := ufsio.NewDagReader(context.Background(), fileNode, dagSvc)
	if err != nil {
		t.Fatal(err)
	}
	handle := &dagFileHandle{reader: dr}
	defer handle.Release(context.Background())

	dest := make([]byte, 100)
	result, errno := handle.Read(context.Background(), dest, 0)
	if errno != 0 {
		t.Fatalf("Read: %v", errno)
	}
	data, _ := result.Bytes(nil)
	if string(data) != content {
		t.Errorf("want %q, got %q", content, data)
	}
}

func TestDagFileHandle_ConcurrentReads(t *testing.T) {
	dagSvc := newTestDAGService(t)

	const unit = "abcdefghij"
	content := strings.Repeat(unit, 20)
	rootNode := ingestSpec(t, dagSvc, fsSpec{"f.txt": content})
	fileNode := findFileNode(t, dagSvc, rootNode, "f.txt")

	dr, err := ufsio.NewDagReader(context.Background(), fileNode, dagSvc)
	if err != nil {
		t.Fatal(err)
	}
	handle := &dagFileHandle{reader: dr}
	defer handle.Release(context.Background())

	const goroutines = 8
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		offset := int64(i * len(unit))
		expected := content[offset : offset+int64(len(unit))]
		go func(off int64, want string) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				dest := make([]byte, len(want))
				result, errno := handle.Read(context.Background(), dest, off)
				if errno != 0 {
					t.Errorf("goroutine offset=%d: Read: %v", off, errno)
					return
				}
				data, _ := result.Bytes(nil)
				if string(data) != want {
					t.Errorf("goroutine offset=%d: want %q, got %q", off, want, data)
				}
			}
		}(offset, expected)
	}
	wg.Wait()
}

func TestIngester_EmptyDirectory(t *testing.T) {
	dagSvc := newTestDAGService(t)
	dir := t.TempDir()
	rootNode, err := store.IngestPath(context.Background(), dir, dagSvc)
	if err != nil {
		t.Fatalf("IngestPath: %v", err)
	}
	if !isDir(rootNode, dagSvc) {
		t.Error("empty directory should produce a UnixFS directory node")
	}

	ufsDir, err := ufsio.NewDirectoryFromNode(dagSvc, rootNode)
	if err != nil {
		t.Fatal(err)
	}
	links, err := ufsDir.Links(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(links) != 0 {
		t.Errorf("empty dir: expected 0 links, got %d", len(links))
	}
}

func TestIngester_SingleFileWrappedInVirtualDir(t *testing.T) {
	dagSvc := newTestDAGService(t)
	content := "single file content"
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "data.txt"), []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	filePath := filepath.Join(dir, "data.txt")

	rootNode, err := store.IngestPath(context.Background(), filePath, dagSvc)
	if err != nil {
		t.Fatalf("IngestPath: %v", err)
	}

	if !isDir(rootNode, dagSvc) {
		t.Fatal("single file IngestPath must return a directory root")
	}

	fileNode := findFileNode(t, dagSvc, rootNode, "data.txt")
	dr, err := ufsio.NewDagReader(context.Background(), fileNode, dagSvc)
	if err != nil {
		t.Fatal(err)
	}
	defer dr.Close()

	got := make([]byte, len(content))
	if _, err := dr.Read(got); err != nil && err.Error() != "EOF" {
		t.Fatal(err)
	}
	if string(got) != content {
		t.Errorf("want %q, got %q", content, got)
	}
}

func TestIngester_NestedDirectoryStructure(t *testing.T) {
	dagSvc := newTestDAGService(t)
	spec := fsSpec{
		"top.txt":		"top level",
		"sub/mid.txt":		"middle",
		"sub/deep/bottom.txt":	"deepest",
	}
	rootNode := ingestSpec(t, dagSvc, spec)

	if !isDir(rootNode, dagSvc) {
		t.Fatal("root must be a directory")
	}

	subNode := findFileNode(t, dagSvc, rootNode, "sub")
	if !isDir(subNode, dagSvc) {
		t.Error("sub should be a directory")
	}

	deepNode := findFileNode(t, dagSvc, subNode, "deep")
	if !isDir(deepNode, dagSvc) {
		t.Error("sub/deep should be a directory")
	}

	bottomNode := findFileNode(t, dagSvc, deepNode, "bottom.txt")
	dr, err := ufsio.NewDagReader(context.Background(), bottomNode, dagSvc)
	if err != nil {
		t.Fatal(err)
	}
	defer dr.Close()
	got := make([]byte, len("deepest"))
	dr.Read(got)
	if string(got) != "deepest" {
		t.Errorf("want %q, got %q", "deepest", got)
	}
}

func TestIngester_ContentAddressed_SameContentSameCID(t *testing.T) {
	dagSvc := newTestDAGService(t)
	spec := fsSpec{"readme.txt": "identical content"}

	node1 := ingestSpec(t, dagSvc, spec)
	node2 := ingestSpec(t, dagSvc, spec)

	if node1.Cid() != node2.Cid() {
		t.Errorf("same content produced different CIDs: %s vs %s", node1.Cid(), node2.Cid())
	}
}

func TestIngester_ContentAddressed_DifferentContentDifferentCID(t *testing.T) {
	dagSvc := newTestDAGService(t)
	node1 := ingestSpec(t, dagSvc, fsSpec{"f.txt": "version one"})
	node2 := ingestSpec(t, dagSvc, fsSpec{"f.txt": "version two"})

	if node1.Cid() == node2.Cid() {
		t.Error("different content must produce different CIDs")
	}
}

func TestIngester_BinaryContent(t *testing.T) {
	dagSvc := newTestDAGService(t)
	var buf bytes.Buffer
	for i := 0; i < 256; i++ {
		buf.WriteByte(byte(i))
	}
	binary := buf.String()

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "bin.dat"), []byte(binary), 0644); err != nil {
		t.Fatal(err)
	}
	rootNode, err := store.IngestPath(context.Background(), dir, dagSvc)
	if err != nil {
		t.Fatal(err)
	}

	fileNode := findFileNode(t, dagSvc, rootNode, "bin.dat")
	dr, err := ufsio.NewDagReader(context.Background(), fileNode, dagSvc)
	if err != nil {
		t.Fatal(err)
	}
	defer dr.Close()

	got := make([]byte, len(binary))
	n, _ := dr.Read(got)
	if string(got[:n]) != binary {
		t.Error("binary content round-trip failed")
	}
}

func TestIngester_FollowsSymlinkToDir(t *testing.T) {
	dagSvc := newTestDAGService(t)
	base := t.TempDir()

	realDir := filepath.Join(base, "real")
	if err := os.Mkdir(realDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(realDir, "f.txt"), []byte("via symlink"), 0644); err != nil {
		t.Fatal(err)
	}

	root := filepath.Join(base, "root")
	if err := os.Mkdir(root, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(realDir, filepath.Join(root, "link")); err != nil {
		t.Skip("symlinks not supported:", err)
	}

	rootNode, err := store.IngestPath(context.Background(), root, dagSvc)
	if err != nil {
		t.Fatalf("IngestPath: %v", err)
	}

	linkNode := findFileNode(t, dagSvc, rootNode, "link")
	if !isDir(linkNode, dagSvc) {
		t.Error("symlink to dir should be ingested as a directory")
	}
	findFileNode(t, dagSvc, linkNode, "f.txt")
}

func TestMount_MountpointNotExist(t *testing.T) {
	dagSvc := newTestDAGService(t)
	_, err := Mount(
		MountOptions{Mountpoint: "/nonexistent/path/datarings-test"},
		&staticRootLister{},
		dagSvc,
	)
	if err == nil {
		t.Fatal("expected error for nonexistent mountpoint")
	}
	if !errors.Is(err, ErrMountpointNotExist) {
		t.Errorf("expected ErrMountpointNotExist, got %v", err)
	}
}

func TestMount_MountAndUnmount(t *testing.T) {
	dagSvc := newTestDAGService(t)
	mountTest(t, &staticRootLister{}, dagSvc)
}

func TestMount_EmptyRegistry(t *testing.T) {
	dagSvc := newTestDAGService(t)
	mountpoint := mountTest(t, &staticRootLister{}, dagSvc)

	entries, err := os.ReadDir(mountpoint)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("empty registry: expected 0 top-level entries, got %d", len(entries))
	}
}

func TestMount_SingleFileWrapped(t *testing.T) {
	dagSvc := newTestDAGService(t)
	content := "single file wrapped in virtual dir"
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "note.txt"), []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	rootNode, err := store.IngestPath(context.Background(), filepath.Join(dir, "note.txt"), dagSvc)
	if err != nil {
		t.Fatal(err)
	}

	lister := &staticRootLister{}
	lister.add(store.Root{CID: rootNode.Cid()})
	mountpoint := mountTest(t, lister, dagSvc)

	mountedFile := filepath.Join(mountpoint, rootNode.Cid().String(), "note.txt")
	data, err := os.ReadFile(mountedFile)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(data) != content {
		t.Errorf("want %q, got %q", content, data)
	}
}

func TestMount_DirectoryRoundtrip(t *testing.T) {
	dagSvc := newTestDAGService(t)
	spec := fsSpec{
		"hello.txt":		"hello world\n",
		"subdir/nested.txt":	"nested content\n",
		"subdir/deep/low.txt":	"deep content\n",
	}
	rootNode := ingestSpec(t, dagSvc, spec)

	lister := &staticRootLister{}
	lister.add(store.Root{CID: rootNode.Cid()})
	mountpoint := mountTest(t, lister, dagSvc)

	cidDir := filepath.Join(mountpoint, rootNode.Cid().String())
	got := readFS(t, cidDir)
	assertFilesMatch(t, spec, got)
}

func TestMount_BinaryFileContent(t *testing.T) {
	dagSvc := newTestDAGService(t)
	var buf bytes.Buffer
	for i := 0; i < 256; i++ {
		buf.WriteByte(byte(i))
	}
	binary := buf.Bytes()

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "bin.dat"), binary, 0644); err != nil {
		t.Fatal(err)
	}
	rootNode, err := store.IngestPath(context.Background(), dir, dagSvc)
	if err != nil {
		t.Fatal(err)
	}

	lister := &staticRootLister{}
	lister.add(store.Root{CID: rootNode.Cid()})
	mountpoint := mountTest(t, lister, dagSvc)

	data, err := os.ReadFile(filepath.Join(mountpoint, rootNode.Cid().String(), "bin.dat"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, binary) {
		t.Error("binary content mismatch after FUSE round-trip")
	}
}

func TestMount_MultipleRoots(t *testing.T) {
	dagSvc := newTestDAGService(t)

	root1 := ingestSpec(t, dagSvc, fsSpec{"a.txt": "aaa"})
	root2 := ingestSpec(t, dagSvc, fsSpec{"b.txt": "bbb"})
	root3 := ingestSpec(t, dagSvc, fsSpec{"c.txt": "ccc"})

	lister := &staticRootLister{}
	lister.add(store.Root{CID: root1.Cid()})
	lister.add(store.Root{CID: root2.Cid()})
	lister.add(store.Root{CID: root3.Cid()})
	mountpoint := mountTest(t, lister, dagSvc)

	entries, err := os.ReadDir(mountpoint)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Errorf("expected 3 top-level entries, got %d", len(entries))
	}

	checks := []struct {
		node	ipld.Node
		file	string
		want	string
	}{
		{root1, "a.txt", "aaa"},
		{root2, "b.txt", "bbb"},
		{root3, "c.txt", "ccc"},
	}
	for _, c := range checks {
		p := filepath.Join(mountpoint, c.node.Cid().String(), c.file)
		data, err := os.ReadFile(p)
		if err != nil {
			t.Errorf("%s: ReadFile: %v", c.file, err)
			continue
		}
		if string(data) != c.want {
			t.Errorf("%s: want %q, got %q", c.file, c.want, data)
		}
	}
}

func TestMount_TopLevelIsReadOnly(t *testing.T) {
	dagSvc := newTestDAGService(t)
	rootNode := ingestSpec(t, dagSvc, fsSpec{"f.txt": "data"})
	lister := &staticRootLister{}
	lister.add(store.Root{CID: rootNode.Cid()})
	mountpoint := mountTest(t, lister, dagSvc)

	if err := os.WriteFile(filepath.Join(mountpoint, "new.txt"), []byte("x"), 0644); err == nil {
		t.Error("write to FUSE root should fail on a read-only filesystem")
	}
	if err := os.Mkdir(filepath.Join(mountpoint, "newdir"), 0755); err == nil {
		t.Error("mkdir in FUSE root should fail on a read-only filesystem")
	}
}

func TestMount_CIDDirIsReadOnly(t *testing.T) {
	dagSvc := newTestDAGService(t)
	rootNode := ingestSpec(t, dagSvc, fsSpec{"f.txt": "data"})
	lister := &staticRootLister{}
	lister.add(store.Root{CID: rootNode.Cid()})
	mountpoint := mountTest(t, lister, dagSvc)

	cidDir := filepath.Join(mountpoint, rootNode.Cid().String())
	if err := os.WriteFile(filepath.Join(cidDir, "new.txt"), []byte("x"), 0644); err == nil {
		t.Error("write into CID directory should fail on a read-only filesystem")
	}
}

func TestMount_ConcurrentReads(t *testing.T) {
	dagSvc := newTestDAGService(t)
	content := strings.Repeat("concurrent read test content\n", 50)
	rootNode := ingestSpec(t, dagSvc, fsSpec{"big.txt": content})

	lister := &staticRootLister{}
	lister.add(store.Root{CID: rootNode.Cid()})
	mountpoint := mountTest(t, lister, dagSvc)

	filePath := filepath.Join(mountpoint, rootNode.Cid().String(), "big.txt")

	const goroutines = 12
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			data, err := os.ReadFile(filePath)
			if err != nil {
				t.Errorf("goroutine %d: ReadFile: %v", id, err)
				return
			}
			if string(data) != content {
				t.Errorf("goroutine %d: content mismatch", id)
			}
		}(i)
	}
	wg.Wait()
}

func TestMount_FilePermissions(t *testing.T) {
	dagSvc := newTestDAGService(t)
	rootNode := ingestSpec(t, dagSvc, fsSpec{"f.txt": "data"})
	lister := &staticRootLister{}
	lister.add(store.Root{CID: rootNode.Cid()})
	mountpoint := mountTest(t, lister, dagSvc)

	cidDir := filepath.Join(mountpoint, rootNode.Cid().String())
	info, err := os.Stat(cidDir)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0555 {
		t.Errorf("CID dir mode: want 0555, got %o", perm)
	}

	fileInfo, err := os.Stat(filepath.Join(cidDir, "f.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if perm := fileInfo.Mode().Perm(); perm != 0444 {
		t.Errorf("file mode: want 0444, got %o", perm)
	}
}

func TestMount_LargeFile(t *testing.T) {
	dagSvc := newTestDAGService(t)

	large := strings.Repeat("X", 512*1024)

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "large.dat"), []byte(large), 0644); err != nil {
		t.Fatal(err)
	}
	rootNode, err := store.IngestPath(context.Background(), dir, dagSvc)
	if err != nil {
		t.Fatal(err)
	}

	lister := &staticRootLister{}
	lister.add(store.Root{CID: rootNode.Cid()})
	mountpoint := mountTest(t, lister, dagSvc)

	data, err := os.ReadFile(filepath.Join(mountpoint, rootNode.Cid().String(), "large.dat"))
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != len(large) {
		t.Errorf("large file: want %d bytes, got %d", len(large), len(data))
	}
	if string(data) != large {
		t.Error("large file content mismatch")
	}
}
