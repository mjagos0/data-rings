package fuse

import (
	"context"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/mjagos0/datarings/store"
)

type virtualRootNode struct {
	gofuse.Inode
	registry	RootLister
	dagSvc		ipld.DAGService
}

var _ = (gofuse.NodeGetattrer)((*virtualRootNode)(nil))
var _ = (gofuse.NodeLookuper)((*virtualRootNode)(nil))
var _ = (gofuse.NodeReaddirer)((*virtualRootNode)(nil))

func newVirtualRootNode(registry RootLister, dagSvc ipld.DAGService) *virtualRootNode {
	return &virtualRootNode{registry: registry, dagSvc: dagSvc}
}

func (n *virtualRootNode) Getattr(_ context.Context, _ gofuse.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFDIR | 0555
	return 0
}

func (n *virtualRootNode) Readdir(_ context.Context) (gofuse.DirStream, syscall.Errno) {
	roots := n.registry.List()
	entries := make([]fuse.DirEntry, 0, len(roots))
	for _, root := range roots {
		name := root.Name
		if name == "" {
			name = root.CID.String()
		}
		entries = append(entries, fuse.DirEntry{
			Mode:	syscall.S_IFDIR,
			Name:	name,
			Ino:	store.NameIno(name),
		})
	}
	return gofuse.NewListDirStream(entries), 0
}

func (n *virtualRootNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	var c cid.Cid
	for _, root := range n.registry.List() {
		if root.Name != "" && root.Name == name {
			c = root.CID
			break
		}
	}
	if !c.Defined() {
		decoded, err := cid.Decode(name)
		if err != nil {
			return nil, syscall.ENOENT
		}
		c = decoded
	}

	rootNode, err := n.dagSvc.Get(ctx, c)
	if err != nil {
		return nil, syscall.ENOENT
	}

	out.Attr.Mode = syscall.S_IFDIR | 0555

	child := n.NewInode(ctx,
		&dagDirNode{node: rootNode, dagSvc: n.dagSvc},
		gofuse.StableAttr{Mode: syscall.S_IFDIR, Ino: store.NameIno(name)},
	)
	return child, 0
}
