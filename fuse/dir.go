package fuse

import (
	"context"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	ufsio "github.com/ipfs/boxo/ipld/unixfs/io"
	ipld "github.com/ipfs/go-ipld-format"
)

type dagDirNode struct {
	gofuse.Inode
	node	ipld.Node
	dagSvc	ipld.DAGService
}

var _ = (gofuse.NodeGetattrer)((*dagDirNode)(nil))
var _ = (gofuse.NodeLookuper)((*dagDirNode)(nil))
var _ = (gofuse.NodeReaddirer)((*dagDirNode)(nil))

func newDagDirNode(node ipld.Node, dagSvc ipld.DAGService) *dagDirNode {
	return &dagDirNode{node: node, dagSvc: dagSvc}
}

func (n *dagDirNode) Getattr(_ context.Context, _ gofuse.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFDIR | 0555
	return 0
}

func (n *dagDirNode) Readdir(ctx context.Context) (gofuse.DirStream, syscall.Errno) {
	dir, err := ufsio.NewDirectoryFromNode(n.dagSvc, n.node)
	if err != nil {
		return nil, syscall.EIO
	}

	links, err := dir.Links(ctx)
	if err != nil {
		return nil, syscall.EIO
	}

	entries := make([]fuse.DirEntry, 0, len(links))
	for _, link := range links {
		child, err := n.dagSvc.Get(ctx, link.Cid)
		if err != nil {
			continue
		}
		mode := uint32(syscall.S_IFREG)
		if isDir(child, n.dagSvc) {
			mode = syscall.S_IFDIR
		}
		entries = append(entries, fuse.DirEntry{
			Mode:	mode,
			Name:	link.Name,
			Ino:	cidIno(link.Cid),
		})
	}
	return gofuse.NewListDirStream(entries), 0
}

func (n *dagDirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	dir, err := ufsio.NewDirectoryFromNode(n.dagSvc, n.node)
	if err != nil {
		return nil, syscall.EIO
	}

	childNode, err := dir.Find(ctx, name)
	if err != nil {
		return nil, syscall.ENOENT
	}

	ino := cidIno(childNode.Cid())

	if isDir(childNode, n.dagSvc) {
		out.Attr.Mode = syscall.S_IFDIR | 0555
		child := n.NewInode(ctx,
			&dagDirNode{node: childNode, dagSvc: n.dagSvc},
			gofuse.StableAttr{Mode: syscall.S_IFDIR, Ino: ino},
		)
		return child, 0
	}

	dr, err := ufsio.NewDagReader(ctx, childNode, n.dagSvc)
	if err != nil {
		return nil, syscall.EIO
	}
	size := dr.Size()
	dr.Close()

	out.Attr.Mode = syscall.S_IFREG | 0444
	out.Attr.Size = size

	child := n.NewInode(ctx,
		&dagFileNode{node: childNode, dagSvc: n.dagSvc, size: size},
		gofuse.StableAttr{Mode: syscall.S_IFREG, Ino: ino},
	)
	return child, 0
}
