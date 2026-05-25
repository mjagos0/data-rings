package fuse

import (
	"context"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	ufsio "github.com/ipfs/boxo/ipld/unixfs/io"
	ipld "github.com/ipfs/go-ipld-format"
)

type dagFileNode struct {
	gofuse.Inode
	node	ipld.Node
	dagSvc	ipld.DAGService
	size	uint64
}

var _ = (gofuse.NodeGetattrer)((*dagFileNode)(nil))
var _ = (gofuse.NodeOpener)((*dagFileNode)(nil))

func (n *dagFileNode) Getattr(_ context.Context, _ gofuse.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFREG | 0444
	out.Size = n.size
	return 0
}

func (n *dagFileNode) Open(_ context.Context, _ uint32) (gofuse.FileHandle, uint32, syscall.Errno) {
	dr, err := ufsio.NewDagReader(context.Background(), n.node, n.dagSvc)
	if err != nil {
		return nil, 0, syscall.EIO
	}
	return &dagFileHandle{reader: dr}, fuse.FOPEN_KEEP_CACHE, 0
}
