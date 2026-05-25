package fuse

import (
	"encoding/binary"

	ufsio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

func cidIno(c cid.Cid) uint64 {
	h := c.Hash()
	if len(h) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(h[len(h)-8:])
}

func isDir(node ipld.Node, dagSvc ipld.DAGService) bool {
	_, err := ufsio.NewDirectoryFromNode(dagSvc, node)
	return err == nil
}
