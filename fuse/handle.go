package fuse

import (
	"context"
	"io"
	"sync"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	ufsio "github.com/ipfs/boxo/ipld/unixfs/io"
)

type dagFileHandle struct {
	mu	sync.Mutex
	reader	ufsio.DagReader
}

var _ = (gofuse.FileReader)((*dagFileHandle)(nil))
var _ = (gofuse.FileReleaser)((*dagFileHandle)(nil))

func (h *dagFileHandle) Read(_ context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, err := h.reader.Seek(off, io.SeekStart); err != nil {
		return nil, syscall.EIO
	}
	n, err := h.reader.Read(dest)
	if err != nil && err != io.EOF {
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:n]), 0
}

func (h *dagFileHandle) Release(_ context.Context) syscall.Errno {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reader.Close()
	return 0
}
