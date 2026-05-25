package fuse

import (
	"errors"
	"fmt"
	"os"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	ipld "github.com/ipfs/go-ipld-format"
)

var ErrMountpointNotExist = errors.New("mountpoint does not exist")

type MountOptions struct {
	Mountpoint	string
	Debug		bool
}

type MountPoint interface {
	Unmount() error
}

type mountPoint struct {
	server *fuse.Server
}

func Mount(opts MountOptions, registry RootLister, dagSvc ipld.DAGService) (MountPoint, error) {
	if _, err := os.Stat(opts.Mountpoint); os.IsNotExist(err) {
		return nil, fmt.Errorf("%w: %q", ErrMountpointNotExist, opts.Mountpoint)
	}

	root := newVirtualRootNode(registry, dagSvc)
	timeout := time.Second

	server, err := gofuse.Mount(opts.Mountpoint, root, &gofuse.Options{
		AttrTimeout:	&timeout,
		EntryTimeout:	&timeout,
		MountOptions: fuse.MountOptions{
			Debug:	opts.Debug,
			FsName:	"datarings",
			Name:	"datarings",
		},
	})
	if err != nil {
		return nil, fmt.Errorf("fuse mount: %w", err)
	}

	return &mountPoint{server: server}, nil
}

func (m *mountPoint) Unmount() error {
	return m.server.Unmount()
}
