package dht

import (
	"context"
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

func ListenReuse(network, address string) (net.Listener, error) {
	lc := net.ListenConfig{Control: controlReuseAddr}
	return lc.Listen(context.Background(), network, address)
}

func listenReuse(network, address string) (net.Listener, error) {
	return ListenReuse(network, address)
}

func controlReuseAddr(_ string, _ string, c syscall.RawConn) error {
	var sockErr error
	err := c.Control(func(fd uintptr) {
		if e := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); e != nil {
			sockErr = e
			return
		}
	})
	if err != nil {
		return err
	}
	return sockErr
}
