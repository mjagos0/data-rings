package fuse

import "github.com/mjagos0/datarings/store"

type RootLister interface {
	List() []store.Root
}
