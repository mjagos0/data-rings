package main

import (
	"time"

	"github.com/mjagos0/datarings/dht"
)

type allRingsDrainer struct {
	publicDring	*dht.PublicDring
	privManager	*privateDringsManager
}

func (d *allRingsDrainer) WaitForReplicationDrain(timeout time.Duration) bool {
	pubOK := make(chan bool, 1)
	go func() {
		if d.publicDring == nil {
			pubOK <- true
			return
		}
		pubOK <- d.publicDring.Node().WaitForReplicationDrain(timeout)
	}()
	privOK := make(chan bool, 1)
	go func() {
		if d.privManager == nil {
			privOK <- true
			return
		}
		privOK <- d.privManager.WaitForReplicationDrain(timeout)
	}()
	return <-pubOK && <-privOK
}
