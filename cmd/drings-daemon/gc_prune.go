package main

import (
	"context"
	"log/slog"

	"github.com/mjagos0/datarings/dht"
	"github.com/mjagos0/datarings/store"
)

type gcWithPrune struct {
	store		*store.Store
	publicDring	*dht.PublicDring
	privManager	*privateDringsManager
}

func (g *gcWithPrune) GC(ctx context.Context) (store.GCResult, error) {
	pruned := 0
	if g.publicDring != nil {
		if n, err := g.publicDring.PruneOutOfWindowBlocks(ctx); err == nil {
			pruned += n
		} else {
			slog.Warn("gc: public-ring prune failed", "error", err)
		}
	}
	if g.privManager != nil {
		pruned += g.privManager.PruneAllRings(ctx)
	}
	if pruned > 0 {
		slog.Info("gc: pruned out-of-window per-ring associations", "count", pruned)
	}
	return g.store.GC(ctx)
}
