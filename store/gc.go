package store

import (
	"context"
	"log/slog"
	"time"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

type GCResult struct {
	Removed	int		`json:"removed"`
	Kept	int		`json:"kept"`
	Elapsed	time.Duration	`json:"elapsed"`
}

func gcWalkDAG(ctx context.Context, dag ipld.DAGService, c cid.Cid, seen map[string]struct{}) error {
	key := c.Hash().String()
	if _, ok := seen[key]; ok {
		return nil
	}
	seen[key] = struct{}{}

	node, err := dag.Get(ctx, c)
	if err != nil {

		slog.Warn("gc: intermediate DAG block missing, subtree will be unreachable", "cid", c)
		return nil
	}

	for _, link := range node.Links() {
		if err := gcWalkDAG(ctx, dag, link.Cid, seen); err != nil {
			return err
		}
	}
	return nil
}
