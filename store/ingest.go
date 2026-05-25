package store

import (
	"context"
	"os"
	"path/filepath"

	boxchunker "github.com/ipfs/boxo/chunker"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	"github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	ufsio "github.com/ipfs/boxo/ipld/unixfs/io"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

func IngestPath(ctx context.Context, path string, dagSvc ipld.DAGService) (ipld.Node, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return ingestDir(ctx, path, dagSvc)
	}

	fileNode, err := ingestFile(ctx, path, dagSvc)
	if err != nil {
		return nil, err
	}
	dir, err := ufsio.NewDirectory(dagSvc)
	if err != nil {
		return nil, err
	}
	if err := dir.AddChild(ctx, info.Name(), fileNode); err != nil {
		return nil, err
	}
	dirNode, err := dir.GetNode()
	if err != nil {
		return nil, err
	}
	return dirNode, dagSvc.Add(ctx, dirNode)
}

func ingestFile(ctx context.Context, path string, dagSvc ipld.DAGService) (ipld.Node, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	params := helpers.DagBuilderParams{
		Maxlinks:	helpers.DefaultLinksPerBlock,
		RawLeaves:	true,
		Dagserv:	dagSvc,
	}
	spl := boxchunker.DefaultSplitter(f)
	db, err := params.New(spl)
	if err != nil {
		return nil, err
	}
	return balanced.Layout(db)
}

func LinksOf(c cid.Cid, data []byte) ([]cid.Cid, error) {
	if c.Prefix().Codec != uint64(cid.DagProtobuf) {
		return nil, nil
	}
	blk, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		return nil, err
	}
	node, err := merkledag.DecodeProtobufBlock(blk)
	if err != nil {
		return nil, err
	}
	links := make([]cid.Cid, len(node.Links()))
	for i, link := range node.Links() {
		links[i] = link.Cid
	}
	return links, nil
}

func ingestDir(ctx context.Context, path string, dagSvc ipld.DAGService) (ipld.Node, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	dir, err := ufsio.NewDirectory(dagSvc)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		childPath := filepath.Join(path, entry.Name())

		info, err := os.Stat(childPath)
		if err != nil {
			return nil, err
		}

		var childNode ipld.Node
		if info.IsDir() {
			childNode, err = ingestDir(ctx, childPath, dagSvc)
		} else {
			childNode, err = ingestFile(ctx, childPath, dagSvc)
		}
		if err != nil {
			return nil, err
		}

		if err := dir.AddChild(ctx, entry.Name(), childNode); err != nil {
			return nil, err
		}
	}

	dirNode, err := dir.GetNode()
	if err != nil {
		return nil, err
	}
	return dirNode, dagSvc.Add(ctx, dirNode)
}
