//go:build system

package system_test

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"testing"

	blockservice "github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	boxchunker "github.com/ipfs/boxo/chunker"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	"github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	ufsio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"

	"github.com/mjagos0/datarings/testrig"
)

func verifyPublicFingers(t *testing.T, nodes []*testrig.TestNode, topo *testrig.RingTopology) {
	t.Helper()
	for _, n := range nodes {
		s, err := n.PublicState()
		if err != nil {
			t.Errorf("%s: fetch state: %v", n.Name(), err)
			continue
		}
		verifyNodeTopology(t, n.Name(), n.ID(), s, topo)
	}
}

func verifyPrivateFingers(t *testing.T, nodes []*testrig.TestNode, assignment *testrig.IdentityAssignment, topo *testrig.RingTopology, group string) {
	t.Helper()
	for _, n := range nodes {
		entry, ok := n.PrivateState(group)
		if !ok {
			t.Errorf("%s: group %q not in /debug/groups", n.Name(), group)
			continue
		}
		verifyNodeTopology(t, n.Name(), assignment.NodeToID[n.Name()], entry.Node, topo)
		t.Logf("  [ok] %s: verified=%d", n.Name(), len(entry.VerifiedPeers))
	}
}

func verifyNodeTopology(t *testing.T, nodeName, expectedID string, s testrig.NodeState, topo *testrig.RingTopology) {
	t.Helper()

	if s.ID != expectedID {
		t.Errorf("%s: NodeID=%s, want %s", nodeName, s.ID, expectedID)
		return
	}

	expectedSucc := topo.Successors[expectedID]
	if s.Successor.ID != expectedSucc {
		t.Errorf("%s: successor=%s, want %s",
			nodeName, testrig.NodeIDHexToShort(s.Successor.ID), testrig.NodeIDHexToShort(expectedSucc))
	}

	expectedPred := topo.Predecessors[expectedID]
	if s.Predecessor == nil {
		t.Errorf("%s: predecessor=nil, want %s", nodeName, testrig.NodeIDHexToShort(expectedPred))
	} else if s.Predecessor.ID != expectedPred {
		t.Errorf("%s: predecessor=%s, want %s",
			nodeName, testrig.NodeIDHexToShort(s.Predecessor.ID), testrig.NodeIDHexToShort(expectedPred))
	}

	expectedSuccList := topo.SuccessorLists[expectedID]
	actualSuccIDs := make([]string, len(s.SuccessorList))
	for i, na := range s.SuccessorList {
		actualSuccIDs[i] = na.ID
	}
	if len(actualSuccIDs) < len(expectedSuccList) {
		t.Errorf("%s: successor_list len=%d, want >=%d",
			nodeName, len(actualSuccIDs), len(expectedSuccList))
	} else {
		for i, expected := range expectedSuccList {
			if i < len(actualSuccIDs) && actualSuccIDs[i] != expected {
				t.Errorf("%s: successor_list[%d]=%s, want %s",
					nodeName, i, testrig.NodeIDHexToShort(actualSuccIDs[i]), testrig.NodeIDHexToShort(expected))
			}
		}
	}

	expectedUnique := topo.UniqueFingers[expectedID]
	actualUnique := testrig.UniqueFingerIDs(s.Fingers)
	sort.Strings(actualUnique)
	if !testrig.StringSliceEqual(actualUnique, expectedUnique) {
		t.Errorf("%s: unique_fingers=%v, want %v",
			nodeName, testrig.ShortIDs(actualUnique), testrig.ShortIDs(expectedUnique))
	}

	t.Logf("  [ok] %s: succ=%s pred=%s fingers=%d/%d",
		nodeName, testrig.NodeIDHexToShort(s.Successor.ID),
		testrig.SafeShortPred(s.Predecessor),
		len(actualUnique), len(expectedUnique))
}

func snapshotPrivateBlockCounts(t *testing.T, nodes []*testrig.TestNode, group string) map[string]int {
	t.Helper()
	m := make(map[string]int, len(nodes))
	for _, n := range nodes {
		count, err := n.PrivateBlockCount(group)
		if err != nil {
			t.Fatalf("%s: baseline block count: %v", n.Name(), err)
		}
		m[n.Name()] = count
	}
	return m
}

func generateContent(targetNodeIDHex string) string {
	counter := binary.BigEndian.Uint32([]byte(targetNodeIDHex[:4]))
	content := fmt.Sprintf("repl-target-%s-%d", testrig.NodeIDHexToShort(targetNodeIDHex), counter)
	_ = sha1.Sum([]byte(content))
	_ = hex.EncodeToString(nil)
	return content
}

type blockInfo struct {
	CID		string
	MultihashHex	string
	RingKey		string
	Size		int
	IsLeaf		bool
}

func cidToMultihashHex(cidStr string) string {
	c, err := cid.Decode(cidStr)
	if err != nil {
		return ""
	}
	return hex.EncodeToString(c.Hash())
}

func buildDAGBlocks(t *testing.T, data []byte) (rootCID string, blocks []blockInfo) {
	t.Helper()
	ctx := context.Background()

	memDS := dssync.MutexWrap(ds.NewMapDatastore())
	bs := blockstore.NewBlockstore(memDS)
	bsvc := blockservice.New(bs, nil)
	dag := merkledag.NewDAGService(bsvc)

	params := helpers.DagBuilderParams{
		Maxlinks:	helpers.DefaultLinksPerBlock,
		RawLeaves:	true,
		Dagserv:	dag,
	}
	spl := boxchunker.DefaultSplitter(bytes.NewReader(data))
	db, err := params.New(spl)
	if err != nil {
		t.Fatalf("dag builder: %v", err)
	}
	fileNode, err := balanced.Layout(db)
	if err != nil {
		t.Fatalf("balanced layout: %v", err)
	}

	dir, err := ufsio.NewDirectory(dag)
	if err != nil {
		t.Fatalf("new directory: %v", err)
	}
	if err := dir.AddChild(ctx, "testdata.bin", fileNode); err != nil {
		t.Fatalf("add child: %v", err)
	}
	dirNode, err := dir.GetNode()
	if err != nil {
		t.Fatalf("get dir node: %v", err)
	}
	if err := dag.Add(ctx, dirNode); err != nil {
		t.Fatalf("add dir node: %v", err)
	}

	rootCID = dirNode.Cid().String()
	seen := make(map[string]bool)
	var walkDAG func(c cid.Cid)
	walkDAG = func(c cid.Cid) {
		cidStr := c.String()
		if seen[cidStr] {
			return
		}
		seen[cidStr] = true

		node, err := dag.Get(ctx, c)
		if err != nil {
			t.Fatalf("dag get %s: %v", cidStr, err)
		}

		rawData := node.RawData()
		ringKey := testrig.CIDToRingKeyHex(cidStr)
		isLeaf := c.Prefix().Codec != uint64(cid.DagProtobuf)

		blocks = append(blocks, blockInfo{
			CID:		cidStr,
			MultihashHex:	hex.EncodeToString(c.Hash()),
			RingKey:	ringKey,
			Size:		len(rawData),
			IsLeaf:		isLeaf,
		})

		for _, link := range node.Links() {
			walkDAG(link.Cid)
		}
	}
	walkDAG(dirNode.Cid())

	return rootCID, blocks
}

type expectedNodeState struct {
	BlockMHs	[]string
	Bytes		int64
	Count		int
}

func computeExpectedPlacement(t *testing.T, c *testrig.Cluster, blocks []blockInfo, k int) map[string]*expectedNodeState {
	t.Helper()
	expected := make(map[string]*expectedNodeState)
	for _, n := range c.Nodes() {
		expected[n.Name()] = &expectedNodeState{}
	}

	for _, blk := range blocks {
		replicas := c.ReplicaNodesForHash(blk.RingKey, k)
		for _, n := range replicas {
			st := expected[n.Name()]
			st.BlockMHs = append(st.BlockMHs, blk.MultihashHex)
			st.Bytes += int64(blk.Size)
			st.Count++
		}
	}
	return expected
}

func verifyNetworkBlockPlacement(t *testing.T, nodes []*testrig.TestNode, expected map[string]*expectedNodeState, publisher string) {
	t.Helper()
	for _, n := range nodes {
		exp := expected[n.Name()]
		actualCIDs, err := n.NetworkBlockCIDs()
		if err != nil {
			t.Fatalf("%s: NetworkBlockCIDs: %v", n.Name(), err)
		}

		expectedSet := make(map[string]bool, len(exp.BlockMHs))
		for _, mh := range exp.BlockMHs {
			expectedSet[mh] = true
		}
		actualSet := make(map[string]bool, len(actualCIDs))
		for _, c := range actualCIDs {
			mh := cidToMultihashHex(c)
			actualSet[mh] = true
		}

		for _, mh := range exp.BlockMHs {
			if !actualSet[mh] {
				t.Errorf("%s: expected network block mh=%s not found", n.Name(), testrig.NodeIDHexToShort(mh))
			}
		}

		for _, c := range actualCIDs {
			mh := cidToMultihashHex(c)
			if !expectedSet[mh] {
				t.Errorf("%s: unexpected network block %s (mh=%s)", n.Name(), testrig.NodeIDHexToShort(c), testrig.NodeIDHexToShort(mh))
			}
		}

		if exp.Count != len(actualCIDs) {
			t.Errorf("%s: network block count=%d, want %d", n.Name(), len(actualCIDs), exp.Count)
		} else {
			t.Logf("  [ok] %s: %d network blocks", n.Name(), len(actualCIDs))
		}
	}
}
