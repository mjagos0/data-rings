package dht

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestSingleNodeRing(t *testing.T) {
	ring := NewTestRing()
	n := ring.AddNode(0)

	succ := n.getSuccessor()
	if !succ.ID.Equal(n.id) {
		t.Fatalf("single node should be its own successor, got %s", succ.ID)
	}

	for _, pos := range []byte{0, 50, 128, 255} {
		id := testNodeID(pos)
		got, err := n.FindSuccessor(context.Background(), id)
		if err != nil {
			t.Fatalf("FindSuccessor(%d): %v", pos, err)
		}
		if !got.ID.Equal(n.id) {
			t.Fatalf("FindSuccessor(%d) = %s, want self", pos, got.ID)
		}
	}
}

func TestTwoNodeRing(t *testing.T) {
	ring := NewTestRing()
	n0 := ring.AddNode(0)
	n100 := ring.AddNode(100)

	ring.StabilizeRounds(20)

	if !n0.getSuccessor().ID.Equal(n100.id) {
		t.Fatalf("n0.successor = %s, want n100", n0.getSuccessor().ID)
	}
	if !n100.getSuccessor().ID.Equal(n0.id) {
		t.Fatalf("n100.successor = %s, want n0", n100.getSuccessor().ID)
	}

	pred0 := n0.getPredecessor()
	if pred0 == nil || !pred0.ID.Equal(n100.id) {
		t.Fatalf("n0.predecessor should be n100")
	}
	pred100 := n100.getPredecessor()
	if pred100 == nil || !pred100.ID.Equal(n0.id) {
		t.Fatalf("n100.predecessor should be n0")
	}
}

func TestMultiNodeRingSuccessors(t *testing.T) {
	ring := NewTestRing()
	for _, pos := range []byte{10, 50, 100, 150, 200} {
		ring.AddNode(pos)
	}
	ring.StabilizeRounds(30)

	expected := map[byte]byte{10: 50, 50: 100, 100: 150, 150: 200, 200: 10}
	for pos, wantSucc := range expected {
		n := ring.FindNode(pos)
		if n == nil {
			t.Fatalf("node %d not found", pos)
		}
		if testNodeIDVal(n.getSuccessor().ID) != wantSucc {
			t.Errorf("node %d: successor = %d, want %d",
				pos, testNodeIDVal(n.getSuccessor().ID), wantSucc)
		}
	}

	expectedPred := map[byte]byte{10: 200, 50: 10, 100: 50, 150: 100, 200: 150}
	for pos, wantPred := range expectedPred {
		n := ring.FindNode(pos)
		pred := n.getPredecessor()
		if pred == nil {
			t.Errorf("node %d: predecessor is nil, want %d", pos, wantPred)
			continue
		}
		if testNodeIDVal(pred.ID) != wantPred {
			t.Errorf("node %d: predecessor = %d, want %d",
				pos, testNodeIDVal(pred.ID), wantPred)
		}
	}
}

func TestFindSuccessor(t *testing.T) {
	ring := NewTestRing()
	for _, pos := range []byte{0, 50, 100, 150, 200} {
		ring.AddNode(pos)
	}
	ring.StabilizeRounds(30)

	n0 := ring.FindNode(0)

	tests := []struct {
		key		byte
		wantSucc	byte
	}{
		{25, 50},
		{50, 50},
		{51, 100},
		{99, 100},
		{175, 200},
		{201, 0},
		{255, 0},
		{0, 0},
		{1, 50},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("key_%d", tt.key), func(t *testing.T) {
			id := testNodeID(tt.key)
			got, err := n0.FindSuccessor(context.Background(), id)
			if err != nil {
				t.Fatalf("FindSuccessor(%d): %v", tt.key, err)
			}
			if testNodeIDVal(got.ID) != tt.wantSucc {
				t.Errorf("FindSuccessor(%d) = %d, want %d",
					tt.key, testNodeIDVal(got.ID), tt.wantSucc)
			}
		})
	}
}

func TestPutAndGet(t *testing.T) {
	ring := NewTestRing()
	for _, pos := range []byte{0, 64, 128, 192} {
		ring.AddNode(pos)
	}
	ring.StabilizeRounds(30)

	ctx := context.Background()
	n0 := ring.FindNode(0)

	testData := map[string]string{
		"alice":	"data_alice",
		"bob":		"data_bob",
		"carol":	"data_carol",
		"dave":		"data_dave",
		"eve":		"data_eve",
		"frank":	"data_frank",
	}

	for name, val := range testData {
		key, data := testBlock(name + ":" + val)
		if err := n0.Put(ctx, key, data); err != nil {
			t.Fatalf("Put(%q): %v", name, err)
		}
	}

	for _, node := range ring.Nodes {
		for name, val := range testData {
			key, wantData := testBlock(name + ":" + val)
			got, err := node.Get(ctx, key)
			if err != nil {
				t.Fatalf("node %d Get(%q): %v", testNodeIDVal(node.id), name, err)
			}
			if string(got) != string(wantData) {
				t.Errorf("node %d Get(%q): got %q, want %q",
					testNodeIDVal(node.id), name, got, wantData)
			}
		}
	}
}

func TestDataDistribution(t *testing.T) {
	ring := NewTestRing()
	for _, pos := range []byte{0, 64, 128, 192} {
		ring.AddNode(pos)
	}
	ring.StabilizeRounds(30)

	ctx := context.Background()
	n0 := ring.FindNode(0)

	for i := 0; i < 50; i++ {
		key, data := testBlock(fmt.Sprintf("block-%d", i))
		if err := n0.Put(ctx, key, data); err != nil {
			t.Fatalf("Put block-%d: %v", i, err)
		}
	}

	total := 0
	for _, node := range ring.Nodes {
		count := node.DataCount()
		total += count
		t.Logf("Node %d has %d blocks", testNodeIDVal(node.id), count)
	}

	if total != 50 {
		t.Fatalf("expected 50 total blocks, got %d", total)
	}

	for _, node := range ring.Nodes {
		if node.DataCount() == 0 {
			t.Errorf("node %d has 0 blocks — distribution seems broken", testNodeIDVal(node.id))
		}
	}
}

func TestRemove(t *testing.T) {
	ring := NewTestRing()
	for _, pos := range []byte{0, 128} {
		ring.AddNode(pos)
	}
	ring.StabilizeRounds(20)

	ctx := context.Background()
	n0 := ring.FindNode(0)

	key, data := testBlock("to-be-deleted")

	if err := n0.Put(ctx, key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	has, err := n0.Has(ctx, key)
	if err != nil || !has {
		t.Fatalf("block should exist before Remove: has=%v err=%v", has, err)
	}

	if err := n0.Remove(ctx, key); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	has, err = n0.Has(ctx, key)
	if err != nil {
		t.Fatalf("Has after Remove: %v", err)
	}
	if has {
		t.Fatal("block should not exist after Remove")
	}
}

func TestKeyTransferOnJoin(t *testing.T) {
	ring := NewTestRing()
	ring.AddNode(0)
	ring.StabilizeRounds(10)

	ctx := context.Background()
	n0 := ring.FindNode(0)

	for i := 0; i < 30; i++ {
		key, data := testBlock(fmt.Sprintf("transfer-%d", i))
		if err := n0.Put(ctx, key, data); err != nil {
			t.Fatalf("Put transfer-%d: %v", i, err)
		}
	}

	initial := n0.DataCount()
	t.Logf("Node 0 initially holds %d blocks", initial)
	if initial != 30 {
		t.Fatalf("expected 30, got %d", initial)
	}

	ring.AddNode(128)
	ring.StabilizeRounds(20)

	n128 := ring.FindNode(128)
	after0 := n0.DataCount()
	after128 := n128.DataCount()
	t.Logf("After join: node 0 = %d, node 128 = %d", after0, after128)

	if after0+after128 != 30 {
		t.Fatalf("block count changed: got %d total, want 30", after0+after128)
	}
	if after128 == 0 {
		t.Error("node 128 should have received some blocks after joining")
	}

	for i := 0; i < 30; i++ {
		key, wantData := testBlock(fmt.Sprintf("transfer-%d", i))
		got, err := n0.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get transfer-%d after join: %v", i, err)
		}
		if string(got) != string(wantData) {
			t.Errorf("transfer-%d: data mismatch", i)
		}
	}
}

func TestNodeFailure(t *testing.T) {
	ring := NewTestRing()
	for _, pos := range []byte{0, 64, 128, 192} {
		ring.AddNode(pos)
	}
	ring.StabilizeRounds(30)

	t.Log("Removing node 64")
	ring.RemoveNode(64)
	ring.StabilizeRounds(30)

	n0 := ring.FindNode(0)
	if testNodeIDVal(n0.getSuccessor().ID) != 128 {
		t.Errorf("after killing 64, n0.successor = %d, want 128",
			testNodeIDVal(n0.getSuccessor().ID))
	}

	for _, pos := range []byte{10, 65, 100, 200} {
		id := testNodeID(pos)
		got, err := n0.FindSuccessor(context.Background(), id)
		if err != nil {
			t.Fatalf("FindSuccessor(%d) after failure: %v", pos, err)
		}
		found := false
		for _, alive := range ring.Nodes {
			if got.ID.Equal(alive.id) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("FindSuccessor(%d) returned dead node %s", pos, got.ID)
		}
	}
}

func TestLookupConsistency(t *testing.T) {
	ring := NewTestRing()
	for _, pos := range []byte{10, 50, 100, 150, 200} {
		ring.AddNode(pos)
	}
	ring.StabilizeRounds(30)

	for _, pos := range []byte{0, 25, 75, 125, 175, 225, 255} {
		id := testNodeID(pos)
		var first byte = 255
		for _, node := range ring.Nodes {
			got, err := node.FindSuccessor(context.Background(), id)
			if err != nil {
				t.Fatalf("node %d FindSuccessor(%d): %v",
					testNodeIDVal(node.id), pos, err)
			}
			val := testNodeIDVal(got.ID)
			if first == 255 {
				first = val
			} else if val != first {
				t.Errorf("inconsistency: key %d → node %d says %d but first said %d",
					pos, testNodeIDVal(node.id), val, first)
			}
		}
	}
}

func TestManyNodes(t *testing.T) {
	ring := NewTestRing()

	positions := []byte{3, 19, 35, 51, 67, 83, 99, 115, 131, 147, 163, 179, 195, 211, 227, 243}
	for _, pos := range positions {
		ring.AddNode(pos)
	}
	ring.StabilizeRounds(60)

	for _, n := range ring.Nodes {
		if n.getSuccessor().ID.Equal(n.id) && len(ring.Nodes) > 1 {
			t.Errorf("node %d: successor is itself in a multi-node ring", testNodeIDVal(n.id))
		}
		if n.getPredecessor() == nil {
			t.Errorf("node %d: predecessor is nil after stabilization", testNodeIDVal(n.id))
		}
	}

	ctx := context.Background()
	first := ring.Nodes[0]
	last := ring.Nodes[len(ring.Nodes)-1]

	for i := 0; i < 100; i++ {
		key, data := testBlock(fmt.Sprintf("stress-%d", i))
		if err := first.Put(ctx, key, data); err != nil {
			t.Fatalf("Put stress-%d: %v", i, err)
		}
	}

	for i := 0; i < 100; i++ {
		key, wantData := testBlock(fmt.Sprintf("stress-%d", i))
		got, err := last.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get stress-%d: %v", i, err)
		}
		if string(got) != string(wantData) {
			t.Errorf("stress-%d: data mismatch", i)
		}
	}
}

func TestNetworkPartition(t *testing.T) {
	ring := NewTestRing()
	ring.AddNode(0)
	ring.AddNode(100)
	ring.StabilizeRounds(20)

	ring.Partition(0, 100)
	ring.StabilizeRounds(5)

	n0 := ring.FindNode(0)
	n0.checkPredecessor()
	ring.StabilizeRounds(5)

	ring.Heal(0, 100)
	ring.StabilizeRounds(20)

	if testNodeIDVal(n0.getSuccessor().ID) != 100 {
		t.Errorf("after partition heal: n0.successor = %d, want 100",
			testNodeIDVal(n0.getSuccessor().ID))
	}
}

func TestConcurrentPut(t *testing.T) {
	ring := NewTestRing()
	for _, pos := range []byte{0, 64, 128, 192} {
		ring.AddNode(pos)
	}
	ring.StabilizeRounds(30)

	ctx := context.Background()
	const numBlocks = 100

	var wg sync.WaitGroup
	var errCount int32

	wg.Add(numBlocks)
	for i := 0; i < numBlocks; i++ {
		go func(i int) {
			defer wg.Done()
			node := ring.Nodes[i%len(ring.Nodes)]
			key, data := testBlock(fmt.Sprintf("concurrent-%d", i))
			if err := node.Put(ctx, key, data); err != nil {
				atomic.AddInt32(&errCount, 1)
			}
		}(i)
	}
	wg.Wait()

	if errCount > 0 {
		t.Errorf("%d Put operations failed", errCount)
	}

	reader := ring.Nodes[len(ring.Nodes)-1]
	for i := 0; i < numBlocks; i++ {
		key, wantData := testBlock(fmt.Sprintf("concurrent-%d", i))
		got, err := reader.Get(ctx, key)
		if err != nil {
			t.Errorf("Get concurrent-%d: %v", i, err)
			continue
		}
		if string(got) != string(wantData) {
			t.Errorf("concurrent-%d: data mismatch", i)
		}
	}
}

func TestDumpAll(t *testing.T) {
	ring := NewTestRing()
	for _, pos := range []byte{0, 50, 100} {
		ring.AddNode(pos)
	}
	ring.StabilizeRounds(20)

	out := ring.DumpAll()
	if len(out) == 0 {
		t.Fatal("DumpAll returned empty string")
	}
	t.Log(out)
}
