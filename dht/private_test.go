package dht

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"sort"
	"sync"
	"testing"
	"time"

	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	format "github.com/ipfs/go-ipld-format"

	"github.com/mjagos0/datarings/store"
)

type privateTestNode struct {
	t		*testing.T
	ident		*Identity
	ring		*PrivateDring
	multiaddr	string
	store		*testMemStore
}

type privateTestRing struct {
	t	*testing.T
	grp	*GroupIdentity
	nodes	[]*privateTestNode

	publicRing	*TestRing
	publicDrings	map[NodeID]*PublicDring
}

func newPrivateTestRing(t *testing.T) *privateTestRing {
	t.Helper()
	grp, err := GenerateGroupIdentity()
	if err != nil {
		t.Fatalf("GenerateGroupIdentity: %v", err)
	}
	pubRing := NewTestRing()
	return &privateTestRing{
		t:		t,
		grp:		grp,
		publicRing:	pubRing,
		publicDrings:	make(map[NodeID]*PublicDring),
	}
}

func (r *privateTestRing) addNode() *privateTestNode {
	r.t.Helper()

	ident, err := GenerateIdentity()
	if err != nil {
		r.t.Fatalf("GenerateIdentity: %v", err)
	}

	bs := newTestMemStore()
	cfg := Config{Replication: 1}
	pd := NewPrivateDring(r.grp, ident, bs, nil, cfg)

	boundAddr, err := pd.StartServer("/ip4/127.0.0.1/tcp/0", "")
	if err != nil {
		r.t.Fatalf("StartServer: %v", err)
	}

	n := &privateTestNode{
		t:		r.t,
		ident:		ident,
		ring:		pd,
		multiaddr:	boundAddr,
		store:		bs,
	}

	if len(r.nodes) == 0 {
		pd.Create()
	} else {
		bootstrap := r.nodes[0]
		peer := NodeAddr{ID: bootstrap.ident.ID, Addr: bootstrap.multiaddr}
		if err := pd.Join(context.Background(), peer); err != nil {
			r.t.Fatalf("Join: %v", err)
		}
	}

	r.nodes = append(r.nodes, n)
	return n
}

func (r *privateTestRing) stabilize() {
	for _, n := range r.nodes {
		n.ring.Node().stabilize()
	}
	for _, n := range r.nodes {
		n.ring.Node().fixFinger()
	}
	for _, n := range r.nodes {
		n.ring.Node().checkPredecessor()
	}
}

func (r *privateTestRing) stabilizeRounds(rounds int) {
	for i := 0; i < rounds; i++ {
		r.stabilize()
	}
}

func (r *privateTestRing) cleanup() {
	for _, n := range r.nodes {
		n.ring.Stop()
	}
}

func (r *privateTestRing) findNode(ident *Identity) *privateTestNode {
	for _, n := range r.nodes {
		if n.ident.ID == ident.ID {
			return n
		}
	}
	return nil
}

func TestAuth_MutualAuthentication(t *testing.T) {
	l, err := newTCPListener()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()

	psk := []byte("test-psk-32-bytes-exactly!!!!!!!!")
	serverID := testNodeID(200)
	clientID := testNodeID(100)

	errCh := make(chan error, 1)
	clientIDCh := make(chan NodeID, 1)

	go func() {
		conn, err := l.Accept()
		if err != nil {
			errCh <- err
			return
		}
		id, err := PerformServerAuth(conn, psk, serverID)
		if err != nil {
			conn.Close()
			errCh <- err
			return
		}
		clientIDCh <- id
		conn.Close()
		errCh <- nil
	}()

	conn, err := dialTCP(l.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if err := PerformClientAuth(conn, psk, clientID, serverID); err != nil {
		t.Fatalf("client auth: %v", err)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("server auth: %v", err)
	}
	if got := <-clientIDCh; got != clientID {
		t.Errorf("server got client ID %s, want %s", got, clientID)
	}
}

func TestAuth_WrongPSK_Rejected(t *testing.T) {
	l, err := newTCPListener()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()

	correctPSK := []byte("correct-psk-32-bytes-exactly!!!!!")
	wrongPSK := []byte("wrong---psk-32-bytes-exactly!!!!!")
	serverID := testNodeID(200)
	clientID := testNodeID(100)

	serverErrCh := make(chan error, 1)
	go func() {
		conn, _ := l.Accept()
		_, err := PerformServerAuth(conn, correctPSK, serverID)
		conn.Close()
		serverErrCh <- err
	}()

	conn, err := dialTCP(l.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	clientErr := PerformClientAuth(conn, wrongPSK, clientID, serverID)
	serverErr := <-serverErrCh

	if clientErr == nil && serverErr == nil {
		t.Fatal("expected auth to fail with wrong PSK, but both sides succeeded")
	}
	t.Logf("client err: %v, server err: %v", clientErr, serverErr)
}

func TestAuth_UnauthenticatedConnection_Rejected(t *testing.T) {
	grp, _ := GenerateGroupIdentity()
	ident, _ := GenerateIdentity()
	bs := newTestMemStore()
	cfg := Config{Replication: 1}
	pd := NewPrivateDring(grp, ident, bs, nil, cfg)
	addr, err := pd.StartServer("/ip4/127.0.0.1/tcp/0", "")
	if err != nil {
		t.Fatalf("StartServer: %v", err)
	}
	defer pd.Stop()

	tcpAddr, _ := MultiaddrToTCPAddr(addr)
	conn, err := net.DialTimeout("tcp", tcpAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	nonce := make([]byte, authNonceSize)
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(conn, nonce); err != nil {
		t.Fatalf("expected server nonce (%d bytes), got error: %v", authNonceSize, err)
	}

	garbage := make([]byte, 20+authHMACSize+authNonceSize)
	for i := range garbage {
		garbage[i] = byte(i % 256)
	}
	if _, err := conn.Write(garbage); err != nil {
		t.Fatalf("write garbage: %v", err)
	}

	buf := make([]byte, 1)
	conn.SetDeadline(time.Now().Add(15 * time.Second))
	_, readErr := conn.Read(buf)
	if readErr == nil {
		t.Fatal("expected server to close connection after invalid auth response, but Read succeeded")
	}
}

func TestAuth_ServerReturnsClientID(t *testing.T) {
	l, _ := newTCPListener()
	defer l.Close()

	grp, _ := GenerateGroupIdentity()
	psk := grp.PSK
	serverID := testNodeID(10)
	clientID := testNodeID(20)

	gotIDCh := make(chan NodeID, 1)
	go func() {
		conn, _ := l.Accept()
		id, _ := PerformServerAuth(conn, psk, serverID)
		conn.Close()
		gotIDCh <- id
	}()

	conn, _ := dialTCP(l.Addr().String())
	_ = PerformClientAuth(conn, psk, clientID, serverID)
	conn.Close()

	if got := <-gotIDCh; got != clientID {
		t.Errorf("server got %s, want %s", got, clientID)
	}
}

func TestAuth_HMACInputOrdering(t *testing.T) {
	psk := []byte("test-psk-32-bytes-exactly!!!!!!!!")
	idA := testNodeID(1)
	idB := testNodeID(2)
	nonce := []byte("0123456789abcdef")

	gotClient := computeAuthHMAC(psk, nonce, idA, idB)
	mac := hmac.New(sha256.New, psk)
	mac.Write(nonce)
	mac.Write(idA[:])
	mac.Write(idB[:])
	wantClient := mac.Sum(nil)
	if !bytes.Equal(gotClient, wantClient) {
		t.Errorf("client HMAC: computeAuthHMAC(psk, nonce, idA, idB) does not equal HMAC(psk, nonce||idA||idB)")
	}

	gotServer := computeAuthHMAC(psk, nonce, idB, idA)
	mac2 := hmac.New(sha256.New, psk)
	mac2.Write(nonce)
	mac2.Write(idB[:])
	mac2.Write(idA[:])
	wantServer := mac2.Sum(nil)
	if !bytes.Equal(gotServer, wantServer) {
		t.Errorf("server HMAC: computeAuthHMAC(psk, nonce, idB, idA) does not equal HMAC(psk, nonce||idB||idA)")
	}

	if bytes.Equal(gotClient, gotServer) {
		t.Error("client and server HMACs are identical — ID arguments may be symmetric (unexpected)")
	}
}

func TestPrivateDring_SuccessorPredecessorInvariant(t *testing.T) {
	r := newPrivateTestRing(t)
	defer r.cleanup()

	const nNodes = 5
	nodes := make([]*privateTestNode, nNodes)
	for i := range nodes {
		nodes[i] = r.addNode()
	}
	r.stabilizeRounds(60)

	sorted := make([]*privateTestNode, nNodes)
	copy(sorted, nodes)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].ident.ID.Less(sorted[j].ident.ID)
	})

	for i, n := range sorted {
		wantSucc := sorted[(i+1)%nNodes].ident.ID
		gotSucc := n.ring.Node().getSuccessor().ID
		if gotSucc != wantSucc {
			t.Errorf("node %s: successor = %s, want %s",
				n.ident.ID, gotSucc, wantSucc)
		}

		wantPred := sorted[(i-1+nNodes)%nNodes].ident.ID
		pred := n.ring.Node().getPredecessor()
		if pred == nil {
			t.Errorf("node %s: predecessor is nil, want %s", n.ident.ID, wantPred)
			continue
		}
		if pred.ID != wantPred {
			t.Errorf("node %s: predecessor = %s, want %s",
				n.ident.ID, pred.ID, wantPred)
		}
	}
}

func TestPrivateDring_BlockPutGet(t *testing.T) {
	r := newPrivateTestRing(t)
	defer r.cleanup()

	b := r.addNode()
	n2 := r.addNode()
	r.stabilizeRounds(40)

	ctx := context.Background()

	const nBlocks = 20
	for i := 0; i < nBlocks; i++ {
		key, data := testBlock(fmt.Sprintf("private-block-%d", i))
		if err := b.ring.Node().Put(ctx, key, data); err != nil {
			t.Fatalf("Put block-%d: %v", i, err)
		}
	}

	for i := 0; i < nBlocks; i++ {
		key, wantData := testBlock(fmt.Sprintf("private-block-%d", i))
		got, err := n2.ring.Node().Get(ctx, key)
		if err != nil {
			t.Fatalf("n2 Get block-%d: %v", i, err)
		}
		if string(got) != string(wantData) {
			t.Errorf("block-%d: data mismatch", i)
		}
	}
}

func TestPrivateDring_KeyTransferOnJoin(t *testing.T) {
	r := newPrivateTestRing(t)
	defer r.cleanup()

	b := r.addNode()
	r.stabilizeRounds(10)

	ctx := context.Background()
	const nBlocks = 30
	for i := 0; i < nBlocks; i++ {
		key, data := testBlock(fmt.Sprintf("transfer-%d", i))
		if err := b.ring.Node().Put(ctx, key, data); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if b.ring.Node().DataCount() != nBlocks {
		t.Fatalf("expected %d blocks before join, got %d", nBlocks, b.ring.Node().DataCount())
	}

	n2 := r.addNode()
	r.stabilizeRounds(40)

	total := b.ring.Node().DataCount() + n2.ring.Node().DataCount()
	if total != nBlocks {
		t.Errorf("block count changed after join: got %d, want %d", total, nBlocks)
	}
	if n2.ring.Node().DataCount() == 0 {
		t.Error("new private-ring node received 0 blocks — key transfer broken")
	}

	for i := 0; i < nBlocks; i++ {
		key, wantData := testBlock(fmt.Sprintf("transfer-%d", i))
		got, err := n2.ring.Node().Get(ctx, key)
		if err != nil {
			t.Fatalf("n2 Get transfer-%d: %v", i, err)
		}
		if string(got) != string(wantData) {
			t.Errorf("transfer-%d: data mismatch", i)
		}
	}
}

func TestPrivateDring_RoutingConsistency(t *testing.T) {
	r := newPrivateTestRing(t)
	defer r.cleanup()

	const nNodes = 4
	for i := 0; i < nNodes; i++ {
		r.addNode()
	}
	r.stabilizeRounds(60)

	ctx := context.Background()

	queryPositions := []NodeID{
		testNodeID(0),
		testNodeID(64),
		testNodeID(128),
		testNodeID(192),
		testNodeID(255),
	}

	for _, id := range queryPositions {
		var first NodeID
		firstSet := false
		for _, n := range r.nodes {
			got, err := n.ring.Node().FindSuccessor(ctx, id)
			if err != nil {
				t.Fatalf("FindSuccessor(%s) from %s: %v", id, n.ident.ID, err)
			}
			if !firstSet {
				first = got.ID
				firstSet = true
			} else if got.ID != first {
				t.Errorf("routing inconsistency for %s: got %s, want %s",
					id, got.ID, first)
			}
		}
	}
}

func TestPrivateDring_WrongPSK_CannotJoin(t *testing.T) {
	r := newPrivateTestRing(t)
	defer r.cleanup()

	bootstrap := r.addNode()
	r.stabilizeRounds(10)

	impostorGrp, _ := GenerateGroupIdentity()
	impostorIdent, _ := GenerateIdentity()
	bs := newTestMemStore()
	cfg := Config{Replication: 1}
	impostor := NewPrivateDring(impostorGrp, impostorIdent, bs, nil, cfg)
	_, err := impostor.StartServer("/ip4/127.0.0.1/tcp/0", "")
	if err != nil {
		t.Fatalf("StartServer for impostor: %v", err)
	}
	defer impostor.Stop()

	bootstrapPeer := NodeAddr{ID: bootstrap.ident.ID, Addr: bootstrap.multiaddr}
	err = impostor.Join(context.Background(), bootstrapPeer)
	if err == nil {
		t.Fatal("impostor with wrong PSK should not be able to join")
	}
	t.Logf("impostor correctly rejected: %v", err)
}

func TestPrivateDring_FingerTableCorrectness(t *testing.T) {
	r := newPrivateTestRing(t)
	defer r.cleanup()

	const nNodes = 4
	for i := 0; i < nNodes; i++ {
		r.addNode()
	}
	r.stabilizeRounds(300)

	positions := make([]NodeID, len(r.nodes))
	for i, n := range r.nodes {
		positions[i] = n.ident.ID
	}

	sort.Slice(positions, func(i, j int) bool {
		return positions[i].Less(positions[j])
	})

	for _, n := range r.nodes {
		node := n.ring.Node()
		node.mu.RLock()
		selfPos := node.id

		for _, fi := range []int{152, 153, 154, 155, 156, 157, 158, 159} {
			start := selfPos.fingerStart(fi)

			expected := fingerSuccessorForPositions(start, positions)
			got := node.fingers[fi].ID
			if got != expected {
				t.Errorf("node %s finger[%d]: start=%s want=%s got=%s",
					selfPos, fi, start, expected, got)
			}
		}
		node.mu.RUnlock()
	}
}

func fingerSuccessorForPositions(start NodeID, positions []NodeID) NodeID {
	sorted := make([]NodeID, len(positions))
	copy(sorted, positions)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Less(sorted[j]) })

	for _, p := range sorted {
		if p == start || start.Less(p) {
			return p
		}
	}
	return sorted[0]
}

func TestPrivateDring_JoinViaPublicDring(t *testing.T) {

	pubRing := NewTestRing()
	pubIdent, _ := GenerateIdentity()
	pubBs := newTestMemStore()
	pubLt := &localTransport{ring: pubRing, nodeID: pubIdent.ID}
	pubNode := NewNode(pubIdent.ID, "pub-node", pubBs, nil, pubLt, Config{})
	pubRing.mu.Lock()
	pubRing.Nodes = append(pubRing.Nodes, pubNode)
	pubRing.nodeMap[pubIdent.ID] = pubNode
	pubRing.mu.Unlock()
	pubNode.Create()
	publicDring := NewPublicDring(pubNode, pubIdent)

	ctx := context.Background()
	grp, _ := GenerateGroupIdentity()

	founderIdent, _ := GenerateIdentity()
	founderBs := newTestMemStore()
	founderPD := NewPrivateDring(grp, founderIdent, founderBs, nil, Config{Replication: 1})
	founderAddr, err := founderPD.StartServer("/ip4/127.0.0.1/tcp/0", "")
	if err != nil {
		t.Fatalf("founder StartServer: %v", err)
	}
	defer founderPD.Stop()
	founderPD.Create()

	founderPeerRec, err := NewPeerIdentityRecord(founderIdent, 1, founderAddr, map[string]string{
		grp.GroupID.String(): founderPD.Multiaddr(),
	})
	if err != nil {
		t.Fatalf("NewPeerIdentityRecord for founder: %v", err)
	}
	founderPeerData, _ := founderPeerRec.Encode()
	if err := pubNode.RecordPut(ctx, founderIdent.ID, founderPeerData); err != nil {
		t.Fatalf("store founder PeerIdentityRecord: %v", err)
	}

	if err := publicDring.PublishGroup(ctx, grp, 1, []GroupMember{{ID: founderIdent.ID}}); err != nil {
		t.Fatalf("PublishGroup: %v", err)
	}

	joinerIdent, _ := GenerateIdentity()
	joinerBs := newTestMemStore()
	joinerPD := NewPrivateDring(grp, joinerIdent, joinerBs, nil, Config{Replication: 1})
	_, err = joinerPD.StartServer("/ip4/127.0.0.1/tcp/0", "")
	if err != nil {
		t.Fatalf("joiner StartServer: %v", err)
	}
	defer joinerPD.Stop()

	if err := joinerPD.JoinViaPublicDring(ctx, publicDring); err != nil {
		t.Fatalf("JoinViaPublicDring: %v", err)
	}

	if err := joinerPD.UpdateGroupRecord(ctx, publicDring); err != nil {
		t.Fatalf("UpdateGroupRecord: %v", err)
	}

	for i := 0; i < 40; i++ {
		founderPD.Node().stabilize()
		joinerPD.Node().stabilize()
		founderPD.Node().fixFinger()
		joinerPD.Node().fixFinger()
		founderPD.Node().checkPredecessor()
		joinerPD.Node().checkPredecessor()
	}

	rec, err := publicDring.LookupGroup(ctx, grp.GroupID)
	if err != nil {
		t.Fatalf("LookupGroup after joiner update: %v", err)
	}
	if len(rec.Data.Peers) != 2 {
		t.Errorf("expected 2 peers in GroupIdentityRecord, got %d", len(rec.Data.Peers))
	}

	key, data := testBlock("private-dag-block")
	if err := founderPD.Node().Put(ctx, key, data); err != nil {
		t.Fatalf("founder Put: %v", err)
	}
	got, err := joinerPD.Node().Get(ctx, key)
	if err != nil {
		t.Fatalf("joiner Get: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("block data mismatch after private-ring transfer")
	}

	if !founderPD.IsVerified(joinerIdent.ID) {
		t.Error("founder should have joiner as verified after successful auth")
	}
}

func TestPrivateDring_MerkleDAGFetch(t *testing.T) {
	r := newPrivateTestRing(t)
	defer r.cleanup()

	putter := r.addNode()
	fetcher := r.addNode()
	r.stabilizeRounds(40)

	ctx := context.Background()

	leaf1Data := []byte("private dag leaf 1")
	leaf2Data := []byte("private dag leaf 2")
	leaf3Data := []byte("private dag leaf 3")

	leaf1CID := testCID(leaf1Data)
	leaf2CID := testCID(leaf2Data)
	leaf3CID := testCID(leaf3Data)

	mid1 := merkledag.NodeWithData(nil)
	mustAddLink(t, mid1, "l1", leaf1CID, uint64(len(leaf1Data)))
	mustAddLink(t, mid1, "l2", leaf2CID, uint64(len(leaf2Data)))
	mid1Data := mid1.RawData()
	mid1CID := mid1.Cid()

	mid2 := merkledag.NodeWithData(nil)
	mustAddLink(t, mid2, "l3", leaf3CID, uint64(len(leaf3Data)))
	mid2Data := mid2.RawData()
	mid2CID := mid2.Cid()

	root := merkledag.NodeWithData(nil)
	mustAddLink(t, root, "m1", mid1CID, uint64(len(mid1Data)))
	mustAddLink(t, root, "m2", mid2CID, uint64(len(mid2Data)))
	rootData := root.RawData()
	rootCID := root.Cid()

	allBlocks := []struct {
		c	interface{ Bytes() []byte }
		data	[]byte
	}{
		{leaf1CID, leaf1Data},
		{leaf2CID, leaf2Data},
		{leaf3CID, leaf3Data},
		{mid1CID, mid1Data},
		{mid2CID, mid2Data},
		{rootCID, rootData},
	}

	for _, b := range allBlocks {
		c, _ := b.c.(interface{ Bytes() []byte })
		_ = c
	}

	if err := putter.ring.Node().Put(ctx, leaf1CID, leaf1Data); err != nil {
		t.Fatalf("put leaf1: %v", err)
	}
	if err := putter.ring.Node().Put(ctx, leaf2CID, leaf2Data); err != nil {
		t.Fatalf("put leaf2: %v", err)
	}
	if err := putter.ring.Node().Put(ctx, leaf3CID, leaf3Data); err != nil {
		t.Fatalf("put leaf3: %v", err)
	}
	if err := putter.ring.Node().Put(ctx, mid1CID, mid1Data); err != nil {
		t.Fatalf("put mid1: %v", err)
	}
	if err := putter.ring.Node().Put(ctx, mid2CID, mid2Data); err != nil {
		t.Fatalf("put mid2: %v", err)
	}
	if err := putter.ring.Node().Put(ctx, rootCID, rootData); err != nil {
		t.Fatalf("put root: %v", err)
	}

	if err := fetcher.ring.Node().FetchDAG(ctx, rootCID); err != nil {
		t.Fatalf("FetchDAG: %v", err)
	}

	allCIDs := []interface{}{leaf1CID, leaf2CID, leaf3CID, mid1CID, mid2CID, rootCID}
	_ = allCIDs

	for _, pair := range []struct {
		name	string
		data	[]byte
	}{
		{"leaf1", leaf1Data},
		{"leaf2", leaf2Data},
		{"leaf3", leaf3Data},
		{"mid1", mid1Data},
		{"mid2", mid2Data},
		{"root", rootData},
	} {
		var c interface {
			Has(ctx context.Context) (bool, error)
		}
		_ = c

		switch pair.name {
		case "leaf1":
			has, _ := fetcher.store.Has(ctx, leaf1CID)
			if !has {
				t.Errorf("fetcher missing %s after FetchDAG", pair.name)
			}
		case "leaf2":
			has, _ := fetcher.store.Has(ctx, leaf2CID)
			if !has {
				t.Errorf("fetcher missing %s after FetchDAG", pair.name)
			}
		case "leaf3":
			has, _ := fetcher.store.Has(ctx, leaf3CID)
			if !has {
				t.Errorf("fetcher missing %s after FetchDAG", pair.name)
			}
		case "mid1":
			has, _ := fetcher.store.Has(ctx, mid1CID)
			if !has {
				t.Errorf("fetcher missing %s after FetchDAG", pair.name)
			}
		case "mid2":
			has, _ := fetcher.store.Has(ctx, mid2CID)
			if !has {
				t.Errorf("fetcher missing %s after FetchDAG", pair.name)
			}
		case "root":
			has, _ := fetcher.store.Has(ctx, rootCID)
			if !has {
				t.Errorf("fetcher missing %s after FetchDAG", pair.name)
			}
		}
	}

	rootDataFetcher, err := fetcher.store.Get(ctx, rootCID)
	if err != nil {
		t.Fatalf("get root from fetcher: %v", err)
	}
	links, err := store.LinksOf(rootCID, rootDataFetcher)
	if err != nil {
		t.Fatalf("LinksOf root: %v", err)
	}
	if len(links) != 2 {
		t.Errorf("root should have 2 links, got %d", len(links))
	}
}

func TestPrivateDring_SequentialJoins(t *testing.T) {
	r := newPrivateTestRing(t)
	defer r.cleanup()

	founder := r.addNode()
	r.stabilizeRounds(10)

	succ := founder.ring.Node().getSuccessor()
	if succ.ID != founder.ident.ID {
		t.Errorf("single-node ring: founder successor should be self, got %s", succ.ID)
	}

	n2 := r.addNode()
	r.stabilizeRounds(40)

	if founder.ring.Node().getSuccessor().ID != n2.ident.ID &&
		n2.ring.Node().getSuccessor().ID != founder.ident.ID {

	}

	if founder.ring.Node().getSuccessor().ID == founder.ident.ID {
		t.Errorf("after 2nd join: founder still points to itself as successor")
	}

	n3 := r.addNode()
	r.stabilizeRounds(40)
	n4 := r.addNode()
	r.stabilizeRounds(40)

	_ = n2
	_ = n3
	_ = n4

	for _, n := range r.nodes {
		if n.ring.Node().getSuccessor().ID == n.ident.ID {
			t.Errorf("node %s still points to itself after %d-node ring",
				n.ident.ID, len(r.nodes))
		}
		if n.ring.Node().getPredecessor() == nil {
			t.Errorf("node %s has nil predecessor after stabilization", n.ident.ID)
		}
	}
}

func TestPrivateDring_DataDistribution(t *testing.T) {
	r := newPrivateTestRing(t)
	defer r.cleanup()

	const nNodes = 4
	for i := 0; i < nNodes; i++ {
		r.addNode()
	}
	r.stabilizeRounds(40)

	ctx := context.Background()
	first := r.nodes[0]

	const nBlocks = 50
	for i := 0; i < nBlocks; i++ {
		key, data := testBlock(fmt.Sprintf("dist-%d", i))
		if err := first.ring.Node().Put(ctx, key, data); err != nil {
			t.Fatalf("Put dist-%d: %v", i, err)
		}
	}

	total := 0
	for _, n := range r.nodes {
		count := n.ring.Node().DataCount()
		total += count
		t.Logf("node %s has %d blocks", n.ident.ID, count)
	}

	if total != nBlocks {
		t.Errorf("total block count: got %d, want %d", total, nBlocks)
	}

	nodesWithBlocks := 0
	for _, n := range r.nodes {
		if n.ring.Node().DataCount() > 0 {
			nodesWithBlocks++
		}
	}
	if nodesWithBlocks < 2 {
		t.Errorf("only %d/%d nodes have blocks — distribution broken", nodesWithBlocks, nNodes)
	}
}

func TestPrivateDring_NotifyRejectsImpostorCallerID(t *testing.T) {
	grp, _ := GenerateGroupIdentity()

	serverIdent, _ := GenerateIdentity()
	bs := newTestMemStore()
	cfg := Config{Replication: 1}
	pd := NewPrivateDring(grp, serverIdent, bs, nil, cfg)
	serverAddr, err := pd.StartServer("/ip4/127.0.0.1/tcp/0", "")
	if err != nil {
		t.Fatalf("StartServer: %v", err)
	}
	defer pd.Stop()
	pd.Create()

	clientIdent, _ := GenerateIdentity()
	tcpAddr, _ := MultiaddrToTCPAddr(serverAddr)

	conn, err := net.DialTimeout("tcp", tcpAddr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if err := PerformClientAuth(conn, grp.PSK, clientIdent.ID, serverIdent.ID); err != nil {
		t.Fatalf("PSK auth: %v", err)
	}

	client := rpc.NewClient(conn)
	defer client.Close()

	correctArgs := &ArgsNotify{
		Caller: NodeAddr{ID: clientIdent.ID, Addr: serverAddr},
	}
	if err := client.Call("ChordNode.Notify", correctArgs, &ReplyNotify{}); err != nil {
		t.Fatalf("Notify with own caller ID rejected (want success): %v", err)
	}

	impostorIdent, _ := GenerateIdentity()
	impostorArgs := &ArgsNotify{
		Caller: NodeAddr{ID: impostorIdent.ID, Addr: serverAddr},
	}
	if err := client.Call("ChordNode.Notify", impostorArgs, &ReplyNotify{}); err == nil {
		t.Fatal("Notify with spoofed caller ID accepted — message-level auth gate missing")
	} else {
		t.Logf("spoofed Notify correctly rejected: %v", err)
	}
}

func TestPrivateDring_BlockReplication(t *testing.T) {

	const k = 3
	const nNodes = k + 2

	grp, _ := GenerateGroupIdentity()
	cfg := Config{Replication: k}

	type pNode struct {
		ident	*Identity
		ring	*PrivateDring
		store	*testMemStore
		addr	string
	}

	nodes := make([]*pNode, 0, nNodes)
	for i := 0; i < nNodes; i++ {
		ident, _ := GenerateIdentity()
		bs := newTestMemStore()
		pd := NewPrivateDring(grp, ident, bs, nil, cfg)
		addr, err := pd.StartServer("/ip4/127.0.0.1/tcp/0", "")
		if err != nil {
			t.Fatalf("node %d StartServer: %v", i, err)
		}
		t.Cleanup(func() { pd.Stop() })
		nodes = append(nodes, &pNode{ident: ident, ring: pd, store: bs, addr: addr})
	}

	nodes[0].ring.Create()

	for i := 1; i < nNodes; i++ {
		peer := NodeAddr{ID: nodes[0].ident.ID, Addr: nodes[0].addr}
		if err := nodes[i].ring.Join(context.Background(), peer); err != nil {
			t.Fatalf("node %d Join: %v", i, err)
		}
	}

	stabilize := func() {
		for _, n := range nodes {
			n.ring.Node().stabilize()
		}
		for _, n := range nodes {
			n.ring.Node().fixFinger()
		}
		for _, n := range nodes {
			n.ring.Node().checkPredecessor()
		}
	}
	for i := 0; i < 60; i++ {
		stabilize()
	}

	ctx := context.Background()

	key, data := testBlock("replication-test-block")
	if err := nodes[0].ring.Node().Put(ctx, key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	for _, n := range nodes {
		n.ring.Node().WaitForReplicationDrain(2 * time.Second)
	}

	ringKey := CIDToNodeID(key)
	responsible, err := nodes[0].ring.Node().FindSuccessor(ctx, ringKey)
	if err != nil {
		t.Fatalf("FindSuccessor: %v", err)
	}

	currentID := responsible.ID
	for i := 0; i < k; i++ {

		var found *pNode
		for _, n := range nodes {
			if n.ident.ID == currentID {
				found = n
				break
			}
		}
		if found == nil {
			t.Fatalf("replica node %s not found in test ring (hop %d)", currentID, i)
		}

		has, err := found.store.Has(ctx, key)
		if err != nil {
			t.Fatalf("Has at replica %d (%s): %v", i, currentID, err)
		}
		if !has {
			t.Errorf("block not replicated to hop %d (node %s), want replication to k=%d successors",
				i, currentID, k)
		}

		if i < k-1 {
			currentID = found.ring.Node().getSuccessor().ID
		}
	}
}

func TestPrivateDring_GracefulLeave(t *testing.T) {

	pubRing := NewTestRing()
	pubIdent, _ := GenerateIdentity()
	pubBs := newTestMemStore()
	pubLt := &localTransport{ring: pubRing, nodeID: pubIdent.ID}
	pubNode := NewNode(pubIdent.ID, "pub-node", pubBs, nil, pubLt, Config{})
	pubRing.mu.Lock()
	pubRing.Nodes = append(pubRing.Nodes, pubNode)
	pubRing.nodeMap[pubIdent.ID] = pubNode
	pubRing.mu.Unlock()
	pubNode.Create()
	publicDring := NewPublicDring(pubNode, pubIdent)

	ctx := context.Background()
	grp, _ := GenerateGroupIdentity()

	founderIdent, _ := GenerateIdentity()
	founderBs := newTestMemStore()
	founderPD := NewPrivateDring(grp, founderIdent, founderBs, nil, Config{Replication: 1})
	if _, err := founderPD.StartServer("/ip4/127.0.0.1/tcp/0", ""); err != nil {
		t.Fatalf("founder StartServer: %v", err)
	}
	defer founderPD.Stop()
	founderPD.Create()

	if err := publicDring.PublishGroup(ctx, grp, 1, []GroupMember{
		{ID: founderIdent.ID},
	}); err != nil {
		t.Fatalf("PublishGroup (founder only): %v", err)
	}

	joinerIdent, _ := GenerateIdentity()
	joinerBs := newTestMemStore()
	joinerPD := NewPrivateDring(grp, joinerIdent, joinerBs, nil, Config{Replication: 1})
	if _, err := joinerPD.StartServer("/ip4/127.0.0.1/tcp/0", ""); err != nil {
		t.Fatalf("joiner StartServer: %v", err)
	}
	joinerPD.Create()

	if err := publicDring.PublishGroup(ctx, grp, 2, []GroupMember{
		{ID: founderIdent.ID},
		{ID: joinerIdent.ID},
	}); err != nil {
		t.Fatalf("PublishGroup (2 members): %v", err)
	}

	rec, err := publicDring.LookupGroup(ctx, grp.GroupID)
	if err != nil {
		t.Fatalf("LookupGroup (before leave): %v", err)
	}
	if len(rec.Data.Peers) != 2 {
		t.Fatalf("expected 2 members before leave, got %d", len(rec.Data.Peers))
	}

	if err := joinerPD.LeaveGroup(ctx, publicDring); err != nil {
		t.Fatalf("LeaveGroup: %v", err)
	}
	joinerPD.Stop()

	recAfter, err := publicDring.LookupGroup(ctx, grp.GroupID)
	if err != nil {
		t.Fatalf("LookupGroup (after leave): %v", err)
	}
	for _, m := range recAfter.Data.Peers {
		if m.ID == joinerIdent.ID {
			t.Error("joiner still present in GroupIdentityRecord after LeaveGroup")
		}
	}
	if len(recAfter.Data.Peers) != 1 {
		t.Errorf("expected 1 member after leave, got %d", len(recAfter.Data.Peers))
	}
}

func TestTTL_GroupIdentityRecord_Republished(t *testing.T) {
	ring := NewTestRing()
	ctx := context.Background()
	cfg := shortTTLConfig()

	pd, ident := makePublicDringWithCfg(t, ring, cfg, nil)
	ring.StabilizeRounds(10)

	grp, _ := GenerateGroupIdentity()
	peers := []GroupMember{{ID: ident.ID}}

	if err := pd.PublishGroup(ctx, grp, 1, peers); err != nil {
		t.Fatalf("initial PublishGroup: %v", err)
	}

	rec1, err := pd.LookupGroup(ctx, grp.GroupID)
	if err != nil {
		t.Fatalf("LookupGroup before expiry: %v", err)
	}
	v1 := rec1.Data.Version

	bs := newTestMemStore()
	privCfg := Config{Replication: 1}
	privDring := NewPrivateDring(grp, ident, bs, nil, privCfg)
	_, err = privDring.StartServer("/ip4/127.0.0.1/tcp/0", "")
	if err != nil {
		t.Fatalf("PrivateDring StartServer: %v", err)
	}
	defer privDring.Stop()
	privDring.Create()

	pd.RegisterGroupRepublisher(func(c context.Context) error {
		return privDring.UpdateGroupRecord(c, pd)
	})
	pd.node.StartBackground(cfg)
	pd.StartRepublishLoop(pd.node.StopCh(), cfg)
	defer pd.node.Stop()

	time.Sleep(6 * time.Second)

	rec2, err := pd.LookupGroup(ctx, grp.GroupID)
	if err != nil {
		t.Fatalf("LookupGroup after republish: %v", err)
	}
	if rec2.Data.Version <= v1 {
		t.Errorf("expected GroupIdentityRecord version > %d after republish, got %d",
			v1, rec2.Data.Version)
	}
}

func TestPrivateDring_RemovedPeerCanStillAuthenticate(t *testing.T) {
	grp, _ := GenerateGroupIdentity()

	serverIdent, _ := GenerateIdentity()
	bs := newTestMemStore()
	cfg := Config{Replication: 1}
	pd := NewPrivateDring(grp, serverIdent, bs, nil, cfg)
	serverAddr, err := pd.StartServer("/ip4/127.0.0.1/tcp/0", "")
	if err != nil {
		t.Fatalf("StartServer: %v", err)
	}
	defer pd.Stop()
	pd.Create()

	removedIdent, _ := GenerateIdentity()

	tcpAddr, err := MultiaddrToTCPAddr(serverAddr)
	if err != nil {
		t.Fatalf("MultiaddrToTCPAddr: %v", err)
	}

	conn, err := net.DialTimeout("tcp", tcpAddr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if err := PerformClientAuth(conn, grp.PSK, removedIdent.ID, serverIdent.ID); err != nil {
		t.Fatalf("peer with correct PSK but absent from member list was rejected: %v", err)
	}
}

func TestPrivateDring_JoinViaPublicDring_AllPeersUnreachable(t *testing.T) {

	pubRing := NewTestRing()
	pubIdent, _ := GenerateIdentity()
	pubBs := newTestMemStore()
	pubLt := &localTransport{ring: pubRing, nodeID: pubIdent.ID}
	pubNode := NewNode(pubIdent.ID, "pub-node", pubBs, nil, pubLt, Config{})
	pubRing.mu.Lock()
	pubRing.Nodes = append(pubRing.Nodes, pubNode)
	pubRing.nodeMap[pubIdent.ID] = pubNode
	pubRing.mu.Unlock()
	pubNode.Create()
	publicDring := NewPublicDring(pubNode, pubIdent)

	ctx := context.Background()
	grp, _ := GenerateGroupIdentity()

	ghostIdent, _ := GenerateIdentity()
	unreachableAddr := "/ip4/127.0.0.1/tcp/19999"

	ghostPeerRec, err := NewPeerIdentityRecord(ghostIdent, 1, unreachableAddr, map[string]string{
		grp.GroupID.String(): unreachableAddr,
	})
	if err != nil {
		t.Fatalf("NewPeerIdentityRecord: %v", err)
	}
	ghostData, _ := ghostPeerRec.Encode()
	if err := pubNode.RecordPut(ctx, ghostIdent.ID, ghostData); err != nil {
		t.Fatalf("store ghost PeerIdentityRecord: %v", err)
	}

	if err := publicDring.PublishGroup(ctx, grp, 1, []GroupMember{{ID: ghostIdent.ID}}); err != nil {
		t.Fatalf("PublishGroup: %v", err)
	}

	joinerIdent, _ := GenerateIdentity()
	joinerBs := newTestMemStore()
	joinerPD := NewPrivateDring(grp, joinerIdent, joinerBs, nil, Config{Replication: 1})
	_, err = joinerPD.StartServer("/ip4/127.0.0.1/tcp/0", "")
	if err != nil {
		t.Fatalf("joiner StartServer: %v", err)
	}
	defer joinerPD.Stop()

	if err := joinerPD.JoinViaPublicDring(ctx, publicDring); err != nil {
		t.Fatalf("expected JoinViaPublicDring to succeed (seeded ring) when all peers are unreachable, got: %v", err)
	}

	state := joinerPD.Node().State()
	if state.Successor.ID != joinerIdent.ID {
		t.Errorf("expected isolated ring (successor == self), got successor %v", state.Successor.ID)
	}
	seeded := false
	for _, s := range state.SuccessorList {
		if s.ID == ghostIdent.ID {
			seeded = true
			break
		}
	}
	if !seeded {
		t.Errorf("expected ghost peer to be seeded in successor list for auto-reconnect, got list: %v", state.SuccessorList)
	}
}

func TestPrivateDring_ConcurrentJoin_AllPeersConnect(t *testing.T) {

	pubRing := NewTestRing()
	pubIdent, _ := GenerateIdentity()
	pubBs := newTestMemStore()
	pubLt := &localTransport{ring: pubRing, nodeID: pubIdent.ID}
	pubNode := NewNode(pubIdent.ID, "pub-node", pubBs, nil, pubLt, Config{})
	pubRing.mu.Lock()
	pubRing.Nodes = append(pubRing.Nodes, pubNode)
	pubRing.nodeMap[pubIdent.ID] = pubNode
	pubRing.mu.Unlock()
	pubNode.Create()
	publicDring := NewPublicDring(pubNode, pubIdent)

	ctx := context.Background()
	grp, _ := GenerateGroupIdentity()
	groupIDHex := grp.GroupID.String()
	cfg := Config{Replication: 1}

	const n = 5

	type testPeer struct {
		ident	*Identity
		pd	*PrivateDring
		addr	string
	}

	peers := make([]testPeer, n)
	for i := range peers {
		ident, _ := GenerateIdentity()
		pd := NewPrivateDring(grp, ident, newTestMemStore(), nil, cfg)
		addr, err := pd.StartServer("/ip4/127.0.0.1/tcp/0", "")
		if err != nil {
			t.Fatalf("peer %d StartServer: %v", i, err)
		}
		peers[i] = testPeer{ident: ident, pd: pd, addr: addr}
	}
	defer func() {
		for _, p := range peers {
			p.pd.Stop()
		}
	}()

	var wg sync.WaitGroup
	joinErrs := make([]error, n)
	for i, p := range peers {
		wg.Add(1)
		go func(idx int, pp testPeer) {
			defer wg.Done()

			rec, err := NewPeerIdentityRecord(pp.ident, 1, pp.addr, map[string]string{
				groupIDHex: pp.addr,
			})
			if err != nil {
				joinErrs[idx] = fmt.Errorf("NewPeerIdentityRecord: %w", err)
				return
			}
			data, _ := rec.Encode()
			if err := pubNode.RecordPut(ctx, pp.ident.ID, data); err != nil {
				joinErrs[idx] = fmt.Errorf("RecordPut: %w", err)
				return
			}

			if err := pp.pd.UpdateGroupRecord(ctx, publicDring); err != nil {
				joinErrs[idx] = fmt.Errorf("UpdateGroupRecord: %w", err)
				return
			}

			if err := pp.pd.JoinViaPublicDring(ctx, publicDring); err != nil {
				joinErrs[idx] = fmt.Errorf("JoinViaPublicDring: %w", err)
			}
		}(i, p)
	}
	wg.Wait()

	for i, err := range joinErrs {
		if err != nil {
			t.Errorf("peer %d join sequence failed: %v", i, err)
		}
	}

	for round := 0; round < 60; round++ {
		for _, p := range peers {
			p.pd.Node().stabilize()
		}
		for _, p := range peers {
			p.pd.Node().fixFinger()
		}
		for _, p := range peers {
			p.pd.Node().checkPredecessor()
		}
	}

	isolated := 0
	for _, p := range peers {
		if p.pd.Node().getSuccessor().ID == p.ident.ID {
			isolated++
		}
	}
	if isolated > 0 {
		t.Errorf("%d/%d peers isolated (successor==self) after concurrent join + stabilization", isolated, n)
		for i, p := range peers {
			state := p.pd.Node().State()
			t.Logf("  peer %d: succ=%v pred=%v succList=%d", i, state.Successor.ID, state.Predecessor, len(state.SuccessorList))
		}
	}

	groupRec, err := publicDring.LookupGroup(ctx, grp.GroupID)
	if err != nil {
		t.Fatalf("LookupGroup: %v", err)
	}
	if len(groupRec.Data.Peers) != n {
		t.Errorf("GroupIdentityRecord has %d peers, want %d", len(groupRec.Data.Peers), n)
	}
}

func TestPrivateDring_LeaveGroup_RemovesFromGroupRecord(t *testing.T) {

	pubRing := NewTestRing()
	pubIdent, _ := GenerateIdentity()
	pubBs := newTestMemStore()
	pubLt := &localTransport{ring: pubRing, nodeID: pubIdent.ID}
	pubNode := NewNode(pubIdent.ID, "pub-node", pubBs, nil, pubLt, Config{})
	pubRing.mu.Lock()
	pubRing.Nodes = append(pubRing.Nodes, pubNode)
	pubRing.nodeMap[pubIdent.ID] = pubNode
	pubRing.mu.Unlock()
	pubNode.Create()
	publicDring := NewPublicDring(pubNode, pubIdent)

	ctx := context.Background()
	grp, _ := GenerateGroupIdentity()

	founderIdent, _ := GenerateIdentity()
	founderPD := NewPrivateDring(grp, founderIdent, newTestMemStore(), nil, Config{Replication: 1})
	founderAddr, err := founderPD.StartServer("/ip4/127.0.0.1/tcp/0", "")
	if err != nil {
		t.Fatalf("founder StartServer: %v", err)
	}
	defer founderPD.Stop()
	founderPD.Create()

	founderPeerRec, _ := NewPeerIdentityRecord(founderIdent, 1, founderAddr, map[string]string{
		grp.GroupID.String(): founderPD.Multiaddr(),
	})
	founderData, _ := founderPeerRec.Encode()
	if err := pubNode.RecordPut(ctx, founderIdent.ID, founderData); err != nil {
		t.Fatalf("store founder PeerIdentityRecord: %v", err)
	}
	if err := publicDring.PublishGroup(ctx, grp, 1, []GroupMember{{ID: founderIdent.ID}}); err != nil {
		t.Fatalf("PublishGroup: %v", err)
	}

	joinerIdent, _ := GenerateIdentity()
	joinerPD := NewPrivateDring(grp, joinerIdent, newTestMemStore(), nil, Config{Replication: 1})
	_, err = joinerPD.StartServer("/ip4/127.0.0.1/tcp/0", "")
	if err != nil {
		t.Fatalf("joiner StartServer: %v", err)
	}
	defer joinerPD.Stop()

	if err := joinerPD.JoinViaPublicDring(ctx, publicDring); err != nil {
		t.Fatalf("JoinViaPublicDring: %v", err)
	}
	if err := joinerPD.UpdateGroupRecord(ctx, publicDring); err != nil {
		t.Fatalf("joiner UpdateGroupRecord: %v", err)
	}

	rec, err := publicDring.LookupGroup(ctx, grp.GroupID)
	if err != nil {
		t.Fatalf("LookupGroup before leave: %v", err)
	}
	if len(rec.Data.Peers) != 2 {
		t.Fatalf("expected 2 peers before leave, got %d", len(rec.Data.Peers))
	}

	if err := joinerPD.LeaveGroup(ctx, publicDring); err != nil {
		t.Fatalf("LeaveGroup: %v", err)
	}

	rec, err = publicDring.LookupGroup(ctx, grp.GroupID)
	if err != nil {
		t.Fatalf("LookupGroup after leave: %v", err)
	}
	for _, m := range rec.Data.Peers {
		if m.ID == joinerIdent.ID {
			t.Errorf("joiner %s still listed in GroupIdentityRecord after LeaveGroup", joinerIdent.ID)
		}
	}
}

func TestPrivateDring_BlockReReplication_OnTopologyChange(t *testing.T) {
	const k = 3
	cfg := Config{Replication: k}

	grp, err := GenerateGroupIdentity()
	if err != nil {
		t.Fatalf("GenerateGroupIdentity: %v", err)
	}

	ctx := context.Background()

	type kNode struct {
		ident	*Identity
		pd	*PrivateDring
		addr	string
		store	*testMemStore
	}

	newKNode := func(bootstrap *kNode) *kNode {
		ident, _ := GenerateIdentity()
		bs := newTestMemStore()
		pd := NewPrivateDring(grp, ident, bs, nil, cfg)
		addr, err := pd.StartServer("/ip4/127.0.0.1/tcp/0", "")
		if err != nil {
			t.Fatalf("StartServer: %v", err)
		}
		if bootstrap == nil {
			pd.Create()
		} else {
			peer := NodeAddr{ID: bootstrap.ident.ID, Addr: bootstrap.addr}
			if err := pd.Join(ctx, peer); err != nil {
				t.Fatalf("Join: %v", err)
			}
		}
		return &kNode{ident: ident, pd: pd, addr: addr, store: bs}
	}

	stabilize := func(nodes []*kNode) {
		for _, n := range nodes {
			n.pd.Node().stabilize()
		}
		for _, n := range nodes {
			n.pd.Node().fixFinger()
		}
		for _, n := range nodes {
			n.pd.Node().checkPredecessor()
		}
	}

	n1 := newKNode(nil)
	n2 := newKNode(n1)
	defer n1.pd.Stop()
	defer n2.pd.Stop()

	for i := 0; i < 40; i++ {
		stabilize([]*kNode{n1, n2})
	}

	const nBlocks = 10
	for i := 0; i < nBlocks; i++ {
		key, data := testBlock(fmt.Sprintf("rereplicate-%d", i))
		if err := n1.pd.Node().Put(ctx, key, data); err != nil {
			t.Fatalf("Put block %d: %v", i, err)
		}
	}

	totalBefore := n1.pd.Node().DataCount() + n2.pd.Node().DataCount()
	if totalBefore < nBlocks {
		t.Fatalf("before 3rd node: expected at least %d total blocks, got %d", nBlocks, totalBefore)
	}

	n3 := newKNode(n1)
	defer n3.pd.Stop()

	for i := 0; i < 60; i++ {
		stabilize([]*kNode{n1, n2, n3})
	}

	n3Count := n3.pd.Node().DataCount()
	if n3Count == 0 {
		t.Error("new node received 0 blocks via re-replication — replicateToNewSuccessors is not working")
	}
	t.Logf("n1=%d n2=%d n3=%d blocks after 3-node ring", n1.pd.Node().DataCount(), n2.pd.Node().DataCount(), n3Count)
}

func TestPrivateDring_NodeAddressChange(t *testing.T) {

	pubRing := NewTestRing()
	pubIdent, _ := GenerateIdentity()
	pubBs := newTestMemStore()
	pubLt := &localTransport{ring: pubRing, nodeID: pubIdent.ID}
	pubNode := NewNode(pubIdent.ID, "pub-node", pubBs, nil, pubLt, Config{})
	pubRing.mu.Lock()
	pubRing.Nodes = append(pubRing.Nodes, pubNode)
	pubRing.nodeMap[pubIdent.ID] = pubNode
	pubRing.mu.Unlock()
	pubNode.Create()
	pub := NewPublicDring(pubNode, pubIdent)

	ctx := context.Background()
	grp, _ := GenerateGroupIdentity()
	groupIDHex := grp.GroupID.String()

	publishPeer := func(ident *Identity, privAddr string, version uint64) {
		t.Helper()
		rec, err := NewPeerIdentityRecord(ident, version, "pub-addr", map[string]string{
			groupIDHex: privAddr,
		})
		if err != nil {
			t.Fatalf("NewPeerIdentityRecord: %v", err)
		}
		data, _ := rec.Encode()
		if err := pubNode.RecordPut(ctx, ident.ID, data); err != nil {
			t.Fatalf("RecordPut: %v", err)
		}
	}

	cfg := Config{Replication: 1}

	identA, _ := GenerateIdentity()
	pdA := NewPrivateDring(grp, identA, newTestMemStore(), nil, cfg)
	addrA, err := pdA.StartServer("/ip4/127.0.0.1/tcp/0", "")
	if err != nil {
		t.Fatalf("A StartServer: %v", err)
	}
	defer pdA.Stop()
	pdA.Create()
	publishPeer(identA, addrA, 1)
	pdA.SetPublicDring(pub)
	if err := pub.PublishGroup(ctx, grp, 1, []GroupMember{{ID: identA.ID}}); err != nil {
		t.Fatalf("PublishGroup (A only): %v", err)
	}

	identB, _ := GenerateIdentity()
	pdB := NewPrivateDring(grp, identB, newTestMemStore(), nil, cfg)
	addrB, err := pdB.StartServer("/ip4/127.0.0.1/tcp/0", "")
	if err != nil {
		t.Fatalf("B StartServer: %v", err)
	}
	publishPeer(identB, addrB, 1)
	pdB.SetPublicDring(pub)
	if err := pdB.JoinViaPublicDring(ctx, pub); err != nil {
		t.Fatalf("B JoinViaPublicDring: %v", err)
	}
	if err := pdB.UpdateGroupRecord(ctx, pub); err != nil {
		t.Fatalf("B UpdateGroupRecord: %v", err)
	}

	identC, _ := GenerateIdentity()
	pdC := NewPrivateDring(grp, identC, newTestMemStore(), nil, cfg)
	addrC, err := pdC.StartServer("/ip4/127.0.0.1/tcp/0", "")
	if err != nil {
		t.Fatalf("C StartServer: %v", err)
	}
	defer pdC.Stop()
	publishPeer(identC, addrC, 1)
	pdC.SetPublicDring(pub)
	if err := pdC.JoinViaPublicDring(ctx, pub); err != nil {
		t.Fatalf("C JoinViaPublicDring: %v", err)
	}
	if err := pdC.UpdateGroupRecord(ctx, pub); err != nil {
		t.Fatalf("C UpdateGroupRecord: %v", err)
	}

	stabilize := func(nodes ...*PrivateDring) {
		for _, n := range nodes {
			n.Node().stabilize()
		}
		for _, n := range nodes {
			n.Node().fixFinger()
		}
		for _, n := range nodes {
			n.Node().checkPredecessor()
		}
	}

	for i := 0; i < 40; i++ {
		stabilize(pdA, pdB, pdC)
	}

	checkConnected := func(label string, nodes ...*PrivateDring) {
		t.Helper()
		ctx2 := context.Background()
		for _, pd := range nodes {
			self := pd.Node().LocalNode().ID
			succ := pd.Node().getSuccessor()
			if succ.ID == self {
				t.Errorf("%s: node %s is isolated (successor == self)", label, self)
				continue
			}

			for _, target := range nodes {
				tID := target.Node().LocalNode().ID
				if tID == self {
					continue
				}
				got, err := pd.Node().FindSuccessor(ctx2, tID)
				if err != nil {
					t.Errorf("%s: node %s FindSuccessor(%s): %v", label, self, tID, err)
					continue
				}
				if got.ID != tID {
					t.Errorf("%s: node %s FindSuccessor(%s) = %s, want exact match", label, self, tID, got.ID)
				}
			}
		}
	}
	checkConnected("initial", pdA, pdB, pdC)

	pdB.Stop()

	pdB2 := NewPrivateDring(grp, identB, newTestMemStore(), nil, cfg)
	addrB2, err := pdB2.StartServer("/ip4/127.0.0.1/tcp/0", "")
	if err != nil {
		t.Fatalf("B2 StartServer: %v", err)
	}
	defer pdB2.Stop()

	publishPeer(identB, addrB2, 2)
	pdB2.SetPublicDring(pub)

	if err := pdB2.JoinViaPublicDring(ctx, pub); err != nil {
		t.Fatalf("B2 JoinViaPublicDring: %v", err)
	}
	if err := pdB2.UpdateGroupRecord(ctx, pub); err != nil {
		t.Fatalf("B2 UpdateGroupRecord: %v", err)
	}

	for i := 0; i < 80; i++ {
		stabilize(pdA, pdB2, pdC)
	}

	checkConnected("after address change", pdA, pdB2, pdC)

	key, data := testBlock("after-addr-change")
	if err := pdA.Node().Put(ctx, key, data); err != nil {
		t.Fatalf("A Put: %v", err)
	}
	got, err := pdC.Node().Get(ctx, key)
	if err != nil {
		t.Fatalf("C Get after address change: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("block data mismatch after address change")
	}
}

var _ = func(t *testing.T, n *merkledag.ProtoNode, name string, _ interface{}, _ uint64) {
	_ = format.Link{}
}
