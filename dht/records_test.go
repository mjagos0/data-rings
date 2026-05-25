package dht

import (
	"bytes"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"google.golang.org/protobuf/proto"

	dhtpb "github.com/mjagos0/datarings/dht/dhtpb"
)

func TestPeerIdentityRecord_PeerIDEqualsHashOfPubKey(t *testing.T) {
	ident, err := GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}
	expected := PubKeyToNodeID(ident.PubKey)
	if ident.ID != expected {
		t.Errorf("PeerID mismatch: got %s, want SHA-1(PubKey)=%s", ident.ID, expected)
	}
}

func TestPeerIdentityRecord_SignAndVerify(t *testing.T) {
	ident, err := GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}

	rec, err := NewPeerIdentityRecord(ident, 1, "/ip4/127.0.0.1/tcp/7000", nil)
	if err != nil {
		t.Fatalf("NewPeerIdentityRecord: %v", err)
	}

	if err := rec.Verify(); err != nil {
		t.Fatalf("Verify: %v", err)
	}

	if rec.Data.PeerID != ident.ID {
		t.Errorf("PeerID mismatch: got %s, want %s", rec.Data.PeerID, ident.ID)
	}
	if rec.Data.Version != 1 {
		t.Errorf("Version: got %d, want 1", rec.Data.Version)
	}
	if rec.Data.Address != "/ip4/127.0.0.1/tcp/7000" {
		t.Errorf("Address: got %s", rec.Data.Address)
	}
}

func TestPeerIdentityRecord_TamperedData_Rejected(t *testing.T) {
	ident, _ := GenerateIdentity()
	rec, _ := NewPeerIdentityRecord(ident, 1, "/ip4/127.0.0.1/tcp/7000", nil)

	rec.Data.Address = "/ip4/1.2.3.4/tcp/9999"
	if err := rec.Verify(); err == nil {
		t.Fatal("expected Verify to fail after data tampering")
	}
}

func TestPeerIdentityRecord_WrongPublicKey_Rejected(t *testing.T) {
	ident, _ := GenerateIdentity()
	other, _ := GenerateIdentity()
	rec, _ := NewPeerIdentityRecord(ident, 1, "/ip4/127.0.0.1/tcp/7000", nil)

	rec.PubKey = []byte(other.PubKey)
	if err := rec.Verify(); err == nil {
		t.Fatal("expected Verify to fail with wrong public key")
	}
}

func TestPeerIdentityRecord_InvalidSignature_Rejected(t *testing.T) {
	ident, _ := GenerateIdentity()
	rec, _ := NewPeerIdentityRecord(ident, 1, "/ip4/127.0.0.1/tcp/7000", nil)

	rec.Signature[0] ^= 0xFF
	if err := rec.Verify(); err == nil {
		t.Fatal("expected Verify to fail with corrupted signature")
	}
}

func TestPeerIdentityRecord_RoundTrip(t *testing.T) {
	ident, _ := GenerateIdentity()
	original, _ := NewPeerIdentityRecord(ident, 42, "/ip4/10.0.0.1/tcp/8080", nil)

	data, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, err := DecodePeerIdentityRecord(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if err := decoded.Verify(); err != nil {
		t.Fatalf("Verify after decode: %v", err)
	}
	if decoded.Data.Version != 42 {
		t.Errorf("Version: got %d, want 42", decoded.Data.Version)
	}
	if decoded.Data.PeerID != original.Data.PeerID {
		t.Error("PeerID mismatch after round-trip")
	}
}

func TestPeerIdentityRecord_CBORDeterminism(t *testing.T) {
	ident, _ := GenerateIdentity()

	encode := func() []byte {
		payload, err := cborSignPayload(peerRecordPrefix, PeerIdentityData{
			PeerID:		ident.ID,
			Version:	7,
			Address:	"/ip4/1.2.3.4/tcp/7000",
		})
		if err != nil {
			t.Fatalf("cborSignPayload: %v", err)
		}
		return payload
	}

	a := encode()
	b := encode()
	if !bytes.Equal(a, b) {
		t.Errorf("CBOR encoding is not deterministic:\n  a=%x\n  b=%x", a, b)
	}

	rec, _ := NewPeerIdentityRecord(ident, 7, "/ip4/1.2.3.4/tcp/7000", nil)
	wire1, _ := rec.Encode()
	decoded, _ := DecodePeerIdentityRecord(wire1)
	wire2, _ := decoded.Encode()
	if !bytes.Equal(wire1, wire2) {
		t.Errorf("encode→decode→re-encode produced different bytes")
	}
}

func TestGroupIdentityRecord_PeerMembers(t *testing.T) {
	grp, _ := GenerateGroupIdentity()
	peer1, _ := GenerateIdentity()
	peer2, _ := GenerateIdentity()

	members := []GroupMember{
		{ID: peer1.ID},
		{ID: peer2.ID},
	}
	rec, err := NewGroupIdentityRecord(grp, 1, members)
	if err != nil {
		t.Fatalf("NewGroupIdentityRecord: %v", err)
	}

	if err := rec.Verify(); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if len(rec.Data.Peers) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(rec.Data.Peers))
	}
	if rec.Data.Peers[0].ID != peer1.ID || rec.Data.Peers[1].ID != peer2.ID {
		t.Error("peer IDs not stored correctly")
	}

	data, err := rec.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	decoded, err := DecodeGroupIdentityRecord(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if err := decoded.Verify(); err != nil {
		t.Fatalf("Verify after decode: %v", err)
	}
	if decoded.Data.Peers[0].ID != peer1.ID {
		t.Errorf("peer ID lost after round-trip: %s", decoded.Data.Peers[0].ID)
	}
}

func TestPeerIdentityRecord_GroupAddrs(t *testing.T) {
	ident, _ := GenerateIdentity()
	grp, _ := GenerateGroupIdentity()

	groupAddrs := map[string]string{
		grp.GroupID.String(): "/ip4/1.2.3.4/tcp/7001",
	}
	rec, err := NewPeerIdentityRecord(ident, 1, "/ip4/1.2.3.4/tcp/7000", groupAddrs)
	if err != nil {
		t.Fatalf("NewPeerIdentityRecord: %v", err)
	}
	if err := rec.Verify(); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if rec.Data.GroupAddrs[grp.GroupID.String()] != "/ip4/1.2.3.4/tcp/7001" {
		t.Errorf("GroupAddrs not stored correctly: %v", rec.Data.GroupAddrs)
	}

	wire, err := rec.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	decoded, err := DecodePeerIdentityRecord(wire)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if err := decoded.Verify(); err != nil {
		t.Fatalf("Verify after decode: %v", err)
	}
	if decoded.Data.GroupAddrs[grp.GroupID.String()] != "/ip4/1.2.3.4/tcp/7001" {
		t.Errorf("GroupAddrs lost after round-trip: %v", decoded.Data.GroupAddrs)
	}
}

func TestGroupIdentityRecord_SignAndVerify(t *testing.T) {
	grp, err := GenerateGroupIdentity()
	if err != nil {
		t.Fatalf("GenerateGroupIdentity: %v", err)
	}

	peers := []GroupMember{{ID: grp.GroupID}}
	rec, err := NewGroupIdentityRecord(grp, 1, peers)
	if err != nil {
		t.Fatalf("NewGroupIdentityRecord: %v", err)
	}

	if err := rec.Verify(); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if rec.Data.GroupID != grp.GroupID {
		t.Errorf("GroupID mismatch")
	}
	if len(rec.Data.Peers) != 1 {
		t.Errorf("Peers: got %d, want 1", len(rec.Data.Peers))
	}
}

func TestGroupIdentityRecord_TamperedPeers_Rejected(t *testing.T) {
	grp, _ := GenerateGroupIdentity()
	other, _ := GenerateIdentity()
	peers := []GroupMember{{ID: grp.GroupID}}
	rec, _ := NewGroupIdentityRecord(grp, 1, peers)

	rec.Data.Peers = append(rec.Data.Peers, GroupMember{ID: other.ID})
	if err := rec.Verify(); err == nil {
		t.Fatal("expected Verify to fail after tampering peers")
	}
}

func TestGroupIdentityRecord_RoundTrip(t *testing.T) {
	grp, _ := GenerateGroupIdentity()
	peers := []GroupMember{{ID: grp.GroupID}}
	original, _ := NewGroupIdentityRecord(grp, 7, peers)

	data, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, err := DecodeGroupIdentityRecord(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if err := decoded.Verify(); err != nil {
		t.Fatalf("Verify after decode: %v", err)
	}
	if decoded.Data.Version != 7 {
		t.Errorf("Version: got %d, want 7", decoded.Data.Version)
	}
}

func TestGroupIdentityRecord_CBORDeterminism(t *testing.T) {
	grp, _ := GenerateGroupIdentity()
	peer1, _ := GenerateIdentity()
	peer2, _ := GenerateIdentity()

	encode := func() []byte {
		payload, err := cborSignPayload(groupRecordPrefix, GroupIdentityData{
			GroupID:	grp.GroupID,
			Version:	3,
			Peers: []GroupMember{
				{ID: peer1.ID},
				{ID: peer2.ID},
			},
		})
		if err != nil {
			t.Fatalf("cborSignPayload: %v", err)
		}
		return payload
	}

	a := encode()
	b := encode()
	if !bytes.Equal(a, b) {
		t.Errorf("CBOR encoding is not deterministic:\n  a=%x\n  b=%x", a, b)
	}

	rec, _ := NewGroupIdentityRecord(grp, 3, []GroupMember{
		{ID: peer1.ID},
		{ID: peer2.ID},
	})
	wire1, _ := rec.Encode()
	decoded, _ := DecodeGroupIdentityRecord(wire1)
	wire2, _ := decoded.Encode()
	if !bytes.Equal(wire1, wire2) {
		t.Errorf("encode→decode→re-encode produced different bytes")
	}
}

func TestGroupIdentity_PSKDerivation(t *testing.T) {
	grp, err := GenerateGroupIdentity()
	if err != nil {
		t.Fatalf("GenerateGroupIdentity: %v", err)
	}

	if len(grp.PSK) != 32 {
		t.Errorf("PSK length: got %d, want 32", len(grp.PSK))
	}

	grp2 := NewGroupIdentity(grp.PrivKey)
	for i := range grp.PSK {
		if grp.PSK[i] != grp2.PSK[i] {
			t.Fatal("PSK is not deterministic")
		}
	}

	other, _ := GenerateGroupIdentity()
	same := true
	for i := range grp.PSK {
		if grp.PSK[i] != other.PSK[i] {
			same = false
			break
		}
	}
	if same {
		t.Fatal("different private keys produced the same PSK — collision or broken derivation")
	}
}

func TestGroupIdentity_GroupIDFromPublicKey(t *testing.T) {
	grp, _ := GenerateGroupIdentity()

	expectedID := PubKeyToNodeID(grp.PubKey)
	if grp.GroupID != expectedID {
		t.Errorf("GroupID mismatch: got %s, want %s", grp.GroupID, expectedID)
	}
}

func TestGroupIdentity_HexRoundTrip(t *testing.T) {
	original, _ := GenerateGroupIdentity()
	hexKey := original.PrivKeyHex()

	decoded, err := GroupIdentityFromHex(hexKey)
	if err != nil {
		t.Fatalf("GroupIdentityFromHex: %v", err)
	}

	if decoded.GroupID != original.GroupID {
		t.Errorf("GroupID mismatch after hex round-trip")
	}
	for i := range original.PSK {
		if original.PSK[i] != decoded.PSK[i] {
			t.Fatal("PSK mismatch after hex round-trip")
		}
	}
}

func TestGroupIdentityFromHex_InvalidKey_Rejected(t *testing.T) {

	if _, err := GroupIdentityFromHex("deadbeef"); err == nil {
		t.Fatal("expected error for too-short hex key")
	}

	if _, err := GroupIdentityFromHex("not-valid-hex!"); err == nil {
		t.Fatal("expected error for invalid hex")
	}

	zeros := make([]byte, ed25519.PrivateKeySize)
	hexZeros := hex.EncodeToString(zeros)
	if _, err := GroupIdentityFromHex(hexZeros); err != nil {

		t.Logf("zero key: %v (may or may not be valid depending on crypto library)", err)
	}
}

func TestProviderRecord_RoundTrip(t *testing.T) {
	var contentHash NodeID
	rand.Read(contentHash[:])

	var providerID NodeID
	rand.Read(providerID[:])

	pr := ProviderRecord{ContentHash: contentHash, Provider: providerID}
	data, err := pr.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	listPB := &dhtpb.ProviderRecordList{
		Providers: []*dhtpb.ProviderRecord{{
			ContentHash:	contentHash[:],
			Provider:	providerID[:],
		}},
	}
	listData, err := proto.Marshal(listPB)
	if err != nil {
		t.Fatalf("proto.Marshal ProviderRecordList: %v", err)
	}

	rs, err := DecodeProviderRecords(listData)
	if err != nil {
		t.Fatalf("DecodeProviderRecords: %v", err)
	}
	if len(rs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(rs))
	}
	if rs[0].ContentHash != contentHash {
		t.Errorf("ContentHash mismatch")
	}
	if rs[0].Provider != providerID {
		t.Errorf("Provider mismatch")
	}

	var rawPR dhtpb.ProviderRecord
	if err := proto.Unmarshal(data, &rawPR); err != nil {
		t.Fatalf("proto.Unmarshal single ProviderRecord: %v", err)
	}
	if len(rawPR.ContentHash) != 20 || len(rawPR.Provider) != 20 {
		t.Errorf("unexpected field lengths: content_hash=%d provider=%d",
			len(rawPR.ContentHash), len(rawPR.Provider))
	}
}

func TestProviderRecord_KeyDerivation_SHA1OfMultihash(t *testing.T) {
	for _, content := range []string{"hello", "world", "test content", ""} {
		c := testCID([]byte(content))

		wantKey := sha1.Sum(c.Hash())
		gotKey := CIDToNodeID(c)
		if gotKey != NodeID(wantKey) {
			t.Errorf("content=%q: CIDToNodeID(%s) = %x, want SHA-1(c.Hash()) = %x",
				content, c, gotKey, wantKey)
		}
	}
}

func TestGroupIdentity_PSK_DerivationUsesLabelPSK(t *testing.T) {
	grp, err := GenerateGroupIdentity()
	if err != nil {
		t.Fatalf("GenerateGroupIdentity: %v", err)
	}

	mac := hmac.New(sha256.New, []byte(grp.PrivKey))
	mac.Write([]byte("psk"))
	want := mac.Sum(nil)

	if !bytes.Equal(grp.PSK, want) {
		t.Errorf("PSK mismatch: got  %x\n\t\twant HMAC-SHA256(privKey, \"psk\") = %x",
			grp.PSK, want)
	}
}
