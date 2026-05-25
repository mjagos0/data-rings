package dht

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/fxamacker/cbor/v2"
	"google.golang.org/protobuf/proto"

	dhtpb "github.com/mjagos0/datarings/dht/dhtpb"
)

var cborEncMode = func() cbor.EncMode {
	opts := cbor.CTAP2EncOptions()
	opts.ByteArray = cbor.ByteArrayToByteSlice
	em, err := opts.EncMode()
	if err != nil {
		panic(fmt.Sprintf("cbor enc mode init: %v", err))
	}
	return em
}()

const (
	peerRecordPrefix	= "drings-peer:"
	groupRecordPrefix	= "drings-group:"
)

type PeerIdentityData struct {
	PeerID		NodeID			`json:"peer_id"     cbor:"peer_id"`
	Version		uint64			`json:"version"     cbor:"version"`
	Address		string			`json:"address"     cbor:"address"`
	GroupAddrs	map[string]string	`json:"group_addrs" cbor:"group_addrs"`
}

type PeerIdentityRecord struct {
	Data		PeerIdentityData	`json:"data"`
	PubKey		[]byte			`json:"pubkey"`
	Signature	[]byte			`json:"signature"`
}

func NewPeerIdentityRecord(ident *Identity, version uint64, address string, groupAddrs map[string]string) (*PeerIdentityRecord, error) {
	r := &PeerIdentityRecord{
		Data: PeerIdentityData{
			PeerID:		ident.ID,
			Version:	version,
			Address:	address,
			GroupAddrs:	groupAddrs,
		},
		PubKey:	[]byte(ident.PubKey),
	}
	if err := r.sign(ident.PrivKey); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *PeerIdentityRecord) sign(priv ed25519.PrivateKey) error {
	payload, err := cborSignPayload(peerRecordPrefix, r.Data)
	if err != nil {
		return fmt.Errorf("encode peer identity data: %w", err)
	}
	r.Signature = ed25519.Sign(priv, payload)
	return nil
}

func (r *PeerIdentityRecord) Verify() error {
	pub := ed25519.PublicKey(r.PubKey)
	if len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("invalid public key length %d", len(pub))
	}
	if PubKeyToNodeID(pub) != r.Data.PeerID {
		return fmt.Errorf("peer_id does not match hash of public key")
	}
	payload, err := cborSignPayload(peerRecordPrefix, r.Data)
	if err != nil {
		return fmt.Errorf("encode peer identity data: %w", err)
	}
	if !ed25519.Verify(pub, payload, r.Signature) {
		return fmt.Errorf("signature verification failed")
	}
	return nil
}

func (r *PeerIdentityRecord) Encode() ([]byte, error) {
	inner, err := cborEncMode.Marshal(r.Data)
	if err != nil {
		return nil, fmt.Errorf("cbor encode peer identity data: %w", err)
	}
	pb := &dhtpb.SignedRecord{
		Data:		inner,
		Pubkey:		r.PubKey,
		Signature:	r.Signature,
	}
	return proto.Marshal(pb)
}

func DecodePeerIdentityRecord(data []byte) (*PeerIdentityRecord, error) {
	var pb dhtpb.SignedRecord
	if err := proto.Unmarshal(data, &pb); err != nil {
		return nil, fmt.Errorf("decode peer identity record (proto): %w", err)
	}
	var d PeerIdentityData
	if err := cbor.Unmarshal(pb.Data, &d); err != nil {
		return nil, fmt.Errorf("decode peer identity record (cbor): %w", err)
	}
	return &PeerIdentityRecord{
		Data:		d,
		PubKey:		pb.Pubkey,
		Signature:	pb.Signature,
	}, nil
}

type GroupMember struct {
	ID NodeID `json:"id" cbor:"id"`
}

type GroupIdentityData struct {
	GroupID	NodeID		`json:"group_id" cbor:"group_id"`
	Version	uint64		`json:"version"  cbor:"version"`
	Peers	[]GroupMember	`json:"peers"    cbor:"peers"`
}

type GroupIdentityRecord struct {
	Data		GroupIdentityData	`json:"data"`
	PubKey		[]byte			`json:"pubkey"`
	Signature	[]byte			`json:"signature"`
}

func NewGroupIdentityRecord(grp *GroupIdentity, version uint64, peers []GroupMember) (*GroupIdentityRecord, error) {
	r := &GroupIdentityRecord{
		Data: GroupIdentityData{
			GroupID:	grp.GroupID,
			Version:	version,
			Peers:		peers,
		},
		PubKey:	[]byte(grp.PubKey),
	}
	if err := r.sign(grp.PrivKey); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *GroupIdentityRecord) sign(priv ed25519.PrivateKey) error {
	payload, err := cborSignPayload(groupRecordPrefix, r.Data)
	if err != nil {
		return fmt.Errorf("encode group identity data: %w", err)
	}
	r.Signature = ed25519.Sign(priv, payload)
	return nil
}

func (r *GroupIdentityRecord) Verify() error {
	pub := ed25519.PublicKey(r.PubKey)
	if len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("invalid public key length %d", len(pub))
	}
	if PubKeyToNodeID(pub) != r.Data.GroupID {
		return fmt.Errorf("group_id does not match hash of public key")
	}
	payload, err := cborSignPayload(groupRecordPrefix, r.Data)
	if err != nil {
		return fmt.Errorf("encode group identity data: %w", err)
	}
	if !ed25519.Verify(pub, payload, r.Signature) {
		return fmt.Errorf("signature verification failed")
	}
	return nil
}

func (r *GroupIdentityRecord) Encode() ([]byte, error) {
	inner, err := cborEncMode.Marshal(r.Data)
	if err != nil {
		return nil, fmt.Errorf("cbor encode group identity data: %w", err)
	}
	pb := &dhtpb.SignedRecord{
		Data:		inner,
		Pubkey:		r.PubKey,
		Signature:	r.Signature,
	}
	return proto.Marshal(pb)
}

func DecodeGroupIdentityRecord(data []byte) (*GroupIdentityRecord, error) {
	var pb dhtpb.SignedRecord
	if err := proto.Unmarshal(data, &pb); err != nil {
		return nil, fmt.Errorf("decode group identity record (proto): %w", err)
	}
	var d GroupIdentityData
	if err := cbor.Unmarshal(pb.Data, &d); err != nil {
		return nil, fmt.Errorf("decode group identity record (cbor): %w", err)
	}
	return &GroupIdentityRecord{
		Data:		d,
		PubKey:		pb.Pubkey,
		Signature:	pb.Signature,
	}, nil
}

type ProviderRecord struct {
	ContentHash	NodeID	`json:"content_hash"`
	Provider	NodeID	`json:"provider"`
}

func (r *ProviderRecord) Encode() ([]byte, error) {
	pb := &dhtpb.ProviderRecord{
		ContentHash:	r.ContentHash[:],
		Provider:	r.Provider[:],
	}
	return proto.Marshal(pb)
}

func DecodeProviderRecords(data []byte) ([]ProviderRecord, error) {
	var list dhtpb.ProviderRecordList
	if err := proto.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("decode provider record list: %w", err)
	}
	rs := make([]ProviderRecord, 0, len(list.Providers))
	for _, p := range list.Providers {
		if len(p.ContentHash) != 20 || len(p.Provider) != 20 {
			return nil, fmt.Errorf("provider record has invalid field lengths")
		}
		var pr ProviderRecord
		copy(pr.ContentHash[:], p.ContentHash)
		copy(pr.Provider[:], p.Provider)
		rs = append(rs, pr)
	}
	return rs, nil
}

func encodeProviderRecordList(rs []ProviderRecord) ([]byte, error) {
	list := &dhtpb.ProviderRecordList{
		Providers: make([]*dhtpb.ProviderRecord, len(rs)),
	}
	for i, r := range rs {
		list.Providers[i] = &dhtpb.ProviderRecord{
			ContentHash:	r.ContentHash[:],
			Provider:	r.Provider[:],
		}
	}
	return proto.Marshal(list)
}

func encodeProviderRecordListWithTimestamps(rs []ProviderRecord, times map[NodeID]time.Time) ([]byte, error) {
	list := &dhtpb.ProviderRecordList{
		Providers:	make([]*dhtpb.ProviderRecord, len(rs)),
		Timestamps:	make([]int64, len(rs)),
	}
	for i, r := range rs {
		list.Providers[i] = &dhtpb.ProviderRecord{
			ContentHash:	r.ContentHash[:],
			Provider:	r.Provider[:],
		}
		if t, ok := times[r.Provider]; ok {
			list.Timestamps[i] = t.UnixNano()
		} else {
			list.Timestamps[i] = time.Now().UnixNano()
		}
	}
	return proto.Marshal(list)
}

type GroupIdentity struct {
	PrivKey	ed25519.PrivateKey
	PubKey	ed25519.PublicKey
	GroupID	NodeID
	PSK	[]byte
}

func GenerateGroupIdentity() (*GroupIdentity, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate group key: %w", err)
	}
	return NewGroupIdentity(priv), nil
}

func NewGroupIdentity(priv ed25519.PrivateKey) *GroupIdentity {
	pub := priv.Public().(ed25519.PublicKey)
	return &GroupIdentity{
		PrivKey:	priv,
		PubKey:		pub,
		GroupID:	PubKeyToNodeID(pub),
		PSK:		DerivePSK(priv),
	}
}

func GroupIdentityFromHex(hexKey string) (*GroupIdentity, error) {
	b, err := hex.DecodeString(hexKey)
	if err != nil {
		return nil, fmt.Errorf("decode hex key: %w", err)
	}
	if len(b) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid key length %d, want %d", len(b), ed25519.PrivateKeySize)
	}
	return NewGroupIdentity(ed25519.PrivateKey(b)), nil
}

func (g *GroupIdentity) PrivKeyHex() string {
	return hex.EncodeToString([]byte(g.PrivKey))
}

func DerivePSK(priv ed25519.PrivateKey) []byte {
	mac := hmac.New(sha256.New, []byte(priv))
	mac.Write([]byte("psk"))
	return mac.Sum(nil)
}

func cborSignPayload(prefix string, v any) ([]byte, error) {
	inner, err := cborEncMode.Marshal(v)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, len(prefix)+len(inner))
	copy(payload, prefix)
	copy(payload[len(prefix):], inner)
	return payload, nil
}
