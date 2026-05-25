package dht

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"strings"

	"github.com/ipfs/go-cid"
	ma "github.com/multiformats/go-multiaddr"
)

var ringSize = new(big.Int).Lsh(big.NewInt(1), 160)

type NodeID [20]byte

func CIDToNodeID(c cid.Cid) NodeID {
	return sha1.Sum(c.Hash())
}

func AddrToNodeID(addr string) NodeID {
	return sha1.Sum([]byte(addr))
}

func MultiaddrToTCPAddr(s string) (string, error) {
	m, err := ma.NewMultiaddr(s)
	if err != nil {

		return s, nil
	}

	var host string
	if v, err := m.ValueForProtocol(ma.P_IP4); err == nil {
		host = v
	} else if v, err := m.ValueForProtocol(ma.P_IP6); err == nil {
		host = v
	} else {
		return "", fmt.Errorf("no IP protocol in multiaddr %q", s)
	}

	port, err := m.ValueForProtocol(ma.P_TCP)
	if err != nil {
		return "", fmt.Errorf("no TCP protocol in multiaddr %q", s)
	}

	return net.JoinHostPort(host, port), nil
}

func TCPAddrToMultiaddr(hostPort string) (string, error) {
	host, port, err := net.SplitHostPort(hostPort)
	if err != nil {
		return "", err
	}
	if strings.Contains(host, ":") {
		return fmt.Sprintf("/ip6/%s/tcp/%s", host, port), nil
	}
	return fmt.Sprintf("/ip4/%s/tcp/%s", host, port), nil
}

func effectiveMultiaddr(boundTCP, advertiseAddr string) (string, error) {
	_, port, err := net.SplitHostPort(boundTCP)
	if err != nil {
		return "", fmt.Errorf("parse bound addr %q: %w", boundTCP, err)
	}
	host := advertiseAddr
	if host == "" {
		host, _, _ = net.SplitHostPort(boundTCP)
	}
	if strings.Contains(host, ":") {
		return fmt.Sprintf("/ip6/%s/tcp/%s", host, port), nil
	}
	return fmt.Sprintf("/ip4/%s/tcp/%s", host, port), nil
}

func (id NodeID) String() string {
	return fmt.Sprintf("%x", id[:])
}

func (id NodeID) MarshalJSON() ([]byte, error) {
	return json.Marshal(hex.EncodeToString(id[:]))
}

func (id *NodeID) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return err
	}
	if len(b) != 20 {
		return fmt.Errorf("NodeID: expected 20 bytes, got %d", len(b))
	}
	copy(id[:], b)
	return nil
}

func (id NodeID) Equal(other NodeID) bool {
	return id == other
}

func (id NodeID) Less(other NodeID) bool {
	for i := 0; i < 20; i++ {
		if id[i] != other[i] {
			return id[i] < other[i]
		}
	}
	return false
}

func (id NodeID) BetweenExclusive(lo, hi NodeID) bool {
	if lo.Equal(hi) {

		return !id.Equal(lo)
	}
	if lo.Less(hi) {

		return lo.Less(id) && id.Less(hi)
	}

	return lo.Less(id) || id.Less(hi)
}

func (id NodeID) BetweenRightInclusive(lo, hi NodeID) bool {
	return id.Equal(hi) || id.BetweenExclusive(lo, hi)
}

func (id NodeID) fingerStart(i int) NodeID {
	n := new(big.Int).SetBytes(id[:])
	pow := new(big.Int).Lsh(big.NewInt(1), uint(i))
	result := new(big.Int).Add(n, pow)
	result.Mod(result, ringSize)

	var out NodeID
	b := result.Bytes()

	if len(b) <= 20 {
		copy(out[20-len(b):], b)
	}
	return out
}
