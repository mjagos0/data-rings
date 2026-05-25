package dht

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
)

type Identity struct {
	PrivKey	ed25519.PrivateKey
	PubKey	ed25519.PublicKey
	ID	NodeID
}

func GenerateIdentity() (*Identity, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate ed25519 key: %w", err)
	}
	return &Identity{
		PrivKey:	priv,
		PubKey:		pub,
		ID:		PubKeyToNodeID(pub),
	}, nil
}

func IdentityFromKey(priv ed25519.PrivateKey) *Identity {
	pub := priv.Public().(ed25519.PublicKey)
	return &Identity{
		PrivKey:	priv,
		PubKey:		pub,
		ID:		PubKeyToNodeID(pub),
	}
}

func PubKeyToNodeID(pub ed25519.PublicKey) NodeID {
	return sha1.Sum([]byte(pub))
}

func LoadOrCreateIdentity(path string) (*Identity, error) {
	raw, err := os.ReadFile(path)
	if err == nil {

		raw = []byte(trimNewlines(string(raw)))
		b, err := hex.DecodeString(string(raw))
		if err != nil || len(b) != ed25519.PrivateKeySize {
			return nil, fmt.Errorf("identity file %s is malformed", path)
		}
		return IdentityFromKey(ed25519.PrivateKey(b)), nil
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read identity file: %w", err)
	}

	ident, err := GenerateIdentity()
	if err != nil {
		return nil, err
	}

	encoded := hex.EncodeToString([]byte(ident.PrivKey))
	if err := os.WriteFile(path, []byte(encoded+"\n"), 0600); err != nil {
		return nil, fmt.Errorf("write identity file: %w", err)
	}
	return ident, nil
}

func trimNewlines(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}
