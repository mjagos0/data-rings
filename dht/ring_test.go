package dht

import (
	"crypto/sha256"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

func testCID(data []byte) cid.Cid {
	h := sha256.Sum256(data)
	hash, _ := mh.Encode(h[:], mh.SHA2_256)
	return cid.NewCidV1(cid.Raw, hash)
}

func testBlock(content string) (cid.Cid, []byte) {
	data := []byte(content)
	return testCID(data), data
}
