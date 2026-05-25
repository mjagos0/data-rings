package main

import (
	"crypto/ed25519"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
)

type identity struct {
	seed	string
	privHex	string
	nodeID	string
}

func main() {

	existing := []identity{
		{seed: "node1", privHex: "010e1b2835424f5c697683909daab7c4d1deebf805121f2c394653606d7a8794098f9f3908b407db2cbfb61df028acc52d5f8da74a9d7e03ab9c2f8656f6a9f0", nodeID: "525b7321283b97becfd1aaaf8e336be8b60815ca"},
		{seed: "node2", privHex: "2633404d5a6774818e9ba8b5c2cfdce9f603101d2a3744515e6b7885929facb965fe02ea01b26069dfa4b196e48da141afcfb14e024b1f2c15a22947e0638a8d", nodeID: "cb71389fdb83e1e348cbfa490626be1e94df3ee8"},
		{seed: "node3", privHex: "4b5865727f8c99a6b3c0cddae7f4010e1b2835424f5c697683909daab7c4d1de0b9afc3527f0e5c024e9459d83bd0437e18fddda5307700263a4a0641e11fdb1", nodeID: "55b186b6b92a5739dddfdc975addfa430e2ddbd8"},
		{seed: "node4", privHex: "707d8a97a4b1becbd8e5f2ff0c192633404d5a6774818e9ba8b5c2cfdce9f60354e728278a9e6791e15300b02b63cee9ad3524475c28e307ea1ad9680a0bd2fb", nodeID: "178316dc209d1f6f8b6ed84f28f9f508ba0eb30a"},
		{seed: "node5", privHex: "95a2afbcc9d6e3f0fd0a1724313e4b5865727f8c99a6b3c0cddae7f4010e1b2897ddf5c415eb6bb51cd18f2f3ceb50d836cd2384c2b98be2f4f8dfb3a391027f", nodeID: "6465b73a9cc3ee572541eed8f3d2f81165a7a99f"},
		{seed: "node6", privHex: "015e6530f6163e26449bdc456f4a7e43886a5def57c5b1cb579000fce6bcb733e7381876f9c1606186d2186100cda248c17fe93f5d99c81d41f65aaf76ab2569", nodeID: "c80dade14cf019154af83a1386f8bacabc477db2"},
		{seed: "node7", privHex: "cf7e80b5c2b3feb16fd29a8ea8919face6ec74a10002f7315e11f36ed11bbced9c3effdadf4ef104bd74ad9f0d49c2759af2a1acf173ccd92fcd2e87f350e389", nodeID: "4db35cf58b792c12df1f487021e81ee89a26d480"},
		{seed: "node8", privHex: "493b32d66f96cf9642df12e6949acb65279f1870f1cab4d0075cf21a6aa9916d72386681e7eab24b35b9c68a82eb63f92cc920eb3902a89dab39feb2837c95cf", nodeID: "df40a797f4870c9bf31b71ccf84626a0ac306b53"},
	}

	var newIDs []identity
	for i := 9; i <= 20; i++ {
		label := fmt.Sprintf("drings-test-identity-%d", i)
		seed := sha256.Sum256([]byte(label))

		priv := ed25519.NewKeyFromSeed(seed[:])
		pub := priv.Public().(ed25519.PublicKey)

		nodeHash := sha1.Sum(pub)

		newIDs = append(newIDs, identity{
			seed:		label,
			privHex:	hex.EncodeToString(priv),
			nodeID:		hex.EncodeToString(nodeHash[:]),
		})
	}

	fmt.Println("// === New identities (9-20) ===")
	for i, id := range newIDs {
		fmt.Printf("// identity %d (seed: %s)\n", i+9, id.seed)
		fmt.Printf("// PrivKey: %s\n", id.privHex)
		fmt.Printf("// NodeID:  %s\n\n", id.nodeID)
	}

	all := append(existing, newIDs...)
	sort.Slice(all, func(i, j int) bool {
		return all[i].nodeID < all[j].nodeID
	})

	fmt.Println("// === Full pool of 20 identities, sorted by NodeID ===")
	fmt.Println("// Format: identityPool[i] = \"hex_private_key\" // NodeID: hex_nodeid")
	fmt.Println()
	for i, id := range all {
		fmt.Printf("identityPool[%d] = \"%s\" // NodeID: %s\n", i, id.privHex, id.nodeID)
	}

	fmt.Println()
	fmt.Println("// === Verification: re-derive NodeIDs for existing identities ===")
	for _, ex := range existing {
		privBytes, _ := hex.DecodeString(ex.privHex)
		priv := ed25519.PrivateKey(privBytes)
		pub := priv.Public().(ed25519.PublicKey)
		nodeHash := sha1.Sum(pub)
		computed := hex.EncodeToString(nodeHash[:])
		match := "OK"
		if computed != ex.nodeID {
			match = fmt.Sprintf("MISMATCH (got %s)", computed)
		}
		fmt.Printf("// %s: %s -> %s\n", ex.seed, ex.nodeID, match)
	}
}
