package testrig

import (
	"crypto/sha1"
	"encoding/hex"
	"math/big"
	"sort"
)

type PoolIdentity struct {
	Index		int
	PrivKeyHex	string
	NodeIDHex	string
}

var IdentityPool = []PoolIdentity{
	{0, "dedc578db8a69858a197bbda1dd34ab4ba6a9d3c502cdf0ab444f4a62f0a61f74e5f1e2c8ba61f5d61b678c973fe0dbfcf8d774ee0dda867335c8369c7a19618", "04fc8b4a5768a80f17d438497ef6a3d29568cc4f"},
	{1, "16ac22e61bef388e1fdcbc6db6fa5bba303bf305a1e07ac05ef3763be60ae6c890a363cb9dd5522de596acc5020d2a9459e0b84329b289551376924ccdba7a1d", "1298e53f733ea29aba090c4d5d0c48fb8ebd6bc6"},
	{2, "3e6ad43fd4153c33d9d319fdbde0eef0ac3f31162e0558dd589294a4096c8fcf06ff5772102867b822188641ec99ce2a792fb516f8fc773af665c9c223f75d2c", "1f80de49ef24d716c86d39acadbe81081768f8e0"},
	{3, "ee4614e9c12cb3e0a2dd02e6a74f12e95b2c630f70294d5cf45691d375d989ffdde3290aad97629901f74af98231499ccff15f0ab29bcec444786f2fc3343eab", "2d23e00cd4fa18efb439efb28a5d2b747fcc5959"},
	{4, "b9a7ea0ddb250fd053b6a1a3515c62a359891bb6ea027c19457a3857794da150c36b6625ea9e753b3fd01770de4b80d7b96f5e23a0c86413dbe4fedb476fda55", "39aa2fb0fba1cf87599e1d1082b3e4fb212428d9"},
	{5, "a6797fafe6c9b3e3273821c637ae1a3c2970c1ac1f4e907c33c086afb65f99f212dd48718f9d18d44e3785b09d844a7a8fcd1298d3b5e7092d4d7131ad9d00dc", "4653b300ba06521ccfca5363097c5ba4120e8b6b"},
	{6, "2533decd7ecf4907fd94b1d2c16e0b56e156dc10e9fe146060b1be9b1901ff8d2838c8c4d528154cd0c00432b87f227624190313163f6da74ce7d0700515e2c1", "533739fd206c5c0a536974ad85aa8cdbeb9843f7"},
	{7, "dadd50cbebdcb1be801b4bfa709ec742e8282fe2972a8f335fc13d9024d91d610836a247e4a606b8656cefc3d112a0004aa76d88956211830be07f44c493ad9c", "60859eb185c923ec475679931655c9f365920e49"},
	{8, "0b67da35d9d96335eee2cb3cd5c23f41962b89b1d05d675d55dc1fa6b424af1c354dc80177409cc5211bf26e0178fe7804cf284ae8c9f4b525d9ac28676a2191", "6cd338509c0ba7ad6b5860ee9da020ced1570d74"},
	{9, "3602c3e1d66a59c5e96d1d36534aa2e1510327354c6b652ae225cb790811a4d5d8df77d3548b2d40caa31a76d6d6ac217c86085377431d77ca51436582f1da49", "78a2a60bdaf5a1384b7582e98739ca721fe8fca6"},
	{10, "f0a56aff2799ee51db6d8db28d2e578570c27ea72c7b247245310bf8fb546825d16a8ea6f1838b022fbe2ccc138a4719f92816d66495d296ceb29f967ecc062f", "88b3f91d4213e32e7be7d1e9d0d505072c52efbc"},
	{11, "345eee784443f95572374a81e192de145a0ed4ed58fd8642ebe5aefb898add5fc66dc2559f11ec91b53fcddf572c21b17c23bfbdcffb71e14014c4c0d8d008e3", "9369ba40ec39365a7f42110d3ced07a84a4f1e8e"},
	{12, "d737c4b6b5d2bdd99813b9daeef64503dc4e04530a93e4546c534934f018acbce75e1f8df16ab3025fc4780a89445c9de0b66a121fbe574623a912344e73e8f7", "9fd7f75acb5cbb33cbcc6e955a110d8b7b5123c3"},
	{13, "712edc996ea95e65c287b368439a993d13ef4217df426ec7882512c46e2d08cd3859127dde88221205587bbad67eac69429477696242f47ea4f126ad2165f036", "acfd0e989ba95b91e945448d98c5dd7b79158afa"},
	{14, "5a8d213b06a4aa991ab02c5d503e0b2ca80a6bd7b0f5691a7ec81186d1e4358b8d567eed8150110b5e287505d51345589a4e7bccb63fbeb35a01c3cbef0b0002", "b9819220295df2268ba90a7c0807d1b0395865c1"},
	{15, "a0ad6a6376a9b3534f20cc5d11bcb6ec75a86ac10ed2c51226983f34cb9d1ff6a239f867e80caf4fc1846ec4918188713cfb6ec0921ff087e821ec7bca8082a7", "c6413662f9bded6dc09b6fec86a2f1445f81aef9"},
	{16, "715d900e3da97f217c28d5e7938497c26872bb90216b4d44ffbbbdce5b830a4d2f13060604e72b7edf96f626f9c2654e1da6e5009a1102e71d209eabaf3df558", "d319aba431c616ed8b659253bd8382a56c61eabf"},
	{17, "f4f6a2d6bb406af5876b628a89b3a8279d370bc7854ef4e63ade4066305cb2430edd1342de54999dea76465c20086d07426f87fa6d2013ae9378c615c1cac6af", "df47531852f7e2a6cae19679484a6be3ce8f7f43"},
	{18, "4d6af888e75d8d67937cd6742718f263736a973b125612b739ec12bf48b9b8653b7a62338d4a35a3a21d20f1db0ae35481e43a6697ba8572867c3486de1274a7", "ece72694f531849390aefe1b740b0b1b6a87d7ee"},
	{19, "a159a4a173c646e2a6b766d4624208cd80cb9a9316f333570bfb15c500f2e76ace3f5891ab7bae269d2f1a88ce9f71ff268761b0d0fd0cb93f6257364e06f9d2", "f96efa215ba3e5cfba488fd47f1390d74083a976"},
}

var AllNodes = []string{
	"node1", "node2", "node3", "node4",
	"node5", "node6", "node7", "node8",
	"node9", "node10", "node11", "node12",
	"node13", "node14", "node15", "node16",
}

const DefaultSuccListSize = 5

func SelectIdentities(n int) []PoolIdentity {
	if n > len(IdentityPool) {
		n = len(IdentityPool)
	}
	if n == len(IdentityPool) {
		result := make([]PoolIdentity, len(IdentityPool))
		copy(result, IdentityPool)
		return result
	}
	selected := make([]PoolIdentity, 0, n)
	step := float64(len(IdentityPool)) / float64(n)
	for i := 0; i < n; i++ {
		idx := int(float64(i) * step)
		if idx >= len(IdentityPool) {
			idx = len(IdentityPool) - 1
		}
		selected = append(selected, IdentityPool[idx])
	}
	return selected
}

var ringSize = new(big.Int).Lsh(big.NewInt(1), 160)

func nodeIDFromHex(h string) [20]byte {
	var id [20]byte
	b, _ := hex.DecodeString(h)
	copy(id[:], b)
	return id
}

func fingerStart(id [20]byte, i int) [20]byte {
	n := new(big.Int).SetBytes(id[:])
	pow := new(big.Int).Lsh(big.NewInt(1), uint(i))
	result := new(big.Int).Add(n, pow)
	result.Mod(result, ringSize)
	var out [20]byte
	b := result.Bytes()
	if len(b) <= 20 {
		copy(out[20-len(b):], b)
	}
	return out
}

func idLessOrEqual(a, b [20]byte) bool {
	for i := 0; i < 20; i++ {
		if a[i] < b[i] {
			return true
		}
		if a[i] > b[i] {
			return false
		}
	}
	return true
}

func findSuccessorOnRing(target [20]byte, sorted []PoolIdentity) string {
	for _, ident := range sorted {
		id := nodeIDFromHex(ident.NodeIDHex)
		if idLessOrEqual(target, id) {
			return ident.NodeIDHex
		}
	}
	return sorted[0].NodeIDHex
}

type RingTopology struct {
	Nodes		[]PoolIdentity
	Successors	map[string]string
	Predecessors	map[string]string
	SuccessorLists	map[string][]string
	UniqueFingers	map[string][]string
	FullFingers	map[string][160]string
}

type IdentityAssignment struct {
	NodeToIdentity	map[string]PoolIdentity
	NodeToID	map[string]string
	IDToNode	map[string]string
	Topology	*RingTopology
}

func AssignIdentities(nodes []string, identities []PoolIdentity, k int) *IdentityAssignment {
	a := &IdentityAssignment{
		NodeToIdentity:	make(map[string]PoolIdentity, len(nodes)),
		NodeToID:	make(map[string]string, len(nodes)),
		IDToNode:	make(map[string]string, len(nodes)),
	}
	for i, node := range nodes {
		a.NodeToIdentity[node] = identities[i]
		a.NodeToID[node] = identities[i].NodeIDHex
		a.IDToNode[identities[i].NodeIDHex] = node
	}
	a.Topology = ComputeTopology(identities, k)
	return a
}

func ComputeTopology(identities []PoolIdentity, k int) *RingTopology {
	sorted := make([]PoolIdentity, len(identities))
	copy(sorted, identities)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].NodeIDHex < sorted[j].NodeIDHex
	})

	n := len(sorted)
	t := &RingTopology{
		Nodes:		sorted,
		Successors:	make(map[string]string, n),
		Predecessors:	make(map[string]string, n),
		SuccessorLists:	make(map[string][]string, n),
		UniqueFingers:	make(map[string][]string, n),
		FullFingers:	make(map[string][160]string, n),
	}

	for i := range sorted {
		succIdx := (i + 1) % n
		predIdx := (i + n - 1) % n
		t.Successors[sorted[i].NodeIDHex] = sorted[succIdx].NodeIDHex
		t.Predecessors[sorted[i].NodeIDHex] = sorted[predIdx].NodeIDHex
	}

	for i := range sorted {
		listLen := k
		if listLen > n-1 {
			listLen = n - 1
		}
		list := make([]string, listLen)
		for j := 0; j < listLen; j++ {
			list[j] = sorted[(i+1+j)%n].NodeIDHex
		}
		t.SuccessorLists[sorted[i].NodeIDHex] = list
	}

	ids := make([][20]byte, n)
	for i, ident := range sorted {
		ids[i] = nodeIDFromHex(ident.NodeIDHex)
	}

	for i := range sorted {
		var fingers [160]string
		uniqueSet := make(map[string]bool)
		for fi := 0; fi < 160; fi++ {
			target := fingerStart(ids[i], fi)
			fingerNode := findSuccessorOnRing(target, sorted)
			fingers[fi] = fingerNode
			uniqueSet[fingerNode] = true
		}
		t.FullFingers[sorted[i].NodeIDHex] = fingers
		unique := make([]string, 0, len(uniqueSet))
		for id := range uniqueSet {
			unique = append(unique, id)
		}
		sort.Strings(unique)
		t.UniqueFingers[sorted[i].NodeIDHex] = unique
	}

	return t
}

func ReplicaNodes(keyHex string, identities []PoolIdentity, k int) []string {
	sorted := make([]PoolIdentity, len(identities))
	copy(sorted, identities)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].NodeIDHex < sorted[j].NodeIDHex
	})

	keyID := nodeIDFromHex(keyHex)
	primary := findSuccessorOnRing(keyID, sorted)
	primaryIdx := -1
	for i, ident := range sorted {
		if ident.NodeIDHex == primary {
			primaryIdx = i
			break
		}
	}

	replicaCount := k
	if replicaCount > len(sorted) {
		replicaCount = len(sorted)
	}
	replicas := make([]string, replicaCount)
	for i := 0; i < replicaCount; i++ {
		replicas[i] = sorted[(primaryIdx+i)%len(sorted)].NodeIDHex
	}
	return replicas
}

func NodeIDHexToShort(h string) string {
	if len(h) > 8 {
		return h[:8]
	}
	return h
}

func CIDKeyHex(cidBytes []byte) string {
	h := sha1.Sum(cidBytes)
	return hex.EncodeToString(h[:])
}
