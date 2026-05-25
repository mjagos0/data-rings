package testrig

func UniqueFingerIDs(fingers []NodeAddr) []string {
	seen := make(map[string]bool)
	for _, f := range fingers {
		seen[f.ID] = true
	}
	result := make([]string, 0, len(seen))
	for id := range seen {
		result = append(result, id)
	}
	return result
}

func ShortIDs(ids []string) []string {
	result := make([]string, len(ids))
	for i, id := range ids {
		result[i] = NodeIDHexToShort(id)
	}
	return result
}

func StringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func SafeShortPred(pred *NodeAddr) string {
	if pred == nil {
		return "(nil)"
	}
	return NodeIDHexToShort(pred.ID)
}
