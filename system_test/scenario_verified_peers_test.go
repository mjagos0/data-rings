//go:build system

package system_test

import (
	"testing"

	"github.com/mjagos0/datarings/testrig"
)

func TestScenario_VerifiedPeers(t *testing.T) {
	c := testrig.NewCluster(t, testrig.StateFreshPrivate)
	nodes := c.Nodes8()
	c.Setup()

	group := c.GroupName()

	c.StabilizePrivate(group, len(nodes)+testrig.DefaultSuccListSize-2)

	verifiedMap := make(map[string]map[string]bool, len(nodes))
	idToName := make(map[string]string, len(nodes))

	for _, n := range nodes {
		entry, ok := n.PrivateState(group)
		if !ok {
			t.Fatalf("%s: group %q not in /debug/groups", n.Name(), group)
		}
		idToName[n.ID()] = n.Name()

		peerSet := make(map[string]bool, len(entry.VerifiedPeers))
		for _, pid := range entry.VerifiedPeers {
			peerSet[pid] = true
		}
		verifiedMap[n.ID()] = peerSet
		t.Logf("  %s: %d verified peers: %v",
			n.Name(), len(entry.VerifiedPeers), testrig.ShortIDs(entry.VerifiedPeers))
	}

	for _, n := range nodes {
		count := len(verifiedMap[n.ID()])
		if count < 2 {
			t.Errorf("[verified-8a] %s: only %d verified peers, want >= 2", n.Name(), count)
		}
	}

	asymmetric := 0
	for _, nA := range nodes {
		for verifiedID := range verifiedMap[nA.ID()] {
			if !verifiedMap[verifiedID][nA.ID()] {
				nameB := idToName[verifiedID]
				if nameB == "" {
					nameB = testrig.NodeIDHexToShort(verifiedID)
				}
				t.Errorf("[verified-8a] asymmetric: %s verified %s, but not vice versa",
					nA.Name(), nameB)
				asymmetric++
			}
		}
	}
	if asymmetric == 0 {
		t.Log("[verified-8a] all verification relationships are symmetric")
	}

	visited := make(map[string]bool)
	queue := []string{nodes[0].ID()}
	visited[nodes[0].ID()] = true

	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		for peer := range verifiedMap[curr] {
			if !visited[peer] {
				visited[peer] = true
				queue = append(queue, peer)
			}
		}
	}

	if len(visited) != len(nodes) {
		var unreachable []string
		for _, n := range nodes {
			if !visited[n.ID()] {
				unreachable = append(unreachable, n.Name())
			}
		}
		t.Errorf("[verified-8a] verification graph is disconnected; unreachable from %s: %v",
			nodes[0].Name(), unreachable)
	} else {
		t.Log("[verified-8a] verification graph is connected (all nodes reachable)")
	}
}
