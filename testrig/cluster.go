package testrig

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
)

type StartState int

const (
	StateFreshPublic	StartState	= iota
	StateFreshPrivate
)

const TestGroupKey = "8461a8890a7537cf2a1db0f2077cf7d1843732867684ee2cca7f457eeb3cf9504c8c6cb5de526a23cc43390feee529944c46de5855b3b562a7749f9797f2a300"

func (s StartState) String() string {
	switch s {
	case StateFreshPublic:
		return "FreshPublic"
	case StateFreshPrivate:
		return "FreshPrivate"
	default:
		return "Unknown"
	}
}

type Backend interface {
	Setup(t *testing.T, nodes []*TestNode, state StartState)
	CreatePrivateRing(t *testing.T, nodes []*TestNode, groupName string) string
	PublicState(n *TestNode) (NodeState, error)
	PrivateState(n *TestNode, groupName string) (PrivateRingEntry, bool)
	Exec(n *TestNode, args ...string) (string, error)
	Stop(n *TestNode) error
	Start(n *TestNode) error
	StoreFile(n *TestNode, data []byte, group string) (string, error)
	StoreFileWithTTL(n *TestNode, data []byte, group string, ttl string) (string, error)
	FetchCID(n *TestNode, cidStr string, group string) error
	ForceStabilize(n *TestNode) error
	ForceStabilizePrivate(n *TestNode, groupRef string) error

	WaitForReplicationDrain(n *TestNode, timeout time.Duration) error
	HasRecord(n *TestNode, keyHex string) (bool, error)
	HasBlock(n *TestNode, cidStr string) (bool, error)
	RecordKeys(n *TestNode) ([]string, error)

	AddLocal(n *TestNode, data []byte) (string, error)

	DeleteBlock(n *TestNode, cidStr string) error

	DeleteCID(n *TestNode, cidStr string, group string) error

	NetworkRoots(n *TestNode) ([]string, error)

	HasNetworkBlock(n *TestNode, cidStr string) (bool, error)

	NetworkBlockCIDs(n *TestNode) ([]string, error)

	RingNetworkBlockCIDs(n *TestNode, ringID string) ([]string, error)

	RingNetworkRoots(n *TestNode, ringID string) ([]string, error)

	RingStorage(n *TestNode, ringID string) (used, max int64, err error)

	DataDir(n *TestNode) string

	RestartOnNewPort(n *TestNode, newDHTPort, newAPIPort int) error

	NodeLogsTail(name string, n int) (string, error)

	RepublishSelf(n *TestNode) error

	DeleteRecord(n *TestNode, keyHex string) error

	NodeLogsSince(name string, since, until time.Time) (string, error)
	Close() error
}

type TestNode struct {
	Name_		string
	Identity	PoolIdentity
	Cluster_	*Cluster
}

func (n *TestNode) Name() string		{ return n.Name_ }
func (n *TestNode) ID() string			{ return n.Identity.NodeIDHex }
func (n *TestNode) GetIdentity() PoolIdentity	{ return n.Identity }

func (n *TestNode) PublicState() (NodeState, error) {
	return n.Cluster_.Backend.PublicState(n)
}

func (n *TestNode) PrivateState(group string) (PrivateRingEntry, bool) {
	return n.Cluster_.Backend.PrivateState(n, group)
}

func (n *TestNode) Exec(args ...string) (string, error) {
	return n.Cluster_.Backend.Exec(n, args...)
}

func (n *TestNode) Stop() error		{ return n.Cluster_.Backend.Stop(n) }
func (n *TestNode) Start() error	{ return n.Cluster_.Backend.Start(n) }

func (n *TestNode) AddLocal(data []byte) (string, error) {
	return n.Cluster_.Backend.AddLocal(n, data)
}

func (n *TestNode) StoreFile(data []byte, group string) (string, error) {
	return n.Cluster_.Backend.StoreFile(n, data, group)
}

func (n *TestNode) StoreFileWithTTL(data []byte, group string, ttl string) (string, error) {
	return n.Cluster_.Backend.StoreFileWithTTL(n, data, group, ttl)
}

func (n *TestNode) FetchCID(cidStr string, group string) error {
	return n.Cluster_.Backend.FetchCID(n, cidStr, group)
}

func (n *TestNode) ForceStabilize() error {
	return n.Cluster_.Backend.ForceStabilize(n)
}

func (n *TestNode) ForceStabilizePrivate(groupRef string) error {
	return n.Cluster_.Backend.ForceStabilizePrivate(n, groupRef)
}

func (n *TestNode) WaitForReplicationDrain(timeout time.Duration) error {
	return n.Cluster_.Backend.WaitForReplicationDrain(n, timeout)
}

func (n *TestNode) HasRecord(keyHex string) (bool, error) {
	return n.Cluster_.Backend.HasRecord(n, keyHex)
}

func (n *TestNode) HasBlock(cidStr string) (bool, error) {
	return n.Cluster_.Backend.HasBlock(n, cidStr)
}

func (n *TestNode) RecordKeys() ([]string, error) {
	return n.Cluster_.Backend.RecordKeys(n)
}

func (n *TestNode) DeleteBlock(cidStr string) error {
	return n.Cluster_.Backend.DeleteBlock(n, cidStr)
}

func (n *TestNode) DeleteCID(cidStr string, group string) error {
	return n.Cluster_.Backend.DeleteCID(n, cidStr, group)
}

func (n *TestNode) NetworkRoots() ([]string, error) {
	return n.Cluster_.Backend.NetworkRoots(n)
}

func (n *TestNode) HasNetworkBlock(cidStr string) (bool, error) {
	return n.Cluster_.Backend.HasNetworkBlock(n, cidStr)
}

func (n *TestNode) NetworkBlockCIDs() ([]string, error) {
	return n.Cluster_.Backend.NetworkBlockCIDs(n)
}

func (n *TestNode) RingNetworkBlockCIDs(ringRef string) ([]string, error) {
	return n.Cluster_.Backend.RingNetworkBlockCIDs(n, n.resolveRingID(ringRef))
}

func (n *TestNode) RingNetworkRoots(ringRef string) ([]string, error) {
	return n.Cluster_.Backend.RingNetworkRoots(n, n.resolveRingID(ringRef))
}

func (n *TestNode) RingStorageUsedBytes(ringRef string) (int64, error) {
	used, _, err := n.Cluster_.Backend.RingStorage(n, n.resolveRingID(ringRef))
	return used, err
}

func (n *TestNode) RingStorageMaxBytes(ringRef string) (int64, error) {
	_, max, err := n.Cluster_.Backend.RingStorage(n, n.resolveRingID(ringRef))
	return max, err
}

func (n *TestNode) RingBlockCount(ringRef string) (int, error) {
	cids, err := n.RingNetworkBlockCIDs(ringRef)
	if err != nil {
		return 0, err
	}
	return len(cids), nil
}

func (n *TestNode) RingNetworkRootCount(ringRef string) (int, error) {
	roots, err := n.RingNetworkRoots(ringRef)
	if err != nil {
		return 0, err
	}
	return len(roots), nil
}

func (n *TestNode) DataDir() string {
	return n.Cluster_.Backend.DataDir(n)
}

func (n *TestNode) RestartOnNewPort(newDHTPort, newAPIPort int) error {
	return n.Cluster_.Backend.RestartOnNewPort(n, newDHTPort, newAPIPort)
}

func (n *TestNode) resolveRingID(ringRef string) string {
	if entry, ok := n.PrivateState(ringRef); ok && entry.GroupID != "" {
		return entry.GroupID
	}
	return ringRef
}

func (n *TestNode) RecordCount() (int, error) {
	s, err := n.PublicState()
	if err != nil {
		return 0, err
	}
	return s.RecordCount, nil
}

func (n *TestNode) BlockCount() (int, error) {
	s, err := n.PublicState()
	if err != nil {
		return 0, err
	}
	return s.BlockCount, nil
}

func (n *TestNode) StorageUsed() (int64, error) {
	s, err := n.PublicState()
	if err != nil {
		return 0, err
	}
	return s.StorageUsedBytes, nil
}

func (n *TestNode) StorageMax() (int64, error) {
	s, err := n.PublicState()
	if err != nil {
		return 0, err
	}
	return s.StorageMaxBytes, nil
}

func (n *TestNode) PrivateBlockCount(group string) (int, error) {
	e, ok := n.PrivateState(group)
	if !ok {
		return -1, nil
	}
	return e.Node.BlockCount, nil
}

type Cluster struct {
	T		*testing.T
	Backend		Backend
	Nodes_		[]*TestNode
	NodeMap		map[string]*TestNode
	State		StartState
	GroupKey_	string
	GroupName_	string
	Topology_	*RingTopology
	Assignment_	*IdentityAssignment
	StartTime	time.Time
	DaemonArgs	[]string
}

func NewCluster(t *testing.T, state StartState) *Cluster {
	t.Helper()
	backend := SelectBackend(t)
	c := &Cluster{
		T:		t,
		Backend:	backend,
		NodeMap:	make(map[string]*TestNode),
		State:		state,
		GroupName_:	"test-ring",
	}
	t.Cleanup(func() { backend.Close() })
	return c
}

func (c *Cluster) Node(name string, identity PoolIdentity) *TestNode {
	n := &TestNode{Name_: name, Identity: identity, Cluster_: c}
	c.Nodes_ = append(c.Nodes_, n)
	c.NodeMap[name] = n
	return n
}

func (c *Cluster) Nodes8() []*TestNode {
	ids := SelectIdentities(8)
	nodes := make([]*TestNode, 8)
	for i := 0; i < 8; i++ {
		nodes[i] = c.Node(AllNodes[i], ids[i])
	}
	return nodes
}

func (c *Cluster) Nodes16() []*TestNode {
	ids := SelectIdentities(16)
	nodes := make([]*TestNode, 16)
	for i := 0; i < 16; i++ {
		nodes[i] = c.Node(AllNodes[i], ids[i])
	}
	return nodes
}

func (c *Cluster) Setup() {
	c.T.Helper()
	c.SetupDaemonsOnly()

	c.Stabilize(len(c.Nodes_) + DefaultSuccListSize - 2)
	c.RepublishSelf()

	if c.State == StateFreshPrivate {
		c.GroupKey_ = c.Backend.CreatePrivateRing(c.T, c.Nodes_, c.GroupName_)
		c.T.Logf("[cluster] private ring %q created", c.GroupName_)
	}
}

func (c *Cluster) SetupDaemonsOnly() {
	c.T.Helper()
	if len(c.Nodes_) == 0 {
		c.T.Fatal("Cluster.SetupDaemonsOnly: no nodes registered")
	}

	c.StartTime = time.Now()

	names := make([]string, len(c.Nodes_))
	identities := make([]PoolIdentity, len(c.Nodes_))
	for i, n := range c.Nodes_ {
		names[i] = n.Name_
		identities[i] = n.Identity
	}
	c.Assignment_ = AssignIdentities(names, identities, DefaultSuccListSize)
	c.Topology_ = c.Assignment_.Topology

	c.T.Logf("[cluster] setup %d nodes, state=%s, start=%s",
		len(c.Nodes_), c.State, c.StartTime.Format(time.RFC3339))
	c.Backend.Setup(c.T, c.Nodes_, c.State)
}

func (c *Cluster) DumpTestLogs() {
	c.T.Helper()
	endTime := time.Now()
	c.T.Logf("[logs] test window: %s to %s", c.StartTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
	for _, node := range c.Nodes_ {
		logs, err := c.Backend.NodeLogsSince(node.Name_, c.StartTime, endTime)
		if err != nil {
			c.T.Logf("[logs] %s: error: %v", node.Name_, err)
			continue
		}
		c.T.Logf("[logs] === %s ===\n%s", node.Name_, logs)
	}
}

func (c *Cluster) NodeTestLogs(name string) string {
	logs, err := c.Backend.NodeLogsSince(name, c.StartTime, time.Now())
	if err != nil {
		return fmt.Sprintf("error fetching logs: %v", err)
	}
	return logs
}

func (c *Cluster) Stabilize(rounds int) {
	c.T.Helper()
	for r := 0; r < rounds; r++ {
		for _, n := range c.Nodes_ {
			if err := n.ForceStabilize(); err != nil {
				c.T.Logf("[stabilize] %s public round %d: %v", n.Name(), r, err)
			}
		}
	}
	c.T.Logf("[cluster] stabilized public ring: %d rounds across %d nodes", rounds, len(c.Nodes_))
}

func (c *Cluster) StabilizeTrace(rounds int) {
	c.T.Helper()
	topo := c.Topology()
	for r := 0; r < rounds; r++ {
		for _, n := range c.Nodes_ {
			if err := n.ForceStabilize(); err != nil {
				c.T.Logf("[trace] %s round %d: %v", n.Name(), r, err)
			}
		}

		for _, n := range c.Nodes_ {
			s, err := n.PublicState()
			if err != nil {
				c.T.Logf("[trace] round %d %s: error: %v", r, n.Name(), err)
				continue
			}
			succOK := s.Successor.ID == topo.Successors[n.ID()]
			predOK := s.Predecessor != nil && s.Predecessor.ID == topo.Predecessors[n.ID()]
			expectedSuccList := topo.SuccessorLists[n.ID()]
			listOK := len(s.SuccessorList) >= len(expectedSuccList)
			if listOK {
				for i, exp := range expectedSuccList {
					if i < len(s.SuccessorList) && s.SuccessorList[i].ID != exp {
						listOK = false
						break
					}
				}
			}
			c.T.Logf("[trace] round %d %s: succ=%s(%v) pred=%v list_len=%d(%v)",
				r, n.Name(),
				NodeIDHexToShort(s.Successor.ID), succOK,
				predOK, len(s.SuccessorList), listOK)
		}
	}
}

func (c *Cluster) StabilizePrivate(groupRef string, rounds int) {
	c.T.Helper()
	for r := 0; r < rounds; r++ {
		for _, n := range c.Nodes_ {
			if err := n.ForceStabilizePrivate(groupRef); err != nil {
				c.T.Logf("[stabilize] %s private(%s) round %d: %v", n.Name(), groupRef, r, err)
			}
		}
	}
	c.T.Logf("[cluster] stabilized private ring %q: %d rounds across %d nodes", groupRef, rounds, len(c.Nodes_))
}

func (c *Cluster) Wait(d time.Duration) {
	c.T.Logf("[cluster] waiting %v", d)
	time.Sleep(d)
}

func (c *Cluster) WaitForConvergence(timeout time.Duration) {
	c.T.Helper()
	topo := c.Topology()
	deadline := time.Now().Add(timeout)
	var lastMismatch string
	rounds := 0
	for time.Now().Before(deadline) {
		c.parallelNodes(func(n *TestNode) error {
			return n.ForceStabilize()
		}, nil)
		rounds++

		allMatch := true
		for _, n := range c.Nodes_ {
			s, err := n.PublicState()
			if err != nil {
				allMatch = false
				lastMismatch = fmt.Sprintf("%s: fetch state: %v", n.Name(), err)
				break
			}
			if msg := matchTopology(n.Name(), n.ID(), s, topo); msg != "" {
				allMatch = false
				lastMismatch = msg
				break
			}
		}
		if allMatch {
			c.T.Logf("[cluster] public ring converged after %d rounds (%v)", rounds, time.Since(c.StartTime))
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	c.T.Fatalf("[cluster] public ring did not converge within %v (%d rounds): %s", timeout, rounds, lastMismatch)
}

func (c *Cluster) WaitForPrivateConvergence(group string, nodes []*TestNode, topo *RingTopology, timeout time.Duration) {
	c.T.Helper()
	deadline := time.Now().Add(timeout)
	assignment := c.Assignment()
	var lastMismatch string
	rounds := 0
	for time.Now().Before(deadline) {
		c.parallelNodes(func(n *TestNode) error {
			return n.ForceStabilizePrivate(group)
		}, nil)
		rounds++

		allMatch := true
		for _, n := range nodes {
			entry, ok := n.PrivateState(group)
			if !ok {
				allMatch = false
				lastMismatch = fmt.Sprintf("%s: group %q not in /debug/groups", n.Name(), group)
				break
			}
			nodeID := assignment.NodeToID[n.Name()]
			if msg := matchTopology(n.Name(), nodeID, entry.Node, topo); msg != "" {
				allMatch = false
				lastMismatch = msg
				break
			}
		}
		if allMatch {
			c.T.Logf("[cluster] private ring %q converged after %d rounds (%v)", group, rounds, time.Since(c.StartTime))
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	c.T.Fatalf("[cluster] private ring %q did not converge within %v (%d rounds): %s", group, timeout, rounds, lastMismatch)
}

func matchTopology(nodeName, expectedID string, s NodeState, topo *RingTopology) string {
	if s.ID != expectedID {
		return fmt.Sprintf("%s: ID=%s, want %s", nodeName, s.ID, expectedID)
	}
	if s.Successor.ID != topo.Successors[expectedID] {
		return fmt.Sprintf("%s: successor=%s, want %s",
			nodeName, NodeIDHexToShort(s.Successor.ID), NodeIDHexToShort(topo.Successors[expectedID]))
	}
	expectedPred := topo.Predecessors[expectedID]
	if s.Predecessor == nil {
		return fmt.Sprintf("%s: predecessor=nil, want %s", nodeName, NodeIDHexToShort(expectedPred))
	}
	if s.Predecessor.ID != expectedPred {
		return fmt.Sprintf("%s: predecessor=%s, want %s",
			nodeName, NodeIDHexToShort(s.Predecessor.ID), NodeIDHexToShort(expectedPred))
	}
	expectedSuccList := topo.SuccessorLists[expectedID]
	actualSuccIDs := make([]string, len(s.SuccessorList))
	for i, na := range s.SuccessorList {
		actualSuccIDs[i] = na.ID
	}
	if len(actualSuccIDs) < len(expectedSuccList) {
		return fmt.Sprintf("%s: successor_list len=%d, want >=%d",
			nodeName, len(actualSuccIDs), len(expectedSuccList))
	}
	for i, expected := range expectedSuccList {
		if i < len(actualSuccIDs) && actualSuccIDs[i] != expected {
			return fmt.Sprintf("%s: successor_list[%d]=%s, want %s",
				nodeName, i, NodeIDHexToShort(actualSuccIDs[i]), NodeIDHexToShort(expected))
		}
	}
	expectedUnique := topo.UniqueFingers[expectedID]
	actualUnique := UniqueFingerIDs(s.Fingers)
	sort.Strings(actualUnique)
	if !StringSliceEqual(actualUnique, expectedUnique) {
		return fmt.Sprintf("%s: unique_fingers=%v, want %v",
			nodeName, ShortIDs(actualUnique), ShortIDs(expectedUnique))
	}
	return ""
}

func (c *Cluster) RepublishSelf() {
	c.T.Helper()
	c.parallelNodes(func(n *TestNode) error {
		return c.Backend.RepublishSelf(n)
	}, func(n *TestNode, err error) {
		c.T.Logf("[republish] %s: %v", n.Name(), err)
	})
	c.T.Log("[cluster] republished PeerIdentityRecords")
}

func (c *Cluster) PruneStaleRecords(k int) {
	c.T.Helper()
	pruned := 0
	for _, n := range c.Nodes_ {
		keys, err := n.RecordKeys()
		if err != nil {
			c.T.Logf("[prune] %s: RecordKeys: %v", n.Name(), err)
			continue
		}
		for _, key := range keys {
			expectedReplicas := c.ReplicaNodesForHash(key, k)
			expectedSet := NameSet(expectedReplicas)
			if !expectedSet[n.Name()] {
				_ = c.Backend.DeleteRecord(n, key)
				pruned++
			}
		}
	}
	c.T.Logf("[cluster] pruned %d stale records", pruned)
}

func (c *Cluster) parallelNodes(fn func(*TestNode) error, onErr func(*TestNode, error)) {
	type result struct {
		node	*TestNode
		err	error
	}
	results := make([]result, len(c.Nodes_))
	var wg sync.WaitGroup
	for i, n := range c.Nodes_ {
		wg.Add(1)
		go func(i int, n *TestNode) {
			defer wg.Done()
			results[i] = result{node: n, err: fn(n)}
		}(i, n)
	}
	wg.Wait()
	for _, r := range results {
		if r.err != nil && onErr != nil {
			onErr(r.node, r.err)
		}
	}
}

func (c *Cluster) Topology() *RingTopology		{ return c.Topology_ }
func (c *Cluster) Assignment() *IdentityAssignment	{ return c.Assignment_ }
func (c *Cluster) GroupName() string			{ return c.GroupName_ }
func (c *Cluster) GroupKey() string			{ return c.GroupKey_ }
func (c *Cluster) NodeByName(name string) *TestNode	{ return c.NodeMap[name] }
func (c *Cluster) Nodes() []*TestNode			{ return c.Nodes_ }

func (c *Cluster) NodeByID(nodeIDHex string) *TestNode {
	for _, n := range c.Nodes_ {
		if n.Identity.NodeIDHex == nodeIDHex {
			return n
		}
	}
	return nil
}

func (c *Cluster) WaitForReplicationDrain(timeout time.Duration) {
	c.T.Helper()
	errs := make(chan error, len(c.Nodes_))
	for _, n := range c.Nodes_ {
		go func(n *TestNode) {
			errs <- n.WaitForReplicationDrain(timeout)
		}(n)
	}
	for i := 0; i < len(c.Nodes_); i++ {
		if err := <-errs; err != nil {
			c.T.Fatalf("[cluster] replication drain: %v", err)
		}
	}
}

func (c *Cluster) WaitForBlockReplicas(cidStr string, k int, timeout time.Duration) ([]string, bool) {
	c.T.Helper()
	deadline := time.Now().Add(timeout)
	var holders []string
	for {
		holders = holders[:0]
		for _, n := range c.Nodes_ {
			has, err := n.HasBlock(cidStr)
			if err == nil && has {
				holders = append(holders, n.Name())
			}
		}
		if len(holders) >= k {
			return holders, true
		}
		if time.Now().After(deadline) {
			return holders, false
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (c *Cluster) WaitForBlockReplicasOnNodes(cidStr string, expected []*TestNode, timeout time.Duration) ([]string, bool) {
	c.T.Helper()
	deadline := time.Now().Add(timeout)
	var holders []string
	for {
		holders = holders[:0]
		for _, n := range expected {
			has, err := n.HasBlock(cidStr)
			if err == nil && has {
				holders = append(holders, n.Name())
			}
		}
		if len(holders) == len(expected) {
			return holders, true
		}
		if time.Now().After(deadline) {
			return holders, false
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (c *Cluster) ReplicaNodesForCID(cidStr string, k int) []*TestNode {
	keyHex := CIDToRingKeyHex(cidStr)
	if keyHex == "" {
		return nil
	}
	return c.ReplicaNodesForHash(keyHex, k)
}

func (c *Cluster) ReplicaNodesForHash(hashHex string, k int) []*TestNode {
	ids := c.poolIdentities()
	replicas := ReplicaNodes(hashHex, ids, k)
	result := make([]*TestNode, 0, len(replicas))
	for _, nodeIDHex := range replicas {
		if n := c.NodeByID(nodeIDHex); n != nil {
			result = append(result, n)
		}
	}
	return result
}

func (c *Cluster) ReplicaNodesForHashIn(hashHex string, members []*TestNode, k int) []*TestNode {
	ids := make([]PoolIdentity, len(members))
	for i, n := range members {
		ids[i] = n.Identity
	}
	replicas := ReplicaNodes(hashHex, ids, k)
	out := make([]*TestNode, 0, len(replicas))
	for _, nodeIDHex := range replicas {
		for _, n := range members {
			if n.Identity.NodeIDHex == nodeIDHex {
				out = append(out, n)
				break
			}
		}
	}
	return out
}

func (c *Cluster) NonReplicaNodes(cidStr string, k int) []*TestNode {
	replicaSet := make(map[string]bool)
	for _, n := range c.ReplicaNodesForCID(cidStr, k) {
		replicaSet[n.Name_] = true
	}
	var result []*TestNode
	for _, n := range c.Nodes_ {
		if !replicaSet[n.Name_] {
			result = append(result, n)
		}
	}
	return result
}

func (c *Cluster) poolIdentities() []PoolIdentity {
	ids := make([]PoolIdentity, len(c.Nodes_))
	for i, n := range c.Nodes_ {
		ids[i] = n.Identity
	}
	return ids
}

func CIDToRingKeyHex(cidStr string) string {
	c, err := cid.Decode(cidStr)
	if err != nil {
		return ""
	}
	h := sha1.Sum(c.Hash())
	return hex.EncodeToString(h[:])
}

func SelectBackend(t *testing.T) Backend {
	t.Helper()
	switch os.Getenv("DRINGS_BACKEND") {
	case "remote":
		t.Log("[backend] remote (AWS)")
		return NewRemoteBackend(t)
	default:
		t.Log("[backend] local (processes)")
		return NewLocalBackend(t)
	}
}

func TestNodeNames(nodes []*TestNode) []string {
	names := make([]string, len(nodes))
	for i, n := range nodes {
		names[i] = n.Name_
	}
	return names
}

func NameSet(nodes []*TestNode) map[string]bool {
	s := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		s[n.Name()] = true
	}
	return s
}

func NodeNamesFromSlice(nodes []*TestNode) []string {
	names := make([]string, len(nodes))
	for i, n := range nodes {
		names[i] = n.Name()
	}
	return names
}

func PeerIDSet(nodes []*TestNode) map[string]bool {
	s := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		s[n.ID()] = true
	}
	return s
}

func PublicStateSSH(t *testing.T, pemFile string, inst *SystemInstance) (NodeState, error) {
	t.Helper()
	raw, err := SSH(pemFile, inst, `curl -sf http://localhost:7423/debug/state 2>/dev/null`)
	if err != nil || raw == "" {
		return NodeState{}, fmt.Errorf("node %s: debug/state unavailable: %v", inst.Name, err)
	}
	var s NodeState
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		return NodeState{}, fmt.Errorf("node %s: parse debug/state: %v (raw: %.200s)", inst.Name, err, raw)
	}
	return s, nil
}

func PrivateStateForGroupSSH(t *testing.T, pemFile string, inst *SystemInstance, groupName string) (PrivateRingEntry, bool) {
	t.Helper()
	raw, err := SSH(pemFile, inst, `curl -sf http://localhost:7423/debug/groups 2>/dev/null`)
	if err != nil || raw == "" || raw == "null" || raw == "[]" {
		return PrivateRingEntry{}, false
	}
	var entries []PrivateRingEntry
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		return PrivateRingEntry{}, false
	}
	for _, e := range entries {
		if e.Name == groupName || len(e.GroupID) > 0 && len(groupName) > 0 && e.GroupID[:min(len(e.GroupID), len(groupName))] == groupName {
			return e, true
		}
	}
	return PrivateRingEntry{}, false
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
