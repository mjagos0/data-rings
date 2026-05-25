package testrig

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type remoteBackend struct {
	t		*testing.T
	cfg		*SystemConfig
	pemFile		string
	bootstrapIP	string
	daemonArgs	[]string
}

func NewRemoteBackend(t *testing.T) *remoteBackend {
	t.Helper()
	cfg := LoadSystemConfig(t)
	pemFile := WritePEM(t, cfg.PEMKey)
	return &remoteBackend{t: t, cfg: cfg, pemFile: pemFile}
}

func (b *remoteBackend) Setup(t *testing.T, nodes []*TestNode, state StartState) {
	t.Helper()

	t.Log("[remote] pre-flight: checking SSH reachability")
	nodeNames := TestNodeNames(nodes)
	for _, name := range nodeNames {
		inst := b.cfg.Instance(name)
		if inst == nil {
			t.Fatalf("[remote] node %s not found in test-fleet.toml", name)
		}
		if _, err := SSH(b.pemFile, inst, "true"); err != nil {
			t.Fatalf("[remote] node %s SSH unreachable: %v", name, err)
		}
	}
	t.Log("[remote] pre-flight: all nodes SSH-reachable")

	names := TestNodeNames(nodes)
	identities := make([]PoolIdentity, len(nodes))
	for i, n := range nodes {
		identities[i] = n.Identity
	}
	assignment := AssignIdentities(names, identities, DefaultSuccListSize)

	bootstrapInst := b.cfg.Instance(names[0])
	if bootstrapInst != nil {
		b.bootstrapIP = bootstrapInst.IPv4
	}
	b.daemonArgs = nodes[0].Cluster_.DaemonArgs

	CleanWithPoolIdentities(t, names, assignment, b.daemonArgs...)
}

func (b *remoteBackend) CreatePrivateRing(t *testing.T, nodes []*TestNode, groupName string) string {
	t.Helper()
	names := TestNodeNames(nodes)
	for i, name := range names {
		t.Logf("[remote] %s joining %s (%d/%d)", name, groupName, i+1, len(names))
		out, err := Deploy(t, "exec", name, "ring", "join", TestGroupKey, groupName)
		if err != nil {
			t.Errorf("[remote] %s join failed: %v\n%s", name, err, out)
		}
	}
	return TestGroupKey
}

func (b *remoteBackend) PublicState(n *TestNode) (NodeState, error) {
	inst := b.cfg.Instance(n.Name_)
	if inst == nil {
		return NodeState{}, fmt.Errorf("instance %s not found in test-fleet.toml", n.Name_)
	}
	return PublicStateSSH(b.t, b.pemFile, inst)
}

func (b *remoteBackend) PrivateState(n *TestNode, groupName string) (PrivateRingEntry, bool) {
	inst := b.cfg.Instance(n.Name_)
	if inst == nil {
		return PrivateRingEntry{}, false
	}
	return PrivateStateForGroupSSH(b.t, b.pemFile, inst, groupName)
}

func (b *remoteBackend) Exec(n *TestNode, args ...string) (string, error) {
	cmdArgs := append([]string{"exec", n.Name_}, args...)
	return Deploy(b.t, cmdArgs...)
}

func (b *remoteBackend) Stop(n *TestNode) error {
	_, err := Deploy(b.t, "stop", n.Name_)
	return err
}

func (b *remoteBackend) Start(n *TestNode) error {
	args := buildRunArgs([]string{n.Name_}, b.bootstrapIP, b.daemonArgs)
	MustDeploy(b.t, args...)
	return nil
}

func (b *remoteBackend) AddLocal(n *TestNode, data []byte) (string, error) {
	inst := b.cfg.Instance(n.Name_)
	if inst == nil {
		return "", fmt.Errorf("instance %s not found", n.Name_)
	}
	encoded := base64.StdEncoding.EncodeToString(data)
	cmd := fmt.Sprintf(
		`export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin; echo '%s' | base64 -d > /tmp/drings-add-local.bin && drings add /tmp/drings-add-local.bin && rm -f /tmp/drings-add-local.bin`,
		encoded)
	raw, err := SSH(b.pemFile, inst, cmd)
	if err != nil {
		return "", fmt.Errorf("add on %s: %w\n%s", n.Name_, err, raw)
	}
	cidStr := ParseCID(raw)
	if cidStr == "" {
		return "", fmt.Errorf("could not parse CID from add output: %s", raw)
	}
	return cidStr, nil
}

func (b *remoteBackend) StoreFile(n *TestNode, data []byte, group string) (string, error) {
	tmpDir := b.t.TempDir()
	fp := filepath.Join(tmpDir, "testdata.bin")
	if err := os.WriteFile(fp, data, 0644); err != nil {
		return "", err
	}
	args := []string{"store", n.Name_, fp}
	if group != "" {
		args = append(args, "--group", group)
	}
	out, err := Deploy(b.t, args...)
	if err != nil {
		return "", fmt.Errorf("store: %w\n%s", err, out)
	}
	cidStr := ParseCID(out)
	if cidStr == "" {
		return "", fmt.Errorf("could not parse CID from store output: %s", out)
	}
	return cidStr, nil
}

func (b *remoteBackend) StoreFileWithTTL(n *TestNode, data []byte, group string, ttl string) (string, error) {
	tmpDir := b.t.TempDir()
	fp := filepath.Join(tmpDir, "testdata.bin")
	if err := os.WriteFile(fp, data, 0644); err != nil {
		return "", err
	}
	args := []string{"store", n.Name_, fp}
	if group != "" {
		args = append(args, "--group", group, "--ttl", ttl)
	} else {
		return "", fmt.Errorf("TTL is only supported for private ring pushes")
	}
	out, err := Deploy(b.t, args...)
	if err != nil {
		return "", fmt.Errorf("store: %w\n%s", err, out)
	}
	cidStr := ParseCID(out)
	if cidStr == "" {
		return "", fmt.Errorf("could not parse CID from store output: %s", out)
	}
	return cidStr, nil
}

func (b *remoteBackend) FetchCID(n *TestNode, cidStr string, group string) error {
	args := []string{"fetch", n.Name_, cidStr}
	if group != "" {
		args = append(args, "--group", group)
	}
	out, err := Deploy(b.t, args...)
	if err != nil {
		return fmt.Errorf("fetch: %w\n%s", err, out)
	}
	return nil
}

func (b *remoteBackend) ForceStabilize(n *TestNode) error {
	inst := b.cfg.Instance(n.Name_)
	if inst == nil {
		return fmt.Errorf("instance %s not found", n.Name_)
	}
	_, err := SSH(b.pemFile, inst, `curl -sf -X POST http://localhost:7423/debug/stabilize`)
	return err
}

func (b *remoteBackend) ForceStabilizePrivate(n *TestNode, groupRef string) error {
	inst := b.cfg.Instance(n.Name_)
	if inst == nil {
		return fmt.Errorf("instance %s not found", n.Name_)
	}
	_, err := SSH(b.pemFile, inst, fmt.Sprintf(`curl -sf -X POST http://localhost:7423/debug/groups/%s/stabilize`, groupRef))
	return err
}

func (b *remoteBackend) WaitForReplicationDrain(n *TestNode, timeout time.Duration) error {
	inst := b.cfg.Instance(n.Name_)
	if inst == nil {
		return fmt.Errorf("instance %s not found", n.Name_)
	}
	cmd := fmt.Sprintf(
		`curl -sf -X POST 'http://localhost:7423/debug/replication-drain?timeout=%s'`,
		timeout)
	_, err := SSH(b.pemFile, inst, cmd)
	return err
}

func (b *remoteBackend) RepublishSelf(n *TestNode) error {
	inst := b.cfg.Instance(n.Name_)
	if inst == nil {
		return fmt.Errorf("instance %s not found", n.Name_)
	}
	_, err := SSH(b.pemFile, inst, `curl -sf -X POST http://localhost:7423/public/peer/publish`)
	return err
}

func (b *remoteBackend) DeleteRecord(n *TestNode, keyHex string) error {
	inst := b.cfg.Instance(n.Name_)
	if inst == nil {
		return fmt.Errorf("instance %s not found", n.Name_)
	}
	_, err := SSH(b.pemFile, inst, fmt.Sprintf(`curl -sf -X DELETE http://localhost:7423/debug/records/%s`, keyHex))
	return err
}

func (b *remoteBackend) HasRecord(n *TestNode, keyHex string) (bool, error) {
	inst := b.cfg.Instance(n.Name_)
	if inst == nil {
		return false, fmt.Errorf("instance %s not found", n.Name_)
	}
	raw, err := SSH(b.pemFile, inst, fmt.Sprintf(`curl -sf http://localhost:7423/debug/records/%s`, keyHex))
	if err != nil {
		return false, fmt.Errorf("node %s: %w", n.Name_, err)
	}
	var result struct {
		Found bool `json:"found"`
	}
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return false, fmt.Errorf("node %s: decode: %w", n.Name_, err)
	}
	return result.Found, nil
}

func (b *remoteBackend) HasBlock(n *TestNode, cidStr string) (bool, error) {
	inst := b.cfg.Instance(n.Name_)
	if inst == nil {
		return false, fmt.Errorf("instance %s not found", n.Name_)
	}
	raw, err := SSH(b.pemFile, inst, fmt.Sprintf(`curl -sf http://localhost:7423/debug/blocks/%s`, cidStr))
	if err != nil {
		return false, fmt.Errorf("node %s: %w", n.Name_, err)
	}
	var result struct {
		Found bool `json:"found"`
	}
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return false, fmt.Errorf("node %s: decode: %w", n.Name_, err)
	}
	return result.Found, nil
}

func (b *remoteBackend) RecordKeys(n *TestNode) ([]string, error) {
	inst := b.cfg.Instance(n.Name_)
	if inst == nil {
		return nil, fmt.Errorf("instance %s not found", n.Name_)
	}
	raw, err := SSH(b.pemFile, inst, `curl -sf http://localhost:7423/debug/records`)
	if err != nil {
		return nil, fmt.Errorf("node %s: %w", n.Name_, err)
	}
	var keys []string
	if err := json.Unmarshal([]byte(raw), &keys); err != nil {
		return nil, fmt.Errorf("node %s: decode: %w", n.Name_, err)
	}
	return keys, nil
}

func (b *remoteBackend) NodeLogsTail(name string, n int) (string, error) {
	inst := b.cfg.Instance(name)
	if inst == nil {
		return "", fmt.Errorf("instance %s not found", name)
	}
	raw, err := SSH(b.pemFile, inst, fmt.Sprintf(`tail -n %d ~/drings-daemon.log 2>/dev/null`, n))
	if err != nil {
		return "", fmt.Errorf("node %s: %w", name, err)
	}
	return raw, nil
}

func (b *remoteBackend) NodeLogsSince(name string, since, until time.Time) (string, error) {
	inst := b.cfg.Instance(name)
	if inst == nil {
		return "", fmt.Errorf("instance %s not found", name)
	}
	sinceStr := since.Format(time.RFC3339)
	untilStr := until.Format(time.RFC3339)

	cmd := fmt.Sprintf(
		`awk -v since=%q -v until=%q '{t=$1; sub(/^time=/,"",t); if(t>=since && t<=until) print}' ~/drings-daemon.log 2>/dev/null`,
		sinceStr, untilStr)
	raw, err := SSH(b.pemFile, inst, cmd)
	if err != nil {
		return "", fmt.Errorf("node %s: %w", name, err)
	}
	return raw, nil
}

func (b *remoteBackend) DeleteBlock(n *TestNode, cidStr string) error {
	inst := b.cfg.Instance(n.Name_)
	if inst == nil {
		return fmt.Errorf("instance %s not found", n.Name_)
	}
	_, err := SSH(b.pemFile, inst, fmt.Sprintf(`curl -sf -X DELETE http://localhost:7423/debug/blocks/%s`, cidStr))
	return err
}

func (b *remoteBackend) DeleteCID(n *TestNode, cidStr string, group string) error {
	out, err := b.Exec(n, "delete-cid", group, cidStr)
	if err != nil {
		return fmt.Errorf("delete-cid: %w\n%s", err, out)
	}
	return nil
}

func (b *remoteBackend) HasNetworkBlock(n *TestNode, cidStr string) (bool, error) {
	inst := b.cfg.Instance(n.Name_)
	if inst == nil {
		return false, fmt.Errorf("instance %s not found", n.Name_)
	}
	raw, err := SSH(b.pemFile, inst, fmt.Sprintf(`curl -sf http://localhost:7423/debug/network-blocks/%s 2>/dev/null`, cidStr))
	if err != nil {
		return false, fmt.Errorf("node %s: %w", n.Name_, err)
	}
	var result struct {
		Found bool `json:"found"`
	}
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return false, fmt.Errorf("node %s: decode: %w", n.Name_, err)
	}
	return result.Found, nil
}

func (b *remoteBackend) NetworkBlockCIDs(n *TestNode) ([]string, error) {
	inst := b.cfg.Instance(n.Name_)
	if inst == nil {
		return nil, fmt.Errorf("node %s not found in fleet", n.Name_)
	}
	raw, err := SSH(b.pemFile, inst, `curl -sf http://localhost:7423/debug/network-blocks 2>/dev/null`)
	if err != nil {
		return nil, fmt.Errorf("node %s: %w", n.Name_, err)
	}
	var result struct {
		Count	int		`json:"count"`
		CIDs	[]string	`json:"cids"`
	}
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return nil, fmt.Errorf("node %s: decode: %w", n.Name_, err)
	}
	return result.CIDs, nil
}

func (b *remoteBackend) NetworkRoots(n *TestNode) ([]string, error) {
	inst := b.cfg.Instance(n.Name_)
	if inst == nil {
		return nil, fmt.Errorf("node %s not found in fleet", n.Name_)
	}
	raw, err := SSH(b.pemFile, inst, `curl -sf http://localhost:7423/debug/network-roots 2>/dev/null`)
	if err != nil {
		return nil, fmt.Errorf("node %s: %w", n.Name_, err)
	}
	var result struct {
		Count	int		`json:"count"`
		Roots	[]string	`json:"roots"`
	}
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return nil, fmt.Errorf("node %s: decode: %w", n.Name_, err)
	}
	return result.Roots, nil
}

func (b *remoteBackend) RingNetworkBlockCIDs(n *TestNode, ringID string) ([]string, error) {
	inst := b.cfg.Instance(n.Name_)
	if inst == nil {
		return nil, fmt.Errorf("node %s not found in fleet", n.Name_)
	}
	raw, err := SSH(b.pemFile, inst, fmt.Sprintf(`curl -sf http://localhost:7423/debug/rings/%s/network-blocks 2>/dev/null`, ringID))
	if err != nil {
		return nil, fmt.Errorf("node %s: %w", n.Name_, err)
	}
	var result struct {
		Ring	string		`json:"ring"`
		Count	int		`json:"count"`
		CIDs	[]string	`json:"cids"`
	}
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return nil, fmt.Errorf("node %s: decode: %w", n.Name_, err)
	}
	return result.CIDs, nil
}

func (b *remoteBackend) RingNetworkRoots(n *TestNode, ringID string) ([]string, error) {
	inst := b.cfg.Instance(n.Name_)
	if inst == nil {
		return nil, fmt.Errorf("node %s not found in fleet", n.Name_)
	}
	raw, err := SSH(b.pemFile, inst, fmt.Sprintf(`curl -sf http://localhost:7423/debug/rings/%s/network-roots 2>/dev/null`, ringID))
	if err != nil {
		return nil, fmt.Errorf("node %s: %w", n.Name_, err)
	}
	var result struct {
		Ring	string		`json:"ring"`
		Count	int		`json:"count"`
		Roots	[]string	`json:"roots"`
	}
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return nil, fmt.Errorf("node %s: decode: %w", n.Name_, err)
	}
	return result.Roots, nil
}

func (b *remoteBackend) DataDir(_ *TestNode) string {

	return ""
}

func (b *remoteBackend) RestartOnNewPort(_ *TestNode, _, _ int) error {

	return fmt.Errorf("remote backend does not support per-test port reassignment")
}

func (b *remoteBackend) RingStorage(n *TestNode, ringID string) (int64, int64, error) {
	inst := b.cfg.Instance(n.Name_)
	if inst == nil {
		return 0, 0, fmt.Errorf("node %s not found in fleet", n.Name_)
	}
	raw, err := SSH(b.pemFile, inst, fmt.Sprintf(`curl -sf http://localhost:7423/debug/rings/%s/storage 2>/dev/null`, ringID))
	if err != nil {
		return 0, 0, fmt.Errorf("node %s: %w", n.Name_, err)
	}
	var result struct {
		Used	int64	`json:"used_bytes"`
		Max	int64	`json:"max_bytes"`
	}
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return 0, 0, fmt.Errorf("node %s: decode: %w", n.Name_, err)
	}
	return result.Used, result.Max, nil
}

func (b *remoteBackend) Close() error	{ return nil }
