//go:build integration

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func projectRoot() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "../.."))
}

func deployBinary() string {
	root := projectRoot()
	local := filepath.Join(root, "drings-deploy")
	if _, err := os.Stat(local); err == nil {
		return local
	}
	return "drings-deploy"
}

func deploy(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(deployBinary(), args...)
	cmd.Dir = projectRoot()
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func mustDeploy(t *testing.T, args ...string) string {
	t.Helper()
	out, err := deploy(t, args...)
	if err != nil {
		t.Fatalf("drings-deploy %v failed: %v\noutput:\n%s", args, err, out)
	}
	return out
}

func parseCIDFromOutput(out string) string {
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToLower(line), "cid:") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				return parts[1]
			}
		}
	}
	return ""
}

func extractIDFromRingsOutput(out string) string {
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "id:") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				return strings.TrimSpace(parts[1])
			}
		}
	}
	return ""
}

const testNode = "node5"

func testCaptureSSH(pemFile string, inst *Instance, cmd string) (string, error) {
	args := []string{
		"ssh",
		"-i", pemFile,
		"-o", "StrictHostKeyChecking=no",
		"-o", "ConnectTimeout=15",
		"-o", "BatchMode=yes",
		fmt.Sprintf("%s@%s", inst.SSHUser, inst.IPv4),
		cmd,
	}
	out, err := exec.Command(args[0], args[1:]...).CombinedOutput()
	return string(out), err
}

func ensureRunning(t *testing.T) {
	t.Helper()
	deploy(t, "run", testNode)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		out, err := deploy(t, "ring-health", testNode)
		if err == nil && strings.Contains(out, "[OK]") {
			return
		}
		time.Sleep(1 * time.Second)
	}
	t.Logf("[ensureRunning] %s did not reach [OK] within 30s, proceeding anyway", testNode)
}

func TestConfig_LoadsSystemToml(t *testing.T) {
	cfg, err := loadConfig(filepath.Join(projectRoot(), "test-fleet.toml"))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if len(cfg.Instances) == 0 {
		t.Error("no instances in config")
	}
	if strings.TrimSpace(cfg.AWSKeyPair) == "" {
		t.Error("aws_key_pair is empty")
	}
	if _, err := resolvePEMFile(cfg); err != nil {
		t.Errorf("resolvePEMFile: %v", err)
	}
}

func TestConfig_InstanceLookupByName(t *testing.T) {
	cfg, err := loadConfig(filepath.Join(projectRoot(), "test-fleet.toml"))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	for _, inst := range cfg.Instances {
		found, _, err := cfg.findInstance(inst.Name)
		if err != nil {
			t.Errorf("findInstance(%q): %v", inst.Name, err)
			continue
		}
		if found.Name != inst.Name {
			t.Errorf("findInstance(%q) returned %q", inst.Name, found.Name)
		}
	}
}

func TestConfig_InstanceLookupByIPv4(t *testing.T) {
	cfg, err := loadConfig(filepath.Join(projectRoot(), "test-fleet.toml"))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	inst := cfg.Instances[0]
	out := mustDeploy(t, "status", inst.IPv4)
	if !strings.Contains(out, inst.Name) {
		t.Errorf("status by IPv4 %s did not mention instance name %s:\n%s", inst.IPv4, inst.Name, out)
	}
}

func TestConfig_StaticIPNodesAreBootstrap(t *testing.T) {
	cfg, err := loadConfig(filepath.Join(projectRoot(), "test-fleet.toml"))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	for _, inst := range cfg.Instances {
		if inst.StaticIP && !inst.IsBootstrap {
			t.Errorf("static-IP node %s should have is_bootstrap=true", inst.Name)
		}
	}
}

func TestConfig_BootstrapNodesAdvertiseTheirOwnIP(t *testing.T) {
	cfg, err := loadConfig(filepath.Join(projectRoot(), "test-fleet.toml"))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	for _, inst := range cfg.Instances {
		if !inst.IsBootstrap {
			continue
		}
		out := mustDeploy(t, "rings", inst.Name)
		if !strings.Contains(out, inst.IPv4) {
			t.Errorf("%s: rings output does not contain configured IP %s:\n%s", inst.Name, inst.IPv4, out)
		}
	}
}

func TestTestdata_GeneratesFilesAndManifest(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "generated")
	mustDeploy(t, "testdata", "--out", outDir, "--files", "5", "--dirs", "2", "--seed", "42")

	if _, err := os.Stat(filepath.Join(outDir, "MANIFEST.txt")); err != nil {
		t.Errorf("MANIFEST.txt not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outDir, "hello.txt")); err != nil {
		t.Errorf("hello.txt not created: %v", err)
	}
	entries, err := os.ReadDir(outDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < 3 {
		t.Errorf("too few entries in testdata output dir: got %d, want >= 3", len(entries))
	}
}

func TestTestdata_SeedIsReproducible(t *testing.T) {
	dir1 := filepath.Join(t.TempDir(), "run1")
	dir2 := filepath.Join(t.TempDir(), "run2")

	mustDeploy(t, "testdata", "--out", dir1, "--files", "3", "--dirs", "1", "--seed", "999")
	mustDeploy(t, "testdata", "--out", dir2, "--files", "3", "--dirs", "1", "--seed", "999")

	m1, err1 := os.ReadFile(filepath.Join(dir1, "MANIFEST.txt"))
	m2, err2 := os.ReadFile(filepath.Join(dir2, "MANIFEST.txt"))
	if err1 != nil || err2 != nil {
		t.Fatalf("reading MANIFEST.txt: %v %v", err1, err2)
	}
	if string(m1) != string(m2) {
		t.Errorf("MANIFEST.txt differs between runs with same seed")
	}
}

func TestStatus_OutputFormat(t *testing.T) {
	ensureRunning(t)
	out := mustDeploy(t, "status", testNode)

	if !strings.Contains(out, testNode) {
		t.Errorf("status output does not mention %s:\n%s", testNode, out)
	}
	if !strings.Contains(out, "daemon: RUNNING") && !strings.Contains(out, "daemon: STOPPED") {
		t.Errorf("status output missing daemon state indicator:\n%s", out)
	}
	if !strings.Contains(out, "--- last 10 log lines ---") {
		t.Errorf("status output missing log section:\n%s", out)
	}
}

func TestRings_OutputFormat(t *testing.T) {
	ensureRunning(t)
	out := mustDeploy(t, "rings", testNode)

	if !strings.Contains(out, testNode) {
		t.Errorf("rings output does not mention %s", testNode)
	}
	if !strings.Contains(out, "id:") {
		t.Errorf("rings output missing 'id:' field:\n%s", out)
	}
	if !strings.Contains(out, "addr:") {
		t.Errorf("rings output missing 'addr:' field:\n%s", out)
	}
}

func TestRingHealth_OutputFormat(t *testing.T) {
	ensureRunning(t)
	out := mustDeploy(t, "ring-health")

	cfg, err := loadConfig(filepath.Join(projectRoot(), "test-fleet.toml"))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	for _, inst := range cfg.Instances {
		if !strings.Contains(out, inst.Name) {
			t.Errorf("ring-health output missing node %s", inst.Name)
		}
	}

	hasOK := strings.Contains(out, "[OK]")
	hasIsolated := strings.Contains(out, "ISOLATED")
	hasError := strings.Contains(out, "ERROR")
	if !hasOK && !hasIsolated && !hasError {
		t.Errorf("ring-health output has no status markers ([OK], ISOLATED, ERROR):\n%s", out)
	}
}

func TestRingDebug_OutputFormat(t *testing.T) {
	ensureRunning(t)
	out := mustDeploy(t, "ring-debug", testNode)

	if !strings.Contains(out, "{") || !strings.Contains(out, "}") {
		t.Errorf("ring-debug output doesn't contain JSON:\n%.500s", out)
	}
}

func TestStore_ReturnsCID(t *testing.T) {
	ensureRunning(t)
	tmpFile, err := os.CreateTemp(t.TempDir(), "cid-check-*.txt")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.WriteString("CID check content for tool test")
	tmpFile.Close()

	storeOut := mustDeploy(t, "store", testNode, tmpFile.Name())
	cid := parseCIDFromOutput(storeOut)
	if cid == "" {
		t.Fatalf("store command did not return a CID:\n%s", storeOut)
	}
	if !strings.HasPrefix(cid, "bafy") && !strings.HasPrefix(cid, "Qm") {
		t.Errorf("CID %q has unexpected format (expected bafy... or Qm...)", cid)
	}
}

func TestExec_RunsDringsCommand(t *testing.T) {
	ensureRunning(t)

	mustDeploy(t, "exec", testNode, "list")
}

func TestExec_KeyList(t *testing.T) {
	ensureRunning(t)
	mustDeploy(t, "exec", testNode, "key", "list")
}

func TestStop_ChangesStatusToStopped(t *testing.T) {
	ensureRunning(t)
	mustDeploy(t, "stop", testNode)

	out := mustDeploy(t, "status", testNode)
	if strings.Contains(out, "daemon: RUNNING") {
		t.Errorf("daemon still RUNNING after stop:\n%s", out)
	}

	t.Cleanup(func() { deploy(t, "run", testNode) })
}

func TestRestart_DaemonRunsAfterRestart(t *testing.T) {
	ensureRunning(t)
	mustDeploy(t, "restart", testNode)
	ensureRunning(t)

	out := mustDeploy(t, "status", testNode)
	if !strings.Contains(out, "daemon: RUNNING") {
		t.Errorf("daemon not RUNNING after restart:\n%s", out)
	}
}

func TestRingHealth_ReportsErrorAfterStop(t *testing.T) {
	ensureRunning(t)
	mustDeploy(t, "stop", testNode)

	out := mustDeploy(t, "ring-health")

	if strings.Contains(out, testNode) {
		nodeSection := ""
		for _, line := range strings.Split(out, "\n") {
			if strings.Contains(line, testNode) {
				nodeSection = line
				break
			}
		}
		if nodeSection != "" && strings.Contains(nodeSection, "[OK]") {
			t.Errorf("%s shows [OK] after stop — should be ERROR or ISOLATED:\n%s", testNode, out)
		}
	}

	t.Cleanup(func() { deploy(t, "run", testNode) })
}

func TestClean_PreservesIdentity(t *testing.T) {
	ensureRunning(t)

	ringsOut1 := mustDeploy(t, "rings", testNode)
	id1 := extractIDFromRingsOutput(ringsOut1)

	mustDeploy(t, "clean", testNode)
	mustDeploy(t, "run", testNode)
	ensureRunning(t)

	ringsOut2 := mustDeploy(t, "rings", testNode)
	id2 := extractIDFromRingsOutput(ringsOut2)

	if id1 == "" || id2 == "" {
		t.Skipf("could not extract peer IDs — before: %q, after: %q", id1, id2)
	}
	if id1 != id2 {
		t.Errorf("peer ID changed after clean: before=%s after=%s — identity should be preserved", id1, id2)
	}

	listOut, _ := deploy(t, "exec", testNode, "ring", "list")
	t.Logf("ring list after clean (before re-join): %s", listOut)

	t.Cleanup(func() {
		deploy(t, "join-groups", testNode)
	})
}

func TestClean_WipeIdentityChangesNodeID(t *testing.T) {
	ensureRunning(t)

	ringsOut1 := mustDeploy(t, "rings", testNode)
	id1 := extractIDFromRingsOutput(ringsOut1)

	mustDeploy(t, "clean", "--wipe-identity", testNode)
	mustDeploy(t, "run", testNode)
	ensureRunning(t)

	ringsOut2 := mustDeploy(t, "rings", testNode)
	id2 := extractIDFromRingsOutput(ringsOut2)

	if id1 == "" || id2 == "" {
		t.Skipf("could not extract peer IDs — before: %q, after: %q", id1, id2)
	}
	if id1 == id2 {
		t.Errorf("peer ID unchanged after clean --wipe-identity: %s — identity should have been regenerated", id1)
	}
	t.Logf("identity changed: %s → %s", id1, id2)

	t.Cleanup(func() {
		deploy(t, "join-groups", testNode)
	})
}

func TestJoinGroups_Idempotent(t *testing.T) {
	ensureRunning(t)
	mustDeploy(t, "join-groups", testNode)

	mustDeploy(t, "join-groups", testNode)
}

func TestCopy_SourcePresentOnRemote(t *testing.T) {
	mustDeploy(t, "copy", testNode)

	cfg, err := loadConfig(filepath.Join(projectRoot(), "test-fleet.toml"))
	if err != nil {
		t.Fatal(err)
	}
	pemFile, err := resolvePEMFile(cfg)
	if err != nil {
		t.Fatal(err)
	}

	inst, _, err := cfg.findInstance(testNode)
	if err != nil {
		t.Fatal(err)
	}

	out, err := testCaptureSSH(pemFile, inst, "test -f ~/data-rings/go.mod && echo EXISTS")
	if err != nil {
		t.Fatalf("SSH check failed: %v", err)
	}
	if !strings.Contains(out, "EXISTS") {
		t.Errorf("go.mod not found on %s after copy", testNode)
	}
}

func TestCopy_SystemTomlNotCopied(t *testing.T) {
	cfg, err := loadConfig(filepath.Join(projectRoot(), "test-fleet.toml"))
	if err != nil {
		t.Fatal(err)
	}
	pemFile, err := resolvePEMFile(cfg)
	if err != nil {
		t.Fatal(err)
	}

	inst, _, err := cfg.findInstance(testNode)
	if err != nil {
		t.Fatal(err)
	}

	testCaptureSSH(pemFile, inst, "rm -f ~/data-rings/test-fleet.toml")

	mustDeploy(t, "copy", testNode)

	out, _ := testCaptureSSH(pemFile, inst, "test -f ~/data-rings/test-fleet.toml && echo EXISTS || echo ABSENT")
	if strings.Contains(out, "EXISTS") {
		t.Errorf("test-fleet.toml was copied to remote (security violation — contains PEM key)")
	}
}
