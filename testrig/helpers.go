package testrig

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/BurntSushi/toml"
)

type SystemConfig struct {
	PEMKey		string			`toml:"pem_key"`
	Groups		[]SystemGroup		`toml:"groups"`
	Instances	[]SystemInstance	`toml:"instances"`
}

type SystemGroup struct {
	Name		string	`toml:"name"`
	PrivateKey	string	`toml:"private_key"`
}

type SystemInstance struct {
	Name	string	`toml:"name"`
	IPv4	string	`toml:"ipv4"`
	SSHUser	string	`toml:"ssh_user"`
}

func (cfg *SystemConfig) Instance(name string) *SystemInstance {
	for i := range cfg.Instances {
		if cfg.Instances[i].Name == name {
			return &cfg.Instances[i]
		}
	}
	return nil
}

func RepoRoot() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(filename), ".."))
}

func DeployBin() string {
	local := filepath.Join(RepoRoot(), "drings-deploy")
	if _, err := os.Stat(local); err == nil {
		return local
	}
	return "drings-deploy"
}

func LoadSystemConfig(t *testing.T) *SystemConfig {
	t.Helper()
	path := filepath.Join(RepoRoot(), "test-fleet.toml")
	var cfg SystemConfig
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		t.Fatalf("load test-fleet.toml: %v", err)
	}
	for i := range cfg.Instances {
		if cfg.Instances[i].SSHUser == "" {
			cfg.Instances[i].SSHUser = "admin"
		}
	}
	if strings.TrimSpace(cfg.PEMKey) == "" {
		t.Fatal("pem_key is empty in test-fleet.toml")
	}
	return &cfg
}

func WritePEM(t *testing.T, pemKey string) string {
	t.Helper()
	f, err := os.CreateTemp("", "system-drings-*.pem")
	if err != nil {
		t.Fatalf("create PEM tmp file: %v", err)
	}
	pemKey = strings.ReplaceAll(pemKey, `\n`, "\n")
	if _, err := f.WriteString(pemKey); err != nil {
		f.Close()
		os.Remove(f.Name())
		t.Fatalf("write PEM: %v", err)
	}
	f.Close()
	os.Chmod(f.Name(), 0600)
	t.Cleanup(func() { os.Remove(f.Name()) })
	return f.Name()
}

func SSH(pemFile string, inst *SystemInstance, cmd string) (string, error) {
	args := []string{
		"-i", pemFile,
		"-o", "StrictHostKeyChecking=no",
		"-o", "ConnectTimeout=15",
		"-o", "BatchMode=yes",
		"-o", "ControlMaster=auto",
		"-o", "ControlPath=/tmp/drings-ssh-%r@%h:%p",
		"-o", "ControlPersist=300",
		fmt.Sprintf("%s@%s", inst.SSHUser, inst.IPv4),
		cmd,
	}
	out, err := exec.Command("ssh", args...).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func Deploy(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(DeployBin(), args...)
	cmd.Dir = RepoRoot()
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func MustDeploy(t *testing.T, args ...string) string {
	t.Helper()
	out, err := Deploy(t, args...)
	if err != nil {
		t.Fatalf("drings-deploy %v failed: %v\noutput:\n%s", args, err, out)
	}
	return out
}

func WriteIdentity(t *testing.T, pemFile string, inst *SystemInstance, identityHex string) {
	t.Helper()
	cmd := fmt.Sprintf(`mkdir -p ~/.datarings && echo '%s' > ~/.datarings/identity && chmod 600 ~/.datarings/identity`, identityHex)
	if _, err := SSH(pemFile, inst, cmd); err != nil {
		t.Fatalf("write identity on %s: %v", inst.Name, err)
	}
}

func CleanWithPoolIdentities(t *testing.T, nodes []string, assignment *IdentityAssignment, extraDaemonArgs ...string) {
	t.Helper()
	cfg := LoadSystemConfig(t)
	pemFile := WritePEM(t, cfg.PEMKey)

	t.Log("[reset] clean --wipe-identity on nodes")
	if out, err := Deploy(t, append([]string{"clean", "--wipe-identity"}, nodes...)...); err != nil {
		t.Logf("[reset] clean --wipe-identity warning: %v\n%s", err, out)
	}

	var wg sync.WaitGroup
	for _, node := range nodes {
		ident := assignment.NodeToIdentity[node]
		inst := cfg.Instance(node)
		if inst == nil {
			t.Fatalf("node %s not found in test-fleet.toml", node)
		}
		t.Logf("[reset] writing identity on %s (NodeID: %s)", node, NodeIDHexToShort(ident.NodeIDHex))
		wg.Add(1)
		go func(node string, inst *SystemInstance, privKey string) {
			defer wg.Done()
			WriteIdentity(t, pemFile, inst, privKey)
		}(node, inst, ident.PrivKeyHex)
	}
	wg.Wait()

	bootstrapInst := cfg.Instance(nodes[0])
	if bootstrapInst == nil {
		t.Fatalf("bootstrap node %s not found in test-fleet.toml", nodes[0])
	}
	bootstrapIP := bootstrapInst.IPv4

	runArgs := buildRunArgs([]string{nodes[0]}, bootstrapIP, extraDaemonArgs)
	MustDeploy(t, runArgs...)

	if len(nodes) > 1 {
		runArgs = buildRunArgs(nodes[1:], bootstrapIP, extraDaemonArgs)
		MustDeploy(t, runArgs...)
	}
}

func buildRunArgs(nodes []string, bootstrapIP string, extraDaemonArgs []string) []string {
	runArgs := []string{"run", "--no-publish", "--no-background-stabilize",
		"--bootstrap=/ip4/" + bootstrapIP + "/tcp/7000"}
	for i := 0; i < len(extraDaemonArgs); i++ {
		arg := extraDaemonArgs[i]
		if strings.HasPrefix(arg, "--") && i+1 < len(extraDaemonArgs) && !strings.HasPrefix(extraDaemonArgs[i+1], "--") {
			runArgs = append(runArgs, arg+"="+extraDaemonArgs[i+1])
			i++
		} else {
			runArgs = append(runArgs, arg)
		}
	}
	runArgs = append(runArgs, nodes...)
	return runArgs
}

func ParseGroupKey(out string) string {
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "group-key:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "group-key:"))
		}
	}
	return ""
}

func ParseCID(out string) string {
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
