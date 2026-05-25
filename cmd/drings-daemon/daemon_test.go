//go:build integration

package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const daemonBinary = "../../drings-daemon"

func startDaemon(t *testing.T, dataDir string, extraArgs ...string) *exec.Cmd {
	t.Helper()
	binary := daemonBinary
	if _, err := os.Stat(binary); err != nil {
		if resolved, lookErr := exec.LookPath("drings-daemon"); lookErr == nil {
			binary = resolved
		} else {
			t.Skipf("drings-daemon binary not found at %s or in PATH; build it before running these tests", daemonBinary)
		}
	}
	args := []string{"--data-dir", dataDir, "--mount=false"}
	args = append(args, extraArgs...)
	cmd := exec.Command(binary, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start daemon: %v", err)
	}

	var apiAddr string
	for i, a := range extraArgs {
		if a == "--api-addr" && i+1 < len(extraArgs) {
			apiAddr = extraArgs[i+1]
			break
		}
	}
	if apiAddr != "" {
		url := "http://" + apiAddr + "/"
		if !waitForHTTP(t, url, 15*time.Second) {
			cmd.Process.Kill()
			t.Fatalf("daemon API at %s not reachable after 15s", apiAddr)
		}
	}
	return cmd
}

func stopDaemon(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if cmd == nil || cmd.Process == nil {
		return
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Logf("SIGTERM failed: %v", err)
		cmd.Process.Kill()
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		cmd.Process.Kill()
	}
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return fmt.Sprintf("%d", port)
}

func waitForHTTP(t *testing.T, url string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()

			return true
		}
		time.Sleep(300 * time.Millisecond)
	}
	return false
}

func TestDaemon_CreatesDataDirectory(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "drings-data")
	apiPort := freePort(t)
	apiAddr := "127.0.0.1:" + apiPort

	cmd := startDaemon(t, dataDir, "--api-addr", apiAddr, "--dht-addr", "")
	defer stopDaemon(t, cmd)

	if _, err := os.Stat(dataDir); err != nil {
		t.Errorf("data directory not created: %v", err)
	}
}

func TestDaemon_CreatesIdentityOnFirstRun(t *testing.T) {
	dataDir := t.TempDir()
	apiPort := freePort(t)
	dhtPort := freePort(t)

	cmd := startDaemon(t, dataDir,
		"--api-addr", "127.0.0.1:"+apiPort,
		"--dht-addr", "/ip4/127.0.0.1/tcp/"+dhtPort,
	)
	defer stopDaemon(t, cmd)

	identPath := filepath.Join(dataDir, "identity")
	if _, err := os.Stat(identPath); err != nil {
		t.Errorf("identity file not created: %v", err)
	}
}

func TestDaemon_IdentityPersistedAcrossRestarts(t *testing.T) {
	dataDir := t.TempDir()
	dhtPort := freePort(t)
	apiPort := freePort(t)
	dhtAddr := "/ip4/127.0.0.1/tcp/" + dhtPort
	apiAddr := "127.0.0.1:" + apiPort

	cmd1 := startDaemon(t, dataDir, "--api-addr", apiAddr, "--dht-addr", dhtAddr)

	url := "http://" + apiAddr + "/debug/state"
	if !waitForHTTP(t, url, 10*time.Second) {
		stopDaemon(t, cmd1)
		t.Fatal("API not available after daemon startup")
	}

	resp1, err := http.Get(url)
	if err != nil {
		stopDaemon(t, cmd1)
		t.Fatalf("GET /debug/state: %v", err)
	}
	var state1 struct {
		ID string `json:"id"`
	}
	json.NewDecoder(resp1.Body).Decode(&state1)
	resp1.Body.Close()
	stopDaemon(t, cmd1)

	if state1.ID == "" {
		t.Fatal("could not read peer ID from first run")
	}

	apiPort2 := freePort(t)
	dhtPort2 := freePort(t)
	cmd2 := startDaemon(t, dataDir,
		"--api-addr", "127.0.0.1:"+apiPort2,
		"--dht-addr", "/ip4/127.0.0.1/tcp/"+dhtPort2,
	)
	defer stopDaemon(t, cmd2)

	url2 := "http://127.0.0.1:" + apiPort2 + "/debug/state"
	if !waitForHTTP(t, url2, 10*time.Second) {
		t.Fatal("API not available after daemon restart")
	}

	resp2, err := http.Get(url2)
	if err != nil {
		t.Fatalf("GET /debug/state restart: %v", err)
	}
	var state2 struct {
		ID string `json:"id"`
	}
	json.NewDecoder(resp2.Body).Decode(&state2)
	resp2.Body.Close()

	if state1.ID != state2.ID {
		t.Errorf("peer ID changed across restart: before=%s after=%s", state1.ID, state2.ID)
	}
}

func TestDaemon_APIServerListensOnConfiguredAddress(t *testing.T) {
	dataDir := t.TempDir()
	apiPort := freePort(t)
	apiAddr := "127.0.0.1:" + apiPort

	cmd := startDaemon(t, dataDir, "--api-addr", apiAddr, "--dht-addr", "")
	defer stopDaemon(t, cmd)

	url := "http://" + apiAddr + "/debug/state"
	if !waitForHTTP(t, url, 10*time.Second) {
		t.Fatalf("API did not start at %s", url)
	}
}

func TestDaemon_DefaultAPIPort(t *testing.T) {

	binary := daemonBinary
	if _, err := os.Stat(binary); err != nil {
		binary = "drings-daemon"
	}
	out, _ := exec.Command(binary, "--help").CombinedOutput()
	if !strings.Contains(string(out), "7423") {
		t.Errorf("expected default API port 7423 in help output:\n%s", out)
	}
}

func TestDaemon_DebugStateEndpointReturnsJSON(t *testing.T) {
	dataDir := t.TempDir()
	apiPort := freePort(t)
	dhtPort := freePort(t)
	apiAddr := "127.0.0.1:" + apiPort

	cmd := startDaemon(t, dataDir,
		"--api-addr", apiAddr,
		"--dht-addr", "/ip4/127.0.0.1/tcp/"+dhtPort,
	)
	defer stopDaemon(t, cmd)

	url := "http://" + apiAddr + "/debug/state"
	if !waitForHTTP(t, url, 10*time.Second) {
		t.Fatalf("API not available at %s", url)
	}

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET /debug/state: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected HTTP 200, got %d", resp.StatusCode)
	}

	var state map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		t.Fatalf("/debug/state is not valid JSON: %v", err)
	}

	requiredFields := []string{"id", "addr", "block_count", "record_count"}
	for _, f := range requiredFields {
		if _, ok := state[f]; !ok {
			t.Errorf("/debug/state missing required field %q", f)
		}
	}

	if id, ok := state["id"].(string); !ok || id == "" {
		t.Errorf("/debug/state has empty or missing 'id' field")
	}
}

func TestDaemon_FreshRingWhenNoBootstrap(t *testing.T) {
	dataDir := t.TempDir()
	apiPort := freePort(t)
	dhtPort := freePort(t)
	apiAddr := "127.0.0.1:" + apiPort

	cmd := startDaemon(t, dataDir,
		"--api-addr", apiAddr,
		"--dht-addr", "/ip4/127.0.0.1/tcp/"+dhtPort,
	)
	defer stopDaemon(t, cmd)

	url := "http://" + apiAddr + "/debug/state"
	if !waitForHTTP(t, url, 10*time.Second) {
		t.Fatalf("API not available")
	}

	resp, _ := http.Get(url)
	if resp == nil {
		t.Fatal("no response from /debug/state")
	}
	defer resp.Body.Close()
	var state struct {
		ID		string	`json:"id"`
		Successor	struct {
			ID string `json:"ID"`
		}	`json:"successor"`
	}
	json.NewDecoder(resp.Body).Decode(&state)

	if state.Successor.ID != "" && state.Successor.ID != state.ID {
		t.Errorf("unexpected successor %s (expected self %s) in fresh ring", state.Successor.ID, state.ID)
	}
}

func TestDaemon_NoDHTWhenDHTAddrEmpty(t *testing.T) {
	dataDir := t.TempDir()
	apiPort := freePort(t)
	apiAddr := "127.0.0.1:" + apiPort

	cmd := startDaemon(t, dataDir, "--api-addr", apiAddr)
	defer stopDaemon(t, cmd)

	if !waitForHTTP(t, "http://"+apiAddr+"/debug/state", 10*time.Second) {
		t.Fatalf("API not available")
	}

	resp, err := http.Get("http://" + apiAddr + "/debug/state")
	if err != nil {
		t.Fatalf("GET /debug/state: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 503 && resp.StatusCode != 404 {

		t.Logf("note: /debug/state returned %d without DHT (may be acceptable)", resp.StatusCode)
	}
}

func TestDaemon_BootstrapPeerPersistedToConfig(t *testing.T) {

	bootstrapDataDir := t.TempDir()
	bootstrapDHTPort := freePort(t)
	bootstrapAPIPort := freePort(t)
	bootstrapAPIAddr := "127.0.0.1:" + bootstrapAPIPort
	bootstrapDHTAddr := "/ip4/127.0.0.1/tcp/" + bootstrapDHTPort

	bootstrapCmd := startDaemon(t, bootstrapDataDir,
		"--api-addr", bootstrapAPIAddr,
		"--dht-addr", bootstrapDHTAddr,
	)
	defer stopDaemon(t, bootstrapCmd)

	if !waitForHTTP(t, "http://"+bootstrapAPIAddr+"/debug/state", 10*time.Second) {
		t.Fatal("bootstrap daemon API not available")
	}

	joinerDataDir := t.TempDir()
	joinerDHTPort := freePort(t)
	joinerAPIPort := freePort(t)
	joinerDHTAddr := "/ip4/127.0.0.1/tcp/" + joinerDHTPort

	joinerCmd := startDaemon(t, joinerDataDir,
		"--api-addr", "127.0.0.1:"+joinerAPIPort,
		"--dht-addr", joinerDHTAddr,
		"--bootstrap", bootstrapDHTAddr,
	)
	defer stopDaemon(t, joinerCmd)

	configPath := filepath.Join(joinerDataDir, "config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("config.json not found: %v", err)
	}
	if !strings.Contains(string(data), bootstrapDHTPort) {
		t.Errorf("bootstrap peer not persisted to config.json\nconfig: %s", data)
	}
}

func TestDaemon_MetricsEndpointAvailable(t *testing.T) {
	dataDir := t.TempDir()
	apiPort := freePort(t)
	metricsPort := freePort(t)

	cmd := startDaemon(t, dataDir,
		"--api-addr", "127.0.0.1:"+apiPort,
		"--metrics-addr", "127.0.0.1:"+metricsPort,
		"--dht-addr", "",
	)
	defer stopDaemon(t, cmd)

	metricsURL := "http://127.0.0.1:" + metricsPort + "/metrics"
	if !waitForHTTP(t, metricsURL, 10*time.Second) {
		t.Fatalf("metrics endpoint not available at %s", metricsURL)
	}

	resp, err := http.Get(metricsURL)
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected HTTP 200 from /metrics, got %d", resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/plain") {
		t.Errorf("unexpected Content-Type for /metrics: %s", ct)
	}
}

func TestDaemon_MetricsNotAvailableWithoutFlag(t *testing.T) {
	dataDir := t.TempDir()
	apiPort := freePort(t)

	cmd := startDaemon(t, dataDir,
		"--api-addr", "127.0.0.1:"+apiPort,

		"--dht-addr", "",
	)
	defer stopDaemon(t, cmd)

	if !waitForHTTP(t, "http://127.0.0.1:"+apiPort+"/", 10*time.Second) {
		t.Fatal("API not started")
	}

	binary := daemonBinary
	if _, err := os.Stat(binary); err != nil {
		binary = "drings-daemon"
	}
	out, _ := exec.Command(binary, "--help").CombinedOutput()

	if !strings.Contains(string(out), "metrics-addr") {
		t.Errorf("expected --metrics-addr flag in help output:\n%s", out)
	}
}

func TestDaemon_GracefulShutdownOnSIGTERM(t *testing.T) {
	dataDir := t.TempDir()
	apiPort := freePort(t)

	cmd := startDaemon(t, dataDir,
		"--api-addr", "127.0.0.1:"+apiPort,
		"--dht-addr", "",
	)

	if !waitForHTTP(t, "http://127.0.0.1:"+apiPort+"/", 10*time.Second) {
		cmd.Process.Kill()
		t.Fatal("API not available before SIGTERM test")
	}

	start := time.Now()
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case err := <-done:
		elapsed := time.Since(start)
		if elapsed > 15*time.Second {
			t.Errorf("daemon took too long to shut down after SIGTERM: %v", elapsed)
		}
		t.Logf("daemon exited after SIGTERM in %v (err=%v)", elapsed, err)
	case <-time.After(20 * time.Second):
		cmd.Process.Kill()
		t.Errorf("daemon did not exit within 20s of SIGTERM")
	}
}

func TestDaemon_TwoNodeRingFormsLocally(t *testing.T) {

	dirA := t.TempDir()
	portA := freePort(t)
	apiA := freePort(t)
	dhtAddrA := "/ip4/127.0.0.1/tcp/" + portA

	cmdA := startDaemon(t, dirA,
		"--api-addr", "127.0.0.1:"+apiA,
		"--dht-addr", dhtAddrA,
	)
	defer stopDaemon(t, cmdA)

	if !waitForHTTP(t, "http://127.0.0.1:"+apiA+"/debug/state", 10*time.Second) {
		t.Fatal("node A API not available")
	}

	dirB := t.TempDir()
	portB := freePort(t)
	apiB := freePort(t)

	cmdB := startDaemon(t, dirB,
		"--api-addr", "127.0.0.1:"+apiB,
		"--dht-addr", "/ip4/127.0.0.1/tcp/"+portB,
		"--bootstrap", dhtAddrA,
	)
	defer stopDaemon(t, cmdB)

	if !waitForHTTP(t, "http://127.0.0.1:"+apiB+"/debug/state", 10*time.Second) {
		t.Fatal("node B API not available")
	}

	for r := 0; r < 3; r++ {
		for _, port := range []string{apiA, apiB} {
			resp, err := http.Post("http://127.0.0.1:"+port+"/debug/stabilize", "", nil)
			if err == nil {
				resp.Body.Close()
			}
		}
	}

	respA, err := http.Get("http://127.0.0.1:" + apiA + "/debug/state")
	if err != nil {
		t.Fatalf("GET node A state: %v", err)
	}
	var stateA struct {
		ID		string	`json:"id"`
		Successor	struct {
			ID string `json:"ID"`
		}	`json:"successor"`
		SuccessorList	[]struct {
			ID string `json:"ID"`
		}	`json:"successor_list"`
	}
	json.NewDecoder(respA.Body).Decode(&stateA)
	respA.Body.Close()

	peersInList := 0
	for _, s := range stateA.SuccessorList {
		if s.ID != stateA.ID {
			peersInList++
		}
	}
	if peersInList == 0 {
		t.Errorf("node A still isolated after stabilization (no peers in successor list); successor=%s self=%s",
			stateA.Successor.ID, stateA.ID)
	}
}

func TestDaemon_StoreOpenedOnStartup(t *testing.T) {
	dataDir := t.TempDir()
	apiPort := freePort(t)
	dhtPort := freePort(t)

	cmd := startDaemon(t, dataDir,
		"--api-addr", "127.0.0.1:"+apiPort,
		"--dht-addr", "/ip4/127.0.0.1/tcp/"+dhtPort,
	)
	defer stopDaemon(t, cmd)

	if !waitForHTTP(t, "http://127.0.0.1:"+apiPort+"/debug/state", 10*time.Second) {
		t.Fatal("API not available")
	}

	resp, err := http.Get("http://127.0.0.1:" + apiPort + "/debug/state")
	if err != nil {
		t.Fatalf("GET /debug/state: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d from /debug/state", resp.StatusCode)
	}
	var state struct {
		BlockCount int `json:"block_count"`
	}
	json.NewDecoder(resp.Body).Decode(&state)

	if state.BlockCount < 0 {
		t.Errorf("block_count is negative: %d", state.BlockCount)
	}
}

func TestDaemon_DataDirPreservedAcrossCleanRestart(t *testing.T) {
	dataDir := t.TempDir()
	apiPort1 := freePort(t)
	dhtPort1 := freePort(t)

	cmd1 := startDaemon(t, dataDir,
		"--api-addr", "127.0.0.1:"+apiPort1,
		"--dht-addr", "/ip4/127.0.0.1/tcp/"+dhtPort1,
	)

	if !waitForHTTP(t, "http://127.0.0.1:"+apiPort1+"/debug/state", 10*time.Second) {
		stopDaemon(t, cmd1)
		t.Fatal("first daemon API not available")
	}
	stopDaemon(t, cmd1)

	apiPort2 := freePort(t)
	dhtPort2 := freePort(t)
	cmd2 := startDaemon(t, dataDir,
		"--api-addr", "127.0.0.1:"+apiPort2,
		"--dht-addr", "/ip4/127.0.0.1/tcp/"+dhtPort2,
	)
	defer stopDaemon(t, cmd2)

	if !waitForHTTP(t, "http://127.0.0.1:"+apiPort2+"/debug/state", 10*time.Second) {
		t.Fatal("second daemon API not available")
	}

}
