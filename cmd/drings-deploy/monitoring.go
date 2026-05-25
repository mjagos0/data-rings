package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func cmdMonitorStatus(cfg *Config) error {
	if !cfg.Monitoring.Enabled {
		fmt.Println("monitoring is disabled in config")
		return nil
	}
	if cfg.Monitoring.LokiURL == "" {
		return fmt.Errorf("monitoring.loki_url is empty in config")
	}

	lokiURL := strings.TrimRight(cfg.Monitoring.LokiURL, "/")
	promURL := strings.Replace(lokiURL, ":3100", ":9090", 1)

	fmt.Printf("Loki   : %s\n", lokiURL)
	fmt.Printf("Prometheus: %s\n\n", promURL)

	lokiReady, err := httpGet(lokiURL + "/ready")
	if err != nil {
		fmt.Printf("Loki   /ready  : ERROR — %v\n", err)
	} else {
		fmt.Printf("Loki   /ready  : %s\n", strings.TrimSpace(lokiReady))
	}

	targetsJSON, err := httpGet(promURL + "/api/v1/targets")
	if err != nil {
		fmt.Printf("Prometheus targets: ERROR — %v\n", err)
		return nil
	}

	up := strings.Count(targetsJSON, `"health":"up"`)
	down := strings.Count(targetsJSON, `"health":"down"`)
	fmt.Printf("Prometheus targets: %d up, %d down\n", up, down)
	if down > 0 {
		chunks := strings.Split(targetsJSON, "},{")
		for _, chunk := range chunks {
			if !strings.Contains(chunk, `"health":"down"`) {
				continue
			}
			inst := extractQuotedValue(chunk, `"instance":"`)
			lastErr := extractQuotedValue(chunk, `"lastError":"`)
			fmt.Printf("  DOWN  instance=%s  error=%s\n", inst, lastErr)
		}
	}
	return nil
}

func cmdMonitorSetup(cfg *Config, pemFile string, instNames []string) error {
	if !cfg.Monitoring.Enabled {
		fmt.Println("monitoring is disabled in config (monitoring.enabled = false); skipping")
		return nil
	}
	if cfg.Monitoring.LokiURL == "" {
		return fmt.Errorf("monitoring.loki_url is empty in config")
	}
	return forEachInstance(cfg, pemFile, instNames, "monitor-setup: install Promtail", func(inst *Instance) error {
		return runSSH(pemFile, inst, promtailSetupScript(inst, cfg.Monitoring.LokiURL))
	})
}

func cmdMonitorDeploy(cfg *Config, pemFile string) error {
	if !cfg.Monitoring.Enabled {
		return fmt.Errorf("monitoring is disabled in config")
	}
	if cfg.Monitoring.LokiURL == "" {
		return fmt.Errorf("monitoring.loki_url is empty in config")
	}

	monHost := strings.TrimPrefix(cfg.Monitoring.LokiURL, "http://")
	monHost = strings.TrimPrefix(monHost, "https://")
	if i := strings.Index(monHost, ":"); i >= 0 {
		monHost = monHost[:i]
	}

	promCfg := buildPromConfig(cfg)

	script := fmt.Sprintf(`set -e

# Deploy prometheus.yml
mkdir -p ~/monitoring/config/prometheus
tee ~/monitoring/config/prometheus/prometheus.yml > /dev/null <<'PROMCFG'
%s
PROMCFG
echo "prometheus.yml deployed"

# Reload Prometheus via lifecycle API (--web.enable-lifecycle)
if curl -sf -X POST http://localhost:9090/-/reload > /dev/null 2>&1; then
    echo "Prometheus reloaded via HTTP"
elif docker exec prometheus kill -HUP 1 2>/dev/null; then
    echo "Prometheus reloaded via SIGHUP"
else
    echo "WARNING: could not reload Prometheus"
fi

mkdir -p ~/monitoring/config/grafana/provisioning/dashboards
`, promCfg)

	monInst := &Instance{Name: "monitoring", IPv4: monHost, SSHUser: "admin"}

	fmt.Printf("Deploying to monitoring node %s ...\n", monHost)
	if err := runSSH(pemFile, monInst, script); err != nil {
		return fmt.Errorf("deploy base config: %w", err)
	}

	dashboards := map[string]string{
		"cluster-overview":	"monitoring/dashboards/cluster-overview.json",
		"node-detail":		"monitoring/dashboards/node-detail.json",
		"private-ring-detail":	"monitoring/dashboards/private-ring-detail.json",
	}
	for name, path := range dashboards {
		content, err := os.ReadFile(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: could not read %s: %v\n", path, err)
			continue
		}
		escaped := strings.ReplaceAll(string(content), "'", "'\\''")
		dashScript := fmt.Sprintf(`tee ~/monitoring/config/grafana/provisioning/dashboards/%s.json > /dev/null <<'DASHBOARD'
%s
DASHBOARD
echo "deployed dashboard: %s"
`, name, escaped, name)
		if err := runSSH(pemFile, monInst, dashScript); err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: failed to deploy %s: %v\n", name, err)
		}
	}

	restartScript := `if docker restart grafana 2>/dev/null; then
    echo "Grafana container restarted"
else
    echo "WARNING: could not restart grafana container"
fi`
	if err := runSSH(pemFile, monInst, restartScript); err != nil {
		fmt.Fprintf(os.Stderr, "WARNING: failed to restart Grafana: %v\n", err)
	}

	fmt.Println("Done.")
	return nil
}

func cmdPrometheusConfig(cfg *Config) error {
	if !cfg.Monitoring.Enabled {
		return fmt.Errorf("monitoring is disabled in config")
	}
	fmt.Println(buildPromConfig(cfg))
	return nil
}

func buildPromConfig(cfg *Config) string {
	port := strings.TrimPrefix(cfg.Monitoring.MetricsPort, ":")
	if port == "" {
		port = "9100"
	}
	var b strings.Builder
	b.WriteString("global:\n  scrape_interval: 15s\n  evaluation_interval: 15s\n\nscrape_configs:\n  - job_name: \"datarings\"\n    scrape_interval: 15s\n    static_configs:\n")
	for _, inst := range cfg.Instances {
		fmt.Fprintf(&b, "      - targets: [\"%s:%s\"]\n        labels:\n          instance: \"%s\"\n", inst.IPv4, port, inst.Name)
	}
	b.WriteString("    relabel_configs:\n      - source_labels: [instance]\n        target_label: instance\n")
	return b.String()
}

func promtailSetupScript(inst *Instance, lokiURL string) string {
	return fmt.Sprintf(`set -e
export DEBIAN_FRONTEND=noninteractive

# Install Promtail if not present
if ! command -v promtail > /dev/null 2>&1; then
    PROMTAIL_VERSION="3.0.0"
    WORKDIR="$HOME/promtail-install"
    mkdir -p "$WORKDIR"
    wget -q -O "$WORKDIR/promtail.zip" \
        "https://github.com/grafana/loki/releases/download/v${PROMTAIL_VERSION}/promtail-linux-amd64.zip" 2>/dev/null || \
    wget -q -O "$WORKDIR/promtail.zip" \
        "https://github.com/grafana/loki/releases/download/v2.9.8/promtail-linux-amd64.zip"
    sudo apt-get install -y -q unzip 2>/dev/null || true
    unzip -o "$WORKDIR/promtail.zip" -d "$WORKDIR/"
    sudo mv "$WORKDIR/promtail-linux-amd64" /usr/local/bin/promtail
    sudo chmod +x /usr/local/bin/promtail
    rm -rf "$WORKDIR"
    echo "Promtail installed"
else
    echo "Promtail already installed: $(promtail --version 2>&1 | head -1)"
fi

# Write Promtail config
sudo mkdir -p /etc/promtail
sudo tee /etc/promtail/config.yml > /dev/null <<'PROMTAILCFG'
server:
  http_listen_port: 9080
  grpc_listen_port: 0

positions:
  filename: /tmp/promtail-positions.yaml

clients:
  - url: %s/loki/api/v1/push

scrape_configs:
  - job_name: drings-daemon
    static_configs:
      - targets:
          - localhost
        labels:
          job: drings-daemon
          instance: %s
          __path__: /home/%s/drings-daemon.log
PROMTAILCFG

# Stop any existing Promtail
pkill promtail 2>/dev/null || true
sleep 1

# Start Promtail in background
nohup promtail -config.file=/etc/promtail/config.yml \
    < /dev/null > ~/promtail.log 2>&1 &
PROMTAIL_PID=$!
disown $PROMTAIL_PID
echo $PROMTAIL_PID > ~/promtail.pid
sleep 2

if kill -0 $PROMTAIL_PID 2>/dev/null; then
    echo "Promtail started (PID: $PROMTAIL_PID) shipping to %s"
else
    echo "ERROR: Promtail failed to start"
    tail -10 ~/promtail.log
    exit 1
fi
`, lokiURL, inst.Name, inst.SSHUser, lokiURL)
}

func httpGet(url string) (string, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func extractQuotedValue(s, prefix string) string {
	i := strings.Index(s, prefix)
	if i < 0 {
		return ""
	}
	rest := s[i+len(prefix):]
	end := strings.Index(rest, `"`)
	if end < 0 {
		return ""
	}
	return rest[:end]
}
