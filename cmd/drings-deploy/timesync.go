package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const chronyConfigScript = `set -e
sudo apt-get install -y -q chrony >/dev/null
sudo tee /etc/chrony/chrony.conf >/dev/null <<'CHRONY'
# Managed by drings-deploy time-sync-setup. AWS Time Sync first; pool fallback.
server 169.254.169.123 prefer iburst minpoll 4 maxpoll 4
pool 2.debian.pool.ntp.org iburst
driftfile /var/lib/chrony/chrony.drift
makestep 1.0 3
rtcsync
leapsectz right/UTC
logdir /var/log/chrony
CHRONY
sudo systemctl enable chrony >/dev/null 2>&1 || true
sudo systemctl restart chrony
sleep 1
echo "chrony tracking:"
chronyc tracking 2>&1 | sed 's/^/  /'
echo "chrony sources:"
chronyc sources 2>&1 | sed 's/^/  /'
`

func cmdTimeSyncSetup(cfg *Config, pemFile string, instNames []string) error {
	return forEachInstance(cfg, pemFile, instNames, "configure chrony / AWS Time Sync", func(inst *Instance) error {
		return runSSH(pemFile, inst, chronyConfigScript)
	})
}

type ProbeReport struct {
	Node		string	`json:"node"`
	Region		string	`json:"region"`
	Samples		int	`json:"samples"`
	BestRTTMillis	float64	`json:"best_rtt_ms"`
	OffsetNs	int64	`json:"offset_ns"`
	OffsetMillis	float64	`json:"offset_ms"`
	UncertaintyMs	float64	`json:"uncertainty_ms"`
	HostTSAtSample	string	`json:"host_ts_at_sample"`
}

type ProbeFile struct {
	HostStartUTC	string		`json:"host_start_utc"`
	Samples		int		`json:"samples_per_node"`
	Reports		[]ProbeReport	`json:"reports"`
}

func cmdClockProbe(cfg *Config, pemFile string, args []string) error {
	fs := flag.NewFlagSet("clock-probe", flag.ContinueOnError)
	out := fs.String("out", "", "write the probe report to this JSON file (in addition to stdout)")
	samples := fs.Int("samples", 10, "samples per node (the lowest-RTT sample wins)")
	if err := fs.Parse(reorderFlags(args)); err != nil {
		return err
	}
	insts, err := cfg.resolveInstances(fs.Args())
	if err != nil {
		return err
	}

	hostStart := time.Now().UTC()
	type result struct {
		report	ProbeReport
		err	error
	}
	resultsCh := make(chan result, len(insts))
	var wg sync.WaitGroup
	for _, inst := range insts {
		inst := inst
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := probeOne(pemFile, inst, *samples)
			resultsCh <- result{report: r, err: err}
		}()
	}
	wg.Wait()
	close(resultsCh)

	var reports []ProbeReport
	for r := range resultsCh {
		if r.err != nil {
			fmt.Fprintf(os.Stderr, "  %-10s FAIL  %v\n", r.report.Node, r.err)
			continue
		}
		reports = append(reports, r.report)
	}
	sort.Slice(reports, func(i, j int) bool {
		return reports[i].Node < reports[j].Node
	})

	fmt.Printf("\nclock-probe (host_start=%s, samples_per_node=%d):\n", hostStart.Format(time.RFC3339Nano), *samples)
	fmt.Printf("  %-8s %-15s %12s %14s %14s\n", "node", "region", "best_rtt_ms", "offset_ms", "uncertainty_ms")
	for _, r := range reports {
		fmt.Printf("  %-8s %-15s %12.3f %14.3f %14.3f\n",
			r.Node, r.Region, r.BestRTTMillis, r.OffsetMillis, r.UncertaintyMs)
	}

	if *out != "" {
		if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
			return fmt.Errorf("mkdir: %w", err)
		}
		f := ProbeFile{
			HostStartUTC:	hostStart.Format(time.RFC3339Nano),
			Samples:	*samples,
			Reports:	reports,
		}
		data, err := json.MarshalIndent(f, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(*out, data, 0o644); err != nil {
			return err
		}
		fmt.Printf("\nwrote %s (%d nodes)\n", *out, len(reports))
	}

	return nil
}

func probeOne(pemFile string, inst *Instance, samples int) (ProbeReport, error) {
	region := inst.Region
	report := ProbeReport{Node: inst.Name, Region: region, Samples: samples}

	var bestRTT time.Duration = math.MaxInt64
	var bestOffset time.Duration
	var bestHostMid time.Time
	for i := 0; i < samples; i++ {

		hostSend := time.Now()
		out, err := captureSSH(pemFile, inst, "curl -sf --max-time 2 http://localhost:7423/debug/clock-probe")
		hostRecv := time.Now()
		if err != nil {
			return report, fmt.Errorf("sample %d: %w (%s)", i+1, err, strings.TrimSpace(out))
		}
		var remoteNs int64
		if _, err := fmt.Sscan(strings.TrimSpace(out), &remoteNs); err != nil {
			return report, fmt.Errorf("sample %d: parse %q: %w", i+1, out, err)
		}
		rtt := hostRecv.Sub(hostSend)
		if rtt < bestRTT {
			bestRTT = rtt
			hostMid := hostSend.Add(rtt / 2)
			bestHostMid = hostMid
			bestOffset = time.Unix(0, remoteNs).Sub(hostMid)
		}
	}

	report.BestRTTMillis = float64(bestRTT.Microseconds()) / 1000.0
	report.OffsetNs = bestOffset.Nanoseconds()
	report.OffsetMillis = float64(bestOffset.Microseconds()) / 1000.0
	report.UncertaintyMs = report.BestRTTMillis / 2.0
	report.HostTSAtSample = bestHostMid.UTC().Format(time.RFC3339Nano)
	return report, nil
}
