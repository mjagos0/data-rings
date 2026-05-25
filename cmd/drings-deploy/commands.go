package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

func cmdDeploy(cfg *Config, pemFile, configPath string, instNames []string) error {
	insts, err := cfg.resolveInstances(instNames)
	if err != nil {
		return err
	}
	bootstrapIP := bootstrapIPFrom(cfg)
	for _, inst := range insts {
		if err := deployOne(cfg, pemFile, configPath, inst, bootstrapIP); err != nil {
			fmt.Fprintf(os.Stderr, "error on %s: %v\n", inst.Name, err)
		}
	}
	return nil
}

func deployOne(cfg *Config, pemFile, configPath string, inst *Instance, bootstrapIP string) error {
	if !inst.Initialized {
		header(inst, "setup (first time)")
		if err := runSSH(pemFile, inst, setupScript(cfg.GoVersion, inst.SSHUser)); err != nil {
			return fmt.Errorf("setup: %w", err)
		}
		inst.Initialized = true
		if err := saveConfig(configPath, cfg); err != nil {
			fmt.Fprintf(os.Stderr, "warning: could not save config: %v\n", err)
		}
	}

	header(inst, "copy source")
	if err := runRsync(pemFile, inst, findProjectRoot(), remoteProjDir); err != nil {
		return fmt.Errorf("copy: %w", err)
	}

	header(inst, "install")
	if err := runSSH(pemFile, inst, installCmd); err != nil {
		return fmt.Errorf("install: %w", err)
	}

	header(inst, "run daemon")
	metricsAddr := metricsAddrFrom(cfg)
	if err := runSSH(pemFile, inst, daemonStartScript(inst, bootstrapIP, metricsAddr, nil)); err != nil {
		return fmt.Errorf("run: %w", err)
	}
	return nil
}

const installCmd = `cd ~/data-rings && mkdir -p ~/tmp && PATH=$PATH:/usr/local/go/bin:$HOME/go/bin TMPDIR=~/tmp GOFLAGS=-p=1 make install`

func cmdCopy(cfg *Config, pemFile string, instNames []string) error {
	insts, err := cfg.resolveInstances(instNames)
	if err != nil {
		return err
	}
	projRoot := findProjectRoot()
	for _, inst := range insts {
		header(inst, "copy source")
		if err := runRsync(pemFile, inst, projRoot, remoteProjDir); err != nil {
			fmt.Fprintf(os.Stderr, "error on %s: %v\n", inst.Name, err)
		}
	}
	return nil
}

func cmdInstall(cfg *Config, pemFile string, instNames []string) error {
	return forEachInstance(cfg, pemFile, instNames, "install", func(inst *Instance) error {
		return runSSH(pemFile, inst, installCmd)
	})
}

func cmdRun(cfg *Config, pemFile string, instNames []string, extraFlags []string) error {
	insts, err := cfg.resolveInstances(instNames)
	if err != nil {
		return err
	}
	bootstrapIP := bootstrapIPFrom(cfg)
	metricsAddr := metricsAddrFrom(cfg)

	var filteredFlags []string
	for _, f := range extraFlags {
		switch {
		case f == "--bootstrap=none":
			bootstrapIP = ""
		case strings.HasPrefix(f, "--bootstrap="):
			bootstrapIP = extractIPFromBootstrapFlag(strings.TrimPrefix(f, "--bootstrap="))
		default:
			filteredFlags = append(filteredFlags, f)
		}
	}

	for _, inst := range insts {
		header(inst, "run daemon")
		if err := runSSH(pemFile, inst, daemonStartScript(inst, bootstrapIP, metricsAddr, filteredFlags)); err != nil {
			fmt.Fprintf(os.Stderr, "error on %s: %v\n", inst.Name, err)
		}
	}
	return nil
}

func extractIPFromBootstrapFlag(val string) string {
	parts := strings.Split(val, "/")
	for i, p := range parts {
		if p == "ip4" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return val
}

func cmdStop(cfg *Config, pemFile string, instNames []string) error {
	return forEachInstance(cfg, pemFile, instNames, "stop daemon", func(inst *Instance) error {
		return runSSH(pemFile, inst, daemonStopScript())
	})
}

func cmdRestart(cfg *Config, pemFile string, instNames []string) error {
	if err := cmdStop(cfg, pemFile, instNames); err != nil {
		return err
	}
	time.Sleep(2 * time.Second)
	return cmdRun(cfg, pemFile, instNames, nil)
}

func cmdClean(cfg *Config, pemFile string, args []string) error {
	fs := flag.NewFlagSet("clean", flag.ContinueOnError)
	wipeIdentity := fs.Bool("wipe-identity", false, "also delete the node identity (keypair)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	insts, err := cfg.resolveInstances(fs.Args())
	if err != nil {
		return err
	}
	for _, inst := range insts {
		label := "clean data"
		if *wipeIdentity {
			label = "clean data + identity"
		}
		header(inst, label)
		if err := runSSH(pemFile, inst, cleanScript(*wipeIdentity)); err != nil {
			fmt.Fprintf(os.Stderr, "error on %s: %v\n", inst.Name, err)
		}
	}
	return nil
}

func cmdJoinGroups(cfg *Config, pemFile string, instNames []string) error {
	insts, err := cfg.resolveInstances(instNames)
	if err != nil {
		return err
	}

	type joinTask struct {
		inst	*Instance
		group	*Group
		founder	bool
	}
	var tasks []joinTask
	for _, inst := range insts {
		for _, gname := range inst.Groups {
			grp, err := cfg.findGroup(gname)
			if err != nil {
				fmt.Fprintf(os.Stderr, "warning: %v\n", err)
				continue
			}
			tasks = append(tasks, joinTask{inst, grp, contains(inst.FounderOf, gname)})
		}
	}
	sort.SliceStable(tasks, func(i, j int) bool {
		return tasks[i].founder && !tasks[j].founder
	})

	for _, t := range tasks {
		header(t.inst, fmt.Sprintf("join group %s", t.group.Name))
		out, err := captureSSH(pemFile, t.inst, joinGroupScript(t.group.Name, t.group.PrivateKey))
		if err != nil {
			if strings.Contains(out, "already joined") || strings.Contains(out, "Conflict") {
				fmt.Printf("  already in group %s (skipping)\n", t.group.Name)
				continue
			}
			fmt.Fprintf(os.Stderr, "error joining %s on %s: %v\n%s\n", t.group.Name, t.inst.Name, err, out)
			continue
		}
		fmt.Println(out)
	}
	return nil
}

type repeatedFlag []string

func (r *repeatedFlag) String() string		{ return strings.Join(*r, " ") }
func (r *repeatedFlag) Set(v string) error	{ *r = append(*r, v); return nil }

func cmdMegaDeploy(cfg *Config, pemFile, configPath string, args []string) error {
	fs := flag.NewFlagSet("mega-deploy", flag.ContinueOnError)
	experimentID := fs.String("experiment", "", "experiment id forwarded to each daemon as --experiment <id> (events written to ~/.datarings/experiments/<id>.ndjson)")
	var daemonFlags repeatedFlag
	fs.Var(&daemonFlags, "daemon-flag", "extra flag passed verbatim to each daemon (repeatable, e.g. --daemon-flag='--dht-replication 4')")
	if err := fs.Parse(reorderFlags(args)); err != nil {
		return err
	}
	return megaDeploy(cfg, pemFile, fs.Args(), false, false, *experimentID, []string(daemonFlags))
}

func cmdMegaDeployClean(cfg *Config, pemFile, configPath string, args []string) error {
	fs := flag.NewFlagSet("mega-deploy-clean", flag.ContinueOnError)
	wipeIdentity := fs.Bool("wipe-identity", false, "also delete node identities (keypairs) during the clean phase")
	experimentID := fs.String("experiment", "", "experiment id forwarded to each daemon as --experiment <id> (events written to ~/.datarings/experiments/<id>.ndjson)")
	var daemonFlags repeatedFlag
	fs.Var(&daemonFlags, "daemon-flag", "extra flag passed verbatim to each daemon (repeatable, e.g. --daemon-flag='--dht-replication 4')")
	if err := fs.Parse(reorderFlags(args)); err != nil {
		return err
	}
	return megaDeploy(cfg, pemFile, fs.Args(), true, *wipeIdentity, *experimentID, []string(daemonFlags))
}

func megaDeploy(cfg *Config, pemFile string, instNames []string, clean, wipeIdentity bool, experimentID string, daemonFlags []string) error {
	insts, err := cfg.resolveInstances(instNames)
	if err != nil {
		return err
	}
	bootstrapIP := bootstrapIPFrom(cfg)
	projRoot := findProjectRoot()
	metricsAddr := metricsAddrFrom(cfg)

	label := "MEGA-DEPLOY"
	if clean {
		label = "MEGA-DEPLOY-CLEAN"
		if wipeIdentity {
			label = "MEGA-DEPLOY-CLEAN+IDENTITY"
		}
	}

	runParallel(insts, func(inst *Instance) {
		if clean {
			header(inst, label+": clean")
			if err := runSSH(pemFile, inst, cleanScript(wipeIdentity)); err != nil {
				fmt.Fprintf(os.Stderr, "clean error on %s: %v\n", inst.Name, err)
			}
		} else {
			header(inst, label+": stop")
			if err := runSSH(pemFile, inst, daemonStopScript()); err != nil {
				fmt.Fprintf(os.Stderr, "stop error on %s: %v\n", inst.Name, err)
			}
		}
	})

	runParallel(insts, func(inst *Instance) {
		header(inst, label+": copy source")
		if err := runRsync(pemFile, inst, projRoot, remoteProjDir); err != nil {
			fmt.Fprintf(os.Stderr, "copy error on %s: %v\n", inst.Name, err)
			return
		}
		header(inst, label+": install")
		if err := runSSH(pemFile, inst, installCmd); err != nil {
			fmt.Fprintf(os.Stderr, "install error on %s: %v\n", inst.Name, err)
		}
	})

	var primaryBootstrap *Instance
	var rest []*Instance
	for _, inst := range insts {
		if primaryBootstrap == nil && inst.IsBootstrap {
			primaryBootstrap = inst
		} else {
			rest = append(rest, inst)
		}
	}
	if primaryBootstrap != nil {
		header(primaryBootstrap, label+": run daemon (primary bootstrap)")
		if err := runSSH(pemFile, primaryBootstrap, daemonStartScriptExperiment(primaryBootstrap, bootstrapIP, metricsAddr, experimentID, daemonFlags)); err != nil {
			fmt.Fprintf(os.Stderr, "run error on %s: %v\n", primaryBootstrap.Name, err)
		}
		fmt.Println("Waiting 5s for primary bootstrap to initialise...")
		time.Sleep(5 * time.Second)
	}
	runParallel(rest, func(inst *Instance) {
		header(inst, label+": run daemon")
		if err := runSSH(pemFile, inst, daemonStartScriptExperiment(inst, bootstrapIP, metricsAddr, experimentID, daemonFlags)); err != nil {
			fmt.Fprintf(os.Stderr, "run error on %s: %v\n", inst.Name, err)
		}
	})

	fmt.Println("Waiting 15s for Chord ring to stabilize...")
	time.Sleep(15 * time.Second)

	names := make([]string, len(insts))
	for i, inst := range insts {
		names[i] = inst.Name
	}
	return cmdJoinGroups(cfg, pemFile, names)
}

func cmdSSHInteractive(pemFile string, inst *Instance) error {
	args := append([]string{"ssh"}, sshArgs(pemFile, inst)...)
	c := exec.Command(args[0], args[1:]...)
	c.Stdin = os.Stdin
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	return c.Run()
}

func cmdStore(cfg *Config, pemFile string, args []string) error {
	fs := flag.NewFlagSet("store", flag.ContinueOnError)
	groupFlag := fs.String("group", "", "private group name to publish into")
	ttlFlag := fs.String("ttl", "", "TTL for the published data (e.g. 3s, 10m)")
	noPubFlag := fs.Bool("no-pub", false, "rsync + drings add only; skip the publish step")
	if err := fs.Parse(reorderFlags(args)); err != nil {
		return err
	}
	if fs.NArg() < 2 {
		return fmt.Errorf("usage: drings-deploy store <instance> <local-path> [--group <name>] [--ttl <duration>]")
	}

	inst, _, err := cfg.findInstance(fs.Arg(0))
	if err != nil {
		return err
	}

	localPath := fs.Arg(1)
	info, err := os.Stat(localPath)
	if err != nil {
		return fmt.Errorf("local path: %w", err)
	}
	name := filepath.Base(localPath)
	remotePath := remoteUploadDir + "/" + name

	header(inst, fmt.Sprintf("store: uploading %s", name))
	if err := runSSH(pemFile, inst, fmt.Sprintf("mkdir -p %s", remoteUploadDir)); err != nil {
		return err
	}
	if info.IsDir() {
		if err := runRsyncPath(pemFile, inst, localPath+"/", remotePath); err != nil {
			return fmt.Errorf("upload dir: %w", err)
		}
	} else {
		if err := runRsyncPath(pemFile, inst, localPath, remoteUploadDir); err != nil {
			return fmt.Errorf("upload: %w", err)
		}
	}

	header(inst, "store: drings add")
	addOut, err := captureSSH(pemFile, inst, fmt.Sprintf(
		`export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin; drings add %s`, remotePath))
	if err != nil {
		return fmt.Errorf("drings add: %w\n%s", err, addOut)
	}
	fmt.Println(addOut)

	cidStr := parseCID(addOut)
	if cidStr == "" {
		return fmt.Errorf("could not parse CID from output:\n%s", addOut)
	}

	if *noPubFlag {
		header(inst, "store: skipping pub (--no-pub)")
		return nil
	}
	if *groupFlag != "" {
		pubCmd := fmt.Sprintf(`export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin; drings pub %s %s`, *groupFlag, cidStr)
		if *ttlFlag != "" {
			pubCmd += fmt.Sprintf(` --ttl %s`, *ttlFlag)
		}
		header(inst, fmt.Sprintf("store: drings pub %s %s", *groupFlag, cidStr))
		pubOut, err := captureSSH(pemFile, inst, pubCmd)
		if err != nil {
			return fmt.Errorf("drings pub: %w\n%s", err, pubOut)
		}
		fmt.Println(pubOut)
	} else {
		header(inst, fmt.Sprintf("store: drings pub %s (public)", cidStr))
		pubOut, err := captureSSH(pemFile, inst, fmt.Sprintf(
			`export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin; drings pub %s`, cidStr))
		if err != nil {
			return fmt.Errorf("drings pub: %w\n%s", err, pubOut)
		}
		fmt.Println(pubOut)
	}
	return nil
}

func cmdFetch(cfg *Config, pemFile string, args []string) error {
	fs := flag.NewFlagSet("fetch", flag.ContinueOnError)
	groupFlag := fs.String("group", "", "private group name to fetch from")
	if err := fs.Parse(reorderFlags(args)); err != nil {
		return err
	}
	if fs.NArg() < 2 {
		return fmt.Errorf("usage: drings-deploy fetch <instance> <cid> [--group <name>]")
	}
	inst, _, err := cfg.findInstance(fs.Arg(0))
	if err != nil {
		return err
	}
	cidStr := fs.Arg(1)

	var remoteCmd string
	if *groupFlag != "" {
		header(inst, fmt.Sprintf("fetch: drings get %s %s", *groupFlag, cidStr))
		remoteCmd = fmt.Sprintf(`export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin; drings get %s %s`, *groupFlag, cidStr)
	} else {
		header(inst, fmt.Sprintf("fetch: drings get %s", cidStr))
		remoteCmd = fmt.Sprintf(`export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin; drings get %s`, cidStr)
	}
	out, err := captureSSH(pemFile, inst, remoteCmd)
	fmt.Println(out)
	return err
}

func cmdExec(cfg *Config, pemFile string, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: drings-deploy exec <instance> <drings-cmd> [args...]")
	}
	inst, _, err := cfg.findInstance(args[0])
	if err != nil {
		return err
	}
	remoteCmd := fmt.Sprintf(`export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin; drings %s`, strings.Join(args[1:], " "))
	out, err := captureSSH(pemFile, inst, remoteCmd)
	fmt.Println(out)
	return err
}

func cmdStatus(cfg *Config, pemFile string, instNames []string) error {
	insts, err := cfg.resolveInstances(instNames)
	if err != nil {
		return err
	}
	for _, inst := range insts {
		header(inst, "status")
		script := fmt.Sprintf(`
if [ -f %s ]; then
    PID=$(cat %s)
    if kill -0 $PID 2>/dev/null; then
        echo "daemon: RUNNING (PID: $PID)"
    else
        echo "daemon: STOPPED (stale PID: $PID)"
    fi
else
    echo "daemon: STOPPED (no PID file)"
fi
echo "--- last 10 log lines ---"
tail -10 %s 2>/dev/null || echo "(no log)"
`, daemonPIDFile, daemonPIDFile, daemonLogFile)
		runSSH(pemFile, inst, script)
	}
	return nil
}

func cmdRejoinGroups(cfg *Config, pemFile string, instNames []string) error {
	insts, err := cfg.resolveInstances(instNames)
	if err != nil {
		return err
	}
	for _, inst := range insts {
		for _, gname := range inst.Groups {
			grp, err := cfg.findGroup(gname)
			if err != nil {
				fmt.Fprintf(os.Stderr, "warning: %v\n", err)
				continue
			}
			header(inst, fmt.Sprintf("rejoin group %s: leave", gname))
			leaveScript := fmt.Sprintf(`export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin
drings ring leave %s 2>&1 || true
`, gname)
			out, _ := captureSSH(pemFile, inst, leaveScript)
			fmt.Println(strings.TrimSpace(out))

			header(inst, fmt.Sprintf("rejoin group %s: join", gname))
			out2, err := captureSSH(pemFile, inst, joinGroupScript(gname, grp.PrivateKey))
			if err != nil {
				fmt.Fprintf(os.Stderr, "error rejoining %s on %s: %v\n%s\n", gname, inst.Name, err, out2)
				continue
			}
			fmt.Println(out2)
		}
	}
	return nil
}

func bootstrapIPFrom(cfg *Config) string {
	if b := cfg.bootstrapInstance(); b != nil {
		return b.IPv4
	}
	return ""
}

func metricsAddrFrom(cfg *Config) string {
	if cfg.Monitoring.Enabled {
		return cfg.Monitoring.MetricsPort
	}
	return ""
}

func runParallel(insts []*Instance, fn func(*Instance)) {
	if len(insts) == 0 {
		return
	}
	var wg sync.WaitGroup
	for _, inst := range insts {
		wg.Add(1)
		go func(inst *Instance) {
			defer wg.Done()
			fn(inst)
		}(inst)
	}
	wg.Wait()
}

func forEachInstance(cfg *Config, pemFile string, instNames []string, step string, fn func(*Instance) error) error {
	insts, err := cfg.resolveInstances(instNames)
	if err != nil {
		return err
	}
	for _, inst := range insts {
		header(inst, step)
		if err := fn(inst); err != nil {
			fmt.Fprintf(os.Stderr, "error on %s: %v\n", inst.Name, err)
		}
	}
	return nil
}

func reorderFlags(args []string) []string {
	var flags, positional []string
	for i := 0; i < len(args); i++ {
		if strings.HasPrefix(args[i], "-") {
			flags = append(flags, args[i])
			if !strings.Contains(args[i], "=") && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				i++
				flags = append(flags, args[i])
			}
		} else {
			positional = append(positional, args[i])
		}
	}
	return append(flags, positional...)
}
