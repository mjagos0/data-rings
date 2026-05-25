package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

func usage() {
	fmt.Fprint(os.Stderr, `Usage: drings-deploy [--config <path>] [--verbose] <command> [args]

Fleet:
  deploy   [instances...]           deploy
  copy     [instances...]           copy source
  install  [instances...]           install
  run      [instances...]           start
  stop     [instances...]           stop
  restart  [instances...]           restart
  clean    [--wipe-identity] [instances...]
                                    stop and wipe
  join-groups [instances...]        join groups
  mega-deploy [--experiment <id>] [instances...]
                                    full deploy
  mega-deploy-clean [--wipe-identity] [--experiment <id>] [instances...]
                                    full deploy with wipe

Single instance:
  ssh    <instance>                 SSH
  status [instances...]             status

Remote drings:
  store <instance> <path> [--group <name>]   upload a file
  fetch <instance> <cid>  [--group <name>]   fetch a CID
  exec  <instance> <drings-cmd> [args...]    run a command

Health:
  ring-health  [instances...]         check reachability

Inspection:
  rings         [instances...]        routing table
  ring-debug    [instances...]        state JSON
  groups-debug  [instances...]        groups JSON
  records-debug [instances...]        records JSON
  debug-dump    [--lines N] [instances...]
                                      state and log dump

Logs:
  logs         [--lines N] [--since T] [--until T] [--level L] [instances...]
                                      tail logs
  log-level    <instance> <level>     set log level
  share-concurrency <instance> <n>    set worker pool size

Monitoring:
  monitor-status                      monitoring status
  monitor-setup [instances...]        install Promtail
  monitor-deploy                      deploy dashboards
  prometheus-config                   print Prometheus config

Recovery:
  rejoin-groups [instances...]        rejoin groups

Provisioning:
  launch   --name <name> [--region <r>] [--key-pair <k>] [--setup]
                                      create an instance
  teardown --region <r> <name>        destroy an instance

Experiments:
  experiment-pull <experiment-id> <local-dir> [instances...]
                                      pull experiment logs

Time:
  time-sync-setup [instances...]      install time sync
  clock-probe [--samples N] [--out FILE] [instances...]
                                      probe clocks

Local:
  testdata [--out <dir>] [--files N] [--dirs N] [--min B] [--max B]
                                      generate test files

Flags:
`)
	flag.PrintDefaults()
}

func flagNewSet(name string) *flag.FlagSet {
	return flag.NewFlagSet(name, flag.ContinueOnError)
}

func main() {
	configPath := flag.String("config", defaultConfig, "path to test-fleet.toml")
	verboseFlag := flag.Bool("verbose", false, "verbose output")
	flag.Usage = usage
	flag.Parse()

	verbose = *verboseFlag

	if flag.NArg() == 0 {
		usage()
		os.Exit(1)
	}

	cfg, err := loadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	allLocal := true
	for _, inst := range cfg.Instances {
		if !inst.Local {
			allLocal = false
			break
		}
	}

	var pemFile string
	if !allLocal {
		pemFile, err = resolvePEMFile(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	}

	cmd := flag.Arg(0)
	args := flag.Args()[1:]

	var cmdErr error
	switch cmd {
	case "deploy":
		cmdErr = cmdDeploy(cfg, pemFile, *configPath, args)
	case "ssh":
		if len(args) == 0 {
			cmdErr = fmt.Errorf("usage: drings-deploy ssh <instance>")
			break
		}
		inst, _, err := cfg.findInstance(args[0])
		if err != nil {
			cmdErr = err
			break
		}
		cmdErr = cmdSSHInteractive(pemFile, inst)
	case "copy":
		cmdErr = cmdCopy(cfg, pemFile, args)
	case "install":
		cmdErr = cmdInstall(cfg, pemFile, args)
	case "run":
		var runArgs, extraFlags []string
		for _, a := range args {
			if strings.HasPrefix(a, "--") && a != "--" {
				extraFlags = append(extraFlags, a)
			} else {
				runArgs = append(runArgs, a)
			}
		}
		cmdErr = cmdRun(cfg, pemFile, runArgs, extraFlags)
	case "stop":
		cmdErr = cmdStop(cfg, pemFile, args)
	case "restart":
		cmdErr = cmdRestart(cfg, pemFile, args)
	case "clean":
		cmdErr = cmdClean(cfg, pemFile, args)
	case "join-groups":
		cmdErr = cmdJoinGroups(cfg, pemFile, args)
	case "mega-deploy":
		cmdErr = cmdMegaDeploy(cfg, pemFile, *configPath, args)
	case "mega-deploy-clean":
		cmdErr = cmdMegaDeployClean(cfg, pemFile, *configPath, args)
	case "store":
		cmdErr = cmdStore(cfg, pemFile, args)
	case "fetch":
		cmdErr = cmdFetch(cfg, pemFile, args)
	case "status":
		cmdErr = cmdStatus(cfg, pemFile, args)
	case "rings":
		cmdErr = cmdRings(cfg, pemFile, args)
	case "ring-health":
		cmdErr = cmdRingHealth(cfg, pemFile, args)
	case "ring-debug":
		cmdErr = cmdRingDebug(cfg, pemFile, args)
	case "groups-debug":
		cmdErr = cmdGroupsDebug(cfg, pemFile, args)
	case "records-debug":
		cmdErr = cmdRecordsDebug(cfg, pemFile, args)
	case "debug-dump":
		cmdErr = cmdDebugDump(cfg, pemFile, args)
	case "logs":
		cmdErr = cmdLogs(cfg, pemFile, args)
	case "share-concurrency":
		cmdErr = cmdShareConcurrency(cfg, pemFile, args)
	case "log-level":
		cmdErr = cmdLogLevel(cfg, pemFile, args)
	case "exec":
		cmdErr = cmdExec(cfg, pemFile, args)
	case "testdata":
		cmdErr = cmdTestdata(args)
	case "rejoin-groups":
		cmdErr = cmdRejoinGroups(cfg, pemFile, args)
	case "monitor-status":
		cmdErr = cmdMonitorStatus(cfg)
	case "monitor-setup":
		cmdErr = cmdMonitorSetup(cfg, pemFile, args)
	case "monitor-deploy":
		cmdErr = cmdMonitorDeploy(cfg, pemFile)
	case "prometheus-config":
		cmdErr = cmdPrometheusConfig(cfg)
	case "launch":
		cmdErr = cmdLaunch(cfg, pemFile, *configPath, args)
	case "teardown":
		cmdErr = cmdTeardown(cfg, *configPath, args)
	case "open-ports":
		cmdErr = cmdOpenPorts(cfg, args)
	case "experiment-pull":
		cmdErr = cmdExperimentPull(cfg, pemFile, args)
	case "time-sync-setup":
		cmdErr = cmdTimeSyncSetup(cfg, pemFile, args)
	case "clock-probe":
		cmdErr = cmdClockProbe(cfg, pemFile, args)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", cmd)
		usage()
		os.Exit(1)
	}

	if cmdErr != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", cmdErr)
		os.Exit(1)
	}
}
