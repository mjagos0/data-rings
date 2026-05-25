package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

func cmdLaunch(cfg *Config, pemFile, configPath string, args []string) error {
	fs := flag.NewFlagSet("launch", flag.ContinueOnError)
	regionFlag := fs.String("region", "eu-central-1", "AWS region")
	nameFlag := fs.String("name", "", "instance name (required)")
	keyPairFlag := fs.String("key-pair", "", "AWS key pair name")
	doSetup := fs.Bool("setup", false, "run setup + start daemon after instance is ready")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *nameFlag == "" {
		return fmt.Errorf("usage: drings-deploy launch --region <region> --name <name> [--key-pair <name>] [--setup]")
	}

	keyPair := *keyPairFlag
	if keyPair == "" {
		keyPair = cfg.AWSKeyPair
	}
	if keyPair == "" {
		return fmt.Errorf("no AWS key pair specified: set aws_key_pair in test-fleet.toml or pass --key-pair")
	}

	for _, inst := range cfg.Instances {
		if inst.Name == *nameFlag {
			return fmt.Errorf("instance %q already exists in config", *nameFlag)
		}
	}

	fmt.Printf("Launching instance %q in %s (blueprint: debian_13, bundle: nano_3_0, key: %s)...\n",
		*nameFlag, *regionFlag, keyPair)

	createOut, err := runAWSCLI(
		"lightsail", "create-instances",
		"--region", *regionFlag,
		"--instance-names", *nameFlag,
		"--availability-zone", *regionFlag+"a",
		"--blueprint-id", "debian_13",
		"--bundle-id", "nano_3_0",
		"--key-pair-name", keyPair,
	)
	if err != nil {
		return fmt.Errorf("create instance: %w\n%s", err, createOut)
	}
	fmt.Println("Instance creation initiated.")

	fmt.Printf("Waiting for instance to become running")
	var ipv4 string
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		fmt.Print(".")
		time.Sleep(5 * time.Second)
		raw, err := runAWSCLI(
			"lightsail", "get-instance",
			"--region", *regionFlag,
			"--instance-name", *nameFlag,
			"--query", "instance.{state:state.name,ip:publicIpAddress}",
			"--output", "json",
		)
		if err != nil {
			continue
		}
		var info struct {
			State	string	`json:"state"`
			IP	string	`json:"ip"`
		}
		if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &info); err != nil {
			continue
		}
		if info.State == "running" && info.IP != "" {
			ipv4 = info.IP
			break
		}
	}
	fmt.Println()

	if ipv4 == "" {
		fmt.Fprintf(os.Stderr, "instance did not reach running state in time, tearing down...\n")
		runAWSCLI("lightsail", "delete-instance", "--region", *regionFlag, "--instance-name", *nameFlag)
		return fmt.Errorf("instance %q did not become running within 5 minutes", *nameFlag)
	}
	fmt.Printf("Instance running at %s\n", ipv4)

	fmt.Printf("Opening firewall ports on %s...\n", *nameFlag)
	if err := openFirewallPorts(*regionFlag, *nameFlag); err != nil {
		fmt.Fprintf(os.Stderr, "WARNING: failed to open firewall ports: %v\n", err)
	} else {
		fmt.Println("Firewall ports opened.")
	}

	newInst := Instance{
		Name:		*nameFlag,
		Region:		*regionFlag,
		IPv4:		ipv4,
		SSHUser:	defaultSSHUser,
		Initialized:	false,
		Groups:		[]string{},
		FounderOf:	[]string{},
	}

	fmt.Printf("Waiting for SSH to become available on %s", ipv4)
	sshReady := false
	sshDeadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(sshDeadline) {
		fmt.Print(".")
		time.Sleep(5 * time.Second)
		out, err := captureSSH(pemFile, &newInst, "echo ok")
		if err == nil && strings.Contains(out, "ok") {
			sshReady = true
			break
		}
	}
	fmt.Println()

	if !sshReady {
		fmt.Fprintf(os.Stderr, "WARNING: SSH not reachable after 5 minutes (instance is still being added to config)\n")
	} else {
		fmt.Println("SSH connection established.")
	}

	cfg.Instances = append(cfg.Instances, newInst)
	if err := saveConfig(configPath, cfg); err != nil {
		return fmt.Errorf("save config: %w", err)
	}
	fmt.Printf("Instance %q (%s) added to %s\n", *nameFlag, ipv4, configPath)

	if *doSetup {
		if !sshReady {
			return fmt.Errorf("cannot run setup: SSH is not available on %s", ipv4)
		}
		bootstrapIP := bootstrapIPFrom(cfg)
		fmt.Printf("Running first-time setup on %s...\n", *nameFlag)
		if err := deployOne(cfg, pemFile, configPath, &newInst, bootstrapIP); err != nil {
			return fmt.Errorf("setup: %w", err)
		}
		fmt.Printf("Setup complete. Daemon started on %s.\n", *nameFlag)
	}

	return nil
}

func cmdTeardown(cfg *Config, configPath string, args []string) error {
	fs := flag.NewFlagSet("teardown", flag.ContinueOnError)
	regionFlag := fs.String("region", "eu-central-1", "AWS region")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() == 0 {
		return fmt.Errorf("usage: drings-deploy teardown --region <region> <instance-name>")
	}
	instName := fs.Arg(0)

	inst, idx, err := cfg.findInstance(instName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: %v (attempting AWS teardown anyway)\n", err)
		idx = -1
		inst = &Instance{Name: instName}
	}

	fmt.Printf("Tearing down instance %q in %s...\n", inst.Name, *regionFlag)
	out, err := runAWSCLI("lightsail", "delete-instance", "--region", *regionFlag, "--instance-name", inst.Name)
	if err != nil {
		return fmt.Errorf("delete instance: %w\n%s", err, out)
	}
	fmt.Printf("Instance %q deletion initiated.\n", inst.Name)

	if idx >= 0 {
		cfg.Instances = append(cfg.Instances[:idx], cfg.Instances[idx+1:]...)
		if err := saveConfig(configPath, cfg); err != nil {
			return fmt.Errorf("save config: %w", err)
		}
		fmt.Printf("Instance %q removed from %s\n", inst.Name, configPath)
	}

	return nil
}

func cmdOpenPorts(cfg *Config, args []string) error {
	fs := flag.NewFlagSet("open-ports", flag.ContinueOnError)
	regionFlag := fs.String("region", "", "AWS region override")
	if err := fs.Parse(args); err != nil {
		return err
	}

	names := fs.Args()
	var instances []Instance
	if len(names) == 0 {
		instances = cfg.Instances
	} else {
		for _, name := range names {
			inst, _, err := cfg.findInstance(name)
			if err != nil {
				return err
			}
			instances = append(instances, *inst)
		}
	}

	for _, inst := range instances {
		region := *regionFlag
		if region == "" {
			region = inst.Region
		}
		if region == "" {
			return fmt.Errorf("no region for instance %q: set Region in config or pass --region", inst.Name)
		}
		awsN := inst.awsName()
		fmt.Printf("Opening firewall ports on %s [aws: %s] (region %s)...\n", inst.Name, awsN, region)
		if err := openFirewallPorts(region, awsN); err != nil {
			return fmt.Errorf("instance %s: %w", inst.Name, err)
		}
		fmt.Println("  done.")
	}
	return nil
}

func openFirewallPorts(region, instanceName string) error {
	portInfos := `[{"fromPort":22,"toPort":22,"protocol":"tcp"},{"fromPort":7000,"toPort":7000,"protocol":"tcp"},{"fromPort":9100,"toPort":9100,"protocol":"tcp"},{"fromPort":30000,"toPort":60000,"protocol":"tcp"}]`
	out, err := runAWSCLI(
		"lightsail", "put-instance-public-ports",
		"--region", region,
		"--instance-name", instanceName,
		"--port-infos", portInfos,
	)
	if err != nil {
		return fmt.Errorf("put-instance-public-ports: %w\n%s", err, out)
	}
	return nil
}

func runAWSCLI(args ...string) (string, error) {
	out, err := exec.Command("aws", args...).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}
