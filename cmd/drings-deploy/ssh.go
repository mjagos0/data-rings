package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

var verbose bool

func sshArgs(pemFile string, inst *Instance) []string {
	if inst.isLocal() {
		return nil
	}
	return []string{
		"-i", pemFile,
		"-o", "StrictHostKeyChecking=no",
		"-o", "ConnectTimeout=15",
		"-o", "BatchMode=yes",
		"-o", "ControlMaster=auto",
		"-o", "ControlPath=/tmp/drings-ssh-%r@%h:%p",
		"-o", "ControlPersist=300",
		fmt.Sprintf("%s@%s", inst.SSHUser, inst.IPv4),
	}
}

func runSSH(pemFile string, inst *Instance, cmd string) error {
	if inst.isLocal() {
		cmd = inst.rewriteLocalCmd(cmd)
		c := exec.Command("sh", "-c", cmd)
		c.Stdout = os.Stdout
		c.Stderr = os.Stderr
		if verbose {
			fmt.Printf("  [local] %s\n", cmd)
		}
		return c.Run()
	}
	args := append(sshArgs(pemFile, inst), cmd)
	c := exec.Command("ssh", args...)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	if verbose {
		fmt.Printf("  [ssh] %s\n", cmd)
	}
	return c.Run()
}

func captureSSH(pemFile string, inst *Instance, cmd string) (string, error) {
	if inst.isLocal() {
		cmd = inst.rewriteLocalCmd(cmd)
		out, err := exec.Command("sh", "-c", cmd).CombinedOutput()
		return strings.TrimSpace(string(out)), err
	}
	args := append(sshArgs(pemFile, inst), cmd)
	out, err := exec.Command("ssh", args...).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func (inst *Instance) rewriteLocalCmd(cmd string) string {
	if inst.APIPort != 0 {
		cmd = strings.ReplaceAll(cmd, "localhost:7423", fmt.Sprintf("127.0.0.1:%d", inst.APIPort))
	}
	if inst.LogFile != "" {
		cmd = strings.ReplaceAll(cmd, "~/drings-daemon.log", inst.LogFile)
		cmd = strings.ReplaceAll(cmd, daemonLogFile, inst.LogFile)
	}
	if inst.DataDir != "" {
		cmd = strings.ReplaceAll(cmd, "~/.datarings", inst.DataDir)
		cmd = strings.ReplaceAll(cmd, remoteDataDir, inst.DataDir)
	}
	return cmd
}

func sshCmdStr(pemFile string) string {
	return fmt.Sprintf("ssh -i %s -o StrictHostKeyChecking=no -o BatchMode=yes -o ControlMaster=auto -o 'ControlPath=/tmp/drings-ssh-%%r@%%h:%%p' -o ControlPersist=300", pemFile)
}

func runRsync(pemFile string, inst *Instance, localSrc, remoteDst string) error {
	dest := fmt.Sprintf("%s@%s:%s", inst.SSHUser, inst.IPv4, remoteDst)
	args := []string{
		"-avz", "--delete", "--delete-excluded",
		"--exclude=.git",
		"--exclude=*.o",
		"--exclude=/drings",
		"--exclude=/drings-daemon",
		"--exclude=/drings-deploy",
		"--exclude=tmp/",
		"--exclude=test-fleet.toml",
		"--exclude=testfile-*.bin",
		"--exclude=experiments/*/runs/",
		"--exclude=.aws-keys/",
		"--exclude=*.pem",
		"-e", sshCmdStr(pemFile),
		localSrc + "/",
		dest,
	}
	return rsyncExec(args)
}

func runRsyncPath(pemFile string, inst *Instance, localPath, remoteDst string) error {
	dest := fmt.Sprintf("%s@%s:%s/", inst.SSHUser, inst.IPv4, remoteDst)
	args := []string{"-avz", "-e", sshCmdStr(pemFile), localPath, dest}
	return rsyncExec(args)
}

func rsyncExec(args []string) error {
	c := exec.Command("rsync", args...)
	if verbose {
		c.Stdout = os.Stdout
	} else {
		c.Stdout = io.Discard
	}
	c.Stderr = os.Stderr
	return c.Run()
}

func header(inst *Instance, step string) {
	fmt.Printf("\n=== [%s / %s] %s ===\n", inst.Name, inst.IPv4, step)
}
