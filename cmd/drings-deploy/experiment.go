package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

func cmdExperimentPull(cfg *Config, pemFile string, args []string) error {
	fs := flag.NewFlagSet("experiment-pull", flag.ContinueOnError)
	flushDelay := fs.Duration("flush-delay", 1500*time.Millisecond, "wait this long before pulling so the daemon's event log flushes its last batch")
	if err := fs.Parse(reorderFlags(args)); err != nil {
		return err
	}
	if fs.NArg() < 2 {
		return fmt.Errorf("usage: drings-deploy experiment-pull <experiment-id> <local-dir> [instances...]")
	}
	experimentID := fs.Arg(0)
	localDir := fs.Arg(1)
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", localDir, err)
	}

	var instNames []string
	if fs.NArg() > 2 {
		instNames = fs.Args()[2:]
	}
	insts, err := cfg.resolveInstances(instNames)
	if err != nil {
		return err
	}

	if *flushDelay > 0 {
		fmt.Printf("Waiting %s for daemon eventlog flush...\n", *flushDelay)
		time.Sleep(*flushDelay)
	}

	var wg sync.WaitGroup
	type result struct {
		inst	*Instance
		path	string
		size	int64
		err	error
	}
	resultsCh := make(chan result, len(insts))
	for _, inst := range insts {
		inst := inst
		wg.Add(1)
		go func() {
			defer wg.Done()
			localPath := filepath.Join(localDir, inst.Name+".ndjson")
			err := pullExperimentFile(pemFile, inst, experimentID, localPath)
			var sz int64
			if err == nil {
				if st, statErr := os.Stat(localPath); statErr == nil {
					sz = st.Size()
				}
			}
			resultsCh <- result{inst: inst, path: localPath, size: sz, err: err}
		}()
	}
	wg.Wait()
	close(resultsCh)

	var anyFailed bool
	for r := range resultsCh {
		if r.err != nil {
			fmt.Fprintf(os.Stderr, "  %-10s FAIL  %v\n", r.inst.Name, r.err)
			anyFailed = true
			continue
		}
		fmt.Printf("  %-10s OK    %s (%d bytes)\n", r.inst.Name, r.path, r.size)
	}
	if anyFailed {
		return fmt.Errorf("one or more nodes failed; see errors above")
	}
	return nil
}

func pullExperimentFile(pemFile string, inst *Instance, experimentID, localPath string) error {
	remoteRel := filepath.Join("experiments", experimentID+".ndjson")
	if inst.isLocal() {
		dataDir := inst.DataDir
		if dataDir == "" {
			return fmt.Errorf("local instance %s has empty data_dir", inst.Name)
		}
		src := filepath.Join(dataDir, remoteRel)
		return copyFile(src, localPath)
	}
	src := fmt.Sprintf("%s@%s:%s/%s", inst.SSHUser, inst.IPv4, remoteDataDir, remoteRel)
	args := []string{
		"-i", pemFile,
		"-o", "StrictHostKeyChecking=no",
		"-o", "BatchMode=yes",
		"-o", "ControlMaster=auto",
		"-o", "ControlPath=/tmp/drings-ssh-%r@%h:%p",
		"-o", "ControlPersist=300",
		src,
		localPath,
	}
	c := exec.Command("scp", args...)
	out, err := c.CombinedOutput()
	if err != nil {
		return fmt.Errorf("scp: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := out.ReadFrom(in); err != nil {
		return err
	}
	return out.Sync()
}
