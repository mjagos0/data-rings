package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

func cmdRings(cfg *Config, pemFile string, instNames []string) error {
	insts, err := cfg.resolveInstances(instNames)
	if err != nil {
		return err
	}

	fmt.Printf("\n%-10s  %-12s  %-12s  %-12s  %-7s  %-6s  %-7s\n",
		"NODE", "ID", "SUCCESSOR", "PREDECESSOR", "FINGERS", "BLOCKS", "RECORDS")
	fmt.Println(strings.Repeat("-", 80))

	for _, inst := range insts {
		raw, err := captureSSH(pemFile, inst, `curl -sf http://localhost:7423/debug/state 2>/dev/null`)
		if err != nil || raw == "" {
			fmt.Printf("%-10s  (daemon unreachable)\n", inst.Name)
			continue
		}

		id := shorten(extractJSON(raw, "id"), 12)
		succ := shorten(extractJSONObj(raw, "successor", "ID"), 12)
		pred := extractJSONObj(raw, "predecessor", "ID")
		if pred == "(none)" {
			pred = "(nil)"
		} else {
			pred = shorten(pred, 12)
		}
		selfID := extractJSON(raw, "id")
		fingerList := uniqueFingers(raw, selfID)
		blocks := extractJSON(raw, "block_count")
		records := extractJSON(raw, "record_count")

		fmt.Printf("%-10s  %-12s  %-12s  %-12s  %-7d  %-6s  %-7s\n",
			inst.Name, id, succ, pred, len(fingerList), blocks, records)
	}
	return nil
}

func cmdRingHealth(cfg *Config, pemFile string, instNames []string) error {
	insts, err := cfg.resolveInstances(instNames)
	if err != nil {
		return err
	}

	type nodeCheck struct {
		name		string
		sshOK		bool
		daemonOK	bool
		nodeID		string
		sshErr		string
		daemonErr	string
	}

	results := make([]nodeCheck, len(insts))
	failures := 0
	for i, inst := range insts {
		c := nodeCheck{name: inst.Name}
		_, sshErr := captureSSH(pemFile, inst, `echo ok`)
		if sshErr != nil {
			c.sshErr = sshErr.Error()
			failures++
			results[i] = c
			continue
		}
		c.sshOK = true
		raw, apiErr := captureSSH(pemFile, inst, `curl -sf http://localhost:7423/debug/state 2>/dev/null`)
		if apiErr != nil || raw == "" {
			c.daemonErr = "API unreachable"
			failures++
			results[i] = c
			continue
		}
		c.daemonOK = true
		c.nodeID = extractJSON(raw, "id")
		results[i] = c
	}

	fmt.Printf("\n%-10s  %-5s  %-8s  %-12s  %s\n", "NODE", "SSH", "DAEMON", "NODE ID", "STATUS")
	fmt.Println(strings.Repeat("-", 60))
	for _, c := range results {
		sshCol := "OK"
		if !c.sshOK {
			sshCol = "FAIL"
		}
		daemonCol := "OK"
		if !c.daemonOK {
			daemonCol = "DOWN"
		}
		if !c.sshOK {
			daemonCol = "-"
		}
		idCol := shorten(c.nodeID, 12)
		if idCol == "" {
			idCol = "-"
		}
		status := "ready"
		if !c.sshOK {
			status = "ssh: " + c.sshErr
		} else if !c.daemonOK {
			status = c.daemonErr
		}
		fmt.Printf("%-10s  %-5s  %-8s  %-12s  %s\n", c.name, sshCol, daemonCol, idCol, status)
	}

	if failures > 0 {
		return fmt.Errorf("%d/%d nodes not ready", failures, len(insts))
	}
	fmt.Printf("\nAll %d nodes ready.\n", len(insts))
	return nil
}

func cmdRingDebug(cfg *Config, pemFile string, instNames []string) error {
	insts, err := cfg.resolveInstances(instNames)
	if err != nil {
		return err
	}
	for _, inst := range insts {
		header(inst, "ring debug state (raw JSON)")
		pretty, err := captureSSH(pemFile, inst,
			`curl -sf http://localhost:7423/debug/state | python3 -m json.tool 2>/dev/null || curl -sf http://localhost:7423/debug/state`)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  curl failed: %v\n%s\n", err, pretty)
			continue
		}
		fmt.Println(pretty)
	}
	return nil
}

func cmdGroupsDebug(cfg *Config, pemFile string, instNames []string) error {
	insts, err := cfg.resolveInstances(instNames)
	if err != nil {
		return err
	}
	for _, inst := range insts {
		header(inst, "private ring debug state")
		out, err := captureSSH(pemFile, inst,
			`curl -sf http://localhost:7423/debug/groups | python3 -m json.tool 2>/dev/null || curl -sf http://localhost:7423/debug/groups`)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  curl failed: %v\n%s\n", err, out)
			continue
		}
		if strings.TrimSpace(out) == "" || strings.TrimSpace(out) == "null" || strings.TrimSpace(out) == "[]" {
			fmt.Println("  (no active private rings)")
		} else {
			fmt.Println(out)
		}
	}
	return nil
}

func cmdRecordsDebug(cfg *Config, pemFile string, instNames []string) error {
	insts, err := cfg.resolveInstances(instNames)
	if err != nil {
		return err
	}
	for _, inst := range insts {
		header(inst, "public-ring record keys")
		out, err := captureSSH(pemFile, inst,
			`curl -sf http://localhost:7423/debug/records 2>/dev/null`)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  curl failed: %v\n%s\n", err, out)
			continue
		}
		if strings.TrimSpace(out) == "" || strings.TrimSpace(out) == "null" || strings.TrimSpace(out) == "[]" {
			fmt.Println("[]")
		} else {
			fmt.Println(strings.TrimSpace(out))
		}
	}
	return nil
}

func cmdDebugDump(cfg *Config, pemFile string, args []string) error {
	fs := flag.NewFlagSet("debug-dump", flag.ContinueOnError)
	lines := fs.Int("lines", 100, "number of log lines to show per node")
	if err := fs.Parse(args); err != nil {
		return err
	}

	insts, err := cfg.resolveInstances(fs.Args())
	if err != nil {
		return err
	}

	for _, inst := range insts {
		header(inst, "debug dump")

		fmt.Println("  --- Public Ring State ---")
		stateRaw, err := captureSSH(pemFile, inst, `curl -sf http://localhost:7423/debug/state 2>/dev/null`)
		if err != nil || stateRaw == "" {
			fmt.Println("  (daemon unreachable)")
		} else {
			printRingState(stateRaw)
		}

		fmt.Println("  --- Private Ring State ---")
		groupsRaw, _ := captureSSH(pemFile, inst,
			`curl -sf http://localhost:7423/debug/groups | python3 -m json.tool 2>/dev/null || curl -sf http://localhost:7423/debug/groups`)
		if strings.TrimSpace(groupsRaw) == "" || groupsRaw == "null" || groupsRaw == "[]" {
			fmt.Println("  (no active private rings)")
		} else {
			fmt.Println(groupsRaw)
		}

		fmt.Println("  --- Record Keys ---")
		recordsRaw, _ := captureSSH(pemFile, inst, `curl -sf http://localhost:7423/debug/records 2>/dev/null`)
		if strings.TrimSpace(recordsRaw) == "" || recordsRaw == "null" || recordsRaw == "[]" {
			fmt.Println("  (no records)")
		} else {
			fmt.Printf("  %s\n", strings.TrimSpace(recordsRaw))
		}

		fmt.Printf("  --- Last %d Log Lines ---\n", *lines)
		logOut, _ := captureSSH(pemFile, inst, fmt.Sprintf(`tail -n %d ~/drings-daemon.log 2>/dev/null`, *lines))
		if logOut == "" {
			fmt.Println("  (no log)")
		} else {
			fmt.Println(logOut)
		}
		fmt.Println()
	}
	return nil
}

func cmdLogs(cfg *Config, pemFile string, args []string) error {
	fs := flag.NewFlagSet("logs", flag.ContinueOnError)
	lines := fs.Int("lines", 100, "number of log lines to show (ignored when --since is set)")
	since := fs.String("since", "", "show logs from this time")
	until := fs.String("until", "", "show logs up to this time (requires --since)")
	level := fs.String("level", "", "change the daemon log level before fetching logs")
	if err := fs.Parse(args); err != nil {
		return err
	}

	insts, err := cfg.resolveInstances(fs.Args())
	if err != nil {
		return err
	}

	for _, inst := range insts {
		if *level != "" {
			setCmd := fmt.Sprintf(`curl -sf -X PUT http://localhost:7423/debug/log-level -H 'Content-Type: application/json' -d '{"level":%q}'`, *level)
			out, serr := captureSSH(pemFile, inst, setCmd)
			if serr != nil {
				fmt.Fprintf(os.Stderr, "  [%s] set log level: %v\n", inst.Name, serr)
			} else {
				fmt.Printf("  [%s] log level set: %s\n", inst.Name, strings.TrimSpace(out))
			}
		}

		var logCmd string
		if *since != "" {
			awkScript := `{
				t = $1;
				sub(/^time=/, "", t);
				if (t >= since`
			if *until != "" {
				awkScript += ` && t <= until`
			}
			awkScript += `) print;
			}`
			awkArgs := fmt.Sprintf(`-v since=%q`, *since)
			if *until != "" {
				awkArgs += fmt.Sprintf(` -v until=%q`, *until)
			}
			logCmd = fmt.Sprintf(`awk %s '%s' ~/drings-daemon.log 2>/dev/null`, awkArgs, awkScript)
			desc := fmt.Sprintf("daemon logs (since %s", *since)
			if *until != "" {
				desc += fmt.Sprintf(" until %s", *until)
			}
			desc += ")"
			header(inst, desc)
		} else {
			logCmd = fmt.Sprintf("tail -n %d ~/drings-daemon.log 2>/dev/null || echo '(log file not found)'", *lines)
			header(inst, fmt.Sprintf("daemon logs (last %d lines)", *lines))
		}

		out, lerr := captureSSH(pemFile, inst, logCmd)
		if lerr != nil {
			fmt.Fprintf(os.Stderr, "  %v\n", lerr)
			continue
		}
		fmt.Println(out)
	}
	return nil
}

func cmdLogLevel(cfg *Config, pemFile string, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: log-level <instance> <debug|info|warn|error>")
	}
	insts, err := cfg.resolveInstances([]string{args[0]})
	if err != nil {
		return err
	}
	for _, inst := range insts {
		setCmd := fmt.Sprintf(`curl -sf -X PUT http://localhost:7423/debug/log-level -H 'Content-Type: application/json' -d '{"level":%q}'`, args[1])
		out, serr := captureSSH(pemFile, inst, setCmd)
		if serr != nil {
			fmt.Fprintf(os.Stderr, "  [%s] error: %v\n", inst.Name, serr)
			continue
		}
		fmt.Printf("[%s] %s\n", inst.Name, strings.TrimSpace(out))
	}
	return nil
}

func cmdShareConcurrency(cfg *Config, pemFile string, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: share-concurrency <instance> <n>")
	}
	insts, err := cfg.resolveInstances([]string{args[0]})
	if err != nil {
		return err
	}
	for _, inst := range insts {
		setCmd := fmt.Sprintf(`curl -sf -X PUT http://localhost:7423/debug/share-concurrency -H 'Content-Type: application/json' -d '{"n":%s}'`, args[1])
		out, serr := captureSSH(pemFile, inst, setCmd)
		if serr != nil {
			fmt.Fprintf(os.Stderr, "  [%s] error: %v\n", inst.Name, serr)
			continue
		}
		fmt.Printf("[%s] %s\n", inst.Name, strings.TrimSpace(out))
	}
	return nil
}

func printRingState(raw string) {
	fields := map[string]string{
		"id":		extractJSON(raw, "id"),
		"addr":		extractJSON(raw, "addr"),
		"successor":	extractJSONObj(raw, "successor", "ID"),
		"predecessor":	extractJSONObj(raw, "predecessor", "ID"),
		"blocks":	extractJSON(raw, "block_count"),
		"records":	extractJSON(raw, "record_count"),
	}
	fmt.Printf("  id:          %s\n", fields["id"])
	fmt.Printf("  addr:        %s\n", fields["addr"])
	fmt.Printf("  successor:   %s\n", fields["successor"])
	fmt.Printf("  predecessor: %s\n", fields["predecessor"])
	fmt.Printf("  blocks:      %s   records: %s\n", fields["blocks"], fields["records"])

	selfID := fields["id"]
	unique := uniqueFingers(raw, selfID)
	if len(unique) > 0 {
		fmt.Printf("  fingers (%d unique): %s\n", len(unique), strings.Join(unique, ", "))
	} else {
		fmt.Printf("  fingers: (all point to self — single-node ring)\n")
	}
}

func extractJSON(raw, key string) string {
	needle := `"` + key + `":`
	idx := strings.Index(raw, needle)
	if idx < 0 {
		return ""
	}
	rest := strings.TrimSpace(raw[idx+len(needle):])
	if strings.HasPrefix(rest, `"`) {
		end := strings.Index(rest[1:], `"`)
		if end < 0 {
			return ""
		}
		return rest[1 : end+1]
	}
	end := strings.IndexAny(rest, ",}\n")
	if end < 0 {
		return strings.TrimSpace(rest)
	}
	return strings.TrimSpace(rest[:end])
}

func extractJSONObj(raw, objKey, fieldKey string) string {
	needle := `"` + objKey + `":`
	idx := strings.Index(raw, needle)
	if idx < 0 {
		return "(none)"
	}
	sub := raw[idx+len(needle):]
	if strings.HasPrefix(strings.TrimSpace(sub), "null") {
		return "(none)"
	}
	val := extractJSON(sub, fieldKey)
	if val == "" {
		return "(none)"
	}
	return val
}

func uniqueFingers(raw, selfID string) []string {
	seen := map[string]bool{}
	var result []string
	idx := strings.Index(raw, `"fingers":`)
	if idx < 0 {
		return nil
	}
	sub := raw[idx+len(`"fingers":`):]
	for {
		i := strings.Index(sub, `"ID":"`)
		if i < 0 {
			break
		}
		sub = sub[i+6:]
		end := strings.Index(sub, `"`)
		if end < 0 {
			break
		}
		id := sub[:end]
		sub = sub[end:]
		if id != selfID && !seen[id] {
			seen[id] = true
			result = append(result, id[:12]+"…")
		}
	}
	return result
}
