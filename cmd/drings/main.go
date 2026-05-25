package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/mjagos0/datarings/dht"
)

const defaultAPI = "http://localhost:7423"

func main() {
	home, _ := os.UserHomeDir()

	api := flag.String("api", defaultAPI, "daemon address")
	dataDir := flag.String("data-dir", filepath.Join(home, ".datarings"), "data directory")
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() == 0 {
		usage()
		os.Exit(1)
	}

	vaultPath := filepath.Join(*dataDir, "keys.json")

	cmd := flag.Arg(0)
	args := flag.Args()[1:]

	var err error
	switch cmd {
	case "add":
		err = cmdAdd(*api, args)
	case "get":
		err = cmdGet(*api, args)
	case "pub":
		err = cmdPub(*api, args)
	case "list", "ls":
		err = cmdList(*api)
	case "remove", "rm":
		err = cmdRemove(*api, args)
	case "setup":
		err = cmdSetup(args)
	case "gc":
		err = cmdGC(*api)
	case "delete-cid":
		err = cmdDeleteCID(*api, args)
	case "config":
		err = cmdConfig(filepath.Join(*dataDir, "config.toml"), args)
	case "key":
		err = cmdKey(vaultPath, args)
	case "ring":
		err = cmdRing(*api, vaultPath, args)

	case "upload":
		err = cmdAdd(*api, args)
	case "publish":
		err = cmdPublishLowLevel(*api, args)
	case "join":
		err = cmdJoin(*api, args)
	case "download", "fetch":
		err = cmdDownload(*api, args)
	case "peer":
		err = cmdPeer(*api, args)
	case "provider":
		err = cmdProvider(*api, args)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", cmd)
		usage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: drings [--api <addr>] <command> [args]

Commands:
  add <path> [name]         add a file or directory (name shows in FUSE)
  get [<group>] <cid> [name]
                            fetch (name shows in FUSE)
  pub [<group>] <cid>       publish
  list                      list roots
  remove <cid|name>         remove a root
  delete-cid <group> <cid>  delete a CID
  gc                        run garbage collection
  setup [path]              set up the mount point

Configuration:
  config                    edit configuration
  config show               print configuration

Keys:
  key add <alias> <key>     add a key
  key remove <alias>        remove a key
  key list                  list keys

Rings:
  ring create [alias]       create a key (alias registers it in the vault)
  ring join <alias>         join a ring
  ring join <key> [name] [--storage-max=<size>] [--listen-addr=<multiaddr>]
                            join a ring (listen-addr defaults to OS-assigned)
  ring list                 list rings
  ring leave <group>        leave a ring
  ring quota <group>        show quota
  ring quota <group> <size> set quota

Flags:
`)
	flag.PrintDefaults()
}

func readError(resp *http.Response) error {
	body, _ := io.ReadAll(resp.Body)
	msg := strings.TrimSpace(string(body))
	if msg == "" {
		msg = resp.Status
	}
	return fmt.Errorf("%s", msg)
}

func loadVault(path string) (*dht.Vault, error) {
	v, err := dht.LoadVault(path)
	if err != nil {
		return nil, fmt.Errorf("load key vault: %w", err)
	}
	return v, nil
}

type addRequest struct {
	Path	string	`json:"path"`
	Name	string	`json:"name"`
}

type addResponse struct {
	ID		string		`json:"id"`
	Name		string		`json:"name"`
	CID		string		`json:"cid"`
	AddedAt		time.Time	`json:"added_at"`
	AlreadyTracked	bool		`json:"already_tracked"`
}

func cmdAdd(api string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: drings add <path> [name]")
	}

	path, err := filepath.Abs(args[0])
	if err != nil {
		return fmt.Errorf("resolve path: %w", err)
	}

	req := addRequest{Path: path}
	if len(args) >= 2 {
		req.Name = args[1]
	}
	body, _ := json.Marshal(req)
	resp, err := http.Post(api+"/add", "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return readError(resp)
	}

	var r addResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	if r.AlreadyTracked {
		fmt.Printf("already tracked\ncid: %s\n", r.CID)
	} else {
		fmt.Printf("added\ncid: %s\n", r.CID)
	}
	return nil
}

func cmdGet(api string, args []string) error {
	switch len(args) {
	case 1:
		return cmdGetPublic(api, args[0], "")
	case 2:

		if looksLikeCID(args[0]) {
			return cmdGetPublic(api, args[0], args[1])
		}
		return cmdGetPrivate(api, args[0], args[1], "")
	case 3:
		return cmdGetPrivate(api, args[0], args[1], args[2])
	default:
		return fmt.Errorf("usage: drings get [<group>] <cid> [name]")
	}
}

func cmdGetPublic(api, cidStr, name string) error {
	req := map[string]string{"cid": cidStr}
	if name != "" {
		req["name"] = name
	}
	body, _ := json.Marshal(req)
	resp, err := http.Post(api+"/dht/get", "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusServiceUnavailable {
		return fmt.Errorf("public dring not enabled on this daemon")
	}
	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	var r addResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if r.AlreadyTracked {
		fmt.Printf("already tracked\ncid: %s\n", r.CID)
	} else {
		fmt.Printf("fetched\ncid:  %s\nname: %s\n", r.CID, r.Name)
	}
	return nil
}

func cmdGetPrivate(api, group, cidStr, name string) error {
	reqBody := map[string]string{"cid": cidStr}
	if name != "" {
		reqBody["name"] = name
	}
	body, _ := json.Marshal(reqBody)
	resp, err := http.Post(api+"/ring/"+group+"/fetch", "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("ring %q not found (use 'drings ring list' to see active rings)", group)
	}
	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	var r addResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if r.AlreadyTracked {
		fmt.Printf("already tracked\ncid: %s\n", r.CID)
	} else {
		fmt.Printf("fetched\ncid:  %s\nname: %s\n", r.CID, r.Name)
	}
	return nil
}

func cmdPub(api string, args []string) error {

	var ttl string
	var positional []string
	for i := 0; i < len(args); i++ {
		if args[i] == "--ttl" && i+1 < len(args) {
			ttl = args[i+1]
			i++
		} else {
			positional = append(positional, args[i])
		}
	}

	switch len(positional) {
	case 1:
		return cmdPubPublic(api, positional[0])
	case 2:
		if looksLikeCID(positional[0]) {
			return cmdPubPublic(api, positional[0])
		}
		return cmdPubPrivate(api, positional[0], positional[1], ttl)
	default:
		return fmt.Errorf("usage: drings pub [<group>] <cid> [--ttl <duration>]")
	}
}

func cmdPubPublic(api, cidStr string) error {
	body, _ := json.Marshal(map[string]string{"cid": cidStr})
	resp, err := http.Post(api+"/public/provider/publish", "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusServiceUnavailable {
		return fmt.Errorf("public dring not enabled on this daemon")
	}
	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	fmt.Printf("published as provider for %s on public dring\n", cidStr)
	return nil
}

func cmdPubPrivate(api, group, cidStr string, ttl string) error {
	payload := map[string]string{"cid": cidStr}
	if ttl != "" {
		payload["ttl"] = ttl
	}
	body, _ := json.Marshal(payload)
	resp, err := http.Post(api+"/ring/"+group+"/push", "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("ring %q not found (use 'drings ring list' to see active rings)", group)
	}
	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	if ttl != "" {
		fmt.Printf("pushed %s into ring %s (ttl=%s)\n", cidStr, group, ttl)
	} else {
		fmt.Printf("pushed %s into ring %s\n", cidStr, group)
	}
	return nil
}

type rootJSON struct {
	ID	string		`json:"id"`
	Name	string		`json:"name"`
	CID	string		`json:"cid"`
	Path	string		`json:"path"`
	AddedAt	time.Time	`json:"added_at"`
}

func cmdList(api string) error {
	resp, err := http.Get(api + "/roots")
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return readError(resp)
	}

	var roots []rootJSON
	if err := json.NewDecoder(resp.Body).Decode(&roots); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	if len(roots) == 0 {
		fmt.Println("no roots tracked")
		return nil
	}

	nameW := len("NAME")
	cidW := len("CID")
	for _, r := range roots {
		if len(r.Name) > nameW {
			nameW = len(r.Name)
		}
		if len(r.CID) > cidW {
			cidW = len(r.CID)
		}
	}

	sep := strings.Repeat("-", nameW+2) + "+" + strings.Repeat("-", cidW+2) + "+" + strings.Repeat("-", 22)
	fmt.Printf(" %-*s | %-*s | %s\n", nameW, "NAME", cidW, "CID", "ADDED AT")
	fmt.Println(sep)
	for _, r := range roots {
		fmt.Printf(" %-*s | %-*s | %s\n",
			nameW, r.Name,
			cidW, r.CID,
			r.AddedAt.Local().Format(time.DateTime),
		)
	}
	return nil
}

type gcResponse struct {
	Removed	int	`json:"removed"`
	Kept	int	`json:"kept"`
	Elapsed	int64	`json:"elapsed"`
}

func cmdGC(api string) error {
	resp, err := http.Post(api+"/gc", "application/json", nil)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return readError(resp)
	}

	var r gcResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	fmt.Printf("gc done\nremoved: %d blocks\nkept:    %d blocks\nelapsed: %s\n",
		r.Removed, r.Kept, time.Duration(r.Elapsed))
	return nil
}

func cmdDeleteCID(api string, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: drings delete-cid <group> <cid>")
	}
	group, cidStr := args[0], args[1]

	req, _ := http.NewRequest(http.MethodDelete, api+"/ring/"+group+"/cid/"+url.PathEscape(cidStr), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		fmt.Printf("deleted CID %s from ring %s\n", cidStr, group)
	case http.StatusNotFound:
		return fmt.Errorf("ring %q not found", group)
	default:
		return readError(resp)
	}
	return nil
}

func cmdRemove(api string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: drings remove <cid|name>")
	}
	ref := args[0]

	req, _ := http.NewRequest(http.MethodDelete, api+"/roots/"+url.PathEscape(ref), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		fmt.Printf("removed %s\n", ref)
	case http.StatusNotFound:
		return fmt.Errorf("%s not found", ref)
	default:
		return readError(resp)
	}
	return nil
}

const defaultMountPoint = "/mnt/datarings"

func cmdSetup(args []string) error {
	mountPoint := defaultMountPoint
	if len(args) > 0 {
		mountPoint = args[0]
	}

	if os.Getuid() != 0 {
		self, err := os.Executable()
		if err != nil {
			return fmt.Errorf("resolve executable: %w", err)
		}
		cmd := exec.Command("sudo", append([]string{self}, os.Args[1:]...)...)
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd.Run()
	}

	u, err := invokeUser()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(mountPoint, 0755); err != nil {
		return fmt.Errorf("create %s: %w", mountPoint, err)
	}

	uid, _ := strconv.Atoi(u.Uid)
	gid, _ := strconv.Atoi(u.Gid)
	if err := os.Chown(mountPoint, uid, gid); err != nil {
		return fmt.Errorf("chown %s: %w", mountPoint, err)
	}

	fmt.Printf("mount point ready: %s\n", mountPoint)
	return nil
}

func invokeUser() (*user.User, error) {
	if name := os.Getenv("SUDO_USER"); name != "" {
		u, err := user.Lookup(name)
		if err != nil {
			return nil, fmt.Errorf("lookup sudo user: %w", err)
		}
		return u, nil
	}
	u, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("get current user: %w", err)
	}
	return u, nil
}

func cmdConfig(configPath string, args []string) error {
	if len(args) > 0 && args[0] == "show" {
		data, err := os.ReadFile(configPath)
		if os.IsNotExist(err) {
			return fmt.Errorf("no config file at %s (run drings-daemon once to generate it)", configPath)
		}
		if err != nil {
			return err
		}
		fmt.Print(string(data))
		return nil
	}

	if len(args) > 0 {
		return fmt.Errorf("usage: drings config [show]")
	}

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return fmt.Errorf("no config file at %s (run drings-daemon once to generate it)", configPath)
	}

	editor := os.Getenv("EDITOR")
	if editor == "" {
		editor = os.Getenv("VISUAL")
	}
	if editor == "" {

		data, err := os.ReadFile(configPath)
		if err != nil {
			return err
		}
		fmt.Printf("# %s (set $EDITOR to edit)\n", configPath)
		fmt.Print(string(data))
		return nil
	}

	cmd := exec.Command(editor, configPath)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func cmdKey(vaultPath string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: drings key <add|remove|list>")
	}
	sub := args[0]
	subArgs := args[1:]

	switch sub {
	case "add":
		return cmdKeyAdd(vaultPath, subArgs)
	case "remove", "rm":
		return cmdKeyRemove(vaultPath, subArgs)
	case "list", "ls":
		return cmdKeyList(vaultPath)
	default:
		return fmt.Errorf("unknown key sub-command: %s", sub)
	}
}

func cmdKeyAdd(vaultPath string, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: drings key add <alias> <hex-key>")
	}
	alias, keyHex := args[0], args[1]

	grp, err := dht.GroupIdentityFromHex(keyHex)
	if err != nil {
		return fmt.Errorf("invalid group key: %w", err)
	}

	v, err := loadVault(vaultPath)
	if err != nil {
		return err
	}
	if err := v.Add(alias, keyHex); err != nil {
		return err
	}

	fmt.Printf("registered\nalias:    %s\ngroup-id: %s\n", alias, grp.GroupID)
	return nil
}

func cmdKeyRemove(vaultPath string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: drings key remove <alias>")
	}
	alias := args[0]

	v, err := loadVault(vaultPath)
	if err != nil {
		return err
	}
	removed, err := v.Remove(alias)
	if err != nil {
		return err
	}
	if !removed {
		return fmt.Errorf("alias %q not found in vault", alias)
	}
	fmt.Printf("removed %s from vault\n", alias)
	return nil
}

func cmdKeyList(vaultPath string) error {
	v, err := loadVault(vaultPath)
	if err != nil {
		return err
	}
	entries := v.List()
	if len(entries) == 0 {
		fmt.Println("no keys registered (use 'drings key add <alias> <key>')")
		return nil
	}

	aliasW := len("ALIAS")
	for _, e := range entries {
		if len(e.Alias) > aliasW {
			aliasW = len(e.Alias)
		}
	}

	fmt.Printf("%-*s  %s\n", aliasW, "ALIAS", "GROUP-ID")
	fmt.Println(strings.Repeat("-", aliasW+2+42))
	for _, e := range entries {
		fmt.Printf("%-*s  %s\n", aliasW, e.Alias, e.GroupID)
	}
	return nil
}

func cmdRing(api, vaultPath string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: drings ring <create|join|list|leave|quota>")
	}

	sub := args[0]
	subArgs := args[1:]

	switch sub {
	case "create":
		return cmdRingCreate(vaultPath, subArgs)
	case "join":
		return cmdRingJoin(api, vaultPath, subArgs)
	case "list":
		return cmdRingList(api)
	case "leave":
		return cmdRingLeave(api, subArgs)
	case "quota":
		return cmdRingQuota(api, subArgs)
	default:
		return fmt.Errorf("unknown ring sub-command: %s", sub)
	}
}

func parseStorageSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		return 0, nil
	}
	upper := strings.ToUpper(s)
	mult := int64(1)
	for _, suffix := range []struct {
		s	string
		m	int64
	}{
		{"TB", 1 << 40}, {"GB", 1 << 30}, {"MB", 1 << 20}, {"KB", 1 << 10},
		{"T", 1 << 40}, {"G", 1 << 30}, {"M", 1 << 20}, {"K", 1 << 10},
		{"B", 1},
	} {
		if strings.HasSuffix(upper, suffix.s) {
			mult = suffix.m
			upper = strings.TrimSuffix(upper, suffix.s)
			break
		}
	}
	upper = strings.TrimSpace(upper)
	var n int64
	if _, err := fmt.Sscanf(upper, "%d", &n); err != nil {
		return 0, fmt.Errorf("invalid size %q: %w", s, err)
	}
	if n < 0 {
		return 0, fmt.Errorf("size must be non-negative")
	}
	return n * mult, nil
}

func formatStorageSize(b int64) string {
	if b == 0 {
		return "0"
	}
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f%cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func cmdRingQuota(api string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: drings ring quota <group> [size]")
	}
	group := args[0]

	if len(args) == 1 {

		resp, err := http.Get(api + "/ring/" + group + "/quota")
		if err != nil {
			return fmt.Errorf("request failed (is the daemon running?): %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			return fmt.Errorf("ring %q not found (use 'drings ring list' to see active rings)", group)
		}
		if resp.StatusCode >= 300 {
			return readError(resp)
		}
		var info struct {
			MaxBytes	int64	`json:"max_bytes"`
			UsedBytes	int64	`json:"used_bytes"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
		if info.MaxBytes == 0 {
			fmt.Printf("ring:  %s\nquota: unlimited (per-ring)\nused:  %s\n", group, formatStorageSize(info.UsedBytes))
		} else {
			fmt.Printf("ring:  %s\nquota: %s\nused:  %s\n", group, formatStorageSize(info.MaxBytes), formatStorageSize(info.UsedBytes))
		}
		return nil
	}

	max, err := parseStorageSize(args[1])
	if err != nil {
		return err
	}
	body, _ := json.Marshal(map[string]int64{"max_bytes": max})
	req, _ := http.NewRequest(http.MethodPut, api+"/ring/"+group+"/quota", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed (is the daemon running?): %w", err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNoContent:
		if max == 0 {
			fmt.Printf("set ring %s quota to unlimited\n", group)
		} else {
			fmt.Printf("set ring %s quota to %s\n", group, formatStorageSize(max))
		}
		return nil
	case http.StatusNotFound:
		return fmt.Errorf("ring %q not found", group)
	case http.StatusConflict:
		return readError(resp)
	default:
		return readError(resp)
	}
}

func cmdRingCreate(vaultPath string, args []string) error {
	grp, err := dht.GenerateGroupIdentity()
	if err != nil {
		return fmt.Errorf("generate group identity: %w", err)
	}
	fmt.Printf("group-id:  %s\n", grp.GroupID)
	fmt.Printf("group-key: %s\n", grp.PrivKeyHex())
	fmt.Println()

	if len(args) >= 1 {
		alias := args[0]
		v, err := loadVault(vaultPath)
		if err != nil {
			return err
		}
		if err := v.Add(alias, grp.PrivKeyHex()); err != nil {
			return err
		}
		fmt.Printf("registered in vault\nalias: %s\n", alias)
		fmt.Println()
		fmt.Printf("Join the ring:  drings ring join %s\n", alias)
		fmt.Println("Share the group-key out-of-band with peers who should join.")
		return nil
	}

	fmt.Println("Register the key with an alias:  drings key add <alias> <group-key>")
	fmt.Println("Then join the ring:               drings ring join <alias>")
	fmt.Println("Share the group-key out-of-band with peers who should join.")
	return nil
}

func cmdRingJoin(api, vaultPath string, args []string) error {

	var storageMaxBytes int64
	var listenAddr string
	var positional []string
	for _, a := range args {
		if strings.HasPrefix(a, "--storage-max=") {
			s := strings.TrimPrefix(a, "--storage-max=")
			n, err := parseStorageSize(s)
			if err != nil {
				return fmt.Errorf("--storage-max: %w", err)
			}
			storageMaxBytes = n
			continue
		}
		if strings.HasPrefix(a, "--listen-addr=") {
			listenAddr = strings.TrimPrefix(a, "--listen-addr=")
			continue
		}
		positional = append(positional, a)
	}
	if len(positional) == 0 {
		return fmt.Errorf("usage: drings ring join <alias-or-key> [name] [--storage-max=<size>] [--listen-addr=<multiaddr>]")
	}

	v, err := loadVault(vaultPath)
	if err != nil {
		return err
	}

	keyHex, alias, err := dht.ResolveGroupKey(v, positional[0])
	if err != nil {
		return err
	}

	if alias == "" && len(positional) >= 2 {
		alias = positional[1]
		if regErr := v.Add(alias, keyHex); regErr != nil {
			fmt.Fprintf(os.Stderr, "warning: could not register key in vault: %v\n", regErr)
		} else {
			grp, _ := dht.GroupIdentityFromHex(keyHex)
			fmt.Printf("registered in vault\nalias: %s\ngroup-id: %s\n\n", alias, grp.GroupID)
		}
	}

	ringName := alias

	body, _ := json.Marshal(struct {
		Key		string	`json:"key"`
		Name		string	`json:"name"`
		ListenAddr	string	`json:"listen_addr,omitempty"`
		StorageMaxBytes	int64	`json:"storage_max_bytes,omitempty"`
	}{Key: keyHex, Name: ringName, ListenAddr: listenAddr, StorageMaxBytes: storageMaxBytes})
	resp, err := http.Post(api+"/ring/join", "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("request failed (is the daemon running?): %w", err)
	}
	defer resp.Body.Close()

	grp, _ := dht.GroupIdentityFromHex(keyHex)
	if resp.StatusCode == http.StatusConflict {
		return fmt.Errorf("already joined ring %s", grp.GroupID)
	}
	if resp.StatusCode == http.StatusServiceUnavailable {
		return fmt.Errorf("public dring not enabled on this daemon (required for private ring discovery)")
	}
	if resp.StatusCode >= 300 {
		return readError(resp)
	}

	var info struct {
		GroupID		string	`json:"group_id"`
		Name		string	`json:"name"`
		ListenAddr	string	`json:"listen_addr"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	fmt.Printf("joined\ngroup-id: %s\n", info.GroupID)
	if info.Name != "" {
		fmt.Printf("name:     %s\n", info.Name)
	}
	fmt.Printf("listen:   %s\n", info.ListenAddr)
	return nil
}

func cmdRingList(api string) error {
	resp, err := http.Get(api + "/ring/")
	if err != nil {
		return fmt.Errorf("request failed (is the daemon running?): %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return readError(resp)
	}

	var rings []struct {
		GroupID		string	`json:"group_id"`
		Name		string	`json:"name"`
		ListenAddr	string	`json:"listen_addr"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rings); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	if len(rings) == 0 {
		fmt.Println("no active private rings")
		return nil
	}

	const nameW = 16
	fmt.Printf("%-42s  %-*s  %s\n", "GROUP-ID", nameW, "NAME", "LISTEN ADDR")
	fmt.Println(strings.Repeat("-", 42+2+nameW+2+30))
	for _, r := range rings {
		fmt.Printf("%-42s  %-*s  %s\n", r.GroupID, nameW, r.Name, r.ListenAddr)
	}
	return nil
}

func cmdRingLeave(api string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: drings ring leave <group>")
	}
	group := args[0]

	req, _ := http.NewRequest(http.MethodDelete, api+"/ring/"+group, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed (is the daemon running?): %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		fmt.Printf("left ring %s\n", group)
	case http.StatusNotFound:
		return fmt.Errorf("ring %q not found (use 'drings ring list' to see active rings)", group)
	default:
		return readError(resp)
	}
	return nil
}

func cmdPublishLowLevel(api string, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: drings publish <public | group> <cid>")
	}
	target, cidStr := args[0], args[1]
	if target == "public" {
		return cmdPubPublic(api, cidStr)
	}
	return cmdPubPrivate(api, target, cidStr, "")
}

func cmdJoin(api string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: drings join <peer-multiaddr>")
	}
	body, _ := json.Marshal(map[string]string{"peer": args[0]})
	resp, err := http.Post(api+"/dht/join", "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusServiceUnavailable {
		return fmt.Errorf("DHT not enabled on this daemon")
	}
	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	fmt.Printf("joined ring via %s\n", args[0])
	return nil
}

func cmdDownload(api string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: drings download <cid> [name]")
	}
	req := map[string]string{"cid": args[0]}
	if len(args) > 1 {
		req["name"] = args[1]
	}
	body, _ := json.Marshal(req)
	resp, err := http.Post(api+"/dht/download", "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusServiceUnavailable {
		return fmt.Errorf("DHT not enabled on this daemon")
	}
	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	var r addResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if r.AlreadyTracked {
		fmt.Printf("already tracked\ncid: %s\n", r.CID)
	} else {
		fmt.Printf("downloaded\ncid:  %s\nname: %s\n", r.CID, r.Name)
	}
	return nil
}

func cmdPeer(api string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: drings peer <publish|lookup>")
	}
	switch args[0] {
	case "publish":
		resp, err := http.Post(api+"/public/peer/publish", "application/json", nil)
		if err != nil {
			return fmt.Errorf("request failed: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusServiceUnavailable {
			return fmt.Errorf("public dring not enabled on this daemon")
		}
		if resp.StatusCode >= 300 {
			return readError(resp)
		}
		fmt.Println("published peer identity record")
		return nil
	case "lookup":
		if len(args) < 2 {
			return fmt.Errorf("usage: drings peer lookup <peer-id-hex>")
		}
		resp, err := http.Get(api + "/public/peer/" + args[1])
		if err != nil {
			return fmt.Errorf("request failed: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			return fmt.Errorf("peer %s not found", args[1])
		}
		if resp.StatusCode >= 300 {
			return readError(resp)
		}
		body, _ := io.ReadAll(resp.Body)
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, body, "", "  "); err != nil {
			fmt.Println(string(body))
		} else {
			fmt.Println(pretty.String())
		}
		return nil
	default:
		return fmt.Errorf("unknown peer sub-command: %s", args[0])
	}
}

func cmdProvider(api string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: drings provider <publish|find>")
	}
	switch args[0] {
	case "publish":
		if len(args) < 2 {
			return fmt.Errorf("usage: drings provider publish <cid>")
		}
		return cmdPubPublic(api, args[1])
	case "find":
		if len(args) < 2 {
			return fmt.Errorf("usage: drings provider find <cid>")
		}
		resp, err := http.Get(api + "/public/provider/" + args[1])
		if err != nil {
			return fmt.Errorf("request failed: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			return fmt.Errorf("no providers found for %s", args[1])
		}
		if resp.StatusCode >= 300 {
			return readError(resp)
		}
		body, _ := io.ReadAll(resp.Body)
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, body, "", "  "); err != nil {
			fmt.Println(string(body))
		} else {
			fmt.Println(pretty.String())
		}
		return nil
	default:
		return fmt.Errorf("unknown provider sub-command: %s", args[0])
	}
}

func looksLikeCID(s string) bool {
	return strings.HasPrefix(s, "bafy") ||
		strings.HasPrefix(s, "bafk") ||
		strings.HasPrefix(s, "Qm") ||
		(strings.HasPrefix(s, "b") && len(s) > 40)
}
