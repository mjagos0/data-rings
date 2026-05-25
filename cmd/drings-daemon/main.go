package main

import (
	"context"
	_ "embed"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/felixge/fgprof"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/mjagos0/datarings/api"
	"github.com/mjagos0/datarings/dht"
	"github.com/mjagos0/datarings/eventlog"
	"github.com/mjagos0/datarings/fuse"
	"github.com/mjagos0/datarings/metrics"
	"github.com/mjagos0/datarings/store"
)

//go:embed config.toml
var defaultConfig []byte

func main() {
	home, _ := os.UserHomeDir()

	dataDir := flag.String("data-dir", filepath.Join(home, ".datarings"), "data directory")
	apiAddr := flag.String("api-addr", ":7423", "API address")
	doMount := flag.Bool("mount", true, "mount filesystem")
	mountPath := flag.String("mount-path", "/mnt/datarings", "mount point")
	debug := flag.Bool("debug", false, "debug logging")
	dhtAddrFlag := flag.String("dht-addr", "/ip4/0.0.0.0/tcp/7000", "listen address")
	advertiseFlag := flag.String("advertise", "", "advertise IP")
	bootstrapFlag := flag.String("bootstrap", "", "bootstrap peer")
	dhtReplication := flag.Int("dht-replication", 3, "replica count")
	dhtStabilize := flag.Duration("dht-stabilize", 500*time.Millisecond, "stabilization interval")
	dhtFixFingers := flag.Duration("dht-fix-fingers", 300*time.Millisecond, "fix-fingers interval")
	dhtCheckPred := flag.Duration("dht-check-pred", time.Second, "check-predecessor interval")
	metricsAddr := flag.String("metrics-addr", "", "metrics address")
	gcInterval := flag.Duration("gc-interval", 0, "GC interval (0 disables)")
	storageMaxFlag := flag.String("storage-max", "0", "storage cap (0 = unlimited)")
	noPublish := flag.Bool("no-publish", false, "skip publish on startup")
	noBgStabilize := flag.Bool("no-background-stabilize", false, "pause stabilization")
	noConfig := flag.Bool("no-config", false, "ignore configuration")
	experimentID := flag.String("experiment", "", "experiment id")
	experimentNode := flag.String("experiment-node", "", "experiment node label")
	flag.Parse()

	logLevelVar := &slog.LevelVar{}
	logLevelVar.Set(slog.LevelInfo)
	if *debug {
		logLevelVar.Set(slog.LevelDebug)
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevelVar,
	})))

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		slog.Error("create data dir", "error", err)
		os.Exit(1)
	}

	var appCfg *dht.AppConfig
	if *noConfig {
		appCfg = &dht.AppConfig{}
		slog.Info("config.toml skipped (--no-config)")
	} else {
		configPath := filepath.Join(*dataDir, "config.toml")
		if _, err := os.Stat(configPath); os.IsNotExist(err) {
			_ = os.WriteFile(configPath, defaultConfig, 0644)
		}
		var err error
		appCfg, err = dht.LoadAppConfig(configPath)
		if err != nil {
			slog.Warn("load config, using defaults", "error", err)
			appCfg = &dht.AppConfig{}
		}
	}

	dhtAddr := *dhtAddrFlag
	if appCfg.ListenAddr != "" && !isFlagSet("dht-addr") {
		dhtAddr = appCfg.ListenAddr
	}

	if appCfg.Replication > 0 && !isFlagSet("dht-replication") {
		*dhtReplication = appCfg.Replication
	}

	if appCfg.StorageMax != "" && !isFlagSet("storage-max") {
		*storageMaxFlag = appCfg.StorageMax
	}

	if appCfg.GCInterval != "" && !isFlagSet("gc-interval") {
		if d, err2 := time.ParseDuration(appCfg.GCInterval); err2 == nil {
			*gcInterval = d
		}
	}

	if appCfg.MountPath != "" && !isFlagSet("mount-path") {
		*mountPath = appCfg.MountPath
	}

	var bootstrapPeers []string
	if *bootstrapFlag == "none" {
		bootstrapPeers = nil
	} else if *bootstrapFlag != "" {
		bootstrapPeers = []string{*bootstrapFlag}
	} else if len(appCfg.BootstrapPeers) > 0 {
		bootstrapPeers = appCfg.BootstrapPeers
	} else if *noConfig {
		bootstrapPeers = nil
	} else {
		bootstrapPeers = dht.DefaultBootstrapPeers
	}

	storageMax, err := parseByteSize(*storageMaxFlag)
	if err != nil {
		slog.Error("invalid storage-max", "value", *storageMaxFlag, "error", err)
		os.Exit(1)
	}

	st, err := store.Open(*dataDir, storageMax)
	if err != nil {
		slog.Error("open store", "error", err)
		os.Exit(1)
	}
	defer st.Close()
	if storageMax > 0 {
		slog.Info("store opened", "path", *dataDir, "roots", len(st.Roots.List()), "storage_max", storageMax)
	} else {
		slog.Info("store opened", "path", *dataDir, "roots", len(st.Roots.List()))
	}

	daemonStart := time.Now()

	if *experimentID != "" {
		nodeName := *experimentNode
		if nodeName == "" {
			if h, err := os.Hostname(); err == nil {
				nodeName = h
			}
		}
		path := filepath.Join(*dataDir, "experiments", *experimentID+".ndjson")
		el, err := eventlog.New(path, nodeName)
		if err != nil {
			slog.Error("eventlog open", "path", path, "error", err)
			os.Exit(1)
		}
		eventlog.SetDefault(el)
		defer el.Close()
		slog.Info("experiment eventlog enabled", "id", *experimentID, "node", nodeName, "path", path)
	}

	var met *metrics.Registry
	var metSrv *http.Server
	if *metricsAddr != "" {
		met = metrics.New()
		metMux := http.NewServeMux()
		metMux.Handle("/metrics", promhttp.HandlerFor(met.Prometheus, promhttp.HandlerOpts{}))

		metMux.HandleFunc("GET /clock-probe", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			fmt.Fprintf(w, "%d\n", time.Now().UnixNano())
		})

		runtime.SetMutexProfileFraction(100)
		runtime.SetBlockProfileRate(1_000_000)
		metMux.Handle("/debug/pprof/", http.DefaultServeMux)

		metMux.Handle("/debug/fgprof", fgprof.Handler())

		metMux.HandleFunc("/debug/setblockrate", func(w http.ResponseWriter, r *http.Request) {
			ns, err := strconv.Atoi(r.URL.Query().Get("ns"))
			if err != nil || ns < 0 {
				http.Error(w, "ns query param required (>=0)", http.StatusBadRequest)
				return
			}
			runtime.SetBlockProfileRate(ns)
			slog.Info("block profile rate changed", "rate_ns", ns)
			fmt.Fprintf(w, "block profile rate set to %d ns\n", ns)
		})
		metSrv = &http.Server{Addr: *metricsAddr, Handler: metMux}
		metLn, err := dht.ListenReuse("tcp", *metricsAddr)
		if err != nil {
			slog.Error("metrics listen", "addr", *metricsAddr, "error", err)
			os.Exit(1)
		}
		go func() {
			slog.Info("metrics server listening", "addr", *metricsAddr)
			if err := metSrv.Serve(metLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("metrics server", "error", err)
			}
		}()
		slog.Info("metrics enabled", "addr", *metricsAddr)
		st.SetMetrics(met)
	}

	var publicDring *dht.PublicDring
	var dhtForAPI api.DHTNode
	var privManager *privateDringsManager

	if dhtAddr != "" {
		cfg := dht.Config{
			ListenAddr:		dhtAddr,
			Replication:		*dhtReplication,
			StabilizeInterval:	*dhtStabilize,
			FixFingersInterval:	*dhtFixFingers,
			CheckPredInterval:	*dhtCheckPred,
		}

		identPath := filepath.Join(*dataDir, "identity")
		ident, err := dht.LoadOrCreateIdentity(identPath)
		if err != nil {
			slog.Error("load or create identity", "error", err)
			os.Exit(1)
		}
		slog.Info("identity loaded", "peer_id", ident.ID)

		tr := dht.NewRPCTransport(5 * time.Second)
		if *metricsAddr != "" {
			tr.SetMetrics(met, "public")
		}
		dhtNode := dht.NewNode(ident.ID, "", st.NetworkBlocks, st.DAG, tr, cfg)
		dhtNode.SetNetworkBlocks(st.NetworkBlocks)
		dhtNode.SetLocalBlocks(st.LocalBlocks)
		if *metricsAddr != "" {
			dhtNode.SetMetrics(met, "public")
		}

		advertiseIP := *advertiseFlag
		if advertiseIP != "" && isWildcardIP(advertiseIP) {
			slog.Warn("--advertise is a wildcard address; ignoring and auto-detecting", "given", advertiseIP)
			advertiseIP = ""
		}
		if advertiseIP == "" && len(bootstrapPeers) > 0 {

			bootstrapIsLoopback := false
			for _, peer := range bootstrapPeers {
				if tcpAddr, err2 := dht.MultiaddrToTCPAddr(peer); err2 == nil {
					if host, _, splitErr := net.SplitHostPort(tcpAddr); splitErr == nil {
						if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
							bootstrapIsLoopback = true
							break
						}
					}
				}
			}

			for _, peer := range bootstrapPeers {
				if tcpAddr, err2 := dht.MultiaddrToTCPAddr(peer); err2 == nil {
					if conn, err2 := net.DialTimeout("tcp", tcpAddr, 3*time.Second); err2 == nil {
						host, _, _ := net.SplitHostPort(conn.LocalAddr().String())
						conn.Close()
						if host != "" && host != "0.0.0.0" && host != "::" {
							advertiseIP = host
							break
						}
					}
				}
			}
			if !bootstrapIsLoopback && (advertiseIP == "" || isPrivateIP(advertiseIP)) {
				if pub := detectPublicIP(); pub != "" {
					advertiseIP = pub
				}
			}
			if advertiseIP != "" {
				slog.Info("auto-detected advertise IP", "ip", advertiseIP)
			}
		}

		if advertiseIP == "" || isWildcardIP(advertiseIP) {
			slog.Warn("could not determine non-wildcard advertise IP; defaulting to 127.0.0.1 — set --advertise for cross-host deployments", "had", advertiseIP)
			advertiseIP = "127.0.0.1"
		}

		srv, boundAddr, err := dht.StartServer(dhtAddr, advertiseIP, dhtNode)
		if err != nil {
			slog.Error("start public dring server", "error", err)
			os.Exit(1)
		}
		defer srv.Stop()

		if ss := st.StorageStatus(); ss != nil {
			dhtNode.SetStorageStatus(func() (int64, int64) {
				s := st.StorageStatus()
				return s.UsedBytes, s.MaxBytes
			})
		}

		dhtNode.SetAddrPublic(boundAddr)
		slog.Info("public dring node started", "peer_id", ident.ID, "addr", boundAddr, "replication", *dhtReplication)

		joined := false
		for _, peer := range bootstrapPeers {
			if err := dhtNode.JoinPeer(context.Background(), peer); err != nil {
				slog.Debug("bootstrap peer unreachable", "peer", peer, "error", err)
				continue
			}
			slog.Info("joined public dring", "via", peer)
			joined = true
			break
		}
		if !joined {
			dhtNode.Create()
			slog.Info("no bootstrap peers reachable, started fresh public ring", "addr", boundAddr)
		}

		dhtNode.StartBackground(cfg)
		if *noBgStabilize {
			dhtNode.PauseBackground()
			slog.Info("background stabilization paused (--no-background-stabilize)")
		}
		defer dhtNode.Stop()

		publicDring = dht.NewPublicDring(dhtNode, ident)
		if *metricsAddr != "" {
			publicDring.SetMetrics(met)
		}
		dhtForAPI = dhtNode

		if !*noPublish {
			if err := publicDring.PublishSelf(context.Background()); err != nil {
				slog.Warn("publish self to public dring", "error", err)
			} else {
				slog.Debug("published self to public dring", "peer_id", ident.ID)
			}
		} else {
			slog.Info("skipping PeerIdentityRecord publish (--no-publish)", "peer_id", ident.ID)
		}

		publicDring.StartRepublishLoop(dhtNode.StopCh(), cfg)

		groupsPath := filepath.Join(*dataDir, "groups.json")

		privManager = &privateDringsManager{
			rings:		make(map[string]*dht.PrivateDring),
			publicDring:	publicDring,
			ident:		ident,
			st:		st,
			groupsPath:	groupsPath,
			met:		met,
			advertiseIP:	advertiseIP,
			dataDir:	*dataDir,
			noBgStabilize:	*noBgStabilize,
			cfg: dht.Config{
				Replication:		*dhtReplication,
				StabilizeInterval:	*dhtStabilize,
				FixFingersInterval:	*dhtFixFingers,
				CheckPredInterval:	*dhtCheckPred,
			},
		}

		groupsFile, err := dht.LoadGroupsFile(groupsPath)
		if err != nil {
			slog.Warn("load groups file, using empty", "error", err)
			groupsFile = &dht.GroupsFile{}
		}

		for _, gcfg := range groupsFile.Groups {
			info, err := privManager.startRing(context.Background(), gcfg.GroupPrivKeyHex, gcfg.ListenAddr, gcfg.Name, gcfg.StorageMaxBytes)
			if err != nil {
				slog.Warn("start private dring", "error", err)
				continue
			}
			slog.Info("private dring active", "group_id", info.GroupID, "name", info.Name, "addr", info.ListenAddr, "storage_max", gcfg.StorageMaxBytes)
		}

	}

	mux := api.NewWithDHT(st.Roots, st.DAG, dhtForAPI).Handler()
	if publicDring != nil {
		mux = api.AddPublicDringHandlers(mux, publicDring, st.Roots)
	}
	if privManager != nil {
		mux = api.AddPrivateDringHandlers(mux, privManager, st.Roots)
	}
	if dhtForAPI != nil {
		if stater, ok := dhtForAPI.(api.NodeStater); ok {
			mux = api.AddDebugHandlers(mux, stater, privManager, logLevelVar, &api.ShareConcurrencyKnob{
				Get:	dht.DAGShareConcurrency,
				Set:	dht.SetDAGShareConcurrency,
			})
		}
	}
	mux = api.AddStorageHandler(mux, st)
	mux = api.AddNetworkRootsHandler(mux, st.NetworkBlocks)
	mux = api.AddNetworkBlocksHandler(mux, st.NetworkBlocks)
	mux = api.AddCIDUsageHandler(mux, st.NetworkBlocks)
	mux = api.AddRingScopedDebugHandlers(mux, st.NetworkBlocks)
	mux = api.AddReplicationDrainHandler(mux, &allRingsDrainer{
		publicDring:	publicDring,
		privManager:	privManager,
	})

	mux = api.AddGCHandler(mux, &gcWithPrune{
		store:		st,
		publicDring:	publicDring,
		privManager:	privManager,
	})

	apiSrv := &http.Server{
		Addr:		*apiAddr,
		Handler:	mux,
	}
	apiLn, err := dht.ListenReuse("tcp", *apiAddr)
	if err != nil {
		slog.Error("API listen", "addr", *apiAddr, "error", err)
		os.Exit(1)
	}
	go func() {
		slog.Info("API server listening", "addr", *apiAddr)
		if err := apiSrv.Serve(apiLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("API server", "error", err)
			os.Exit(1)
		}
	}()

	if *doMount {
		slog.Info("mounting FUSE filesystem", "path", *mountPath)
		mp, err := fuse.Mount(fuse.MountOptions{
			Mountpoint:	*mountPath,
			Debug:		*debug,
		}, st.Roots, st.DAG)
		if err != nil {
			if errors.Is(err, fuse.ErrMountpointNotExist) {
				slog.Error("mount point does not exist, create it first", "path", *mountPath)
				os.Exit(1)
			}

			slog.Warn("mount FUSE filesystem failed, continuing without mount", "error", err)
		} else {
			defer func() {
				slog.Info("unmounting FUSE filesystem")
				if err := mp.Unmount(); err != nil {
					slog.Warn("unmount", "error", err)
				}
			}()
		}
	}

	if met != nil {

		if dhtForAPI != nil {
			if stater, ok := dhtForAPI.(interface{ State() dht.NodeState }); ok {
				state := stater.State()
				met.NodeInfo.WithLabelValues(state.ID).Set(1)
			}
		}

		go func() {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			for range ticker.C {

				met.NodeUptimeSeconds.Set(time.Since(daemonStart).Seconds())

				for _, entry := range []struct {
					label	string
					sub	string
				}{
					{"local", "local-blocks"},
					{"network", "network-blocks"},
				} {
					var sz int64
					filepath.WalkDir(filepath.Join(*dataDir, entry.sub), func(_ string, d fs.DirEntry, err error) error {
						if err != nil || d.IsDir() {
							return nil
						}
						if info, err := d.Info(); err == nil {
							sz += info.Size()
						}
						return nil
					})
					met.BlockStoreSizeBytes.WithLabelValues(entry.label).Set(float64(sz))
				}

				met.BlockStoreCount.WithLabelValues("network").Set(float64(st.NetworkBlocks.BlockCount()))
				if localCount, err := countLocalBlocks(st); err == nil {
					met.BlockStoreCount.WithLabelValues("local").Set(float64(localCount))
				}

				if dhtForAPI != nil {
					if stater, ok := dhtForAPI.(interface{ State() dht.NodeState }); ok {
						state := stater.State()
						met.PublicRecordCount.Set(float64(state.RecordCount))
						uniqueFingers := make(map[string]struct{})
						for _, f := range state.Fingers {
							uniqueFingers[f.ID.String()] = struct{}{}
						}
						met.PublicFingerCount.Set(float64(len(uniqueFingers)))
					}
				}

				met.LocalRootCount.Set(float64(len(st.Roots.List())))

				met.NetworkRootCount.WithLabelValues("all").Set(float64(st.NetworkBlocks.RootCount()))

				if ss := st.StorageStatus(); ss != nil {
					met.StorageQuotaBytes.Set(float64(ss.MaxBytes))
					if ss.MaxBytes > 0 {
						met.StorageUsedRatio.Set(float64(ss.UsedBytes) / float64(ss.MaxBytes))
					}
				}

				met.CIDStorageBytes.Reset()
				for cidStr, bytes := range st.NetworkBlocks.CIDUsage() {
					met.CIDStorageBytes.WithLabelValues(cidStr).Set(float64(bytes))
				}

				met.RingStorageUsedBytes.Reset()
				met.RingStorageMaxBytes.Reset()
				met.RingBlockCount.Reset()
				met.RingNetworkRootCount.Reset()
				for _, ringID := range st.NetworkBlocks.RingsKnown() {
					rv := st.NetworkBlocks.Ring(ringID)
					met.RingStorageUsedBytes.WithLabelValues(ringID).Set(float64(rv.UsedBytes()))
					met.RingStorageMaxBytes.WithLabelValues(ringID).Set(float64(rv.Quota()))
					met.RingBlockCount.WithLabelValues(ringID).Set(float64(rv.BlockCount()))
					met.RingNetworkRootCount.WithLabelValues(ringID).Set(float64(rv.RootCount()))
				}

				if privManager != nil {
					met.ActivePrivateRings.Reset()
					for _, info := range privManager.ListRings() {
						met.ActivePrivateRings.WithLabelValues(info.GroupID).Set(1)
					}
				}

				if dhtForAPI != nil {
					if stater, ok := dhtForAPI.(interface{ State() dht.NodeState }); ok {
						state := stater.State()
						uniqueSuccessors := make(map[string]struct{})
						for _, s := range state.SuccessorList {
							uniqueSuccessors[s.ID.String()] = struct{}{}
						}
						met.PublicSuccessorCount.Set(float64(len(uniqueSuccessors)))
						if len(uniqueSuccessors) <= 1 {
							slog.Warn("chord: ring appears isolated (successor list has only self)", "node", state.ID)
						}
					}
				}
				if privManager != nil && publicDring != nil {
					ctx := context.Background()
					met.GroupRecordVersion.Reset()
					met.GroupRecordMembers.Reset()
					met.PrivateSuccessorCount.Reset()
					met.PrivateVerifiedPeers.Reset()
					for _, info := range privManager.ListRings() {
						var groupID dht.NodeID
						if b, err := hex.DecodeString(info.GroupID); err == nil && len(b) == 20 {
							copy(groupID[:], b)
							rec, err := publicDring.LookupGroup(ctx, groupID)
							if err == nil {
								met.GroupRecordVersion.WithLabelValues(info.GroupID).Set(float64(rec.Data.Version))
								met.GroupRecordMembers.WithLabelValues(info.GroupID).Set(float64(len(rec.Data.Peers)))
							}
						}
						privManager.mu.RLock()
						ring := privManager.rings[info.GroupID]
						privManager.mu.RUnlock()
						if ring != nil {
							state := ring.Node().State()
							uniqueSucc := make(map[string]struct{})
							for _, s := range state.SuccessorList {
								uniqueSucc[s.ID.String()] = struct{}{}
							}
							met.PrivateSuccessorCount.WithLabelValues(info.GroupID).Set(float64(len(uniqueSucc)))
							if len(uniqueSucc) <= 1 {
								slog.Warn("private ring appears isolated (successor list has only self)", "group_id", info.GroupID, "node", state.ID)
							}
							met.PrivateVerifiedPeers.WithLabelValues(info.GroupID).Set(float64(len(ring.VerifiedPeers())))
						}
					}
				}
			}
		}()
	}

	if *gcInterval > 0 {
		go func() {
			ticker := time.NewTicker(*gcInterval)
			defer ticker.Stop()
			for range ticker.C {
				slog.Info("block-store GC started", "interval", gcInterval)
				result, err := st.GC(context.Background())
				if err != nil {
					slog.Error("block-store GC failed", "error", err)
					continue
				}
				slog.Info("block-store GC done",
					"removed", result.Removed,
					"kept", result.Kept,
					"elapsed", result.Elapsed)
			}
		}()
	}

	slog.Info("daemon ready")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	slog.Info("daemon shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	apiSrv.Shutdown(ctx)
	if metSrv != nil {
		metSrv.Shutdown(ctx)
	}
	if privManager != nil {
		privManager.Stop()
	}
}

func parseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		return 0, nil
	}
	upper := strings.ToUpper(s)
	multiplier := int64(1)
	switch {
	case strings.HasSuffix(upper, "GB"):
		multiplier = 1 << 30
		s = strings.TrimSuffix(upper, "GB")
	case strings.HasSuffix(upper, "MB"):
		multiplier = 1 << 20
		s = strings.TrimSuffix(upper, "MB")
	case strings.HasSuffix(upper, "KB"):
		multiplier = 1 << 10
		s = strings.TrimSuffix(upper, "KB")
	}
	s = strings.TrimSpace(s)
	n, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size %q: %w", s, err)
	}
	if n < 0 {
		return 0, fmt.Errorf("byte size must be non-negative: %s", s)
	}
	return int64(n * float64(multiplier)), nil
}

func countLocalBlocks(st *store.Store) (int, error) {
	ch, err := st.LocalBlocks.AllKeysChan(context.Background())
	if err != nil {
		return 0, err
	}
	var n int
	for range ch {
		n++
	}
	return n, nil
}

func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

func isPrivateIP(ip string) bool {
	addr := net.ParseIP(ip)
	if addr == nil {
		return false
	}
	return addr.IsLoopback() || addr.IsLinkLocalUnicast() || addr.IsPrivate()
}

func isWildcardIP(ip string) bool {
	addr := net.ParseIP(ip)
	return addr != nil && addr.IsUnspecified()
}

func detectPublicIP() string {
	for _, url := range []string{"https://ifconfig.me", "https://icanhazip.com"} {
		client := &http.Client{Timeout: 3 * time.Second}
		resp, err := client.Get(url)
		if err != nil {
			continue
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64))
		resp.Body.Close()
		if resp.StatusCode == 200 {
			ip := strings.TrimSpace(string(body))
			if net.ParseIP(ip) != nil {
				return ip
			}
		}
	}
	return ""
}

func isFlagSet(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}
