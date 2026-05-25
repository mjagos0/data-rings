package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/mjagos0/datarings/store"
)

type Server struct {
	roots	RootAdder
	dagSvc	ipld.DAGService
	dht	DHTNode
}

func New(roots RootAdder, dagSvc ipld.DAGService) *Server {
	return &Server{roots: roots, dagSvc: dagSvc}
}

func NewWithDHT(roots RootAdder, dagSvc ipld.DAGService, dht DHTNode) *Server {
	return &Server{roots: roots, dagSvc: dagSvc, dht: dht}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /add", s.handleAdd)
	mux.HandleFunc("GET /roots", s.handleRoots)
	mux.HandleFunc("DELETE /roots/{id}", s.handleRemove)
	mux.HandleFunc("PATCH /roots/{id}", s.handleRename)
	mux.HandleFunc("POST /dht/join", s.handleDHTJoin)
	mux.HandleFunc("POST /dht/download", s.handleDHTDownload)
	return mux
}

func AddPublicDringHandlers(base http.Handler, pub PublicDHT, roots RootAdder) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /public/peer/publish", func(w http.ResponseWriter, r *http.Request) {
		if err := pub.PublishSelf(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("GET /public/peer/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		data, err := pub.LookupPeerJSON(r.Context(), id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	})

	mux.HandleFunc("GET /public/group/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		data, err := pub.LookupGroupJSON(r.Context(), id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	})

	mux.HandleFunc("POST /public/provider/publish", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CID string `json:"cid"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.CID == "" {
			http.Error(w, `"cid" is required`, http.StatusBadRequest)
			return
		}
		if err := pub.PublishProvider(r.Context(), req.CID); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("GET /public/provider/{cid}", func(w http.ResponseWriter, r *http.Request) {
		cidStr := r.PathValue("cid")
		data, err := pub.FindProvidersJSON(r.Context(), cidStr)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	})

	mux.HandleFunc("POST /dht/get", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CID	string	`json:"cid"`
			Name	string	`json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.CID == "" {
			http.Error(w, `"cid" is required`, http.StatusBadRequest)
			return
		}
		ctx := r.Context()
		if err := pub.FetchDAGFromProvidersStr(ctx, req.CID); err != nil {
			http.Error(w, fmt.Sprintf("fetch: %v", err), http.StatusInternalServerError)
			return
		}
		rootCID, err := cid.Decode(req.CID)
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid CID: %v", err), http.StatusBadRequest)
			return
		}
		name := req.Name
		if name == "" {
			name = rootCID.String()[:12]
		}
		root := store.Root{Name: name, CID: rootCID, AddedAt: time.Now()}
		registered, err := roots.Add(root)
		alreadyTracked := errors.Is(err, store.ErrAlreadyTracked)
		if err != nil && !alreadyTracked {
			http.Error(w, fmt.Sprintf("register root: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(addResponse{
			ID:		registered.ID,
			Name:		registered.Name,
			CID:		registered.CID.String(),
			AddedAt:	registered.AddedAt,
			AlreadyTracked:	alreadyTracked,
		})
	})

	mux.Handle("/", base)
	return mux
}

func AddPrivateDringHandlers(base http.Handler, drings PrivateDrings, roots RootAdder) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /ring/", func(w http.ResponseWriter, r *http.Request) {
		list := drings.ListRings()
		if list == nil {
			list = []PrivateRingInfo{}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(list)
	})

	mux.HandleFunc("POST /ring/{group}/push", func(w http.ResponseWriter, r *http.Request) {
		group := r.PathValue("group")
		var req struct {
			CID	string	`json:"cid"`
			TTL	string	`json:"ttl,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.CID == "" {
			http.Error(w, `"cid" is required`, http.StatusBadRequest)
			return
		}
		var ttl time.Duration
		if req.TTL != "" {
			var err error
			ttl, err = time.ParseDuration(req.TTL)
			if err != nil {
				http.Error(w, fmt.Sprintf("invalid ttl %q: %v", req.TTL, err), http.StatusBadRequest)
				return
			}
		}
		if err := drings.PushDAG(r.Context(), group, req.CID, ttl); err != nil {
			status := http.StatusInternalServerError
			if isGroupNotFound(err) {
				status = http.StatusNotFound
			}
			http.Error(w, err.Error(), status)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("POST /ring/{group}/fetch", func(w http.ResponseWriter, r *http.Request) {
		group := r.PathValue("group")
		var req struct {
			CID	string	`json:"cid"`
			Name	string	`json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.CID == "" {
			http.Error(w, `"cid" is required`, http.StatusBadRequest)
			return
		}

		if err := drings.FetchDAG(r.Context(), group, req.CID); err != nil {
			status := http.StatusInternalServerError
			if isGroupNotFound(err) {
				status = http.StatusNotFound
			}
			http.Error(w, err.Error(), status)
			return
		}

		rootCID, err := cid.Decode(req.CID)
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid CID: %v", err), http.StatusBadRequest)
			return
		}
		name := req.Name
		if name == "" {
			name = rootCID.String()[:12]
		}
		root := store.Root{Name: name, CID: rootCID, AddedAt: time.Now()}
		registered, err := roots.Add(root)
		alreadyTracked := errors.Is(err, store.ErrAlreadyTracked)
		if err != nil && !alreadyTracked {
			http.Error(w, fmt.Sprintf("register root: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(addResponse{
			ID:		registered.ID,
			Name:		registered.Name,
			CID:		registered.CID.String(),
			AddedAt:	registered.AddedAt,
			AlreadyTracked:	alreadyTracked,
		})
	})

	mux.HandleFunc("POST /ring/join", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Key		string	`json:"key"`
			Name		string	`json:"name"`
			ListenAddr	string	`json:"listen_addr,omitempty"`
			StorageMaxBytes	int64	`json:"storage_max_bytes,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key == "" {
			http.Error(w, `"key" (hex GroupPrivKey) is required`, http.StatusBadRequest)
			return
		}
		info, err := drings.JoinRing(r.Context(), req.Key, req.ListenAddr, req.Name, req.StorageMaxBytes)
		if err != nil {
			status := http.StatusInternalServerError
			if strings.Contains(err.Error(), "already joined") {
				status = http.StatusConflict
			} else if strings.Contains(err.Error(), "invalid") {
				status = http.StatusBadRequest
			}
			http.Error(w, err.Error(), status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(info)
	})

	mux.HandleFunc("GET /ring/{group}/quota", func(w http.ResponseWriter, r *http.Request) {
		group := r.PathValue("group")
		max, used, err := drings.RingQuota(group)
		if err != nil {
			status := http.StatusInternalServerError
			if isGroupNotFound(err) {
				status = http.StatusNotFound
			}
			http.Error(w, err.Error(), status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]int64{
			"max_bytes":	max,
			"used_bytes":	used,
		})
	})
	mux.HandleFunc("PUT /ring/{group}/quota", func(w http.ResponseWriter, r *http.Request) {
		group := r.PathValue("group")
		var req struct {
			MaxBytes int64 `json:"max_bytes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		if err := drings.SetRingQuota(group, req.MaxBytes); err != nil {
			status := http.StatusInternalServerError
			switch {
			case isGroupNotFound(err):
				status = http.StatusNotFound
			case strings.Contains(err.Error(), "below current ring usage"):
				status = http.StatusConflict
			case strings.Contains(err.Error(), "non-negative"):
				status = http.StatusBadRequest
			}
			http.Error(w, err.Error(), status)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("DELETE /ring/{group}/cid/{cid}", func(w http.ResponseWriter, r *http.Request) {
		group := r.PathValue("group")
		cidStr := r.PathValue("cid")
		if err := drings.DeleteCID(r.Context(), group, cidStr); err != nil {
			status := http.StatusInternalServerError
			if isGroupNotFound(err) {
				status = http.StatusNotFound
			}
			http.Error(w, err.Error(), status)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("DELETE /ring/{group}", func(w http.ResponseWriter, r *http.Request) {
		group := r.PathValue("group")
		if err := drings.LeaveRing(r.Context(), group); err != nil {
			status := http.StatusInternalServerError
			if isGroupNotFound(err) {
				status = http.StatusNotFound
			}
			http.Error(w, err.Error(), status)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.Handle("/", base)
	return mux
}

func isGroupNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), "group") && strings.Contains(err.Error(), "not found")
}

type ShareConcurrencyKnob struct {
	Get	func() int
	Set	func(int) int
}

func AddDebugHandlers(base http.Handler, stater NodeStater, privStater PrivateRingStater, logLevel *slog.LevelVar, shareKnob *ShareConcurrencyKnob) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/state", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(stater.StateJSON())
	})

	mux.HandleFunc("GET /debug/clock-probe", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "%d\n", time.Now().UnixNano())
	})
	if privStater != nil {
		mux.HandleFunc("GET /debug/groups", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write(privStater.RingStatesJSON())
		})
		if privStabilizer, ok := privStater.(PrivateRingStabilizer); ok {
			mux.HandleFunc("POST /debug/groups/{group}/stabilize", func(w http.ResponseWriter, r *http.Request) {
				groupRef := r.PathValue("group")
				if err := privStabilizer.StabilizeRing(groupRef); err != nil {
					http.Error(w, err.Error(), http.StatusNotFound)
					return
				}
				w.WriteHeader(http.StatusNoContent)
			})
		}
	}
	if logLevel != nil {
		mux.HandleFunc("GET /debug/log-level", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"level": logLevel.Level().String()})
		})
		mux.HandleFunc("PUT /debug/log-level", func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				Level string `json:"level"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "invalid JSON", http.StatusBadRequest)
				return
			}
			var l slog.Level
			if err := l.UnmarshalText([]byte(req.Level)); err != nil {
				http.Error(w, fmt.Sprintf("unknown level %q (use debug, info, warn, error)", req.Level), http.StatusBadRequest)
				return
			}
			logLevel.Set(l)
			slog.Info("log level changed", "level", l)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"level": l.String()})
		})
	}
	if shareKnob != nil && shareKnob.Get != nil && shareKnob.Set != nil {
		mux.HandleFunc("GET /debug/share-concurrency", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]int{"n": shareKnob.Get()})
		})
		mux.HandleFunc("PUT /debug/share-concurrency", func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				N int `json:"n"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "invalid JSON", http.StatusBadRequest)
				return
			}
			if req.N <= 0 {
				http.Error(w, `"n" must be a positive integer`, http.StatusBadRequest)
				return
			}
			applied := shareKnob.Set(req.N)
			slog.Info("share concurrency changed", "requested", req.N, "applied", applied)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]int{"n": applied})
		})
	}
	if stabilizer, ok := stater.(NodeStabilizer); ok {
		mux.HandleFunc("POST /debug/stabilize", func(w http.ResponseWriter, r *http.Request) {
			stabilizer.StabilizeFull()
			w.WriteHeader(http.StatusNoContent)
		})
	}

	if bgCtrl, ok := stater.(BackgroundStabilizer); ok {
		mux.HandleFunc("POST /debug/background-stabilize", func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				Enabled bool `json:"enabled"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "invalid JSON", http.StatusBadRequest)
				return
			}
			if req.Enabled {
				bgCtrl.ResumeBackground()
			} else {
				bgCtrl.PauseBackground()
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]bool{"enabled": req.Enabled})
		})
		mux.HandleFunc("GET /debug/background-stabilize", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]bool{"enabled": !bgCtrl.BackgroundPaused()})
		})
	}

	if privStater != nil {
		if privBgCtrl, ok := privStater.(PrivateRingBackgroundControl); ok {
			mux.HandleFunc("POST /debug/groups/{group}/background-stabilize", func(w http.ResponseWriter, r *http.Request) {
				groupRef := r.PathValue("group")
				var req struct {
					Enabled bool `json:"enabled"`
				}
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					http.Error(w, "invalid JSON", http.StatusBadRequest)
					return
				}
				if req.Enabled {
					if err := privBgCtrl.ResumeRingBackground(groupRef); err != nil {
						http.Error(w, err.Error(), http.StatusNotFound)
						return
					}
				} else {
					if err := privBgCtrl.PauseRingBackground(groupRef); err != nil {
						http.Error(w, err.Error(), http.StatusNotFound)
						return
					}
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]bool{"enabled": req.Enabled})
			})
			mux.HandleFunc("GET /debug/groups/{group}/background-stabilize", func(w http.ResponseWriter, r *http.Request) {
				groupRef := r.PathValue("group")
				paused, err := privBgCtrl.RingBackgroundPaused(groupRef)
				if err != nil {
					http.Error(w, err.Error(), http.StatusNotFound)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]bool{"enabled": !paused})
			})
		}
	}
	if introspector, ok := stater.(RecordIntrospector); ok {
		mux.HandleFunc("GET /debug/records", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write(introspector.RecordKeysJSON())
		})
		mux.HandleFunc("GET /debug/records/{key}", func(w http.ResponseWriter, r *http.Request) {
			key := r.PathValue("key")
			found := introspector.HasRecord(key)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]bool{"found": found})
		})
		mux.HandleFunc("DELETE /debug/records/{key}", func(w http.ResponseWriter, r *http.Request) {
			key := r.PathValue("key")
			deleted := introspector.DeleteRecord(key)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]bool{"deleted": deleted})
		})
	}
	if blockIntro, ok := stater.(BlockIntrospector); ok {
		mux.HandleFunc("GET /debug/blocks/{cid}", func(w http.ResponseWriter, r *http.Request) {
			cidStr := r.PathValue("cid")
			found, err := blockIntro.HasLocalBlock(cidStr)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]bool{"found": found})
		})
		mux.HandleFunc("DELETE /debug/blocks/{cid}", func(w http.ResponseWriter, r *http.Request) {
			cidStr := r.PathValue("cid")
			if err := blockIntro.DeleteLocalBlock(cidStr); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		})
	}
	mux.Handle("/", base)
	return mux
}

func AddStorageHandler(base http.Handler, reporter StorageReporter) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/storage", func(w http.ResponseWriter, r *http.Request) {
		status := reporter.StorageStatus()
		if status == nil {
			status = &store.StorageStatus{}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	})
	mux.Handle("/", base)
	return mux
}

func AddNetworkRootsHandler(base http.Handler, nr NetworkRootLister) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/network-roots", func(w http.ResponseWriter, r *http.Request) {
		roots := nr.ListRoots()
		if roots == nil {
			roots = []string{}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"count":	nr.RootCount(),
			"roots":	roots,
		})
	})
	mux.Handle("/", base)
	return mux
}

func AddNetworkBlocksHandler(base http.Handler, intro NetworkBlockIntrospector) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/network-blocks/{cid}", func(w http.ResponseWriter, r *http.Request) {
		cidStr := r.PathValue("cid")
		found, err := intro.HasBlockStr(cidStr)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]bool{"found": found})
	})
	mux.HandleFunc("GET /debug/network-blocks", func(w http.ResponseWriter, r *http.Request) {
		cids, err := intro.ListBlockCIDs()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if cids == nil {
			cids = []string{}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"count":	len(cids),
			"cids":		cids,
		})
	})
	mux.Handle("/", base)
	return mux
}

func AddRingScopedDebugHandlers(base http.Handler, nbs *store.NetworkBlockStore) http.Handler {
	mux := http.NewServeMux()

	type ringSummary struct {
		Ring		string	`json:"ring"`
		RootCount	int	`json:"root_count"`
		BlockCount	int	`json:"block_count"`
		UsedBytes	int64	`json:"used_bytes"`
		MaxBytes	int64	`json:"max_bytes"`
	}

	mux.HandleFunc("GET /debug/rings", func(w http.ResponseWriter, r *http.Request) {
		rings := nbs.RingsKnown()
		out := make([]ringSummary, 0, len(rings))
		for _, ringID := range rings {
			rv := nbs.Ring(ringID)
			out = append(out, ringSummary{
				Ring:		ringID,
				RootCount:	rv.RootCount(),
				BlockCount:	int(rv.BlockCount()),
				UsedBytes:	rv.UsedBytes(),
				MaxBytes:	rv.Quota(),
			})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"aggregate_used_bytes":		nbs.UsedBytes(),
			"aggregate_max_bytes":		nbs.MaxBytes(),
			"aggregate_block_count":	nbs.BlockCount(),
			"rings":			out,
		})
	})

	mux.HandleFunc("GET /debug/rings/{ring}/network-roots", func(w http.ResponseWriter, r *http.Request) {
		ring := r.PathValue("ring")
		roots := nbs.Ring(ring).ListRoots()
		if roots == nil {
			roots = []string{}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"ring":		ring,
			"count":	len(roots),
			"roots":	roots,
		})
	})

	mux.HandleFunc("GET /debug/rings/{ring}/network-blocks", func(w http.ResponseWriter, r *http.Request) {
		ring := r.PathValue("ring")
		blocks, err := nbs.Ring(ring).Blocks(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		cids := make([]string, len(blocks))
		for i, c := range blocks {
			cids[i] = c.String()
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"ring":		ring,
			"count":	len(cids),
			"cids":		cids,
		})
	})

	mux.HandleFunc("GET /debug/rings/{ring}/cid-usage", func(w http.ResponseWriter, r *http.Request) {
		ring := r.PathValue("ring")
		usage := nbs.Ring(ring).CIDUsage()
		if usage == nil {
			usage = make(map[string]int64)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"ring":	ring,
			"cids":	usage,
		})
	})

	mux.HandleFunc("GET /debug/rings/{ring}/storage", func(w http.ResponseWriter, r *http.Request) {
		ring := r.PathValue("ring")
		rv := nbs.Ring(ring)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]int64{
			"used_bytes":	rv.UsedBytes(),
			"max_bytes":	rv.Quota(),
			"block_count":	rv.BlockCount(),
		})
	})

	mux.Handle("/", base)
	return mux
}

func AddCIDUsageHandler(base http.Handler, reporter CIDUsageReporter) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/cid-usage", func(w http.ResponseWriter, r *http.Request) {
		usage := reporter.CIDUsage()
		if usage == nil {
			usage = make(map[string]int64)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"cids": usage,
		})
	})
	mux.Handle("/", base)
	return mux
}

func AddGCHandler(base http.Handler, gc GarbageCollector) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /gc", func(w http.ResponseWriter, r *http.Request) {
		result, err := gc.GC(r.Context())
		if err != nil {
			http.Error(w, fmt.Sprintf("gc: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})
	mux.Handle("/", base)
	return mux
}

type ReplicationDrainer interface {
	WaitForReplicationDrain(timeout time.Duration) bool
}

func AddReplicationDrainHandler(base http.Handler, drainer ReplicationDrainer) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /debug/replication-drain", func(w http.ResponseWriter, r *http.Request) {
		timeout := 30 * time.Second
		if v := r.URL.Query().Get("timeout"); v != "" {
			if parsed, err := time.ParseDuration(v); err == nil && parsed > 0 {
				timeout = parsed
			}
		}
		if drainer.WaitForReplicationDrain(timeout) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.Error(w, "replication drain timed out", http.StatusGatewayTimeout)
	})
	mux.Handle("/", base)
	return mux
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
	AlreadyTracked	bool		`json:"already_tracked,omitempty"`
}

func (s *Server) handleAdd(w http.ResponseWriter, r *http.Request) {
	var req addRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.Path == "" {
		http.Error(w, `"path" is required`, http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		req.Name = filepath.Base(req.Path)
	}

	ctx := context.Background()
	rootNode, err := store.IngestPath(ctx, req.Path, s.dagSvc)
	if err != nil {
		http.Error(w, fmt.Sprintf("ingest: %v", err), http.StatusInternalServerError)
		return
	}

	root := store.Root{
		Name:		req.Name,
		CID:		rootNode.Cid(),
		Path:		req.Path,
		AddedAt:	time.Now(),
	}

	registered, err := s.roots.Add(root)
	alreadyTracked := errors.Is(err, store.ErrAlreadyTracked)
	if err != nil && !alreadyTracked {
		http.Error(w, fmt.Sprintf("register root: %v", err), http.StatusInternalServerError)
		return
	}

	status := http.StatusCreated
	if alreadyTracked {
		status = http.StatusOK
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(addResponse{
		ID:		registered.ID,
		Name:		registered.Name,
		CID:		registered.CID.String(),
		AddedAt:	registered.AddedAt,
		AlreadyTracked:	alreadyTracked,
	})
}

func (s *Server) handleRoots(w http.ResponseWriter, r *http.Request) {
	roots := s.roots.List()
	type rootJSON struct {
		ID	string		`json:"id"`
		Name	string		`json:"name"`
		CID	string		`json:"cid"`
		Path	string		`json:"path"`
		AddedAt	time.Time	`json:"added_at"`
	}
	list := make([]rootJSON, len(roots))
	for i, root := range roots {
		list[i] = rootJSON{
			ID:		root.ID,
			Name:		root.Name,
			CID:		root.CID.String(),
			Path:		root.Path,
			AddedAt:	root.AddedAt,
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(list)
}

func (s *Server) handleRemove(w http.ResponseWriter, r *http.Request) {
	ref := r.PathValue("id")
	if ref == "" {
		http.Error(w, `"id" is required`, http.StatusBadRequest)
		return
	}

	id, err := s.resolveRootID(ref)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	if err := s.roots.Remove(id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("remove: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) resolveRootID(ref string) (string, error) {

	if c, err := cid.Decode(ref); err == nil {
		if roots, err := s.roots.GetByCID(c); err == nil && len(roots) > 0 {
			return roots[0].ID, nil
		}
	}

	if roots, err := s.roots.GetByName(ref); err == nil && len(roots) > 0 {
		return roots[0].ID, nil
	}

	return ref, nil
}

type renameRequest struct {
	Name string `json:"name"`
}

func (s *Server) handleRename(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, `"id" is required`, http.StatusBadRequest)
		return
	}
	var req renameRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		http.Error(w, `"name" is required`, http.StatusBadRequest)
		return
	}
	if err := s.roots.Rename(id, req.Name); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("rename: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type dhtJoinRequest struct {
	Peer string `json:"peer"`
}

func (s *Server) handleDHTJoin(w http.ResponseWriter, r *http.Request) {
	if s.dht == nil {
		http.Error(w, "DHT not enabled on this node", http.StatusServiceUnavailable)
		return
	}
	var req dhtJoinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Peer == "" {
		http.Error(w, `"peer" multiaddr is required`, http.StatusBadRequest)
		return
	}
	if err := s.dht.JoinPeer(r.Context(), req.Peer); err != nil {
		http.Error(w, fmt.Sprintf("join: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type dhtDownloadRequest struct {
	CID	string	`json:"cid"`
	Name	string	`json:"name"`
}

func (s *Server) handleDHTDownload(w http.ResponseWriter, r *http.Request) {
	if s.dht == nil {
		http.Error(w, "DHT not enabled on this node", http.StatusServiceUnavailable)
		return
	}
	var req dhtDownloadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.CID == "" {
		http.Error(w, `"cid" is required`, http.StatusBadRequest)
		return
	}

	rootCID, err := cid.Decode(req.CID)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid CID: %v", err), http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	if err := s.dht.FetchDAG(ctx, rootCID); err != nil {
		http.Error(w, fmt.Sprintf("fetch DAG: %v", err), http.StatusInternalServerError)
		return
	}

	name := req.Name
	if name == "" {
		name = rootCID.String()[:12]
	}

	root := store.Root{
		Name:		name,
		CID:		rootCID,
		AddedAt:	time.Now(),
	}
	registered, err := s.roots.Add(root)
	alreadyTracked := errors.Is(err, store.ErrAlreadyTracked)
	if err != nil && !alreadyTracked {
		http.Error(w, fmt.Sprintf("register root: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(addResponse{
		ID:		registered.ID,
		Name:		registered.Name,
		CID:		registered.CID.String(),
		AddedAt:	registered.AddedAt,
		AlreadyTracked:	alreadyTracked,
	})
}
