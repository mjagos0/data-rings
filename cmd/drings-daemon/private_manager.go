package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/mjagos0/datarings/api"
	"github.com/mjagos0/datarings/dht"
	"github.com/mjagos0/datarings/metrics"
	"github.com/mjagos0/datarings/store"
)

type privateDringsManager struct {
	mu	sync.RWMutex
	rings	map[string]*dht.PrivateDring
	infos	[]api.PrivateRingInfo

	publicDring	*dht.PublicDring
	ident		*dht.Identity
	st		*store.Store
	groupsPath	string
	dataDir		string
	met		*metrics.Registry
	cfg		dht.Config
	advertiseIP	string
	noBgStabilize	bool
}

func (m *privateDringsManager) startRing(ctx context.Context, keyHex, listenAddr, name string, storageMaxBytes int64) (api.PrivateRingInfo, error) {
	grp, err := dht.GroupIdentityFromHex(keyHex)
	if err != nil {
		return api.PrivateRingInfo{}, fmt.Errorf("invalid group key: %w", err)
	}

	groupIDStr := grp.GroupID.String()

	m.mu.RLock()
	_, exists := m.rings[groupIDStr]
	m.mu.RUnlock()
	if exists {
		return api.PrivateRingInfo{}, fmt.Errorf("already joined ring %s", groupIDStr)
	}

	if listenAddr == "" {
		listenAddr = "/ip4/0.0.0.0/tcp/0"
	} else {
		if tcp, err2 := dht.MultiaddrToTCPAddr(listenAddr); err2 == nil {
			if _, port, err2 := net.SplitHostPort(tcp); err2 == nil {
				listenAddr = "/ip4/0.0.0.0/tcp/" + port
			}
		}
	}

	privDring := dht.NewPrivateDring(grp, m.ident, m.st.NetworkBlocks, m.st.DAG, m.cfg)
	privDring.Node().SetNetworkBlocks(m.st.NetworkBlocks)
	privDring.Node().SetLocalBlocks(m.st.LocalBlocks)

	m.st.NetworkBlocks.MarkRingKnown(groupIDStr)

	if storageMaxBytes > 0 {
		if err := m.st.NetworkBlocks.Ring(groupIDStr).SetQuota(storageMaxBytes); err != nil {
			return api.PrivateRingInfo{}, fmt.Errorf("apply per-ring storage quota: %w", err)
		}
	}
	if m.met != nil {
		privDring.SetMetrics(m.met)
		privDring.Node().SetMetrics(m.met, grp.GroupID.String())
	}
	boundAddr, err := privDring.StartServer(listenAddr, m.advertiseIP)
	if err != nil {
		return api.PrivateRingInfo{}, fmt.Errorf("start private dring server: %w", err)
	}

	if m.dataDir != "" {
		groupCacheDir := filepath.Join(m.dataDir, "groups")
		if mkErr := os.MkdirAll(groupCacheDir, 0755); mkErr == nil {
			privDring.SetGroupRecordCachePath(filepath.Join(groupCacheDir, grp.GroupID.String()+".record"))
		}
	}

	privDring.SetPublicDring(m.publicDring)

	if err := m.publicDring.PublishSelf(ctx); err != nil {
		slog.Warn("publish self before group join", "group_id", groupIDStr, "error", err)
	}

	if err := privDring.UpdateGroupRecord(ctx, m.publicDring); err != nil {
		slog.Warn("update group record", "group_id", groupIDStr, "error", err)
	}

	if err := privDring.JoinViaPublicDring(ctx, m.publicDring); err != nil {
		slog.Debug("join via public dring failed, creating new ring", "group_id", groupIDStr, "error", err)
		privDring.Create()
	}

	privDring.StartBackground(m.cfg)
	if m.noBgStabilize {
		privDring.Node().PauseBackground()
	}

	info := api.PrivateRingInfo{
		GroupID:	groupIDStr,
		Name:		name,
		ListenAddr:	boundAddr,
	}

	m.mu.Lock()
	m.rings[groupIDStr] = privDring
	m.infos = append(m.infos, info)
	m.mu.Unlock()

	return info, nil
}

func (m *privateDringsManager) resolve(groupRef string) (*dht.PrivateDring, string, error) {
	for _, info := range m.infos {
		if info.Name == groupRef {
			return m.rings[info.GroupID], info.GroupID, nil
		}
	}
	if r, ok := m.rings[groupRef]; ok {
		return r, groupRef, nil
	}
	var matched *dht.PrivateDring
	var matchedID string
	for id, r := range m.rings {
		if strings.HasPrefix(id, groupRef) {
			if matched != nil {
				return nil, "", fmt.Errorf("group ref %q is ambiguous (matches %s and %s)", groupRef, matchedID, id)
			}
			matched = r
			matchedID = id
		}
	}
	if matched != nil {
		return matched, matchedID, nil
	}
	return nil, "", fmt.Errorf("group %q not found", groupRef)
}

func (m *privateDringsManager) resolveRing(groupRef string) (*dht.PrivateDring, error) {
	m.mu.RLock()
	ring, _, err := m.resolve(groupRef)
	m.mu.RUnlock()
	return ring, err
}

func (m *privateDringsManager) ListRings() []api.PrivateRingInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]api.PrivateRingInfo, len(m.infos))
	copy(out, m.infos)
	return out
}

func (m *privateDringsManager) RingStatesJSON() []byte {
	type ringStateEntry struct {
		GroupID		string			`json:"group_id"`
		Name		string			`json:"name,omitempty"`
		ListenAddr	string			`json:"listen_addr"`
		Node		dht.NodeState		`json:"node"`
		VerifiedPeers	[]string		`json:"verified_peers"`
		ConnectionPool	map[string]string	`json:"connection_pool,omitempty"`
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]ringStateEntry, 0, len(m.infos))
	for _, info := range m.infos {
		ring := m.rings[info.GroupID]
		if ring == nil {
			continue
		}
		peers := ring.VerifiedPeers()
		peerStrs := make([]string, len(peers))
		for i, p := range peers {
			peerStrs[i] = p.String()
		}
		out = append(out, ringStateEntry{
			GroupID:	info.GroupID,
			Name:		info.Name,
			ListenAddr:	info.ListenAddr,
			Node:		ring.Node().State(),
			VerifiedPeers:	peerStrs,
			ConnectionPool:	ring.TransportPoolState(),
		})
	}
	data, _ := json.Marshal(out)
	return data
}

func (m *privateDringsManager) StabilizeRing(groupRef string) error {
	ring, err := m.resolveRing(groupRef)
	if err != nil {
		return err
	}
	ring.Node().StabilizeFull()
	return nil
}

func (m *privateDringsManager) WaitForReplicationDrain(timeout time.Duration) bool {
	m.mu.RLock()
	rings := make([]*dht.PrivateDring, 0, len(m.rings))
	for _, r := range m.rings {
		rings = append(rings, r)
	}
	m.mu.RUnlock()
	if len(rings) == 0 {
		return true
	}
	results := make(chan bool, len(rings))
	for _, ring := range rings {
		go func(r *dht.PrivateDring) {
			results <- r.Node().WaitForReplicationDrain(timeout)
		}(ring)
	}
	allOK := true
	for i := 0; i < len(rings); i++ {
		if !<-results {
			allOK = false
		}
	}
	return allOK
}

func (m *privateDringsManager) PauseRingBackground(groupRef string) error {
	ring, err := m.resolveRing(groupRef)
	if err != nil {
		return err
	}
	ring.Node().PauseBackground()
	return nil
}

func (m *privateDringsManager) ResumeRingBackground(groupRef string) error {
	ring, err := m.resolveRing(groupRef)
	if err != nil {
		return err
	}
	ring.Node().ResumeBackground()
	return nil
}

func (m *privateDringsManager) RingBackgroundPaused(groupRef string) (bool, error) {
	ring, err := m.resolveRing(groupRef)
	if err != nil {
		return false, err
	}
	return ring.Node().BackgroundPaused(), nil
}

func (m *privateDringsManager) PushDAG(ctx context.Context, groupRef, cidStr string, ttl time.Duration) error {
	ring, err := m.resolveRing(groupRef)
	if err != nil {
		return err
	}
	c, err := cid.Decode(cidStr)
	if err != nil {
		return fmt.Errorf("invalid CID %q: %w", cidStr, err)
	}
	if ttl > 0 {
		return ring.ShareDAGWithTTL(ctx, c, ttl)
	}
	return ring.ShareDAG(ctx, c)
}

func (m *privateDringsManager) FetchDAG(ctx context.Context, groupRef, cidStr string) error {
	ring, err := m.resolveRing(groupRef)
	if err != nil {
		return err
	}
	c, err := cid.Decode(cidStr)
	if err != nil {
		return fmt.Errorf("invalid CID %q: %w", cidStr, err)
	}
	return ring.FetchDAG(ctx, c)
}

func (m *privateDringsManager) PruneAllRings(ctx context.Context) int {
	m.mu.RLock()
	rings := make([]*dht.PrivateDring, 0, len(m.rings))
	for _, r := range m.rings {
		rings = append(rings, r)
	}
	m.mu.RUnlock()

	total := 0
	for _, r := range rings {
		if n, err := r.PruneOutOfWindowBlocks(ctx); err == nil {
			total += n
		}
	}
	return total
}

func (m *privateDringsManager) DeleteCID(ctx context.Context, groupRef, cidStr string) error {
	ring, err := m.resolveRing(groupRef)
	if err != nil {
		return err
	}
	c, err := cid.Decode(cidStr)
	if err != nil {
		return fmt.Errorf("invalid CID %q: %w", cidStr, err)
	}
	return ring.DeleteCID(ctx, c)
}

func (m *privateDringsManager) JoinRing(ctx context.Context, keyHex, listenAddr, name string, storageMaxBytes int64) (api.PrivateRingInfo, error) {
	info, err := m.startRing(ctx, keyHex, listenAddr, name, storageMaxBytes)
	if err != nil {
		return api.PrivateRingInfo{}, err
	}

	f, loadErr := dht.LoadGroupsFile(m.groupsPath)
	if loadErr != nil {
		f = &dht.GroupsFile{}
	}
	f.AddGroup(dht.GroupConfig{
		GroupPrivKeyHex:	keyHex,
		ListenAddr:		info.ListenAddr,
		Name:			name,
		StorageMaxBytes:	storageMaxBytes,
	})
	if err := dht.SaveGroupsFile(m.groupsPath, f); err != nil {
		slog.Warn("save groups file", "error", err)
	}

	slog.Info("joined private ring", "group_id", info.GroupID, "name", name, "addr", info.ListenAddr, "storage_max", storageMaxBytes)
	return info, nil
}

func (m *privateDringsManager) SetRingQuota(groupRef string, max int64) error {
	if max < 0 {
		return fmt.Errorf("quota must be non-negative")
	}
	m.mu.RLock()
	_, groupID, err := m.resolve(groupRef)
	m.mu.RUnlock()
	if err != nil {
		return err
	}
	if err := m.st.NetworkBlocks.Ring(groupID).SetQuota(max); err != nil {
		return err
	}

	f, ferr := dht.LoadGroupsFile(m.groupsPath)
	if ferr != nil {
		f = &dht.GroupsFile{}
	}
	updated := false
	for i := range f.Groups {
		grp, gerr := dht.GroupIdentityFromHex(f.Groups[i].GroupPrivKeyHex)
		if gerr != nil {
			continue
		}
		if grp.GroupID.String() == groupID {
			f.Groups[i].StorageMaxBytes = max
			updated = true
			break
		}
	}
	if updated {
		if serr := dht.SaveGroupsFile(m.groupsPath, f); serr != nil {
			slog.Warn("save groups file after quota update", "error", serr)
		}
	}
	slog.Info("set per-ring storage quota", "group_id", groupID, "max", max)
	return nil
}

func (m *privateDringsManager) RingQuota(groupRef string) (int64, int64, error) {
	m.mu.RLock()
	_, groupID, err := m.resolve(groupRef)
	m.mu.RUnlock()
	if err != nil {
		return 0, 0, err
	}
	rv := m.st.NetworkBlocks.Ring(groupID)
	return rv.Quota(), rv.UsedBytes(), nil
}

func (m *privateDringsManager) LeaveRing(ctx context.Context, groupRef string) error {
	m.mu.Lock()
	ring, groupID, err := m.resolve(groupRef)
	if err != nil {
		m.mu.Unlock()
		return err
	}
	delete(m.rings, groupID)
	for i, info := range m.infos {
		if info.GroupID == groupID {
			m.infos = append(m.infos[:i], m.infos[i+1:]...)
			break
		}
	}
	m.mu.Unlock()

	slog.Info("leaving private ring", "group_id", groupID)

	if err := ring.LeaveGroup(ctx, m.publicDring); err != nil {
		slog.Warn("graceful leave ring", "group_id", groupID, "error", err)
	}
	ring.Stop()

	f, err := dht.LoadGroupsFile(m.groupsPath)
	if err == nil {
		if f.RemoveGroup(groupID) {
			if err := dht.SaveGroupsFile(m.groupsPath, f); err != nil {
				slog.Warn("save groups file after leave", "error", err)
			}
		}
	}

	return nil
}

func (m *privateDringsManager) Stop() {
	m.mu.Lock()
	rings := make([]*dht.PrivateDring, 0, len(m.rings))
	for _, r := range m.rings {
		rings = append(rings, r)
	}
	m.rings = map[string]*dht.PrivateDring{}
	m.infos = nil
	m.mu.Unlock()

	for _, r := range rings {
		r.Stop()
	}
}
