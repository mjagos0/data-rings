package dht

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/mjagos0/datarings/metrics"
)

type PublicDring struct {
	node		*Node
	identity	*Identity
	version		uint64
	met		*metrics.Registry

	republishMu		sync.Mutex
	announcedCIDs		map[cid.Cid]struct{}
	groupRepublishers	[]func(ctx context.Context) error

	groupAddrMu		sync.Mutex
	groupAddrProviders	map[NodeID]func() string
}

func (p *PublicDring) SetMetrics(m *metrics.Registry)	{ p.met = m }

func (p *PublicDring) PruneOutOfWindowBlocks(ctx context.Context) (int, error) {
	return p.node.PruneOutOfWindowBlocks(ctx)
}

func NewPublicDring(node *Node, identity *Identity) *PublicDring {
	return &PublicDring{
		node:			node,
		identity:		identity,
		announcedCIDs:		make(map[cid.Cid]struct{}),
		groupAddrProviders:	make(map[NodeID]func() string),
	}
}

func (p *PublicDring) RegisterGroupRepublisher(fn func(ctx context.Context) error) {
	p.republishMu.Lock()
	p.groupRepublishers = append(p.groupRepublishers, fn)
	p.republishMu.Unlock()
}

func (p *PublicDring) RegisterGroupAddrProvider(groupID NodeID, fn func() string) {
	p.groupAddrMu.Lock()
	p.groupAddrProviders[groupID] = fn
	p.groupAddrMu.Unlock()
}

func (p *PublicDring) UnregisterGroupAddrProvider(groupID NodeID) {
	p.groupAddrMu.Lock()
	delete(p.groupAddrProviders, groupID)
	p.groupAddrMu.Unlock()
}

func (p *PublicDring) collectGroupAddrs() map[string]string {
	p.groupAddrMu.Lock()
	defer p.groupAddrMu.Unlock()
	if len(p.groupAddrProviders) == 0 {
		return nil
	}
	addrs := make(map[string]string, len(p.groupAddrProviders))
	for id, fn := range p.groupAddrProviders {
		if addr := fn(); addr != "" {
			addrs[id.String()] = addr
		}
	}
	return addrs
}

func (p *PublicDring) StartRepublishLoop(stopCh <-chan struct{}, cfg Config) {
	peerInterval := cfg.peerRepublishInterval()
	providerInterval := cfg.providerRepublishInterval()

	go func() {
		tick := time.NewTicker(peerInterval)
		defer tick.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-tick.C:
				ctx := context.Background()
				if err := p.PublishSelf(ctx); err != nil {
					slog.Warn("republish: failed to re-publish PeerIdentityRecord", "peer", p.identity.ID, "error", err)
				} else {
					slog.Debug("republish: PeerIdentityRecord refreshed", "peer", p.identity.ID)
				}
				p.republishMu.Lock()
				fns := make([]func(ctx context.Context) error, len(p.groupRepublishers))
				copy(fns, p.groupRepublishers)
				p.republishMu.Unlock()
				for _, fn := range fns {
					if err := fn(ctx); err != nil {
						slog.Warn("republish: failed to re-publish GroupIdentityRecord", "error", err)
					}
				}
			}
		}
	}()

	go func() {
		tick := time.NewTicker(providerInterval)
		defer tick.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-tick.C:
				ctx := context.Background()
				p.republishMu.Lock()
				cids := make([]cid.Cid, 0, len(p.announcedCIDs))
				for c := range p.announcedCIDs {
					cids = append(cids, c)
				}
				p.republishMu.Unlock()
				for _, c := range cids {
					if err := p.publishProvider(ctx, c); err != nil {
						slog.Warn("republish: failed to re-publish ProviderRecord", "cid", c, "error", err)
					} else {
						slog.Debug("republish: ProviderRecord refreshed", "cid", c)
					}
				}
			}
		}
	}()
}

func (p *PublicDring) Node() *Node	{ return p.node }

func (p *PublicDring) PublishSelf(ctx context.Context) error {
	v := atomic.AddUint64(&p.version, 1)
	if err := p.publishSelfVersion(ctx, v); err != nil {
		if !isVersionConflict(err) {
			return err
		}

		storedVersion, fetchErr := p.fetchStoredPeerVersion(ctx)
		if fetchErr != nil {
			return fmt.Errorf("version conflict and failed to fetch stored version: %w (original: %v)", fetchErr, err)
		}
		newV := storedVersion + 1
		atomic.StoreUint64(&p.version, newV)
		slog.Info("republish: recovering from version conflict", "peer", p.identity.ID, "stored_version", storedVersion, "retry_version", newV)
		return p.publishSelfVersion(ctx, newV)
	}
	return nil
}

func (p *PublicDring) publishSelfVersion(ctx context.Context, v uint64) error {
	rec, err := NewPeerIdentityRecord(p.identity, v, p.node.addr, p.collectGroupAddrs())
	if err != nil {
		return fmt.Errorf("build peer record: %w", err)
	}
	data, err := rec.Encode()
	if err != nil {
		return fmt.Errorf("encode peer record: %w", err)
	}
	if err := p.node.RecordPut(ctx, p.identity.ID, data); err != nil {
		return err
	}
	if p.met != nil {
		p.met.RecordsStored.WithLabelValues("peer").Inc()
	}
	return nil
}

func (p *PublicDring) fetchStoredPeerVersion(ctx context.Context) (uint64, error) {
	rec, err := p.LookupPeer(ctx, p.identity.ID)
	if err != nil {
		return 0, err
	}
	return rec.Data.Version, nil
}

func (p *PublicDring) LookupPeer(ctx context.Context, peerID NodeID) (*PeerIdentityRecord, error) {
	data, err := p.node.RecordGet(ctx, peerID)
	if err != nil {
		return nil, fmt.Errorf("fetch peer record for %s: %w", peerID, err)
	}
	rec, err := DecodePeerIdentityRecord(data)
	if err != nil {
		return nil, fmt.Errorf("decode peer record: %w", err)
	}
	if err := rec.Verify(); err != nil {
		return nil, fmt.Errorf("invalid peer record: %w", err)
	}
	return rec, nil
}

func (p *PublicDring) PublishGroup(ctx context.Context, grp *GroupIdentity, version uint64, peers []GroupMember) error {
	rec, err := NewGroupIdentityRecord(grp, version, peers)
	if err != nil {
		return fmt.Errorf("build group record: %w", err)
	}
	data, err := rec.Encode()
	if err != nil {
		return fmt.Errorf("encode group record: %w", err)
	}
	if err := p.node.RecordPut(ctx, grp.GroupID, data); err != nil {
		return err
	}
	if p.met != nil {
		p.met.RecordsStored.WithLabelValues("group").Inc()
	}
	return nil
}

func (p *PublicDring) LookupGroup(ctx context.Context, groupID NodeID) (*GroupIdentityRecord, error) {
	data, err := p.node.RecordGet(ctx, groupID)
	if err != nil {
		return nil, fmt.Errorf("fetch group record for %s: %w", groupID, err)
	}
	rec, err := DecodeGroupIdentityRecord(data)
	if err != nil {
		return nil, fmt.Errorf("decode group record: %w", err)
	}
	if err := rec.Verify(); err != nil {
		return nil, fmt.Errorf("invalid group record: %w", err)
	}
	return rec, nil
}

func (p *PublicDring) AnnounceProvider(ctx context.Context, c cid.Cid) error {
	return p.publishProvider(ctx, c)
}

func (p *PublicDring) FindProviders(ctx context.Context, c cid.Cid) ([]ProviderRecord, error) {
	key := CIDToNodeID(c)
	data, err := p.node.RecordGet(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("no providers for %s", c)
	}
	rs, err := DecodeProviderRecords(data)
	if err != nil {
		return nil, fmt.Errorf("decode providers: %w", err)
	}
	if len(rs) == 0 {
		return nil, fmt.Errorf("no providers for %s", c)
	}
	return rs, nil
}

func (p *PublicDring) LookupPeerJSON(ctx context.Context, peerIDHex string) ([]byte, error) {
	b, err := hex.DecodeString(peerIDHex)
	if err != nil || len(b) != 20 {
		return nil, fmt.Errorf("invalid peer ID hex: %q", peerIDHex)
	}
	var id NodeID
	copy(id[:], b)
	rec, err := p.LookupPeer(ctx, id)
	if err != nil {
		return nil, err
	}
	return json.Marshal(rec)
}

func (p *PublicDring) LookupGroupJSON(ctx context.Context, groupIDHex string) ([]byte, error) {
	b, err := hex.DecodeString(groupIDHex)
	if err != nil || len(b) != 20 {
		return nil, fmt.Errorf("invalid group ID hex: %q", groupIDHex)
	}
	var id NodeID
	copy(id[:], b)
	rec, err := p.LookupGroup(ctx, id)
	if err != nil {
		return nil, err
	}
	return json.Marshal(rec)
}

func (p *PublicDring) PublishProvider(ctx context.Context, cidStr string) error {
	c, err := cid.Decode(cidStr)
	if err != nil {
		return fmt.Errorf("invalid CID %q: %w", cidStr, err)
	}
	return p.publishProvider(ctx, c)
}

func (p *PublicDring) publishProvider(ctx context.Context, c cid.Cid) error {
	key := CIDToNodeID(c)
	pr := ProviderRecord{ContentHash: key, Provider: p.identity.ID}
	data, err := pr.Encode()
	if err != nil {
		return fmt.Errorf("encode provider record: %w", err)
	}
	if err := p.node.RecordPut(ctx, key, data); err != nil {
		return err
	}

	p.republishMu.Lock()
	p.announcedCIDs[c] = struct{}{}
	p.republishMu.Unlock()

	if p.met != nil {
		p.met.RecordsStored.WithLabelValues("provider").Inc()
	}
	return nil
}

func (p *PublicDring) FindProvidersJSON(ctx context.Context, cidStr string) ([]byte, error) {
	c, err := cid.Decode(cidStr)
	if err != nil {
		return nil, fmt.Errorf("invalid CID %q: %w", cidStr, err)
	}
	providers, err := p.FindProviders(ctx, c)
	if err != nil {
		return nil, err
	}
	return json.Marshal(providers)
}

func (p *PublicDring) FetchDAGFromProviders(ctx context.Context, root cid.Cid) error {
	providers, err := p.FindProviders(ctx, root)
	if err != nil {
		return fmt.Errorf("find providers for %s: %w", root, err)
	}

	var lastErr error
	for _, pr := range providers {
		peerRec, err := p.LookupPeer(ctx, pr.Provider)
		if err != nil {
			lastErr = fmt.Errorf("lookup provider %s: %w", pr.Provider, err)
			continue
		}
		peer := NodeAddr{ID: pr.Provider, Addr: peerRec.Data.Address}
		if err := p.node.FetchDAGFromPeer(ctx, root, peer); err != nil {
			lastErr = fmt.Errorf("fetch from %s: %w", peerRec.Data.Address, err)
			continue
		}
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("no reachable providers for %s", root)
}

func (p *PublicDring) FetchDAGFromProvidersStr(ctx context.Context, cidStr string) error {
	c, err := cid.Decode(cidStr)
	if err != nil {
		return fmt.Errorf("invalid CID %q: %w", cidStr, err)
	}
	return p.FetchDAGFromProviders(ctx, c)
}
