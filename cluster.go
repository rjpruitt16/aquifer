package aquifer

import (
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultClusterPruneTTLSeconds = 30
	clusterForwardedHeader        = "X-Aquifer-Cluster-Forwarded"
)

type ClusterMember struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

func (m ClusterMember) String() string {
	return m.ID
}

type ClusterConfig struct {
	Enabled  bool
	Self     ClusterMember
	Members  []ClusterMember
	PruneTTL time.Duration
}

type ClusterRouter struct {
	self        ClusterMember
	members     map[string]ClusterMember
	pruneTTL    time.Duration
	mu          sync.Mutex
	prunedUntil map[string]time.Time
}

func LoadClusterConfig() ClusterConfig {
	cfg := ClusterConfig{
		Enabled:  envBool("AQUIFER_CLUSTER_ENABLED", false),
		Self:     ClusterMember{ID: os.Getenv("AQUIFER_CLUSTER_SELF_ID"), Address: os.Getenv("AQUIFER_CLUSTER_SELF_ADDR")},
		Members:  parseClusterMembers(os.Getenv("AQUIFER_CLUSTER_MEMBERS")),
		PruneTTL: time.Duration(envInt64("AQUIFER_CLUSTER_PRUNE_TTL_SECONDS", defaultClusterPruneTTLSeconds)) * time.Second,
	}
	if cfg.PruneTTL < 0 {
		cfg.PruneTTL = defaultClusterPruneTTLSeconds * time.Second
	}
	return cfg
}

func NewClusterRouter(cfg ClusterConfig) *ClusterRouter {
	if !cfg.Enabled {
		return nil
	}
	if cfg.Self.ID == "" || cfg.Self.Address == "" {
		log.Printf("cluster: AQUIFER_CLUSTER_ENABLED=true but self id/address is missing; cluster routing disabled")
		return nil
	}

	membersByID := make(map[string]ClusterMember, len(cfg.Members)+1)
	for _, m := range cfg.Members {
		if m.ID == "" || m.Address == "" {
			continue
		}
		membersByID[m.ID] = m
	}
	membersByID[cfg.Self.ID] = cfg.Self

	if len(membersByID) == 0 {
		return nil
	}

	return &ClusterRouter{
		self:        cfg.Self,
		members:     membersByID,
		pruneTTL:    cfg.PruneTTL,
		prunedUntil: make(map[string]time.Time),
	}
}

func (r *ClusterRouter) OwnerFor(key string) (ClusterMember, bool) {
	ranked := r.RankedOwners(key)
	if len(ranked) == 0 {
		return ClusterMember{}, false
	}
	return ranked[0], true
}

func (r *ClusterRouter) IsOwner(key string) bool {
	owner, ok := r.OwnerFor(key)
	return ok && owner.ID == r.self.ID
}

func (r *ClusterRouter) RankedOwners(key string) []ClusterMember {
	if r == nil || key == "" {
		return nil
	}

	now := time.Now()
	r.mu.Lock()
	r.expirePrunedLocked(now)
	pruned := make(map[string]bool, len(r.prunedUntil))
	for id, until := range r.prunedUntil {
		if now.Before(until) {
			pruned[id] = true
		}
	}
	r.mu.Unlock()

	type rankedMember struct {
		member ClusterMember
		score  string
	}
	ranked := make([]rankedMember, 0, len(r.members))
	for _, member := range r.members {
		if pruned[member.ID] {
			continue
		}
		ranked = append(ranked, rankedMember{
			member: member,
			score:  rendezvousScore(member.ID, key),
		})
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].score == ranked[j].score {
			return ranked[i].member.ID < ranked[j].member.ID
		}
		return ranked[i].score > ranked[j].score
	})

	owners := make([]ClusterMember, 0, len(ranked))
	for _, item := range ranked {
		owners = append(owners, item.member)
	}
	return owners
}

func (r *ClusterRouter) PruneMember(id string) {
	if r == nil || id == "" || id == r.self.ID || r.pruneTTL <= 0 {
		return
	}
	r.mu.Lock()
	r.prunedUntil[id] = time.Now().Add(r.pruneTTL)
	r.mu.Unlock()
}

func (r *ClusterRouter) expirePrunedLocked(now time.Time) {
	for id, until := range r.prunedUntil {
		if !now.Before(until) {
			delete(r.prunedUntil, id)
		}
	}
}

func (r *ClusterRouter) prunedCount() int {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expirePrunedLocked(now)
	return len(r.prunedUntil)
}

func (r *ClusterRouter) Snapshot() map[string]any {
	if r == nil {
		return nil
	}
	return map[string]any{
		"self":              r.self,
		"members":           len(r.members),
		"algorithm":         "rendezvous",
		"prune_ttl_seconds": int64(r.pruneTTL / time.Second),
		"pruned_members":    r.prunedCount(),
	}
}

func parseClusterMembers(raw string) []ClusterMember {
	var members []ClusterMember
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, address, ok := strings.Cut(part, "=")
		if !ok {
			log.Printf("cluster: ignoring malformed member %q, expected id=http://host:port", part)
			continue
		}
		id = strings.TrimSpace(id)
		address = strings.TrimRight(strings.TrimSpace(address), "/")
		if id == "" || address == "" {
			continue
		}
		members = append(members, ClusterMember{ID: id, Address: address})
	}
	return members
}

func clusterRoutingKey(req JobRequest) string {
	return req.UserID
}

func hasClusterForwardedHeader(r *http.Request) bool {
	return r.Header.Get(clusterForwardedHeader) == "true"
}
