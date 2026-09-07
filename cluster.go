package aquifer

import (
	"hash/fnv"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/buraksezer/consistent"
)

const (
	defaultClusterPartitionCount    = 16384
	defaultClusterReplicationFactor = 20
	defaultClusterLoad              = 1.25
	clusterForwardedHeader          = "X-Aquifer-Cluster-Forwarded"
)

type ClusterMember struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

func (m ClusterMember) String() string {
	return m.ID
}

type ClusterConfig struct {
	Enabled           bool
	Self              ClusterMember
	Members           []ClusterMember
	PartitionCount    int
	ReplicationFactor int
	Load              float64
}

type fnvHasher struct{}

func (fnvHasher) Sum64(data []byte) uint64 {
	h := fnv.New64a()
	h.Write(data)
	return h.Sum64()
}

type ClusterRouter struct {
	self              ClusterMember
	members           map[string]ClusterMember
	ring              *consistent.Consistent
	partitionCount    int
	replicationFactor int
	load              float64
}

func LoadClusterConfig() ClusterConfig {
	cfg := ClusterConfig{
		Enabled:           envBool("AQUIFER_CLUSTER_ENABLED", false),
		Self:              ClusterMember{ID: os.Getenv("AQUIFER_CLUSTER_SELF_ID"), Address: os.Getenv("AQUIFER_CLUSTER_SELF_ADDR")},
		Members:           parseClusterMembers(os.Getenv("AQUIFER_CLUSTER_MEMBERS")),
		PartitionCount:    int(envInt64("AQUIFER_CLUSTER_PARTITIONS", defaultClusterPartitionCount)),
		ReplicationFactor: int(envInt64("AQUIFER_CLUSTER_REPLICATION_FACTOR", defaultClusterReplicationFactor)),
		Load:              envFloat64("AQUIFER_CLUSTER_LOAD", defaultClusterLoad),
	}
	if cfg.PartitionCount <= 0 {
		cfg.PartitionCount = defaultClusterPartitionCount
	}
	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = defaultClusterReplicationFactor
	}
	if cfg.Load < 1 {
		cfg.Load = defaultClusterLoad
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

	members := make([]consistent.Member, 0, len(membersByID))
	for _, m := range membersByID {
		members = append(members, m)
	}
	if len(members) == 0 {
		return nil
	}

	return &ClusterRouter{
		self:    cfg.Self,
		members: membersByID,
		ring: consistent.New(members, consistent.Config{
			PartitionCount:    cfg.PartitionCount,
			ReplicationFactor: cfg.ReplicationFactor,
			Load:              cfg.Load,
			Hasher:            fnvHasher{},
		}),
		partitionCount:    cfg.PartitionCount,
		replicationFactor: cfg.ReplicationFactor,
		load:              cfg.Load,
	}
}

func (r *ClusterRouter) OwnerFor(key string) (ClusterMember, bool) {
	if r == nil || key == "" || r.ring == nil {
		return ClusterMember{}, false
	}
	member := r.ring.LocateKey([]byte(key))
	if member == nil {
		return ClusterMember{}, false
	}
	owner, ok := r.members[member.String()]
	return owner, ok
}

func (r *ClusterRouter) IsOwner(key string) bool {
	owner, ok := r.OwnerFor(key)
	return ok && owner.ID == r.self.ID
}

func (r *ClusterRouter) Snapshot() map[string]any {
	if r == nil {
		return nil
	}
	return map[string]any{
		"self":               r.self,
		"members":            len(r.members),
		"partition_count":    r.partitionCount,
		"replication_factor": r.replicationFactor,
		"load":               r.load,
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

func envFloat64(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		log.Printf("cluster: invalid %s=%q, using default %v", key, v, def)
		return def
	}
	return n
}

func clusterRoutingKey(req JobRequest) string {
	return req.UserID
}

func hasClusterForwardedHeader(r *http.Request) bool {
	return r.Header.Get(clusterForwardedHeader) == "true"
}
