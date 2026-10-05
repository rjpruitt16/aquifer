package aquifer

import (
	"context"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultClusterPruneTTLSeconds       = 30
	defaultClusterHeartbeatSeconds      = 5
	defaultClusterInstanceTTLSeconds    = 15
	defaultClusterAssignmentIdleSeconds = 300
	defaultClusterRequestTimeoutMillis  = 50
	defaultClusterMaxActiveUsers        = 100
	defaultClusterValkeyPrefix          = "aqueduct:"
	clusterForwardedHeader              = "X-Aquifer-Cluster-Forwarded"
	clusterProviderStatic               = "static"
	clusterProviderValkey               = "valkey"
)

type ClusterMember struct {
	ID          string `json:"id"`
	Address     string `json:"address"`
	State       string `json:"state,omitempty"`
	ActiveUsers int    `json:"active_users,omitempty"`
	Capacity    int    `json:"user_capacity,omitempty"`
	ReportedAt  int64  `json:"reported_at,omitempty"`
}

func (m ClusterMember) String() string { return m.ID }

type ClusterStateStore interface {
	Heartbeat(context.Context, ClusterMember, time.Duration) error
	Members(context.Context) ([]ClusterMember, error)
	Assign(context.Context, string, []ClusterMember, time.Duration) (ClusterMember, error)
	Renew(context.Context, []string, string, time.Duration) error
	Release(context.Context, string, string) error
	RemoveInstance(context.Context, string) error
	Close() error
}

type ClusterConfig struct {
	Enabled           bool
	Provider          string
	Self              ClusterMember
	Members           []ClusterMember
	PruneTTL          time.Duration
	HeartbeatInterval time.Duration
	InstanceTTL       time.Duration
	AssignmentIdle    time.Duration
	RequestTimeout    time.Duration
	ValkeyURL         string
	ValkeyPrefix      string
	StateStore        ClusterStateStore
}

type clusterScopeActivity struct {
	lastSeen    time.Time
	outstanding int
}

type ClusterRouter struct {
	self        ClusterMember
	provider    string
	pruneTTL    time.Duration
	idleTTL     time.Duration
	timeout     time.Duration
	heartbeat   time.Duration
	instanceTTL time.Duration
	stateStore  ClusterStateStore

	mu          sync.Mutex
	members     map[string]ClusterMember
	prunedUntil map[string]time.Time
	localScopes map[string]*clusterScopeActivity
	nodeState   string
	heartbeatMu sync.Mutex
	syncMu      sync.Mutex
	scopeLocks  [64]sync.Mutex

	storeErrors atomic.Int64
	stop        chan struct{}
	done        chan struct{}
	closeOnce   sync.Once
}

func LoadClusterConfig() ClusterConfig {
	provider := strings.ToLower(strings.TrimSpace(os.Getenv("AQUIFER_CLUSTER_PROVIDER")))
	if provider == "" {
		provider = clusterProviderStatic
	}
	valkeyURL := os.Getenv("AQUIFER_CLUSTER_VALKEY_URL")
	if valkeyURL == "" {
		valkeyURL = os.Getenv("AQUIFER_VALKEY_URL")
	}
	capacity := int(envInt64("AQUIFER_CLUSTER_MAX_ACTIVE_USERS", defaultClusterMaxActiveUsers))
	if capacity <= 0 {
		capacity = defaultClusterMaxActiveUsers
	}
	prefix := os.Getenv("AQUIFER_CLUSTER_VALKEY_PREFIX")
	if prefix == "" {
		prefix = defaultClusterValkeyPrefix
	}

	cfg := ClusterConfig{
		Enabled:           envBool("AQUIFER_CLUSTER_ENABLED", false),
		Provider:          provider,
		Self:              ClusterMember{ID: os.Getenv("AQUIFER_CLUSTER_SELF_ID"), Address: os.Getenv("AQUIFER_CLUSTER_SELF_ADDR"), State: string(LifecycleStateActive), Capacity: capacity},
		Members:           parseClusterMembers(os.Getenv("AQUIFER_CLUSTER_MEMBERS")),
		PruneTTL:          time.Duration(envInt64("AQUIFER_CLUSTER_PRUNE_TTL_SECONDS", defaultClusterPruneTTLSeconds)) * time.Second,
		HeartbeatInterval: time.Duration(envInt64("AQUIFER_CLUSTER_HEARTBEAT_SECONDS", defaultClusterHeartbeatSeconds)) * time.Second,
		InstanceTTL:       time.Duration(envInt64("AQUIFER_CLUSTER_INSTANCE_TTL_SECONDS", defaultClusterInstanceTTLSeconds)) * time.Second,
		AssignmentIdle:    time.Duration(envInt64("AQUIFER_CLUSTER_ASSIGNMENT_IDLE_SECONDS", defaultClusterAssignmentIdleSeconds)) * time.Second,
		RequestTimeout:    time.Duration(envInt64("AQUIFER_CLUSTER_VALKEY_TIMEOUT_MS", defaultClusterRequestTimeoutMillis)) * time.Millisecond,
		ValkeyURL:         valkeyURL,
		ValkeyPrefix:      prefix,
	}
	if cfg.PruneTTL < 0 {
		cfg.PruneTTL = defaultClusterPruneTTLSeconds * time.Second
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = defaultClusterHeartbeatSeconds * time.Second
	}
	if cfg.InstanceTTL < 3*cfg.HeartbeatInterval {
		cfg.InstanceTTL = 3 * cfg.HeartbeatInterval
	}
	if cfg.AssignmentIdle <= 0 {
		cfg.AssignmentIdle = defaultClusterAssignmentIdleSeconds * time.Second
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = defaultClusterRequestTimeoutMillis * time.Millisecond
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
	if cfg.Provider == "" {
		cfg.Provider = clusterProviderStatic
	}
	if cfg.Provider != clusterProviderStatic && cfg.Provider != clusterProviderValkey {
		log.Printf("cluster: unsupported provider %q; using static rendezvous", cfg.Provider)
		cfg.Provider = clusterProviderStatic
	}
	if cfg.Self.State == "" {
		cfg.Self.State = string(LifecycleStateActive)
	}
	if cfg.Self.Capacity <= 0 {
		cfg.Self.Capacity = defaultClusterMaxActiveUsers
	}
	if cfg.PruneTTL <= 0 {
		cfg.PruneTTL = defaultClusterPruneTTLSeconds * time.Second
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = defaultClusterHeartbeatSeconds * time.Second
	}
	if cfg.InstanceTTL < 3*cfg.HeartbeatInterval {
		cfg.InstanceTTL = 3 * cfg.HeartbeatInterval
	}
	if cfg.AssignmentIdle <= 0 {
		cfg.AssignmentIdle = defaultClusterAssignmentIdleSeconds * time.Second
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = defaultClusterRequestTimeoutMillis * time.Millisecond
	}

	members := make(map[string]ClusterMember, len(cfg.Members)+1)
	for _, member := range cfg.Members {
		if member.ID == "" || member.Address == "" {
			continue
		}
		if member.Capacity <= 0 {
			member.Capacity = cfg.Self.Capacity
		}
		members[member.ID] = member
	}
	members[cfg.Self.ID] = cfg.Self

	store := cfg.StateStore
	if cfg.Provider == clusterProviderValkey && store == nil {
		var err error
		store, err = NewValkeyClusterStateStore(cfg.ValkeyURL, cfg.ValkeyPrefix)
		if err != nil {
			log.Printf("cluster: Valkey coordination unavailable at startup, using cached/static rendezvous: %v", err)
		}
	}

	router := &ClusterRouter{
		self:        cfg.Self,
		provider:    cfg.Provider,
		pruneTTL:    cfg.PruneTTL,
		idleTTL:     cfg.AssignmentIdle,
		timeout:     cfg.RequestTimeout,
		heartbeat:   cfg.HeartbeatInterval,
		instanceTTL: cfg.InstanceTTL,
		stateStore:  store,
		members:     members,
		prunedUntil: make(map[string]time.Time),
		localScopes: make(map[string]*clusterScopeActivity),
		nodeState:   string(LifecycleStateActive),
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}
	if store != nil {
		router.syncState()
		go router.stateLoop()
	}
	return router
}

func (r *ClusterRouter) OwnerFor(key string) (ClusterMember, bool) {
	return r.ResolveOwner(key, nil)
}

func (r *ClusterRouter) ResolveOwner(key string, excluded map[string]bool) (ClusterMember, bool) {
	ranked := r.rankedOwners(key, excluded)
	if len(ranked) == 0 {
		return ClusterMember{}, false
	}
	owner := ranked[0]
	if r.stateStore != nil {
		scopeLock := r.scopeLock(key)
		scopeLock.Lock()
		defer scopeLock.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
		assigned, err := r.stateStore.Assign(ctx, key, ranked, r.assignmentTTL())
		cancel()
		if err != nil {
			r.storeErrors.Add(1)
		} else if assigned.ID != "" && assigned.Address != "" {
			owner = assigned
		}
		if owner.ID == r.self.ID {
			r.touchLocalScopeLocked(key)
		}
	}
	return owner, true
}

func (r *ClusterRouter) IsOwner(key string) bool {
	owner, ok := r.OwnerFor(key)
	return ok && owner.ID == r.self.ID
}

func (r *ClusterRouter) RankedOwners(key string) []ClusterMember {
	return r.rankedOwners(key, nil)
}

func (r *ClusterRouter) rankedOwners(key string, excluded map[string]bool) []ClusterMember {
	if r == nil || key == "" {
		return nil
	}
	now := time.Now()
	r.mu.Lock()
	r.expirePrunedLocked(now)
	members := make([]ClusterMember, 0, len(r.members))
	for _, member := range r.members {
		if excluded[member.ID] || now.Before(r.prunedUntil[member.ID]) || member.State == string(LifecycleStateDraining) || member.State == string(LifecycleStateOffline) {
			continue
		}
		members = append(members, member)
	}
	r.mu.Unlock()

	sort.Slice(members, func(i, j int) bool {
		left := rendezvousScore(members[i].ID, key)
		right := rendezvousScore(members[j].ID, key)
		if left == right {
			return members[i].ID < members[j].ID
		}
		return left > right
	})
	return members
}

func (r *ClusterRouter) MarkUnavailable(key, id string) {
	if r == nil || id == "" {
		return
	}
	r.PruneMember(id)
	if r.stateStore != nil {
		ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
		if err := r.stateStore.Release(ctx, key, id); err != nil {
			r.storeErrors.Add(1)
		}
		cancel()
	}
}

func (r *ClusterRouter) PruneMember(id string) {
	if r == nil || id == "" || id == r.self.ID || r.pruneTTL <= 0 {
		return
	}
	r.mu.Lock()
	r.prunedUntil[id] = time.Now().Add(r.pruneTTL)
	r.mu.Unlock()
}

func (r *ClusterRouter) TrackLocalJob(key string) {
	if r == nil || r.stateStore == nil || key == "" {
		return
	}
	scopeLock := r.scopeLock(key)
	scopeLock.Lock()
	defer scopeLock.Unlock()

	r.mu.Lock()
	activity := r.localScopes[key]
	if activity == nil {
		activity = &clusterScopeActivity{}
		r.localScopes[key] = activity
	}
	activity.lastSeen = time.Now()
	activity.outstanding++
	r.mu.Unlock()
}

func (r *ClusterRouter) CompleteLocalJob(key string) {
	if r == nil || r.stateStore == nil || key == "" {
		return
	}
	scopeLock := r.scopeLock(key)
	scopeLock.Lock()
	defer scopeLock.Unlock()

	r.mu.Lock()
	if activity := r.localScopes[key]; activity != nil {
		if activity.outstanding > 0 {
			activity.outstanding--
		}
		activity.lastSeen = time.Now()
	}
	r.mu.Unlock()
}

func (r *ClusterRouter) SetState(state LifecycleState) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.nodeState = string(state)
	self := r.members[r.self.ID]
	self.State = string(state)
	r.members[r.self.ID] = self
	r.mu.Unlock()
	if r.stateStore != nil {
		r.publishHeartbeat(time.Now())
	}
}

func (r *ClusterRouter) Close() {
	if r == nil {
		return
	}
	r.closeOnce.Do(func() {
		if r.stateStore == nil {
			return
		}
		close(r.stop)
		<-r.done
		ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
		_ = r.stateStore.RemoveInstance(ctx, r.self.ID)
		cancel()
		_ = r.stateStore.Close()
	})
}

func (r *ClusterRouter) stateLoop() {
	defer close(r.done)
	ticker := time.NewTicker(r.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.syncState()
		case <-r.stop:
			return
		}
	}
}

func (r *ClusterRouter) syncState() {
	if r == nil || r.stateStore == nil {
		return
	}
	r.syncMu.Lock()
	defer r.syncMu.Unlock()

	now := time.Now()
	var renew, release []string
	r.mu.Lock()
	for key, activity := range r.localScopes {
		if activity.outstanding == 0 && now.Sub(activity.lastSeen) >= r.idleTTL {
			release = append(release, key)
			continue
		}
		renew = append(renew, key)
	}
	r.mu.Unlock()

	self := r.publishHeartbeat(now)

	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	if err := r.stateStore.Renew(ctx, renew, r.self.ID, r.assignmentTTL()); err != nil {
		r.storeErrors.Add(1)
	}
	cancel()

	for _, key := range release {
		scopeLock := r.scopeLock(key)
		scopeLock.Lock()

		r.mu.Lock()
		activity := r.localScopes[key]
		stillIdle := activity != nil && activity.outstanding == 0 && time.Since(activity.lastSeen) >= r.idleTTL
		r.mu.Unlock()
		if !stillIdle {
			scopeLock.Unlock()
			continue
		}

		ctx, cancel = context.WithTimeout(context.Background(), r.timeout)
		err := r.stateStore.Release(ctx, key, r.self.ID)
		cancel()
		if err != nil {
			r.storeErrors.Add(1)
			scopeLock.Unlock()
			continue
		}
		r.mu.Lock()
		delete(r.localScopes, key)
		r.mu.Unlock()
		scopeLock.Unlock()
	}

	ctx, cancel = context.WithTimeout(context.Background(), r.timeout)
	members, err := r.stateStore.Members(ctx)
	cancel()
	if err != nil {
		r.storeErrors.Add(1)
		return
	}
	updated := make(map[string]ClusterMember, len(members)+1)
	for _, member := range members {
		if member.ID != "" && member.Address != "" {
			updated[member.ID] = member
		}
	}
	r.mu.Lock()
	self.State = r.nodeState
	self.ActiveUsers = r.activeScopeCountLocked(time.Now())
	updated[r.self.ID] = self
	r.members = updated
	r.mu.Unlock()
}

func (r *ClusterRouter) publishHeartbeat(now time.Time) ClusterMember {
	r.heartbeatMu.Lock()
	defer r.heartbeatMu.Unlock()

	r.mu.Lock()
	self := r.self
	self.ActiveUsers = r.activeScopeCountLocked(now)
	self.State = r.nodeState
	self.ReportedAt = now.UnixMilli()
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	if err := r.stateStore.Heartbeat(ctx, self, r.instanceTTL); err != nil {
		r.storeErrors.Add(1)
	}
	cancel()
	return self
}

func (r *ClusterRouter) activeScopeCountLocked(now time.Time) int {
	active := 0
	for _, activity := range r.localScopes {
		if activity.outstanding > 0 || now.Sub(activity.lastSeen) < r.idleTTL {
			active++
		}
	}
	return active
}

func (r *ClusterRouter) assignmentTTL() time.Duration {
	return r.idleTTL + 2*r.heartbeat
}

func (r *ClusterRouter) touchLocalScopeLocked(key string) {
	r.mu.Lock()
	activity := r.localScopes[key]
	if activity == nil {
		activity = &clusterScopeActivity{}
		r.localScopes[key] = activity
	}
	activity.lastSeen = time.Now()
	r.mu.Unlock()
}

func (r *ClusterRouter) scopeLock(key string) *sync.Mutex {
	score := rendezvousScore("aqueduct-scope-lock", key)
	index := (int(score[0])*31 + int(score[1])) % len(r.scopeLocks)
	return &r.scopeLocks[index]
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
	r.mu.Lock()
	members := len(r.members)
	active := r.activeScopeCountLocked(time.Now())
	state := r.nodeState
	self := r.self
	self.ActiveUsers = active
	self.State = state
	r.mu.Unlock()
	algorithm := "rendezvous"
	if r.stateStore != nil {
		algorithm = "bounded-load-rendezvous"
	}
	return map[string]any{
		"self":                  self,
		"members":               members,
		"algorithm":             algorithm,
		"provider":              r.provider,
		"state":                 state,
		"active_users":          active,
		"user_capacity":         r.self.Capacity,
		"prune_ttl_seconds":     int64(r.pruneTTL / time.Second),
		"pruned_members":        r.prunedCount(),
		"coordination_failures": r.storeErrors.Load(),
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
		if id != "" && address != "" {
			members = append(members, ClusterMember{ID: id, Address: address})
		}
	}
	return members
}

// Shared-scope requests route by their dedup identity so every caller asking
// for the same resource lands on the node whose store can coalesce them;
// everything else keeps per-user routing.
func clusterRoutingKey(req JobRequest) string {
	if req.IdempotencyScope == IdempotencyScopeShared {
		return "shared\x00" + req.IdempotentKey
	}
	return req.UserID
}

func hasClusterForwardedHeader(r *http.Request) bool {
	return r.Header.Get(clusterForwardedHeader) == "true"
}
