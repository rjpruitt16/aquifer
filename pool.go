package aquifer

import (
	"container/heap"
	"math"
	"sync"
	"time"
)

const (
	// defaultReputationFloor is the threshold below which a member is a
	// candidate for eviction. Under the halving-per-failure scheme, 4
	// consecutive failures brings reputation to 2^-4 = 0.0625, just above
	// this floor — so a member has to be consistently bad, not unlucky
	// once, before it's a candidate for removal at all.
	defaultReputationFloor = 0.05

	// defaultFloorEvictionWindow is how long reputation must stay at or
	// below the floor, continuously, before a member is actually removed.
	// This (not the failure count alone) is what answers "how many 500s
	// before we conclude it's gone" — a member recovering even once
	// resets the clock, so a single successful request in the middle of a
	// bad streak doesn't count against it, but sustained badness does.
	defaultFloorEvictionWindow = 12 * time.Second

	// reputationRecoveryFactor nudges reputation back toward 1.0 on each
	// success rather than resetting it instantly — mirrors the existing
	// Retry-After backoff's "reset to base the moment a request is
	// allowed again" being a step change, but reputation recovery is
	// deliberately gradual since a single success after a long bad streak
	// shouldn't immediately restore full trust.
	reputationRecoveryFactor = 1.5

	// defaultHeartbeatMissLimit is how many consecutive missed heartbeats
	// (relative to the member's own declared interval) evict a member for
	// having gone silent, independent of the reputation/failure path.
	defaultHeartbeatMissLimit = 3

	// heartbeatSweepInterval is how often the registry checks all pools
	// for members that have gone silent.
	heartbeatSweepInterval = 5 * time.Second
)

// PoolMember is one registered target within a pool — a service instance
// that pinged in with its own address and declared capacity.
type PoolMember struct {
	ID                string
	Address           string
	DeclaredRPS       float64
	HeartbeatInterval time.Duration
	LastHeartbeat     time.Time

	reputation      float64 // 1.0 = fully trusted, decays toward 0 on failure
	virtualPosition float64 // heap key for virtual-time weighted round robin
	floorSince      time.Time
	heapIndex       int
}

// weight is the member's effective share of dispatch: its own declared
// capacity scaled by how much it's currently trusted. A member reporting
// a high rate but failing consistently still gets throttled down by this,
// even if its last-reported header was optimistic — this is the system's
// own defense in depth on top of operators being told to under-report
// their true capacity as a buffer.
func (m *PoolMember) weight() float64 {
	w := m.DeclaredRPS * m.reputation
	if w <= 0 {
		w = 0.001
	}
	return w
}

func (m *PoolMember) Reputation() float64 { return m.reputation }

// memberHeap is a min-heap on virtualPosition implementing container/heap,
// with heapIndex tracked on each element so a specific member can be
// updated in place (heap.Fix) in O(log n) instead of needing a linear
// scan to find it first.
type memberHeap []*PoolMember

func (h memberHeap) Len() int           { return len(h) }
func (h memberHeap) Less(i, j int) bool { return h[i].virtualPosition < h[j].virtualPosition }
func (h memberHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

func (h *memberHeap) Push(x any) {
	m := x.(*PoolMember)
	m.heapIndex = len(*h)
	*h = append(*h, m)
}

func (h *memberHeap) Pop() any {
	old := *h
	n := len(old)
	m := old[n-1]
	old[n-1] = nil
	m.heapIndex = -1
	*h = old[:n-1]
	return m
}

// Pool is one named group of registered members. Selection uses
// virtual-time weighted round robin: each pick advances the chosen
// member's virtual position by 1/weight (a higher-weight member advances
// less per pick, so it comes back to the front of the heap sooner and
// gets picked proportionally more often), giving O(log n) per dispatch —
// the naive nginx-style smooth-weighted-round-robin algorithm (recompute
// every member's running weight on every pick) is O(n) per pick instead;
// this avoids that by only ever touching the single member actually
// chosen.
type Pool struct {
	mu      sync.Mutex
	id      string
	members map[string]*PoolMember
	h       memberHeap
}

func newPool(id string) *Pool {
	return &Pool{id: id, members: make(map[string]*PoolMember)}
}

// Register adds a new member or refreshes an existing one. The same call
// serves as both initial registration and heartbeat — re-calling it
// resets LastHeartbeat and updates declared capacity, so a member that
// wants to lower or raise its reported rate just re-registers.
func (p *Pool) Register(id, address string, declaredRPS float64, heartbeatInterval time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if m, ok := p.members[id]; ok {
		m.Address = address
		m.DeclaredRPS = declaredRPS
		m.HeartbeatInterval = heartbeatInterval
		m.LastHeartbeat = time.Now()
		return
	}

	// A brand new member is seeded at the current minimum virtual
	// position (not 0) so it competes fairly immediately — starting at 0
	// when existing members have advanced well past that would let a
	// newcomer dominate selection until it "catches up."
	startPos := 0.0
	if len(p.h) > 0 {
		startPos = p.h[0].virtualPosition
	}
	m := &PoolMember{
		ID:                id,
		Address:           address,
		DeclaredRPS:       declaredRPS,
		HeartbeatInterval: heartbeatInterval,
		LastHeartbeat:     time.Now(),
		reputation:        1.0,
		virtualPosition:   startPos,
	}
	p.members[id] = m
	heap.Push(&p.h, m)
}

// Pick selects a member proportional to declared_rate x reputation and
// advances its virtual position. Returns nil if the pool has no members.
func (p *Pool) Pick() *PoolMember {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.h) == 0 {
		return nil
	}
	m := p.h[0]
	m.virtualPosition += 1.0 / m.weight()
	heap.Fix(&p.h, 0)
	return m
}

// RecordSuccess nudges a member's reputation back toward full trust and
// unconditionally clears any in-progress floor-eviction timer — the
// eviction criterion is sustained badness with zero successes in
// between, not a numeric reputation threshold held for a duration, so
// any success resets the clock even if the reputation number itself
// hasn't recovered above the floor yet.
func (p *Pool) RecordSuccess(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	m, ok := p.members[id]
	if !ok {
		return
	}
	m.reputation = math.Min(1.0, m.reputation*reputationRecoveryFactor)
	m.floorSince = time.Time{}
}

// RecordFailure halves a member's reputation — the same shape as the
// existing Retry-After backoff (doubles per consecutive event), just
// applied as a share reduction instead of a delay increase. Once
// reputation has been at or below the floor continuously for
// defaultFloorEvictionWindow, the member is evicted. Returns whether this
// call caused an eviction.
func (p *Pool) RecordFailure(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	m, ok := p.members[id]
	if !ok {
		return false
	}
	m.reputation *= 0.5
	if m.reputation <= defaultReputationFloor {
		if m.floorSince.IsZero() {
			m.floorSince = time.Now()
		} else if time.Since(m.floorSince) >= defaultFloorEvictionWindow {
			p.removeLocked(id)
			return true
		}
	} else {
		m.floorSince = time.Time{}
	}
	return false
}

func (p *Pool) removeLocked(id string) {
	m, ok := p.members[id]
	if !ok {
		return
	}
	if m.heapIndex >= 0 && m.heapIndex < len(p.h) {
		heap.Remove(&p.h, m.heapIndex)
	}
	delete(p.members, id)
}

// sweepHeartbeats evicts members that have missed too many consecutive
// expected heartbeats, independent of the reputation/failure path — this
// catches a member that's gone silent (crashed, network partitioned)
// even if it was never actually dispatched to and so never had a chance
// to fail a request.
func (p *Pool) sweepHeartbeats(missLimit int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	for id, m := range p.members {
		if m.HeartbeatInterval <= 0 {
			continue
		}
		if now.Sub(m.LastHeartbeat) > m.HeartbeatInterval*time.Duration(missLimit) {
			p.removeLocked(id)
		}
	}
}

// TotalCapacity is the live sum of every member's current effective
// weight — the pool's aggregate ceiling grows and shrinks automatically
// as members register, degrade, or drop out, rather than being a static
// number an operator configures once.
func (p *Pool) TotalCapacity() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	var total float64
	for _, m := range p.members {
		total += m.weight()
	}
	return total
}

func (p *Pool) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.members)
}

func (p *Pool) Snapshot() []map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]map[string]any, 0, len(p.members))
	for _, m := range p.members {
		out = append(out, map[string]any{
			"id":           m.ID,
			"address":      m.Address,
			"declared_rps": m.DeclaredRPS,
			"reputation":   m.reputation,
		})
	}
	return out
}

// PoolRegistry owns every named pool. One registry per Aquifer instance.
type PoolRegistry struct {
	mu    sync.Mutex
	pools map[string]*Pool
	stop  chan struct{}
}

func NewPoolRegistry() *PoolRegistry {
	pr := &PoolRegistry{
		pools: make(map[string]*Pool),
		stop:  make(chan struct{}),
	}
	go pr.heartbeatSweepLoop()
	return pr
}

func (pr *PoolRegistry) Register(poolID, memberID, address string, declaredRPS float64, heartbeatInterval time.Duration) {
	pr.mu.Lock()
	p, ok := pr.pools[poolID]
	if !ok {
		p = newPool(poolID)
		pr.pools[poolID] = p
	}
	pr.mu.Unlock()
	p.Register(memberID, address, declaredRPS, heartbeatInterval)
}

// Get returns the named pool, creating an empty one if nobody has
// registered to it yet. A job dispatched to a pool that exists but has
// zero members is a different, correctly-handled state (fails cleanly
// with "no pool members registered") from a job that isn't pool-backed
// at all — returning nil here would collapse that distinction and let a
// pool-backed job silently fall through to a non-pool dispatch path with
// an empty URL instead.
func (pr *PoolRegistry) Get(poolID string) *Pool {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	p, ok := pr.pools[poolID]
	if !ok {
		p = newPool(poolID)
		pr.pools[poolID] = p
	}
	return p
}

func (pr *PoolRegistry) heartbeatSweepLoop() {
	ticker := time.NewTicker(heartbeatSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			pr.mu.Lock()
			pools := make([]*Pool, 0, len(pr.pools))
			for _, p := range pr.pools {
				pools = append(pools, p)
			}
			pr.mu.Unlock()
			for _, p := range pools {
				p.sweepHeartbeats(defaultHeartbeatMissLimit)
			}
		case <-pr.stop:
			return
		}
	}
}

func (pr *PoolRegistry) Stop() {
	close(pr.stop)
}

func (pr *PoolRegistry) Snapshot() map[string]any {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	out := make(map[string]any, len(pr.pools))
	for id, p := range pr.pools {
		out[id] = map[string]any{
			"members":            p.Snapshot(),
			"total_capacity_rps": p.TotalCapacity(),
		}
	}
	return out
}
