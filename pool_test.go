package aquifer

import (
	"testing"
	"time"
)

// TestPoolPickIsProportionalToWeight verifies the smooth-weighted-round-robin
// selection actually distributes picks proportional to each member's
// declared rate — not equal-split round robin, which would either starve
// a high-capacity member or overwhelm a low-capacity one.
func TestPoolPickIsProportionalToWeight(t *testing.T) {
	p := newPool("test")
	p.Register("fast", "http://fast", 100, time.Minute)
	p.Register("slow", "http://slow", 25, time.Minute)

	counts := map[string]int{}
	const n = 2000
	for i := 0; i < n; i++ {
		m := p.Pick()
		if m == nil {
			t.Fatal("expected a member, got nil")
		}
		counts[m.ID]++
	}

	// fast (weight 100) should get roughly 4x the picks of slow (weight
	// 25) — allow generous tolerance since this is a scheduling algorithm,
	// not exact arithmetic, but it must be clearly proportional, not equal.
	ratio := float64(counts["fast"]) / float64(counts["slow"])
	if ratio < 3.0 || ratio > 5.5 {
		t.Fatalf("expected roughly 4x more picks for the 100rps member vs the 25rps member, got fast=%d slow=%d (ratio %.2f)", counts["fast"], counts["slow"], ratio)
	}
}

// TestPoolNewMemberSeedsAtCurrentMinimum confirms a newcomer doesn't
// dominate selection just because it starts at virtual position 0 while
// existing members have already advanced past that.
func TestPoolNewMemberSeedsAtCurrentMinimum(t *testing.T) {
	p := newPool("test")
	p.Register("veteran", "http://veteran", 50, time.Minute)

	// Advance the veteran's virtual position well past 0.
	for i := 0; i < 100; i++ {
		p.Pick()
	}

	p.Register("newcomer", "http://newcomer", 50, time.Minute)

	counts := map[string]int{}
	for i := 0; i < 200; i++ {
		counts[p.Pick().ID]++
	}

	// With equal weight and a fair seed, both should get a reasonably
	// even share going forward — a newcomer seeded at 0 instead of the
	// current minimum would dominate this window almost completely.
	if counts["newcomer"] < 60 || counts["newcomer"] > 140 {
		t.Fatalf("expected roughly even split after fair seeding, got veteran=%d newcomer=%d", counts["veteran"], counts["newcomer"])
	}
}

func TestPoolPickOnEmptyPoolReturnsNil(t *testing.T) {
	p := newPool("empty")
	if m := p.Pick(); m != nil {
		t.Fatalf("expected nil from an empty pool, got %+v", m)
	}
}

// TestPoolReputationDecaysAndRecovers verifies the halving-on-failure,
// nudge-up-on-success shape, and that a single success doesn't instantly
// restore full trust after a bad streak.
func TestPoolReputationDecaysAndRecovers(t *testing.T) {
	p := newPool("test")
	p.Register("flaky", "http://flaky", 10, time.Minute)

	if got := p.members["flaky"].Reputation(); got != 1.0 {
		t.Fatalf("expected reputation 1.0 on registration, got %v", got)
	}

	p.RecordFailure("flaky")
	after1 := p.members["flaky"].Reputation()
	if after1 != 0.5 {
		t.Fatalf("expected reputation 0.5 after one failure, got %v", after1)
	}

	p.RecordFailure("flaky")
	after2 := p.members["flaky"].Reputation()
	if after2 != 0.25 {
		t.Fatalf("expected reputation 0.25 after two failures, got %v", after2)
	}

	p.RecordSuccess("flaky")
	afterRecover := p.members["flaky"].Reputation()
	if afterRecover <= after2 || afterRecover >= 1.0 {
		t.Fatalf("expected a partial recovery after one success, not full trust — got %v (was %v)", afterRecover, after2)
	}
}

func TestPoolHeartbeatGraduallyRecoversReputation(t *testing.T) {
	p := newPool("test")
	p.Register("restarted", "http://old", 10, time.Minute)
	p.RecordFailure("restarted")
	p.RecordFailure("restarted")

	before := p.members["restarted"].Reputation()
	p.Register("restarted", "http://new", 10, time.Minute)
	after := p.members["restarted"].Reputation()

	if after <= before {
		t.Fatalf("expected heartbeat registration to recover reputation, got %.2f -> %.2f", before, after)
	}
	if after >= 1.0 {
		t.Fatalf("heartbeat should recover reputation gradually, not reset to full trust; got %.2f", after)
	}
}

// TestPoolEvictsAfterSustainedFloor is the concrete answer to "how many
// 500s before we conclude it's gone": not a fixed count on its own, but
// reputation staying at or below the floor for the sustained window.
// Under halving, 4 consecutive failures reaches ~0.0625, just above the
// 0.05 floor by design (5 failures crosses it), so this drives eviction
// off the same decay mechanism rather than a separate counter.
func TestPoolEvictsAfterSustainedFloor(t *testing.T) {
	p := newPool("test")
	p.Register("dying", "http://dying", 10, time.Minute)

	// Drive reputation below the floor without yet waiting out the
	// sustained window — should not evict immediately even once at the
	// floor, since the window hasn't elapsed.
	var evicted bool
	for i := 0; i < 5; i++ {
		evicted = p.RecordFailure("dying")
	}
	if evicted {
		t.Fatal("should not evict immediately on reaching the floor — must stay there for the sustained window first")
	}
	if p.Size() != 1 {
		t.Fatalf("member should still be present immediately after reaching the floor, got size %d", p.Size())
	}

	// A success partway through should reset the floor timer entirely —
	// simulate by recovering reputation, forcing it back to the floor
	// won't retrigger eviction until a fresh sustained window elapses.
	p.RecordSuccess("dying")
	if !p.members["dying"].floorSince.IsZero() {
		t.Fatal("a success should clear the floor timer even if reputation is still low")
	}
}

// TestPoolEvictsAfterSustainedFloorElapses confirms eviction actually
// happens once the sustained window passes with no interrupting success —
// the companion to TestPoolEvictsAfterSustainedFloor, which only checks
// that it doesn't happen prematurely.
func TestPoolEvictsAfterSustainedFloorElapses(t *testing.T) {
	p := newPool("test")
	p.Register("dying", "http://dying", 10, time.Minute)

	for i := 0; i < 5; i++ {
		p.RecordFailure("dying")
	}
	if p.Size() != 1 {
		t.Fatalf("member should still be present right after reaching the floor, got size %d", p.Size())
	}

	// Manually backdate floorSince past the eviction window rather than
	// sleeping defaultFloorEvictionWindow (12s) in a unit test.
	p.mu.Lock()
	p.members["dying"].floorSince = time.Now().Add(-defaultFloorEvictionWindow - time.Second)
	p.mu.Unlock()

	evicted := p.RecordFailure("dying")
	if !evicted {
		t.Fatal("expected eviction once reputation has been at the floor for the sustained window with no interrupting success")
	}
	if p.Size() != 0 {
		t.Fatalf("expected member to be gone after eviction, got size %d", p.Size())
	}
}

func TestPoolHeartbeatSweepEvictsSilentMembers(t *testing.T) {
	p := newPool("test")
	p.Register("responsive", "http://responsive", 10, 100*time.Millisecond)
	p.Register("silent", "http://silent", 10, 100*time.Millisecond)

	// Refresh "responsive"'s heartbeat partway through — by sweep time,
	// silent will be 350ms stale (past the 300ms/3-miss threshold) while
	// responsive will only be 250ms stale relative to its own refresh.
	time.Sleep(100 * time.Millisecond)
	p.Register("responsive", "http://responsive", 10, 100*time.Millisecond)
	time.Sleep(250 * time.Millisecond)

	p.sweepHeartbeats(defaultHeartbeatMissLimit)

	if p.Size() != 1 {
		t.Fatalf("expected only the responsive member to survive the sweep, got size %d", p.Size())
	}
	if _, ok := p.members["responsive"]; !ok {
		t.Fatal("responsive member should have survived the heartbeat sweep")
	}
}

func TestPoolTotalCapacityIsLiveSum(t *testing.T) {
	p := newPool("test")
	p.Register("a", "http://a", 30, time.Minute)
	p.Register("b", "http://b", 20, time.Minute)

	if got := p.TotalCapacity(); got != 50 {
		t.Fatalf("expected total capacity 50, got %v", got)
	}

	// Degrading one member's reputation should reduce the live total —
	// this is what makes the pool's aggregate ceiling dynamic instead of
	// a static number an operator configures once.
	p.RecordFailure("a")
	if got := p.TotalCapacity(); got != 35 { // 15 (a, halved) + 20 (b)
		t.Fatalf("expected total capacity 35 after halving a's reputation, got %v", got)
	}
}

func TestPoolRegistryRoutesByPoolID(t *testing.T) {
	pr := NewPoolRegistry()
	t.Cleanup(pr.Stop)

	pr.Register("writers", "w1", "http://writer-1", 40, time.Minute)
	pr.Register("readers", "r1", "http://reader-1", 100, time.Minute)

	writers := pr.Get("writers")
	readers := pr.Get("readers")
	if writers == nil || readers == nil {
		t.Fatal("expected both pools to exist after registration")
	}
	if writers.Size() != 1 || readers.Size() != 1 {
		t.Fatalf("expected one member per pool, got writers=%d readers=%d", writers.Size(), readers.Size())
	}
	empty := pr.Get("nonexistent")
	if empty == nil {
		t.Fatal("expected Get to lazily create an empty pool, not return nil, so a pool-backed job always gets pool-mode dispatch behavior")
	}
	if empty.Size() != 0 {
		t.Fatalf("expected the lazily-created pool to have no members, got %d", empty.Size())
	}
}
