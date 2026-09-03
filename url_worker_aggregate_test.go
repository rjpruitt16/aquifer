package aquifer

import (
	"path/filepath"
	"testing"
	"time"
)

// TestAggregateBudgetThrottlesSiblingQueues proves the fix for the gap
// found this session: NewAccountQueue always received the URLWorker's
// full configured RPS regardless of how many sibling queues already
// existed for the same upstream, so N simultaneously active tenants could
// each independently dispatch at the full ceiling — real aggregate load
// up to N times the intended rate. checkAndThrottle should bring the sum
// back down to the worker's actual budget.
func TestAggregateBudgetThrottlesSiblingQueues(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(filepath.Join(dir, "aquifer.db"))
	t.Cleanup(func() {
		store.Close()
		time.Sleep(20 * time.Millisecond)
	})
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))

	const ceiling = 10.0
	w := NewURLWorker("https://example.com", ceiling, 5, nil, store, broker, l8, NoopMetricsAdapter{}, func(string, string, string, map[string]any) {}, func(string) {})

	// Three tenant queues, each spawned with the worker's full ceiling —
	// this exact call shape (w.rps, unchanged, per queue) is what
	// url_worker.go:Enqueue does for every new AccountQueue, confirmed
	// via code read earlier this session. Before the aggregate-budget
	// fix, nothing bounded their combined rate.
	q1 := NewAccountQueue("tenant-1", w.domain, ceiling, 5, nil, store, broker, l8, NoopMetricsAdapter{}, func(string, string, string, map[string]any) {}, func(string) {}, false, func(bool) {})
	q2 := NewAccountQueue("tenant-2", w.domain, ceiling, 5, nil, store, broker, l8, NoopMetricsAdapter{}, func(string, string, string, map[string]any) {}, func(string) {}, false, func(bool) {})
	q3 := NewAccountQueue("tenant-3", w.domain, ceiling, 5, nil, store, broker, l8, NoopMetricsAdapter{}, func(string, string, string, map[string]any) {}, func(string) {}, false, func(bool) {})

	// Give each queue's run() goroutine a moment to set its initial
	// currentRPS before we read it.
	time.Sleep(20 * time.Millisecond)

	w.mu.Lock()
	w.queues["tenant-1"] = q1
	w.queues["tenant-2"] = q2
	w.queues["tenant-3"] = q3
	w.mu.Unlock()

	before := q1.RPS() + q2.RPS() + q3.RPS()
	if before <= ceiling {
		t.Fatalf("test setup bug: expected the three queues to start out already over budget (3x%.0f), got sum %.2f", ceiling, before)
	}

	w.checkAndThrottle()

	// Throttle() delivers asynchronously via each queue's done channel —
	// give the dispatch loops a moment to apply it.
	time.Sleep(50 * time.Millisecond)

	after := q1.RPS() + q2.RPS() + q3.RPS()
	if after > ceiling+0.01 {
		t.Fatalf("expected aggregate rate to be throttled down to the %.0f budget, got %.2f (was %.2f before)", ceiling, after, before)
	}
}

// TestAggregateBudgetLeavesSingleQueueAlone confirms a lone active queue
// is never throttled by this mechanism — there's nothing to divide a
// shared budget between with only one tenant active.
func TestAggregateBudgetLeavesSingleQueueAlone(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(filepath.Join(dir, "aquifer.db"))
	t.Cleanup(func() {
		store.Close()
		time.Sleep(20 * time.Millisecond)
	})
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))

	const ceiling = 10.0
	w := NewURLWorker("https://example.com", ceiling, 5, nil, store, broker, l8, NoopMetricsAdapter{}, func(string, string, string, map[string]any) {}, func(string) {})
	q1 := NewAccountQueue("tenant-1", w.domain, ceiling, 5, nil, store, broker, l8, NoopMetricsAdapter{}, func(string, string, string, map[string]any) {}, func(string) {}, false, func(bool) {})

	time.Sleep(20 * time.Millisecond)
	w.mu.Lock()
	w.queues["tenant-1"] = q1
	w.mu.Unlock()

	before := q1.RPS()
	w.checkAndThrottle()
	time.Sleep(50 * time.Millisecond)
	after := q1.RPS()

	if before != after {
		t.Fatalf("a single active queue should never be throttled by the aggregate check, got %.2f -> %.2f", before, after)
	}
}
