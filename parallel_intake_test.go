package aquifer

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Submissions no longer hold the registry or worker lock while writing to
// the store and handing jobs to a queue. That must not lose a job that
// arrives while its queue (or worker) is retiring after going idle: bursts
// are timed to land around the 1s idle timeout, and every job must reach
// the upstream.
func TestEnqueueRacingIdleRetirementLosesNothing(t *testing.T) {
	t.Setenv("AQUIFER_IDLE_TIMEOUT_SECONDS", "1")
	var hits atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		if r.URL.Path == "/work" {
			hits.Add(1)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	dir := t.TempDir()
	store := NewPebbleStore(filepath.Join(dir, "pebble"))
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	registry := NewRegistry(store, &Config{Defaults: RateConfig{RPS: 100000, MaxConcurrent: 64}}, NewBroker(), l8, NoopMetricsAdapter{}, nil)
	app := NewAquifer(store, registry, NewBroker(), l8, nil, nil)
	defer app.Close()

	const rounds, perRound = 6, 40
	gaps := []time.Duration{900 * time.Millisecond, 1000 * time.Millisecond, 1100 * time.Millisecond, 1200 * time.Millisecond, 1050 * time.Millisecond}
	for round := 0; round < rounds; round++ {
		var wg sync.WaitGroup
		for i := 0; i < perRound; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, err := app.Enqueue(JobRequest{UserID: fmt.Sprintf("u%d", i%5), IdempotentKey: fmt.Sprintf("r%d-%d", round, i), URL: upstream.URL + "/work", Method: "POST", WebhookURL: upstream.URL + "/hook"})
				if err != nil {
					t.Error(err)
				}
			}(i)
		}
		wg.Wait()
		if round < rounds-1 {
			time.Sleep(gaps[round%len(gaps)])
		}
	}

	want := int64(rounds * perRound)
	deadline := time.Now().Add(15 * time.Second)
	for hits.Load() < want && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := hits.Load(); got != want {
		t.Fatalf("upstream received %d of %d jobs; some were lost to a retiring queue", got, want)
	}
}
