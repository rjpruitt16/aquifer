package aquifer

// Performance benchmarks: run with `make perf`. They measure Aquifer's own
// overhead (storage writes, queueing, dispatch, webhooks), with pacing set so
// high it never waits. Compare numbers on the same machine only: storage sync
// cost varies a lot by OS and disk (macOS fsync is far slower than Linux).

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

type perfApp struct {
	app      *Aquifer
	upstream *httptest.Server
	webhook  *httptest.Server
	arrived  chan time.Time
	hits     atomic.Int64
	hooks    atomic.Int64
}

func newPerfApp(b *testing.B, backend string) *perfApp {
	b.Helper()
	dir := b.TempDir()
	var store JobStore
	switch backend {
	case "sqlite":
		store = NewStore(filepath.Join(dir, "aquifer.db"))
	case "pebble":
		store = NewPebbleStore(filepath.Join(dir, "pebble"))
	}
	p := &perfApp{arrived: make(chan time.Time, 1024)}
	p.upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		p.hits.Add(1)
		select {
		case p.arrived <- time.Now():
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	p.webhook = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		p.hooks.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	cfg := &Config{Defaults: RateConfig{RPS: 100000, MaxConcurrent: 64}}
	registry := NewRegistry(store, cfg, broker, l8, NoopMetricsAdapter{}, nil)
	p.app = NewAquifer(store, registry, broker, l8, NewAdmissionController(AdmissionLimits{}, filepath.Join(dir, "aquifer.db")), nil)
	b.Cleanup(func() {
		p.app.Close()
		p.upstream.Close()
		p.webhook.Close()
	})
	return p
}

func (p *perfApp) job(key string) JobRequest {
	return JobRequest{UserID: "perf", IdempotentKey: key, URL: p.upstream.URL, Method: "POST", WebhookURL: p.webhook.URL}
}

// BenchmarkJobLatency sends one job at a time and splits its trip into:
// accept (validated and durably stored, i.e. POST /jobs returns), queue
// (accepted until the upstream receives it), and total.
func BenchmarkJobLatency(b *testing.B) {
	for _, backend := range []string{"sqlite", "pebble"} {
		b.Run(backend, func(b *testing.B) {
			p := newPerfApp(b, backend)
			var accept, queue time.Duration
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				if _, err := p.app.Enqueue(p.job(fmt.Sprintf("lat-%d", i))); err != nil {
					b.Fatal(err)
				}
				accepted := time.Now()
				select {
				case arrived := <-p.arrived:
					accept += accepted.Sub(start)
					queue += arrived.Sub(accepted)
				case <-time.After(10 * time.Second):
					b.Fatal("job never reached the upstream")
				}
			}
			b.StopTimer()
			n := float64(b.N)
			b.ReportMetric(float64(accept.Microseconds())/n/1000, "accept_ms/op")
			b.ReportMetric(float64(queue.Microseconds())/n/1000, "queue_ms/op")
			b.ReportMetric(float64((accept+queue).Microseconds())/n/1000, "total_ms/op")
		})
	}
}

// BenchmarkPipelineThroughput submits jobs from many concurrent callers and
// reports how many per second make it all the way through: accepted,
// dispatched, completed, and their webhooks delivered.
func BenchmarkPipelineThroughput(b *testing.B) {
	for _, backend := range []string{"sqlite", "pebble"} {
		b.Run(backend, func(b *testing.B) {
			p := newPerfApp(b, backend)
			const callers = 8
			jobs := b.N
			b.ResetTimer()
			start := time.Now()

			var next atomic.Int64
			var wg sync.WaitGroup
			for c := 0; c < callers; c++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						i := next.Add(1) - 1
						if i >= int64(jobs) {
							return
						}
						if _, err := p.app.Enqueue(p.job(fmt.Sprintf("tp-%d", i))); err != nil {
							b.Error(err)
							return
						}
					}
				}()
			}
			wg.Wait()
			deadline := time.Now().Add(2 * time.Minute)
			for p.hooks.Load() < int64(jobs) && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			elapsed := time.Since(start)
			b.StopTimer()
			if done := p.hooks.Load(); done < int64(jobs) {
				b.Fatalf("only %d of %d jobs completed with a webhook", done, jobs)
			}
			b.ReportMetric(float64(jobs)/elapsed.Seconds(), "jobs/s")
		})
	}
}
