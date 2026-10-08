// loadgen: open-loop ramp of POST /jobs against one target, plus the instant
// upstream (/work) and webhook receiver (/hook) the jobs point at. A step is
// sustained when the target accepts what is offered and completes (webhook
// delivered) about as fast as it accepts, without the backlog growing.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var jobIDRe = regexp.MustCompile(`"job_id"\s*:\s*"([^"]+)"`)

type tracker struct {
	mu        sync.Mutex
	submitted map[string]time.Time
	e2e       []time.Duration
	hooks     atomic.Int64
	work      atomic.Int64
}

func (t *tracker) hook(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	now := time.Now()
	t.hooks.Add(1)
	if m := jobIDRe.FindSubmatch(body); m != nil {
		t.mu.Lock()
		if at, ok := t.submitted[string(m[1])]; ok {
			t.e2e = append(t.e2e, now.Sub(at))
			delete(t.submitted, string(m[1]))
		}
		t.mu.Unlock()
	}
	w.Header().Set("X-Aqueduct-Rps", "100000")
	w.Header().Set("X-Aqueduct-Max-Concurrent", "512")
	w.WriteHeader(200)
}

func pct(d []time.Duration, p float64) float64 {
	if len(d) == 0 {
		return 0
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	return float64(d[int(float64(len(d)-1)*p)].Microseconds()) / 1000
}

func main() {
	target := flag.String("target", "", "target base URL")
	self := flag.String("self", "", "base URL the target uses to reach this machine")
	rates := flag.String("rates", "100,200,400,700,1000,1500,2000,3000,4000,6000,8000", "steps, jobs/s")
	stepDur := flag.Duration("step", 20*time.Second, "duration per step")
	serveOnly := flag.Bool("serve", false, "only run the sink")
	flag.Parse()

	t := &tracker{submitted: map[string]time.Time{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/l8", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) })
	mux.HandleFunc("/work", func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		t.work.Add(1)
		w.Header().Set("X-Aqueduct-Rps", "100000")
		w.Header().Set("X-Aqueduct-Max-Concurrent", "512")
		w.WriteHeader(200)
		w.Write([]byte(`{"ok":true}`))
	})
	mux.HandleFunc("/hook", t.hook)
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(err)
	}
	go http.Serve(ln, mux)
	if *serveOnly {
		select {}
	}

	client := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{
		MaxIdleConns: 4096, MaxIdleConnsPerHost: 4096, IdleConnTimeout: 60 * time.Second,
	}}
	run := strconv.FormatInt(time.Now().Unix(), 36)
	sem := make(chan struct{}, 4096)
	var totalAccepted int64

	fmt.Printf("target %s\n%7s %8s %8s %7s %6s %9s %9s %9s %8s %9s %9s\n", *target,
		"offered", "acc/s", "done/s", "429/s", "err/s", "acc_p50", "acc_p99", "backlog", "e2e_p50", "e2e_p99", "verdict")

	for _, rs := range strings.Split(*rates, ",") {
		rate, _ := strconv.Atoi(rs)
		var accepted, rejected, errs atomic.Int64
		var latMu sync.Mutex
		var lats []time.Duration
		hooksBefore := t.hooks.Load()
		t.mu.Lock()
		t.e2e = nil
		t.mu.Unlock()

		start := time.Now()
		tick := time.NewTicker(10 * time.Millisecond)
		sent := 0
		var wg sync.WaitGroup
		for now := range tick.C {
			el := now.Sub(start)
			if el >= *stepDur {
				break
			}
			due := int(el.Seconds() * float64(rate))
			for ; sent < due; sent++ {
				i := sent
				select {
				case sem <- struct{}{}:
				default:
					errs.Add(1) // generator saturated: count as failure to accept
					continue
				}
				wg.Add(1)
				go func() {
					defer func() { <-sem; wg.Done() }()
					body := fmt.Sprintf(`{"user_id":"load-%d","idempotent_key":"%s-%d-%d","url":"%s/work","method":"POST","webhook_url":"%s/hook"}`,
						i%50, run, rate, i, *self, *self)
					at := time.Now()
					resp, err := client.Post(*target+"/jobs", "application/json", bytes.NewBufferString(body))
					if err != nil {
						errs.Add(1)
						return
					}
					rb, _ := io.ReadAll(resp.Body)
					resp.Body.Close()
					d := time.Since(at)
					switch {
					case resp.StatusCode == 201:
						accepted.Add(1)
						if m := jobIDRe.FindSubmatch(rb); m != nil {
							t.mu.Lock()
							t.submitted[string(m[1])] = at
							t.mu.Unlock()
						}
						latMu.Lock()
						lats = append(lats, d)
						latMu.Unlock()
					case resp.StatusCode == 429 || resp.StatusCode == 503:
						rejected.Add(1)
					default:
						errs.Add(1)
					}
				}()
			}
		}
		tick.Stop()
		wg.Wait()
		secs := time.Since(start).Seconds()
		done := t.hooks.Load() - hooksBefore
		totalAccepted += accepted.Load()
		backlog := totalAccepted - t.hooks.Load()
		t.mu.Lock()
		e2e := append([]time.Duration(nil), t.e2e...)
		t.mu.Unlock()

		accRate := float64(accepted.Load()) / secs
		doneRate := float64(done) / secs
		verdict := "ok"
		switch {
		case float64(errs.Load()) > 0.01*float64(sent):
			verdict = "errors"
		case float64(rejected.Load()) > 0.01*float64(sent):
			verdict = "shedding"
		case doneRate < 0.9*accRate || backlog > int64(rate):
			verdict = "falling behind"
		}
		fmt.Printf("%7d %8.0f %8.0f %7.0f %6.0f %8.1fms %8.1fms %9d %7.0fms %8.0fms %9s\n", rate, accRate, doneRate,
			float64(rejected.Load())/secs, float64(errs.Load())/secs, pct(lats, 0.5), pct(lats, 0.99), backlog, pct(e2e, 0.5), pct(e2e, 0.99), verdict)

		if verdict != "ok" {
			// Let the target catch up so the next run starts clean, then stop.
			deadline := time.Now().Add(60 * time.Second)
			for totalAccepted-t.hooks.Load() > 0 && time.Now().Before(deadline) {
				time.Sleep(500 * time.Millisecond)
			}
			fmt.Printf("stopped; backlog after 60s drain wait: %d\n", totalAccepted-t.hooks.Load())
			break
		}
		// Short pause so each step starts with an empty backlog.
		deadline := time.Now().Add(15 * time.Second)
		for totalAccepted-t.hooks.Load() > 0 && time.Now().Before(deadline) {
			time.Sleep(200 * time.Millisecond)
		}
	}
	fmt.Printf("upstream hits %d, webhooks %d\n", t.work.Load(), t.hooks.Load())
	os.Exit(0)
}
