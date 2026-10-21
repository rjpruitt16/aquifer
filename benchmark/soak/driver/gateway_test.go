// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestParseBackendsAndScenariosNamingThem(t *testing.T) {
	nodes, _ := parseNodes("a=http://a:8080|http://a:9090,b=http://b:8080|http://b:9090")
	backends, err := parseBackends("q1=http://q1:8080|http://q1:9090")
	if err != nil || len(backends) != 1 || backends[0].Supervisor != "http://q1:9090" {
		t.Fatalf("backends %v, %v", backends, err)
	}
	if _, err := parseBackends("q1=http://q1:8080"); err == nil {
		t.Fatal("a backend needs a supervisor")
	}
	plan, err := parseScenarios("10m:kill:a,10m:hold:q1:5m", append(append([]*clusterNode{}, nodes...), backends...))
	if err != nil || plan[1].Nodes[0] != "q1" {
		t.Fatalf("plan %+v, %v", plan, err)
	}
	c := newCluster(nodes, plan, false)
	c.addBackends(backends)
	if !c.isGateway() || len(c.nodes) != 2 || c.byID["q1"] == nil {
		t.Fatal("backends are scenario targets but not load targets")
	}
	for i := 0; i < 50; i++ {
		if c.pick().ID == "q1" {
			t.Fatal("load must never go straight to a backend")
		}
	}
	var none *cluster
	if none.isGateway() {
		t.Fatal("no cluster is no gateway")
	}
}

func TestLineAcceptRecognizesCanalisLineStreams(t *testing.T) {
	waited := "event: canalis_queued\ndata: {\"reason\":\"no_capacity\",\"ahead\":3}\n\nevent: canalis_timeout\ndata: {}\n\n"
	if id, ok := lineAccept(200, []byte(waited)); !ok || id != "" {
		t.Fatalf("a job still in line is accepted with no id yet: %q %v", id, ok)
	}
	handed := "event: canalis_queued\ndata: {}\n\nevent: canalis_assigned\ndata: {}\n\nevent: queued\ndata: {\"job_id\":\"j-42\",\"status\":\"queued\"}\n\n"
	if id, ok := lineAccept(200, []byte(handed)); !ok || id != "j-42" {
		t.Fatalf("a job handed over while we listened keeps its id: %q %v", id, ok)
	}
	if _, ok := lineAccept(201, []byte(`{"job_id":"j1"}`)); ok {
		t.Fatal("a plain 201 is the ordinary accept, not the line")
	}
	if _, ok := lineAccept(200, []byte(`{"job_id":"j1","duplicate":true}`)); ok {
		t.Fatal("a 200 without the stream isn't the line")
	}
}

// fakeGateway answers GET /jobs/soak-<seq> like Canalis: lineStatus maps
// seq to the status code and body it returns.
func fakeGateway(t *testing.T, answers map[string]func(w http.ResponseWriter)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("user_id") == "" {
			http.Error(w, "need user_id", http.StatusBadRequest)
			return
		}
		if answer, ok := answers[r.URL.Path]; ok {
			answer(w)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
}

func TestGatewayJobStatusFindsJobsWhereverTheyAre(t *testing.T) {
	// Backend q1 has j-done; q2 has nothing.
	q1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/jobs/j-done" || r.URL.Path == "/jobs/j-handed" {
			fmt.Fprint(w, `{"status":"completed"}`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer q1.Close()
	q2 := httptest.NewServer(http.NotFoundHandler())
	defer q2.Close()
	// Gateway node a has nothing; b still holds seq 7 in line and remembers
	// handing seq 8 over.
	a := fakeGateway(t, nil)
	defer a.Close()
	b := fakeGateway(t, map[string]func(http.ResponseWriter){
		"/jobs/soak-7": func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusAccepted)
			fmt.Fprint(w, `{"status":"queued","ahead":2}`)
		},
		"/jobs/soak-8": func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusCreated)
			fmt.Fprint(w, `{"job_id":"j-handed","status":"queued"}`)
		},
	})
	defer b.Close()

	c := newCluster([]*clusterNode{{ID: "a", Target: a.URL}, {ID: "b", Target: b.URL}}, nil, false)
	c.addBackends([]*clusterNode{{ID: "q1", Target: q1.URL}, {ID: "q2", Target: q2.URL}})
	d := &driver{client: &http.Client{Timeout: 5 * time.Second}, cluster: c, cfg: config{Users: 50}, ledger: openLedger(t.TempDir())}
	d.ledger.acceptedOn(8, "", lineNode, time.Now())

	cases := []struct {
		seq  uint64
		id   string
		want string
	}{
		{1, "j-done", "completed"}, // id known: asked of every backend
		{2, "j-gone", ""},          // id known, no backend has it: lost
		{7, "", "queued"},          // still in line on node b
		{8, "", "completed"},       // handed over: b names the id, q1 has it
		{9, "", "unknown"},         // no trace anywhere (yet)
	}
	for _, tc := range cases {
		if got := d.gatewayJobStatus(tc.seq, pending{JobID: tc.id}); got != tc.want {
			t.Errorf("seq %d (id %q): got %q, want %q", tc.seq, tc.id, got, tc.want)
		}
	}
	if p := d.ledger.s.Outstanding[8]; p == nil || p.JobID != "j-handed" {
		t.Fatalf("the id the gateway named should be remembered: %+v", p)
	}
}

func TestAJobWithNoTraceInTheLineIsLostOnlyAfterAWhile(t *testing.T) {
	l := openLedger(t.TempDir())
	now := time.Now()
	l.acceptedOn(1, "", lineNode, now.Add(-10*time.Minute))
	l.acceptedOn(2, "", lineNode, now.Add(-lostInLineAfter-time.Minute))
	l.resolve(1, "unknown", now)
	l.resolve(2, "unknown", now)
	c := l.view().Counters
	if c.LostInLine != 1 || c.Lost != 0 {
		t.Fatalf("only the job past %v should count, as lost in the line: %+v", lostInLineAfter, c)
	}
	if _, still := l.s.Outstanding[1]; !still {
		t.Fatal("the recent one should still be waiting for its webhook")
	}
	// A webhook for the recent one closes it normally.
	if _, _, dup := l.webhook(1, "completed", now); dup {
		t.Fatal("first webhook isn't a duplicate")
	}
	if c := l.view().Counters; c.Completed != 1 || c.LostInLine != 1 {
		t.Fatalf("counters after the webhook: %+v", c)
	}
}

func TestAJobThatFinishesOnTheLineStreamIsDone(t *testing.T) {
	body := []byte("event: canalis_queued\ndata: {}\n\nevent: canalis_assigned\ndata: {\"job_id\":\"j1\"}\n\nevent: queued\ndata: {\"job_id\":\"j1\"}\n\nevent: completed\ndata: {\"job_id\":\"j1\",\"response_status\":200}\n\n")
	if lineOutcome(body) != "completed" || lineOutcome([]byte("event: failed\n")) != "failed" || lineOutcome([]byte("event: canalis_queued\n")) != "" {
		t.Fatal("lineOutcome")
	}
	l := openLedger(t.TempDir())
	now := time.Now()
	l.acceptedOn(3, "j1", lineNode, now)
	if dup := l.finishedOnStream(3, "completed", now); dup {
		t.Fatal("first finish isn't a duplicate")
	}
	c := l.view().Counters
	if c.CompletedOnStream != 1 || c.Completed != 0 {
		t.Fatalf("counted as finished on the stream, not as a webhook: %+v", c)
	}
	if _, still := l.s.Outstanding[3]; still {
		t.Fatal("a finished job is no longer outstanding")
	}
	// A webhook for the same job afterwards would be a double delivery.
	if _, _, dup := l.webhook(3, "completed", now); !dup {
		t.Fatal("a webhook after the stream said completed is a duplicate")
	}
}

func TestScenarioDeltasIncludeTheGatewayCounters(t *testing.T) {
	a := Counters{LostInLine: 3, CompletedOnStream: 10, FailedOnStream: 2}
	b := Counters{LostInLine: 1, CompletedOnStream: 4}
	if d := subCounters(a, b); d.LostInLine != 2 || d.CompletedOnStream != 6 || d.FailedOnStream != 2 {
		t.Fatalf("delta %+v", d)
	}
}

// One stalled supervisor marks only that node down, and doesn't hold up the
// rest of the minute's sample past the deadline.
func TestNodeSnapshotsDontWaitOnAStalledNode(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"rss_bytes":104857600,"open_fds":12,"pebble_bytes":1048576,"restarts":1}`)
	}))
	defer ok.Close()
	release := make(chan struct{})
	stalled := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	defer stalled.Close()
	defer close(release)

	start := time.Now()
	got := nodeSnapshots([]*clusterNode{{ID: "a", Supervisor: ok.URL}, {ID: "b", Supervisor: stalled.URL}})
	if took := time.Since(start); took > 7*time.Second {
		t.Fatalf("took %v; the stalled node should be cut off at the deadline", took)
	}
	if got["a"].Down || got["a"].RSSMB != 100 || got["a"].OpenFDs != 12 {
		t.Fatalf("a: %+v", got["a"])
	}
	if !got["b"].Down {
		t.Fatalf("b should be down: %+v", got["b"])
	}
}
