// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseNodesAndScenarios(t *testing.T) {
	nodes, err := parseNodes("a=http://a:8080|http://a:9090, b=http://b:8080|http://b:9090,c=http://c:8080|http://c:9090")
	if err != nil || len(nodes) != 3 || nodes[1].Supervisor != "http://b:9090" {
		t.Fatalf("nodes %v, %v", nodes, err)
	}
	if _, err := parseNodes("a=http://a:8080|http://a:9090"); err == nil {
		t.Fatal("one node isn't a cluster")
	}
	plan, err := parseScenarios("30m:kill:a, 25m:hold:b:5m,25m:freeze:c:20s,25m:valkey:5m,25m:hold:a+b:5m,25m:drain:c,20m:end", nodes)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan) != 7 || plan[1].For != 5*time.Minute || len(plan[4].Nodes) != 2 || plan[3].Action != "valkey" {
		t.Fatalf("plan %+v", plan)
	}
	for _, bad := range []string{"10m:hold:a", "10m:kill:z", "10m:explode:a", "soon:kill:a", "10m:valkey"} {
		if _, err := parseScenarios(bad, nodes); err == nil {
			t.Errorf("%q: want an error", bad)
		}
	}
}

// A 504 is retried once with the same body, and the owner's answer counts.
// A node that refuses connections is skipped for another.
func TestClusterPostRetriesSlowOwnerAndFailsOver(t *testing.T) {
	var calls atomic.Int64
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusGatewayTimeout)
			return
		}
		w.Header().Set("X-Aquifer-Cluster-Owner", "b")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"job_id":"j1","duplicate":true}`))
	}))
	defer slow.Close()
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()

	nodes := []*clusterNode{{ID: "a", Target: deadURL}, {ID: "b", Target: slow.URL}}
	d := &driver{client: &http.Client{Timeout: 5 * time.Second}, cluster: newCluster(nodes, nil, false)}
	var status int
	var owner string
	for i := 0; i < 20 && status == 0; i++ { // until the random pick hits the dead node first
		calls.Store(0)
		var err error
		status, owner, _, err = d.clusterPost(`{}`)
		if err != nil {
			t.Fatal(err)
		}
	}
	if status != 200 || owner != "b" {
		t.Fatalf("status %d owner %q", status, owner)
	}
	if d.cluster.retried504.Load() == 0 {
		t.Fatal("the 504 should have been retried")
	}
	if nodes[1].accepted.Load() == 0 {
		t.Fatal("the owner's accept should be counted")
	}
}

// Scenarios call the right supervisor endpoint and record restarts for the
// flush-window rule, except for a freeze.
func TestScenarioActionsReachTheSupervisor(t *testing.T) {
	var got []string
	sup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.URL.Path+"?"+r.URL.RawQuery)
		w.Write([]byte(`{}`))
	}))
	defer sup.Close()
	nodes := []*clusterNode{{ID: "a", Supervisor: sup.URL}, {ID: "b", Supervisor: sup.URL}}
	d := &driver{client: sup.Client(), cluster: newCluster(nodes, nil, false),
		ledger: openLedger(t.TempDir()), events: openEventLog(t.TempDir())}

	for _, sc := range []scenario{
		{Action: "hold", Nodes: []string{"a", "b"}, For: 5 * time.Minute},
		{Action: "freeze", Nodes: []string{"b"}, For: 20 * time.Second},
		{Action: "drain", Nodes: []string{"a"}},
	} {
		if err := d.act(sc); err != nil {
			t.Fatal(err)
		}
	}
	want := "/hold?for=5m0s /hold?for=5m0s /freeze?for=20s /drain?"
	if strings.Join(got, " ") != want {
		t.Fatalf("calls %q, want %q", strings.Join(got, " "), want)
	}
	if n := len(d.ledger.s.Restarts); n != 3 {
		t.Fatalf("restarts recorded %d, want 3 (two holds and a drain, not the freeze)", n)
	}
	if err := d.act(scenario{Action: "valkey", For: time.Minute}); err == nil {
		t.Fatal("a valkey outage needs -valkey")
	}
}
