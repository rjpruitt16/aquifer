// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// The cluster failure test: load spread over several Aquifer nodes running
// cluster routing (AQUIFER_CLUSTER_PROVIDER=valkey), and a schedule of
// failures (-scenarios) aimed at single nodes, pairs of nodes, and the
// Valkey they coordinate through. Off unless -nodes is set.
//
// Senders may hit any node; the receiving node forwards to the user's
// owner and reports it in X-Aquifer-Cluster-Owner. Queues and job state stay
// on the owner, so the ledger remembers each job's owner and asks that node
// whether a job finished. Asking another node would get a 404 and count a
// healthy job as lost.

type clusterNode struct {
	ID         string `json:"id"`
	Target     string `json:"target"`
	Supervisor string `json:"supervisor"`
	accepted   atomic.Int64
}

// parseNodes reads id=target|supervisor,... e.g.
// a=http://aquifer-cluster-a.internal:8080|http://aquifer-cluster-a.internal:9090
func parseNodes(s string) ([]*clusterNode, error) {
	var nodes []*clusterNode
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, urls, ok := strings.Cut(part, "=")
		target, sup, ok2 := strings.Cut(urls, "|")
		if !ok || !ok2 || id == "" || target == "" || sup == "" {
			return nil, fmt.Errorf("node %q: want id=target|supervisor", part)
		}
		nodes = append(nodes, &clusterNode{ID: id, Target: target, Supervisor: sup})
	}
	if len(nodes) < 2 {
		return nil, fmt.Errorf("-nodes needs at least two nodes, got %d", len(nodes))
	}
	return nodes, nil
}

// A scenario waits After from the previous one, then acts.
type scenario struct {
	After  time.Duration `json:"after"`
	Action string        `json:"action"` // kill, hold, freeze, drain, valkey, end
	Nodes  []string      `json:"nodes,omitempty"`
	For    time.Duration `json:"for,omitempty"`
}

// parseScenarios reads after:action[:nodes][:for],... e.g.
// 30m:kill:a, 25m:hold:b:5m, 25m:freeze:c:20s, 25m:valkey:5m, 25m:hold:a+b:5m, 25m:drain:c, 20m:end
func parseScenarios(s string, nodes []*clusterNode) ([]scenario, error) {
	known := map[string]bool{}
	for _, n := range nodes {
		known[n.ID] = true
	}
	var out []scenario
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		f := strings.Split(part, ":")
		if len(f) < 2 {
			return nil, fmt.Errorf("scenario %q: want after:action[:nodes][:for]", part)
		}
		after, err := time.ParseDuration(f[0])
		if err != nil {
			return nil, fmt.Errorf("scenario %q: %v", part, err)
		}
		sc := scenario{After: after, Action: f[1]}
		rest := f[2:]
		needNodes := map[string]bool{"kill": true, "hold": true, "freeze": true, "drain": true}[sc.Action]
		needFor := map[string]bool{"hold": true, "freeze": true, "valkey": true}[sc.Action]
		if !needNodes && !needFor && sc.Action != "end" {
			return nil, fmt.Errorf("scenario %q: unknown action %q", part, sc.Action)
		}
		if needNodes {
			if len(rest) == 0 {
				return nil, fmt.Errorf("scenario %q: %s needs nodes", part, sc.Action)
			}
			for _, id := range strings.Split(rest[0], "+") {
				if !known[id] {
					return nil, fmt.Errorf("scenario %q: unknown node %q", part, id)
				}
				sc.Nodes = append(sc.Nodes, id)
			}
			rest = rest[1:]
		}
		if needFor {
			if len(rest) == 0 {
				return nil, fmt.Errorf("scenario %q: %s needs a duration", part, sc.Action)
			}
			if sc.For, err = time.ParseDuration(rest[0]); err != nil {
				return nil, fmt.Errorf("scenario %q: %v", part, err)
			}
		}
		out = append(out, sc)
	}
	return out, nil
}

// scenarioResult is what the ledger counted between one scenario starting
// and the next: lost and duplicated jobs are counted when they're found,
// so a window can include a few from just before it.
type scenarioResult struct {
	scenario
	At      time.Time `json:"at"`
	Error   string    `json:"error,omitempty"`
	Before  Counters  `json:"-"`
	Delta   Counters  `json:"delta"`
	Retries int64     `json:"retried_504s"`
	Bad504  int64     `json:"gateway_errors"`
	Ended   bool      `json:"ended"`
}

type cluster struct {
	nodes  []*clusterNode
	byID   map[string]*clusterNode
	plan   []scenario
	valkey *valkeyProc
	// backends are the instances behind a gateway (gateway.go); nil otherwise.
	backends []*clusterNode

	retried504 atomic.Int64 // 504 (slow owner) retried with the same key
	lined      atomic.Int64 // gateway mode: jobs accepted into the gateway's line
	failovers  atomic.Int64 // connection errors retried on another node
	gateway    atomic.Int64 // 502/504 we couldn't turn into an accept

	mu      sync.Mutex
	results []*scenarioResult
}

func newCluster(nodes []*clusterNode, plan []scenario, valkey bool) *cluster {
	c := &cluster{nodes: nodes, byID: map[string]*clusterNode{}, plan: plan}
	for _, n := range nodes {
		c.byID[n.ID] = n
	}
	if valkey {
		c.valkey = &valkeyProc{}
	}
	return c
}

func (c *cluster) pick() *clusterNode { return c.nodes[rand.IntN(len(c.nodes))] }

func (c *cluster) other(not *clusterNode) *clusterNode {
	for {
		if n := c.pick(); n != not {
			return n
		}
	}
}

// targetFor is where to ask about a job: its owner, else the first node.
func (c *cluster) targetFor(owner string) string {
	if n, ok := c.byID[owner]; ok {
		return n.Target
	}
	return c.nodes[0].Target
}

// post submits a job to a random node, the way a load balancer in front
// of the cluster would. Two retries, each once:
//   - A connection error (the node is down) goes to another node, as a
//     load balancer would send it.
//   - A 504 means the owner got the request but didn't answer in time.
//     Aquifer's contract is that retrying with the same idempotency key
//     reaches the same owner, which returns the job if it accepted the
//     first attempt. The retry tests that promise.
func (d *driver) clusterPost(body string) (status int, owner string, rb []byte, err error) {
	c := d.cluster
	node := c.pick()
	failedOver, retried := false, false
	for {
		var resp *http.Response
		resp, err = d.client.Post(node.Target+"/jobs", "application/json", strings.NewReader(body))
		if err != nil {
			if failedOver {
				return 0, "", nil, err
			}
			failedOver = true
			c.failovers.Add(1)
			node = c.other(node)
			continue
		}
		rb, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		status, owner = resp.StatusCode, resp.Header.Get("X-Aquifer-Cluster-Owner")
		if status == http.StatusGatewayTimeout && !retried {
			retried = true
			c.retried504.Add(1)
			time.Sleep(time.Second)
			continue
		}
		if status == http.StatusBadGateway || status == http.StatusGatewayTimeout {
			c.gateway.Add(1)
		}
		if n, ok := c.byID[owner]; ok && (status == 200 || status == 201) {
			n.accepted.Add(1)
		}
		return status, owner, rb, nil
	}
}

// runScenarios works through the plan, recording the ledger's counters at
// each step so /status can show what each failure cost.
func (d *driver) runScenarios(stop <-chan struct{}) {
	c := d.cluster
	var prev *scenarioResult
	for _, sc := range c.plan {
		select {
		case <-stop:
			return
		case <-time.After(sc.After):
		}
		now := d.ledger.view().Counters
		c.mu.Lock()
		if prev != nil {
			prev.Delta = subCounters(now, prev.Before)
			prev.Retries = c.retried504.Load() - prev.Retries
			prev.Bad504 = c.gateway.Load() - prev.Bad504
			prev.Ended = true
		}
		r := &scenarioResult{scenario: sc, At: time.Now().UTC(), Before: now,
			Retries: c.retried504.Load(), Bad504: c.gateway.Load()}
		c.results = append(c.results, r)
		c.mu.Unlock()
		prev = r

		if sc.Action == "end" {
			d.events.add("cluster_end", nil)
			d.rate.Store(0)
			d.phase.Store("cluster_done")
			r.Ended = true
			return
		}
		if err := d.act(sc); err != nil {
			r.Error = err.Error()
		}
	}
}

func (d *driver) act(sc scenario) error {
	d.events.add("cluster_"+sc.Action, map[string]any{"nodes": sc.Nodes, "for_s": int(sc.For.Seconds())})
	if sc.Action == "valkey" {
		if d.cluster.valkey == nil {
			return fmt.Errorf("valkey: not run by this driver (-valkey)")
		}
		return d.cluster.valkey.outage(sc.For, d.events)
	}
	var errs []string
	for _, id := range sc.Nodes {
		n := d.cluster.byID[id]
		path := "/" + sc.Action
		if sc.For > 0 {
			path += "?for=" + sc.For.String()
		}
		at := time.Now()
		resp, err := d.client.Post(n.Supervisor+path, "application/json", nil)
		if err != nil {
			errs = append(errs, id+": "+err.Error())
			continue
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			errs = append(errs, fmt.Sprintf("%s: supervisor answered %d", id, resp.StatusCode))
			continue
		}
		// The flush-window rule applies to a process that dies; a frozen
		// one keeps its memory.
		if sc.Action != "freeze" {
			d.ledger.recordRestart(at)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}

func (c *cluster) status(d *driver) map[string]any {
	if c == nil {
		return nil
	}
	nodes := make([]map[string]any, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, map[string]any{"id": n.ID, "target": n.Target, "accepted_as_owner": n.accepted.Load()})
	}
	now := d.ledger.view().Counters
	c.mu.Lock()
	results := make([]map[string]any, 0, len(c.results))
	for _, r := range c.results {
		delta, retries, bad := r.Delta, r.Retries, r.Bad504
		if !r.Ended {
			delta = subCounters(now, r.Before)
			retries = c.retried504.Load() - r.Retries
			bad = c.gateway.Load() - r.Bad504
		}
		results = append(results, map[string]any{
			"at": r.At, "action": r.Action, "nodes": r.Nodes, "for": r.For.String(), "error": r.Error,
			"done": r.Ended, "delta": delta, "retried_504s": retries, "gateway_errors": bad,
		})
	}
	c.mu.Unlock()
	return map[string]any{
		"nodes":                      nodes,
		"plan":                       c.plan,
		"scenarios":                  results,
		"retried_504s":               c.retried504.Load(),
		"failovers":                  c.failovers.Load(),
		"gateway_errors":             c.gateway.Load(),
		"valkey_running":             c.valkey != nil && c.valkey.running(),
		"backends":                   backendIDs(c.backends),
		"accepted_into_gateway_line": c.lined.Load(),
	}
}

func subCounters(a, b Counters) Counters {
	return Counters{
		Submitted:                a.Submitted - b.Submitted,
		Accepted:                 a.Accepted - b.Accepted,
		Shed:                     a.Shed - b.Shed,
		ClientErrors:             a.ClientErrors - b.ClientErrors,
		Completed:                a.Completed - b.Completed,
		Failed:                   a.Failed - b.Failed,
		Duplicates:               a.Duplicates - b.Duplicates,
		DuplicatesNearRestart:    a.DuplicatesNearRestart - b.DuplicatesNearRestart,
		TerminalNoWebhook:        a.TerminalNoWebhook - b.TerminalNoWebhook,
		LateWebhooks:             a.LateWebhooks - b.LateWebhooks,
		AcceptedAfterClientError: a.AcceptedAfterClientError - b.AcceptedAfterClientError,
		UnknownWebhooks:          a.UnknownWebhooks - b.UnknownWebhooks,
		GapWebhooks:              a.GapWebhooks - b.GapWebhooks,
		Lost:                     a.Lost - b.Lost,
		LostInFlushWindow:        a.LostInFlushWindow - b.LostInFlushWindow,
		Stuck:                    a.Stuck - b.Stuck,
	}
}

// valkeyProc is the Valkey the nodes coordinate through, run by the driver
// so a scenario can take it down. No persistence: an outage loses every
// assignment, the way a crashed, unreplicated Valkey would.
type valkeyProc struct {
	mu  sync.Mutex
	cmd *exec.Cmd
}

func (v *valkeyProc) start() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	cmd := exec.Command("valkey-server", "--port", "6379", "--bind", "0.0.0.0", "::",
		"--protected-mode", "no", "--save", "", "--appendonly", "no")
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	v.cmd = cmd
	go cmd.Wait()
	return nil
}

func (v *valkeyProc) running() bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.cmd != nil
}

func (v *valkeyProc) outage(d time.Duration, events *eventLog) error {
	v.mu.Lock()
	cmd := v.cmd
	v.cmd = nil
	v.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return fmt.Errorf("valkey: not running")
	}
	if err := cmd.Process.Kill(); err != nil {
		return err
	}
	go func() {
		time.Sleep(d)
		if err := v.start(); err != nil {
			events.add("valkey_restart_failed", map[string]any{"error": err.Error()})
			return
		}
		events.add("valkey_back", nil)
	}()
	return nil
}
