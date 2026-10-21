// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Gateway mode (-nodes plus -backends): the -nodes are a gateway, Canalis,
// in front of the -backends, the Aquifer or Spillway instances doing the
// work. Load goes only to the gateway. Scenarios can kill, hold or freeze
// either. Two things differ from talking to Aquifer directly:
//
//   - When no instance has room, Canalis keeps the job in its durable line
//     and answers with an event stream (canalis_queued, then the instance's
//     own events once one takes it) instead of a 201. canalis_queued comes
//     only after the job is on disk, so it counts as accepted. The job id is
//     known only if an instance took it before the stream ended.
//   - A job lives on whichever instance it was handed to, so its status is
//     asked of every backend by id. A job whose id the driver never saw is
//     asked of the gateway by its key; the gateway's answer names the id
//     once the job has been handed over.

// lineNode marks a job accepted into the gateway's line.
const lineNode = "line"

// lostInLineAfter is how long a job accepted into the line may go with no
// webhook and no trace on the gateway or a backend before it counts as lost.
// It's generous: Canalis keeps a handed-over job's result for 5 minutes, and
// a long hold can keep a job in line well past that.
const lostInLineAfter = 30 * time.Minute

// lineAccept recognizes the gateway's answer for a job it put in its line:
// a 200 event stream that says canalis_queued, which Canalis sends only
// after the job is on disk. id is the instance's job id if one took the job
// before the stream ended, else "".
func lineAccept(status int, body []byte) (id string, ok bool) {
	if status != http.StatusOK || !bytes.Contains(body, []byte("event: canalis_queued")) {
		return "", false
	}
	if m := jobIDRe.FindSubmatch(body); m != nil {
		return string(m[1]), true
	}
	return "", true
}

// lineOutcome is how a job ended if its line stream already says so: when
// the gateway hands a job over while its caller is still listening, it
// relays the instance's own events, through completed or failed. "" if the
// stream ended before that.
func lineOutcome(body []byte) string {
	switch {
	case bytes.Contains(body, []byte("event: completed")):
		return "completed"
	case bytes.Contains(body, []byte("event: failed")):
		return "failed"
	}
	return ""
}

func backendIDs(backends []*clusterNode) []string {
	ids := make([]string, 0, len(backends))
	for _, b := range backends {
		ids = append(ids, b.ID)
	}
	return ids
}

func parseBackends(s string) ([]*clusterNode, error) {
	var nodes []*clusterNode
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, urls, ok := strings.Cut(part, "=")
		target, sup, ok2 := strings.Cut(urls, "|")
		if !ok || !ok2 || id == "" || target == "" || sup == "" {
			return nil, fmt.Errorf("backend %q: want id=target|supervisor", part)
		}
		nodes = append(nodes, &clusterNode{ID: id, Target: target, Supervisor: sup})
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("-backends names no instances")
	}
	return nodes, nil
}

// addBackends puts the instances behind the gateway where scenarios can
// find them, without adding them to the nodes load is spread over.
func (c *cluster) addBackends(backends []*clusterNode) {
	for _, b := range backends {
		c.byID[b.ID] = b
	}
	c.backends = backends
}

func (c *cluster) isGateway() bool { return c != nil && len(c.backends) > 0 }

// allNodes is the gateway nodes, then the backends: everything with a
// supervisor to watch.
func (c *cluster) allNodes() []*clusterNode {
	return append(append([]*clusterNode{}, c.nodes...), c.backends...)
}

// gatewayJobStatus is jobStatus in gateway mode: the job's status on the
// backend holding it, "" if no backend has it, "error" if a check failed.
func (d *driver) gatewayJobStatus(seq uint64, p pending) string {
	id := p.JobID
	if id == "" {
		var status string
		id, status = d.lineStatus(seq)
		switch {
		case status != "":
			return status
		case id == "":
			// Not in the line and not handed over that the gateway still
			// remembers. A webhook may yet come; ledger.resolve decides
			// when it has waited long enough.
			return "unknown"
		}
		d.ledger.setJobID(seq, id)
	}
	sawError := false
	for _, b := range d.cluster.backends {
		switch status := d.jobStatusAt(b.Target, id); status {
		case "":
		case "error":
			sawError = true
		default:
			return status
		}
	}
	if sawError {
		return "error"
	}
	return ""
}

// lineStatus asks every gateway node about a job by its key: a handed-over
// job's result stays only on the node that handed it over. It returns the
// instance's job id once the job has been handed over, or a status
// ("queued" while it waits in line, "error" if no node could be asked).
func (d *driver) lineStatus(seq uint64) (id, status string) {
	q := url.Values{"user_id": {fmt.Sprintf("soak-%d", seq%uint64(d.cfg.Users))}}
	answered := false
	for _, node := range d.cluster.nodes {
		id, status, ok := d.lineStatusAt(node.Target, seq, q)
		if !ok {
			continue
		}
		answered = true
		if id != "" || status != "" {
			return id, status
		}
	}
	if !answered {
		return "", "error"
	}
	return "", ""
}

func (d *driver) lineStatusAt(target string, seq uint64, q url.Values) (id, status string, ok bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/jobs/soak-%d?%s", target, seq, q.Encode()), nil)
	resp, err := d.client.Do(req)
	if err != nil {
		return "", "", false
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	switch resp.StatusCode {
	case http.StatusAccepted:
		return "", "queued", true
	case http.StatusNotFound:
		return "", "", true
	case http.StatusOK, http.StatusCreated:
		if m := jobIDRe.FindSubmatch(body); m != nil {
			return string(m[1]), "", true
		}
	}
	return "", "", false
}
