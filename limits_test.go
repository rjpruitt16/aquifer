// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/rjpruitt16/aquifer/internal/dispatch"
)

func TestLimitListenerHoldsAtMostItsLimit(t *testing.T) {
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln := limitListen(inner, 1)
	defer ln.Close()

	go func() {
		for i := 0; i < 2; i++ {
			if c, err := net.Dial("tcp", inner.Addr().String()); err == nil {
				defer c.Close()
			}
		}
		time.Sleep(time.Second)
	}()

	first, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			accepted <- c
		}
	}()
	select {
	case <-accepted:
		t.Fatal("accepted a second connection while the first was open")
	case <-time.After(100 * time.Millisecond):
	}
	first.Close()
	first.Close() // a second Close must not free a second slot
	select {
	case c := <-accepted:
		c.Close()
	case <-time.After(time.Second):
		t.Fatal("closing the first connection did not let the second in")
	}
}

func TestLimitListenerCloseUnblocksAccept(t *testing.T) {
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln := limitListen(inner, 1)
	go net.Dial("tcp", inner.Addr().String())
	held, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	done := make(chan error, 1)
	go func() {
		_, err := ln.Accept()
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	ln.Close()
	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Accept after Close = %v, want net.ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Accept still blocked after Close")
	}
}

// With every stream slot taken, a caller asking to stream a queued job is
// told to poll, and polling says where the job stands.
func TestFullStreamsAnswerWithPollAndEstimate(t *testing.T) {
	t.Setenv("AQUIFER_MAX_HELD_STREAMS", "1")
	t.Setenv("AQUIFER_READY_ESTIMATES", "true")
	h := newStreamDeliveryHarness(t)

	t.Setenv("AQUIFER_STREAM_MAX_WAIT_SECONDS", "0")
	h.submit() // takes the only in-flight slot and holds it
	waiting := h.submit()
	queued := h.submit()
	_, cancel := h.stream(waiting) // takes the only stream slot
	defer cancel()

	resp, err := http.Get(h.server.URL + "/jobs/" + queued + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("stream with no free slot: status %d, want 202", resp.StatusCode)
	}
	var poll map[string]any
	json.NewDecoder(resp.Body).Decode(&poll)
	if poll["poll"] != "/jobs/"+queued || poll["reason"] != "streams_full" {
		t.Fatalf("poll response = %v", poll)
	}
	if n, err := strconv.Atoi(resp.Header.Get("Retry-After")); err != nil || n < 1 || n > int(dispatch.MaxPollAfter/time.Second) {
		t.Fatalf("Retry-After = %q", resp.Header.Get("Retry-After"))
	}

	status, err := http.Get(h.server.URL + poll["poll"].(string))
	if err != nil {
		t.Fatal(err)
	}
	defer status.Body.Close()
	var body map[string]any
	json.NewDecoder(status.Body).Decode(&body)
	if pos, _ := body["queue_position"].(float64); pos != 2 {
		t.Fatalf("queue_position = %v, want 2 (behind the streamed job): %v", body["queue_position"], body)
	}
	if _, ok := body["estimated_ready_seconds"]; !ok {
		t.Fatalf("no estimated_ready_seconds: %v", body)
	}
	if status.Header.Get("Retry-After") == "" {
		t.Fatal("polling a queued job gave no Retry-After")
	}
}

func TestJobStatusHasNoEstimateByDefault(t *testing.T) {
	h := newStreamDeliveryHarness(t)
	h.submit()
	queued := h.submit()
	resp, err := http.Get(h.server.URL + "/jobs/" + queued)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	json.NewDecoder(resp.Body).Decode(&body)
	if _, ok := body["queue_position"]; ok || resp.Header.Get("Retry-After") != "" {
		t.Fatalf("estimates off: got %v, Retry-After %q", body, resp.Header.Get("Retry-After"))
	}
}

// A caller whose job is expected to take longer than the stream limit is
// told when to come back instead of being held.
func TestLongWaitAnswersWithPoll(t *testing.T) {
	t.Setenv("AQUIFER_READY_ESTIMATES", "true")
	t.Setenv("AQUIFER_STREAM_MAX_WAIT_SECONDS", "1")
	h := newStreamDeliveryHarness(t)

	h.submit() // dispatched at once, then held: the queue's rate is 1 per second
	h.submit()
	far := h.submit()
	time.Sleep(1100 * time.Millisecond)

	resp, err := http.Get(h.server.URL + "/jobs/" + far + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var poll map[string]any
	json.NewDecoder(resp.Body).Decode(&poll)
	if resp.StatusCode != http.StatusAccepted || poll["reason"] != "long_wait" {
		t.Fatalf("far-back job: status %d, body %v; want 202 long_wait", resp.StatusCode, poll)
	}
	ready, _ := poll["estimated_ready_seconds"].(float64)
	ms, err := strconv.Atoi(resp.Header.Get("X-Aqueduct-Retry-After-Ms"))
	if err != nil || ready <= 1 || float64(ms) > ready*1000 || float64(ms) < ready*800-1 {
		t.Fatalf("estimate %v s, retry after %q ms: want 80-100%% of the estimate", ready, resp.Header.Get("X-Aqueduct-Retry-After-Ms"))
	}
}
