// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// A stream opened after the job finished used to send queued, then only
// keepalives: the final event went out before anyone listened (spillway#91
// found the same bug there). It now sends the stored result and ends.
func TestStreamOfAFinishedJobSendsItsResultAndEnds(t *testing.T) {
	h := newStreamDeliveryHarness(t)
	jobID := h.submit()
	close(h.release)
	if !waitUntil(5*time.Second, func() bool {
		job, err := h.app.GetJob(jobID)
		return err == nil && job.Status == StatusCompleted
	}) {
		t.Fatal("job never completed")
	}

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(h.server.URL + "/jobs/" + jobID + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("stream for a finished job didn't end: %v", err)
	}
	body := string(raw)

	_, after, ok := strings.Cut(body, "event: completed\ndata: ")
	if !ok {
		t.Fatalf("no completed event in %q", body)
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(strings.SplitN(after, "\n", 2)[0]), &data); err != nil {
		t.Fatal(err)
	}
	if data["job_id"] != jobID || data["response_status"] != float64(200) || data["body"] != `{"answer":42}` {
		t.Fatalf("completed event data = %v", data)
	}
}
