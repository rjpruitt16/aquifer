// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"testing"

	"github.com/rjpruitt16/aquifer/internal/jobs"
)

func webhookJobFor(userID string) *jobs.Job {
	return &jobs.Job{ID: jobs.NewID(), UserID: userID, WebhookURL: ""}
}

func userJob(userID string) *jobs.Job {
	return &jobs.Job{ID: jobs.NewID(), UserID: userID, WebhookURL: "https://example.com/hook"}
}

func TestWebhookBacklogDecision(t *testing.T) {
	t.Setenv("AQUIFER_MAX_PENDING_WEBHOOKS_PER_USER", "1000")
	tracker := newUserLoadTracker()
	for i := 0; i < 1500; i++ {
		tracker.add(webhookJobFor("busy"))
	}

	if rejected, _, _ := tracker.webhookBacklogDecision("busy", 0); rejected {
		t.Fatal("a user alone on the instance must never be rejected for webhook backlog")
	}

	tracker.add(userJob("neighbor"))
	if rejected, limit, backlog := tracker.webhookBacklogDecision("busy", 0.4); !rejected || limit != 1000 || backlog != 1500 {
		t.Fatalf("1500/1000 gives p=0.5, so a 0.4 draw must reject (got rejected=%v limit=%d backlog=%d)", rejected, limit, backlog)
	}
	if rejected, _, _ := tracker.webhookBacklogDecision("busy", 0.6); rejected {
		t.Fatal("1500/1000 gives p=0.5, so a 0.6 draw must be admitted")
	}
	if rejected, _, _ := tracker.webhookBacklogDecision("neighbor", 0); rejected {
		t.Fatal("a user with no webhook backlog must never be rejected")
	}

	for i := 0; i < 600; i++ {
		tracker.done(webhookJobFor("busy"))
	}
	if rejected, _, _ := tracker.webhookBacklogDecision("busy", 0); rejected {
		t.Fatal("at or under the limit, the user is admitted again")
	}

	t.Setenv("AQUIFER_MAX_PENDING_WEBHOOKS_PER_USER", "0")
	for i := 0; i < 5000; i++ {
		tracker.add(webhookJobFor("busy"))
	}
	if rejected, _, _ := tracker.webhookBacklogDecision("busy", 0); rejected {
		t.Fatal("a limit of 0 disables the rule")
	}
}
