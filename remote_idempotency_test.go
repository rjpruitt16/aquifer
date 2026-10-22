// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"testing"

	"github.com/rjpruitt16/aquifer/internal/jobs"
)

type fakeRemoteIdempotency struct {
	lookupHash string
	entry      RemoteIdempotencyEntry
	found      bool
	recorded   []DrainEvent
	recordOK   bool
	resultHash string
	result     JobResult
	resultOK   bool
}

func (f *fakeRemoteIdempotency) Lookup(hash string) (RemoteIdempotencyEntry, bool) {
	f.lookupHash = hash
	return f.entry, f.found
}

func (f *fakeRemoteIdempotency) Record(events []DrainEvent) bool {
	f.recorded = append([]DrainEvent(nil), events...)
	return f.recordOK
}

func (f *fakeRemoteIdempotency) LookupResult(hash string) (JobResult, bool) {
	f.resultHash = hash
	return f.result, f.resultOK
}

func TestRemoteIdempotencyDuplicateDeletesLocalAcceptedJob(t *testing.T) {
	app, store := testAquiferWithLimits(t, AdmissionLimits{})
	remote := &fakeRemoteIdempotency{
		entry: RemoteIdempotencyEntry{
			JobID:      "remote-job",
			Status:     StatusCompleted,
			RecordedAt: 123,
			Source:     "test",
			ResultKey:  "aqueduct:result:remote-key",
		},
		found: true,
	}
	app.SetRemoteIdempotency(remote)

	req := sampleJobRequest("user-1", "remote-key")
	result, err := app.Enqueue(req)
	if err != nil {
		t.Fatalf("expected remote duplicate to succeed, got: %v", err)
	}
	if !result.Duplicate {
		t.Fatalf("expected duplicate result, got %+v", result)
	}
	if result.JobID != "remote-job" || result.Status != StatusCompleted {
		t.Fatalf("expected remote result returned, got %+v", result)
	}
	if result.ResultKey != "aqueduct:result:remote-key" {
		t.Fatalf("expected remote duplicate to return result key, got %+v", result)
	}
	if remote.lookupHash != jobs.HashKey(req.UserID+":"+req.IdempotentKey) {
		t.Fatalf("expected lookup by Aquifer idempotency hash, got %q", remote.lookupHash)
	}
	if entries := store.ListIdempotentKeys(); len(entries) != 0 {
		t.Fatalf("expected local speculative insert deleted after remote duplicate, got %+v", entries)
	}
}

func TestGetJobResultLooksUpRemoteByIdempotencyHash(t *testing.T) {
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	remote := &fakeRemoteIdempotency{
		result: JobResult{
			JobID:          "remote-job",
			Status:         StatusCompleted,
			ResponseStatus: 200,
			ContentType:    "application/json",
			Body:           `{"ok":true}`,
			RecordedAt:     123,
			Source:         "test",
		},
		resultOK: true,
	}
	app.SetRemoteIdempotency(remote)

	result, err := app.GetJobResult("user-1", "key-1", "")
	if err != nil {
		t.Fatalf("expected remote result, got error: %v", err)
	}
	if remote.resultHash != jobs.HashKey("user-1:key-1") {
		t.Fatalf("expected lookup by idempotency hash, got %q", remote.resultHash)
	}
	if result.JobID != "remote-job" || result.Body != `{"ok":true}` {
		t.Fatalf("unexpected result: %+v", result)
	}
}
