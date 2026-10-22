// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"testing"

	"github.com/rjpruitt16/aquifer/internal/jobs"
	"github.com/rjpruitt16/aquifer/internal/storage"
)

func TestDrainValkeySinkRecordsAndAcknowledges(t *testing.T) {
	remote := &fakeRemoteIdempotency{recordOK: true}
	r := drainTestRegistry(t, NoopMetricsAdapter{})
	r.drainCfg = DrainConfig{Enabled: true, TimerSeconds: 1, Sink: "valkey", BatchMaxEvents: 10}
	r.applyDrainEvents()
	r.SetDrainRemote(remote)
	seedLedgerEntry(t, r)

	events, ok := r.flushDrainEventBatch("ledger_batch")
	if !ok {
		t.Fatalf("expected valkey sink batch flush to succeed")
	}
	if len(remote.recorded) != 1 || len(events) != 1 {
		t.Fatalf("expected one event recorded to remote sink, got remote=%+v events=%+v", remote.recorded, events)
	}
	if remaining := r.store.ListDrainEvents(10); len(remaining) != 0 {
		t.Fatalf("expected acknowledged drain event deleted, got %+v", remaining)
	}
}

func TestDrainValkeySinkFailureDoesNotAcknowledge(t *testing.T) {
	remote := &fakeRemoteIdempotency{recordOK: false}
	r := drainTestRegistry(t, NoopMetricsAdapter{})
	r.drainCfg = DrainConfig{Enabled: true, TimerSeconds: 1, Sink: "valkey", BatchMaxEvents: 10}
	r.applyDrainEvents()
	r.SetDrainRemote(remote)
	seedLedgerEntry(t, r)

	if _, ok := r.flushDrainEventBatch("ledger_batch"); ok {
		t.Fatalf("expected valkey sink batch flush to fail")
	}
	if remaining := r.store.ListDrainEvents(10); len(remaining) != 1 {
		t.Fatalf("expected unacknowledged drain event retained, got %+v", remaining)
	}
}

type fakeRemoteIdempotency struct {
	lookupHash string
	entry      storage.RemoteEntry
	found      bool
	recorded   []jobs.DrainEvent
	recordOK   bool
	resultHash string
	result     jobs.Result
	resultOK   bool
}

func (f *fakeRemoteIdempotency) Lookup(hash string) (storage.RemoteEntry, bool) {
	f.lookupHash = hash
	return f.entry, f.found
}
func (f *fakeRemoteIdempotency) Record(events []jobs.DrainEvent) bool {
	f.recorded = append([]jobs.DrainEvent(nil), events...)
	return f.recordOK
}
func (f *fakeRemoteIdempotency) LookupResult(hash string) (jobs.Result, bool) {
	f.resultHash = hash
	return f.result, f.resultOK
}
