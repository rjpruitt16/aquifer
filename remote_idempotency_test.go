package aquifer

import (
	"bufio"
	"strings"
	"testing"
)

type fakeRemoteIdempotency struct {
	lookupHash string
	entry      RemoteIdempotencyEntry
	found      bool
	recorded   []DrainEvent
	recordOK   bool
}

func (f *fakeRemoteIdempotency) Lookup(hash string) (RemoteIdempotencyEntry, bool) {
	f.lookupHash = hash
	return f.entry, f.found
}

func (f *fakeRemoteIdempotency) Record(events []DrainEvent) bool {
	f.recorded = append([]DrainEvent(nil), events...)
	return f.recordOK
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
	if remote.lookupHash != hashKey(req.UserID+":"+req.IdempotentKey) {
		t.Fatalf("expected lookup by Aquifer idempotency hash, got %q", remote.lookupHash)
	}
	if entries := store.ListIdempotentKeys(); len(entries) != 0 {
		t.Fatalf("expected local speculative insert deleted after remote duplicate, got %+v", entries)
	}
}

func TestLoadRemoteIdempotencyConfigResultDefaults(t *testing.T) {
	t.Setenv("AQUIFER_REMOTE_IDEMPOTENCY_ENABLED", "true")
	t.Setenv("AQUIFER_VALKEY_URL", "redis://localhost:6379")

	cfg := LoadRemoteIdempotencyConfig()
	if cfg.ResultEnabled {
		t.Fatalf("expected remote result recording to default off")
	}
	if cfg.ResultPrefix != defaultRemoteResultPrefix {
		t.Fatalf("expected default result prefix %q, got %q", defaultRemoteResultPrefix, cfg.ResultPrefix)
	}
	if cfg.ResultMaxBytes != defaultRemoteResultMaxBytes {
		t.Fatalf("expected default result max bytes %d, got %d", defaultRemoteResultMaxBytes, cfg.ResultMaxBytes)
	}
}

func TestTruncateStringBytes(t *testing.T) {
	got, truncated := truncateStringBytes("abcdef", 3)
	if got != "abc" || !truncated {
		t.Fatalf("expected truncated abc, got %q truncated=%v", got, truncated)
	}

	got, truncated = truncateStringBytes("abcdef", 0)
	if got != "" || !truncated {
		t.Fatalf("expected empty truncated body for max 0, got %q truncated=%v", got, truncated)
	}

	got, truncated = truncateStringBytes("abc", 10)
	if got != "abc" || truncated {
		t.Fatalf("expected unchanged body, got %q truncated=%v", got, truncated)
	}
}

func TestNewValkeyRemoteIdempotencyDisabledReturnsNilInterface(t *testing.T) {
	var remote RemoteIdempotency = NewValkeyRemoteIdempotency(RemoteIdempotencyConfig{})
	if remote != nil {
		t.Fatalf("expected disabled Valkey remote idempotency to return a nil interface, got %T", remote)
	}
}

func TestDrainValkeySinkRecordsAndAcknowledges(t *testing.T) {
	remote := &fakeRemoteIdempotency{recordOK: true}
	r := drainTestRegistry(t, NoopMetricsAdapter{})
	r.drainCfg = DrainConfig{Enabled: true, TimerSeconds: 1, Sink: "valkey", BatchMaxEvents: 10}
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
	r.SetDrainRemote(remote)
	seedLedgerEntry(t, r)

	if _, ok := r.flushDrainEventBatch("ledger_batch"); ok {
		t.Fatalf("expected valkey sink batch flush to fail")
	}
	if remaining := r.store.ListDrainEvents(10); len(remaining) != 1 {
		t.Fatalf("expected unacknowledged drain event retained, got %+v", remaining)
	}
}

func TestReadRESPBulkStringAndNil(t *testing.T) {
	got, err := readRESP(bufio.NewReader(strings.NewReader("$5\r\nhello\r\n")))
	if err != nil || got != "hello" {
		t.Fatalf("expected bulk string hello, got %q err=%v", got, err)
	}

	got, err = readRESP(bufio.NewReader(strings.NewReader("$-1\r\n")))
	if err != nil || got != "" {
		t.Fatalf("expected nil bulk string as empty result, got %q err=%v", got, err)
	}
}
