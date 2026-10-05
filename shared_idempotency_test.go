package aquifer

import (
	"path/filepath"
	"strings"
	"testing"
)

func sharedRequest(userID, key string) JobRequest {
	req := sampleJobRequest(userID, key)
	req.IdempotencyScope = IdempotencyScopeShared
	return req
}

func TestSharedScopeCoalescesAcrossUsers(t *testing.T) {
	t.Setenv("AQUIFER_SHARED_IDEMPOTENCY_ENABLED", "true")
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})

	first, err := app.Enqueue(sharedRequest("agent-a", "weather:sf"))
	if err != nil || first.Duplicate {
		t.Fatalf("first shared enqueue: %+v, %v", first, err)
	}
	second, err := app.Enqueue(sharedRequest("agent-b", "weather:sf"))
	if err != nil {
		t.Fatalf("second shared enqueue: %v", err)
	}
	if !second.Duplicate || second.JobID != first.JobID {
		t.Fatalf("expected agent-b to join agent-a's job %s, got %+v", first.JobID, second)
	}
}

func TestUserScopeStaysIndependentAcrossUsers(t *testing.T) {
	t.Setenv("AQUIFER_SHARED_IDEMPOTENCY_ENABLED", "true")
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})

	if _, err := app.Enqueue(sharedRequest("agent-a", "weather:sf")); err != nil {
		t.Fatal(err)
	}
	for _, user := range []string{"agent-a", "agent-b"} {
		res, err := app.Enqueue(sampleJobRequest(user, "weather:sf"))
		if err != nil || res.Duplicate {
			t.Fatalf("user-scoped %s must not join the shared job: %+v, %v", user, res, err)
		}
	}
}

func TestSharedScopeRequiresFlag(t *testing.T) {
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	_, err := app.Enqueue(sharedRequest("agent-a", "weather:sf"))
	if err == nil || !strings.Contains(err.Error(), "AQUIFER_SHARED_IDEMPOTENCY_ENABLED") {
		t.Fatalf("expected flag error, got %v", err)
	}
}

func TestIdempotencyScopeValidation(t *testing.T) {
	t.Setenv("AQUIFER_SHARED_IDEMPOTENCY_ENABLED", "true")
	req := sampleJobRequest("agent-a", "k")
	req.IdempotencyScope = "global"
	if msg := req.Validate(); !strings.Contains(msg, "idempotency_scope") {
		t.Fatalf("expected scope validation error, got %q", msg)
	}
	req = sampleJobRequest("shared\x00x", "k")
	if msg := req.Validate(); !strings.Contains(msg, "NUL") {
		t.Fatalf("expected NUL user_id rejection, got %q", msg)
	}
}

func TestSharedScopeHashIsUserIndependent(t *testing.T) {
	a := dedupHash("agent-a", "k", IdempotencyScopeShared)
	if a != dedupHash("agent-b", "k", IdempotencyScopeShared) {
		t.Fatal("shared hash must not depend on user_id")
	}
	if a == dedupHash("agent-a", "k", "") || a == dedupHash("shared", "k", "") {
		t.Fatal("shared hash must not collide with a per-user hash")
	}
	if dedupHash("u", "k", "") != hashKey("u:k") {
		t.Fatal("per-user hash must stay identical to the pre-existing format")
	}
}

func TestSQLiteReloadKeepsDedupHash(t *testing.T) {
	t.Setenv("AQUIFER_SHARED_IDEMPOTENCY_ENABLED", "true")
	store := NewStore(filepath.Join(t.TempDir(), "aquifer.db"))
	t.Cleanup(func() { store.Close() })
	req := sharedRequest("agent-a", "weather:sf")
	job := NewJob(&req)
	if _, dup := store.CheckOrInsert(job); dup {
		t.Fatal("unexpected duplicate")
	}
	reloaded := store.GetJob(job.ID)
	if reloaded == nil || reloaded.dedupHash() != job.dedupHash() {
		t.Fatalf("reloaded job lost its dedup hash: %+v", reloaded)
	}
}

func TestPebbleSharedScopeCoalesces(t *testing.T) {
	t.Setenv("AQUIFER_SHARED_IDEMPOTENCY_ENABLED", "true")
	store := NewPebbleStore(filepath.Join(t.TempDir(), "pebble"))
	t.Cleanup(func() { store.Close() })
	reqA, reqB := sharedRequest("agent-a", "k"), sharedRequest("agent-b", "k")
	a, b := NewJob(&reqA), NewJob(&reqB)
	if _, dup := store.CheckOrInsert(a); dup {
		t.Fatal("unexpected duplicate for first job")
	}
	if existing, dup := store.CheckOrInsert(b); !dup || existing != a.ID {
		t.Fatalf("expected pebble to coalesce onto %s, got %q dup=%v", a.ID, existing, dup)
	}
}

func TestGetJobResultSharedScope(t *testing.T) {
	t.Setenv("AQUIFER_SHARED_IDEMPOTENCY_ENABLED", "true")
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	remote := &fakeRemoteIdempotency{result: JobResult{JobID: "job-1", Status: StatusCompleted}, resultOK: true}
	app.SetRemoteIdempotency(remote)

	if _, err := app.GetJobResult("", "weather:sf", IdempotencyScopeShared); err != nil {
		t.Fatalf("shared lookup without user_id: %v", err)
	}
	if remote.resultHash != dedupHash("", "weather:sf", IdempotencyScopeShared) {
		t.Fatalf("expected shared hash lookup, got %q", remote.resultHash)
	}
	if _, err := app.GetJobResult("", "weather:sf", ""); err == nil {
		t.Fatal("user-scoped lookup must still require user_id")
	}
}
