package aquifer

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// A webhook receiver that doesn't speak L8 is probed once, not before every
// delivery: each probe was an extra request (and, with the old per-call
// client, a new TCP connection) per webhook.
func TestEnsureTrustRemembersNonL8Receivers(t *testing.T) {
	var probes atomic.Int64
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/l8" {
			probes.Add(1)
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer receiver.Close()

	dir := t.TempDir()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	defer l8.Close()

	for i := 0; i < 20; i++ {
		l8.EnsureTrust(receiver.URL + "/hook")
	}
	if got := probes.Load(); got != 1 {
		t.Fatalf("expected 1 probe of a non-L8 receiver across 20 webhooks, got %d", got)
	}
}
