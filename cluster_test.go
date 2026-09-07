package aquifer

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testClusterAquifer(t *testing.T) (*Aquifer, *Store) {
	t.Helper()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "aquifer.db")
	store := NewStore(dbPath)
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	cfg := &Config{Defaults: RateConfig{RPS: 100, MaxConcurrent: 1}}
	registry := NewRegistry(store, cfg, broker, l8, NoopMetricsAdapter{}, nil)
	admission := NewAdmissionController(AdmissionLimits{}, dbPath)
	app := NewAquifer(store, registry, broker, l8, admission, nil)
	t.Cleanup(app.Close)
	return app, store
}

func testClusterRouter(self ClusterMember, members ...ClusterMember) *ClusterRouter {
	return NewClusterRouter(ClusterConfig{
		Enabled:  true,
		Self:     self,
		Members:  members,
		PruneTTL: 30 * time.Second,
	})
}

func keyOwnedBy(t *testing.T, router *ClusterRouter, ownerID string) string {
	t.Helper()

	for i := 0; i < 10000; i++ {
		key := "user-" + strconvItoa(i)
		owner, ok := router.OwnerFor(key)
		if ok && owner.ID == ownerID {
			return key
		}
	}
	t.Fatalf("could not find a key owned by %s", ownerID)
	return ""
}

func keyRankedBy(t *testing.T, router *ClusterRouter, firstID, secondID string) string {
	t.Helper()

	for i := 0; i < 10000; i++ {
		key := "user-" + strconvItoa(i)
		ranked := router.RankedOwners(key)
		if len(ranked) >= 2 && ranked[0].ID == firstID && ranked[1].ID == secondID {
			return key
		}
	}
	t.Fatalf("could not find a key ranked %s then %s", firstID, secondID)
	return ""
}

func TestClusterRouterLocatesStableOwner(t *testing.T) {
	self := ClusterMember{ID: "node-a", Address: "http://node-a"}
	other := ClusterMember{ID: "node-b", Address: "http://node-b"}
	router := testClusterRouter(self, self, other)

	first, ok := router.OwnerFor("tenant-1")
	if !ok {
		t.Fatal("expected an owner")
	}
	second, ok := router.OwnerFor("tenant-1")
	if !ok {
		t.Fatal("expected an owner on repeat lookup")
	}
	if first.ID != second.ID {
		t.Fatalf("expected stable owner, got %q then %q", first.ID, second.ID)
	}
}

func TestClusterRouterRanksOwnersStably(t *testing.T) {
	self := ClusterMember{ID: "node-a", Address: "http://node-a"}
	router := testClusterRouter(
		self,
		self,
		ClusterMember{ID: "node-b", Address: "http://node-b"},
		ClusterMember{ID: "node-c", Address: "http://node-c"},
	)

	first := router.RankedOwners("tenant-1")
	second := router.RankedOwners("tenant-1")
	if len(first) != 3 || len(second) != 3 {
		t.Fatalf("expected three ranked owners, got %d and %d", len(first), len(second))
	}
	for i := range first {
		if first[i].ID != second[i].ID {
			t.Fatalf("expected stable ranking, got %+v then %+v", first, second)
		}
	}
}

func TestClusterRouterSoftPrunesMember(t *testing.T) {
	self := ClusterMember{ID: "node-a", Address: "http://node-a"}
	other := ClusterMember{ID: "node-b", Address: "http://node-b"}
	router := NewClusterRouter(ClusterConfig{
		Enabled:  true,
		Self:     self,
		Members:  []ClusterMember{self, other},
		PruneTTL: time.Hour,
	})
	key := keyOwnedBy(t, router, other.ID)

	router.PruneMember(other.ID)

	owner, ok := router.OwnerFor(key)
	if !ok {
		t.Fatal("expected an owner after pruning")
	}
	if owner.ID == other.ID {
		t.Fatalf("expected pruned member not to own key, got %q", owner.ID)
	}
	if router.prunedCount() != 1 {
		t.Fatalf("expected one pruned member, got %d", router.prunedCount())
	}
}

func TestClusterJobsForwardToOwnerBeforeLocalPersistence(t *testing.T) {
	ownerApp, ownerStore := testClusterAquifer(t)
	ownerHTTP := httptest.NewServer(NewServer(ownerApp).Routes())
	t.Cleanup(ownerHTTP.Close)

	peerApp, peerStore := testClusterAquifer(t)
	owner := ClusterMember{ID: "owner", Address: ownerHTTP.URL}
	peer := ClusterMember{ID: "peer", Address: "http://peer.invalid"}
	router := testClusterRouter(peer, owner, peer)
	userID := keyOwnedBy(t, router, owner.ID)
	peerApp.SetClusterRouter(router)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(webhook.Close)

	body, _ := json.Marshal(JobRequest{
		UserID:        userID,
		IdempotentKey: "cluster-jobs-key",
		URL:           upstream.URL,
		Method:        "POST",
		WebhookURL:    webhook.URL,
	})
	rec := httptest.NewRecorder()
	NewServer(peerApp).Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body)))

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected owner response to be relayed, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-Aquifer-Cluster-Owner") != owner.ID {
		t.Fatalf("expected owner header %q, got %q", owner.ID, rec.Header().Get("X-Aquifer-Cluster-Owner"))
	}
	var result EnqueueResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("expected enqueue result json, got %q: %v", rec.Body.String(), err)
	}
	if ownerStore.GetJob(result.JobID) == nil {
		t.Fatal("expected owner to persist the returned job")
	}
	if peerStore.Counts().TotalJobs != 0 {
		t.Fatalf("expected peer to forward before local persistence, got counts %+v", peerStore.Counts())
	}
}

func TestClusterJobsPruneFailedOwnerAndForwardToNextRankedPeer(t *testing.T) {
	fallbackApp, fallbackStore := testClusterAquifer(t)
	fallbackHTTP := httptest.NewServer(NewServer(fallbackApp).Routes())
	t.Cleanup(fallbackHTTP.Close)

	peerApp, peerStore := testClusterAquifer(t)
	down := ClusterMember{ID: "down", Address: "http://127.0.0.1:1"}
	fallback := ClusterMember{ID: "fallback", Address: fallbackHTTP.URL}
	peer := ClusterMember{ID: "peer", Address: "http://peer.invalid"}
	router := testClusterRouter(peer, down, fallback, peer)
	userID := keyRankedBy(t, router, down.ID, fallback.ID)
	peerApp.SetClusterRouter(router)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(webhook.Close)

	body, _ := json.Marshal(JobRequest{
		UserID:        userID,
		IdempotentKey: "cluster-prune-key",
		URL:           upstream.URL,
		Method:        "POST",
		WebhookURL:    webhook.URL,
	})
	rec := httptest.NewRecorder()
	NewServer(peerApp).Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body)))

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected fallback response to be relayed, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-Aquifer-Cluster-Owner") != fallback.ID {
		t.Fatalf("expected fallback owner header %q, got %q", fallback.ID, rec.Header().Get("X-Aquifer-Cluster-Owner"))
	}
	if router.prunedCount() != 1 {
		t.Fatalf("expected failed owner to be pruned, got %d pruned", router.prunedCount())
	}
	var result EnqueueResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("expected enqueue result json, got %q: %v", rec.Body.String(), err)
	}
	if fallbackStore.GetJob(result.JobID) == nil {
		t.Fatal("expected fallback to persist the returned job")
	}
	if peerStore.Counts().TotalJobs != 0 {
		t.Fatalf("expected peer to forward before local persistence, got counts %+v", peerStore.Counts())
	}
}

func TestClusterProxyForwardsAndRelaysDirectResponse(t *testing.T) {
	ownerApp, _ := testClusterAquifer(t)
	ownerHTTP := httptest.NewServer(NewServer(ownerApp).Routes())
	t.Cleanup(ownerHTTP.Close)

	peerApp, peerStore := testClusterAquifer(t)
	owner := ClusterMember{ID: "owner", Address: ownerHTTP.URL}
	peer := ClusterMember{ID: "peer", Address: "http://peer.invalid"}
	router := testClusterRouter(peer, owner, peer)
	userID := keyOwnedBy(t, router, owner.ID)
	peerApp.SetClusterRouter(router)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream", "ok")
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte("proxied"))
	}))
	t.Cleanup(upstream.Close)

	body, _ := json.Marshal(JobRequest{
		UserID:        userID,
		IdempotentKey: "cluster-proxy-key",
		URL:           upstream.URL,
		Method:        "POST",
		WebhookURL:    "https://example.com/callback",
	})
	rec := httptest.NewRecorder()
	NewServer(peerApp).Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/proxy", bytes.NewReader(body)))

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected relayed 202, got %d: %s", rec.Code, rec.Body.String())
	}
	if strings.TrimSpace(rec.Body.String()) != "proxied" {
		t.Fatalf("expected relayed body, got %q", rec.Body.String())
	}
	if rec.Header().Get("X-Upstream") != "ok" {
		t.Fatalf("expected upstream header to be relayed")
	}
	if peerStore.Counts().TotalJobs != 0 {
		t.Fatalf("expected peer to forward before local persistence, got counts %+v", peerStore.Counts())
	}
}

func strconvItoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
