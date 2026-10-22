// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"path/filepath"
	"testing"

	"github.com/rjpruitt16/aquifer/internal/jobs"
)

func TestNewJobStoreDefaultsToPebble(t *testing.T) {
	t.Setenv("AQUIFER_STORE_BACKEND", "")
	store := Open(filepath.Join(t.TempDir(), "aquifer.db"))
	defer store.Close()
	if _, ok := store.(*Pebble); !ok {
		t.Fatalf("a fresh DB_PATH should default to Pebble, got %T", store)
	}
}

func TestNewJobStoreKeepsAnExistingSQLiteDatabase(t *testing.T) {
	t.Setenv("AQUIFER_STORE_BACKEND", "")
	path := filepath.Join(t.TempDir(), "aquifer.db")
	NewSQLite(path).Close()

	store := Open(path)
	defer store.Close()
	if _, ok := store.(*SQLite); !ok {
		t.Fatalf("an existing SQLite file must keep SQLite so queued jobs aren't orphaned, got %T", store)
	}
}

func TestNewJobStoreHonorsExplicitBackend(t *testing.T) {
	t.Setenv("AQUIFER_STORE_BACKEND", "sqlite")
	store := Open(filepath.Join(t.TempDir(), "aquifer.db"))
	defer store.Close()
	if _, ok := store.(*SQLite); !ok {
		t.Fatalf("AQUIFER_STORE_BACKEND=sqlite must select SQLite, got %T", store)
	}
}

func sampleJobRequest(userID, idempotentKey string) jobs.Request {
	return jobs.Request{
		UserID:        userID,
		IdempotentKey: idempotentKey,
		URL:           "https://example.com/webhook",
		Method:        "POST",
		WebhookURL:    "https://example.com/callback",
	}
}
