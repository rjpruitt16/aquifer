package aquifer

import (
	"path/filepath"
	"testing"
)

func TestNewJobStoreDefaultsToPebble(t *testing.T) {
	t.Setenv("AQUIFER_STORE_BACKEND", "")
	store := NewJobStore(filepath.Join(t.TempDir(), "aquifer.db"))
	defer store.Close()
	if _, ok := store.(*PebbleStore); !ok {
		t.Fatalf("a fresh DB_PATH should default to Pebble, got %T", store)
	}
}

func TestNewJobStoreKeepsAnExistingSQLiteDatabase(t *testing.T) {
	t.Setenv("AQUIFER_STORE_BACKEND", "")
	path := filepath.Join(t.TempDir(), "aquifer.db")
	NewStore(path).Close()

	store := NewJobStore(path)
	defer store.Close()
	if _, ok := store.(*Store); !ok {
		t.Fatalf("an existing SQLite file must keep SQLite so queued jobs aren't orphaned, got %T", store)
	}
}

func TestNewJobStoreHonorsExplicitBackend(t *testing.T) {
	t.Setenv("AQUIFER_STORE_BACKEND", "sqlite")
	store := NewJobStore(filepath.Join(t.TempDir(), "aquifer.db"))
	defer store.Close()
	if _, ok := store.(*Store); !ok {
		t.Fatalf("AQUIFER_STORE_BACKEND=sqlite must select SQLite, got %T", store)
	}
}
