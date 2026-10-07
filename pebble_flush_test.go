package aquifer

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// With the flush interval (100ms by default), writes are acknowledged before
// they are synced. A process killed after the flush interval must still
// have them on restart: the timer's WAL sync covers every earlier NoSync
// write. The child exits without Close, like a kill -9.
func TestPebbleFlushIntervalSurvivesCrash(t *testing.T) {
	if dir := os.Getenv("AQUIFER_PEBBLE_FLUSH_CHILD_DIR"); dir != "" {
		store := NewPebbleStore(dir)
		for i := 0; i < 50; i++ {
			job := NewJob(&JobRequest{UserID: "u", IdempotentKey: fmt.Sprintf("k-%d", i), URL: "http://x", Method: "POST", WebhookURL: "http://w"})
			job.ID = fmt.Sprintf("job-%d", i)
			if _, dup := store.CheckOrInsert(job); dup {
				os.Exit(2)
			}
		}
		time.Sleep(300 * time.Millisecond)
		os.Exit(0)
	}

	dir := filepath.Join(t.TempDir(), "pebble")
	cmd := exec.Command(os.Args[0], "-test.run", "^TestPebbleFlushIntervalSurvivesCrash$")
	cmd.Env = append(os.Environ(), "AQUIFER_PEBBLE_FLUSH_CHILD_DIR="+dir, "AQUIFER_PEBBLE_FLUSH_INTERVAL_MS=100")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child: %v\n%s", err, out)
	}

	store := NewPebbleStore(dir)
	defer store.Close()
	for i := 0; i < 50; i++ {
		if store.GetJob(fmt.Sprintf("job-%d", i)) == nil {
			t.Fatalf("job-%d lost after a crash that came after the flush interval", i)
		}
	}
}

// Writes made just before a crash, inside the flush window, are the ones
// the default allows to be lost. AQUIFER_PEBBLE_FLUSH_INTERVAL_MS=0 syncs
// every write: the same crash right after writing loses nothing.
func TestPebbleFlushIntervalZeroSyncsEveryWrite(t *testing.T) {
	if dir := os.Getenv("AQUIFER_PEBBLE_SYNC_CHILD_DIR"); dir != "" {
		store := NewPebbleStore(dir)
		for i := 0; i < 50; i++ {
			job := NewJob(&JobRequest{UserID: "u", IdempotentKey: fmt.Sprintf("k-%d", i), URL: "http://x", Method: "POST", WebhookURL: "http://w"})
			job.ID = fmt.Sprintf("job-%d", i)
			store.CheckOrInsert(job)
		}
		os.Exit(0)
	}

	dir := filepath.Join(t.TempDir(), "pebble")
	cmd := exec.Command(os.Args[0], "-test.run", "^TestPebbleFlushIntervalZeroSyncsEveryWrite$")
	cmd.Env = append(os.Environ(), "AQUIFER_PEBBLE_SYNC_CHILD_DIR="+dir, "AQUIFER_PEBBLE_FLUSH_INTERVAL_MS=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child: %v\n%s", err, out)
	}

	store := NewPebbleStore(dir)
	defer store.Close()
	for i := 0; i < 50; i++ {
		if store.GetJob(fmt.Sprintf("job-%d", i)) == nil {
			t.Fatalf("job-%d lost with per-write sync", i)
		}
	}
}
