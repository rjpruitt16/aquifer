// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package admission

import (
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"testing"
	"time"
)

func TestLoadAdmissionLimitsDefaultsOnForSizeChecksOffForMemory(t *testing.T) {
	for _, key := range []string{"AQUIFER_MEMORY_LIMIT_MB", "AQUIFER_MAX_BODY_BYTES", "AQUIFER_DB_MAX_BYTES", "AQUIFER_RETRY_AFTER_SECONDS"} {
		old, had := os.LookupEnv(key)
		os.Unsetenv(key)
		if had {
			t.Cleanup(func() { os.Setenv(key, old) })
		}
	}

	limits := LoadLimits()

	if limits.MemoryLimitMB != 0 {
		t.Fatalf("expected memory limit to stay disabled by default, got %d", limits.MemoryLimitMB)
	}
	if limits.MaxBodyBytes != defaultMaxBodyBytes {
		t.Fatalf("expected default max body bytes %d, got %d", defaultMaxBodyBytes, limits.MaxBodyBytes)
	}
	if limits.DBMaxBytes != autoDBMaxBytes {
		t.Fatalf("expected an unset AQUIFER_DB_MAX_BYTES to mean auto (%d), got %d", autoDBMaxBytes, limits.DBMaxBytes)
	}
}

func TestAutoDBMaxBytesIsDerivedFromTheDisk(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "aquifer.db")
	free, ok := volumeAvailableBytes(filepath.Dir(dbPath))
	if !ok {
		t.Skip("disk space can't be measured on this platform")
	}

	c := NewController(Limits{DBMaxBytes: autoDBMaxBytes}, dbPath)

	got := c.limits.DBMaxBytes
	if got <= 0 {
		t.Fatalf("expected a positive derived ceiling, got %d", got)
	}
	// The disk's free space moves while tests run; allow a little slack
	// around 80% of it.
	want := int64(float64(free) * dbShareOfDisk)
	if diff := got - want; diff > want/20 || diff < -want/20 {
		t.Fatalf("expected about %d (80%% of %d free), got %d", want, free, got)
	}
}

func TestExplicitDBMaxBytesIsNotDerived(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "aquifer.db")
	if got := NewController(Limits{DBMaxBytes: 12345}, dbPath).limits.DBMaxBytes; got != 12345 {
		t.Fatalf("expected explicit ceiling 12345 to be kept, got %d", got)
	}
	if got := NewController(Limits{DBMaxBytes: 0}, dbPath).limits.DBMaxBytes; got != 0 {
		t.Fatalf("expected explicit 0 to keep the check disabled, got %d", got)
	}
}

func TestLoadAdmissionLimitsExplicitZeroStillDisables(t *testing.T) {
	os.Setenv("AQUIFER_MAX_BODY_BYTES", "0")
	os.Setenv("AQUIFER_DB_MAX_BYTES", "0")
	t.Cleanup(func() {
		os.Unsetenv("AQUIFER_MAX_BODY_BYTES")
		os.Unsetenv("AQUIFER_DB_MAX_BYTES")
	})

	limits := LoadLimits()

	if limits.MaxBodyBytes != 0 {
		t.Fatalf("expected explicit AQUIFER_MAX_BODY_BYTES=0 to disable the check, got %d", limits.MaxBodyBytes)
	}
	if limits.DBMaxBytes != 0 {
		t.Fatalf("expected explicit AQUIFER_DB_MAX_BYTES=0 to disable the check, got %d", limits.DBMaxBytes)
	}
}

func TestRetryAfterDoublesOnConsecutiveRejections(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "aquifer.db")
	// 1MB is far below any running process's actual footprint, so every
	// Check() below deterministically rejects.
	c := NewController(Limits{MemoryLimitMB: 1, RetryAfterSeconds: 5}, dbPath)

	want := []int{5, 10, 20, 40, 60, 60, 60}
	for i, w := range want {
		c.Check()
		if got := c.RetryAfterSeconds(); got != w {
			t.Fatalf("rejection #%d: expected Retry-After=%d, got %d", i+1, w, got)
		}
	}
}

func TestRetryAfterResetsAfterAllowedRequest(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "aquifer.db")
	c := NewController(Limits{MemoryLimitMB: 1, RetryAfterSeconds: 5}, dbPath)

	c.Check()
	c.Check()
	c.Check()
	if got := c.RetryAfterSeconds(); got != 20 {
		t.Fatalf("expected backoff to have grown to 20 after 3 rejections, got %d", got)
	}

	// Raise the limit so the next Check() is allowed, then drop it back down.
	c.limits.MemoryLimitMB = 1_000_000
	c.Check()
	c.limits.MemoryLimitMB = 1

	if got := c.RetryAfterSeconds(); got != 5 {
		t.Fatalf("expected Retry-After to reset to base 5 after an allowed request, got %d", got)
	}
}

func TestRetryAfterDefaultsWhenUnconfigured(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "aquifer.db")
	c := NewController(Limits{MemoryLimitMB: 1}, dbPath)

	if got := c.RetryAfterSeconds(); got != 5 {
		t.Fatalf("expected default base Retry-After=5 with no rejections yet, got %d", got)
	}
}

// Storm test 2: one spike past the limit left MemStats.Sys above it for
// good, and admission refused every job from then on. The measure admission
// uses now has to come back down once the runtime returns the memory.
func TestAdmissionMemoryComesBackDownAfterASpike(t *testing.T) {
	debug.FreeOSMemory()
	before := processMemoryMB()

	spike := make([][]byte, 0, 256)
	for i := 0; i < 256; i++ {
		b := make([]byte, 1<<20)
		for j := range b {
			b[j] = byte(j)
		}
		spike = append(spike, b)
	}
	during := processMemoryMB()
	if during < before+200 {
		t.Fatalf("a 256MB spike should show: before %dMB, during %dMB", before, during)
	}
	runtime.KeepAlive(spike)
	spike = nil

	debug.FreeOSMemory()
	after := processMemoryMB()
	if after > before+64 {
		t.Fatalf("memory in use stayed up after the spike was freed: before %dMB, during %dMB, after %dMB", before, during, after)
	}
	if reserved := reservedMemoryMB(); reserved < during {
		t.Fatalf("reserved memory (the old measure) should still count the spike: %dMB < %dMB", reserved, during)
	}

	c := NewController(Limits{MemoryLimitMB: during - 64}, "")
	if d := c.Check(); !d.Allowed {
		t.Fatalf("after the spike, admission still refuses: %+v", d)
	}
}

func TestDefaultMemoryLimitFollowsTheMachine(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	missing := filepath.Join(dir, "missing")
	meminfo := write("meminfo", "MemTotal:        2048000 kB\nMemFree:          100000 kB\n")

	// A container limit wins over the machine's memory.
	v2 := write("memory.max", "1073741824\n") // 1GiB
	if got := deriveMemoryLimitMB(memorySources{cgroupV2: v2, cgroupV1: missing, meminfo: meminfo}); got != 716 {
		t.Fatalf("cgroup v2 1GiB: got %dMB, want 716 (70%%)", got)
	}

	// No limit ("max" in v2, a huge number in v1): fall back to MemTotal.
	unlimited := write("memory.max.unlimited", "max\n")
	v1huge := write("limit_in_bytes", "9223372036854771712\n")
	if got := deriveMemoryLimitMB(memorySources{cgroupV2: unlimited, cgroupV1: v1huge, meminfo: meminfo}); got != 1400 {
		t.Fatalf("2,048,000kB machine: got %dMB, want 1400", got)
	}

	v1 := write("limit_in_bytes.512m", "536870912\n")
	if got := deriveMemoryLimitMB(memorySources{cgroupV2: missing, cgroupV1: v1, meminfo: meminfo}); got != 358 {
		t.Fatalf("cgroup v1 512MiB: got %dMB, want 358", got)
	}

	// Nothing readable (macOS): unbounded, as before.
	if got := deriveMemoryLimitMB(memorySources{cgroupV2: missing, cgroupV1: missing, meminfo: missing}); got != 0 {
		t.Fatalf("nothing to read: got %dMB, want 0", got)
	}
}

func TestExplicitMemoryLimitZeroStaysOff(t *testing.T) {
	t.Setenv("AQUIFER_MEMORY_LIMIT_MB", "0")
	if got := LoadLimits().MemoryLimitMB; got != 0 {
		t.Fatalf("AQUIFER_MEMORY_LIMIT_MB=0: got %d", got)
	}
	t.Setenv("AQUIFER_MEMORY_LIMIT_MB", "900")
	if got := LoadLimits().MemoryLimitMB; got != 900 {
		t.Fatalf("AQUIFER_MEMORY_LIMIT_MB=900: got %d", got)
	}
}

type fixedSyncLag time.Duration

func (f fixedSyncLag) SyncLag() time.Duration { return time.Duration(f) }

// Storm test 1: under memory pressure the WAL sync fell 30 to 50 seconds
// behind, and an OOM kill lost 1,440 jobs that had already been accepted.
// Admission now says "later" instead of accepting while syncs are behind.
func TestAdmissionRefusesWhileWALSyncIsBehind(t *testing.T) {
	c := NewController(Limits{MaxSyncLagMS: 1000}, "")
	c.WatchSyncLag(fixedSyncLag(1500 * time.Millisecond))
	d := c.Check()
	if d.Allowed || d.Reason != "sync_lag" || d.Limit != 1000 || d.Current != 1500 {
		t.Fatalf("1.5s behind a 1s limit: %+v", d)
	}
	if got := c.FullnessRatio(); got < 1 {
		t.Fatalf("fullness should be full while refusing: %v", got)
	}

	c.WatchSyncLag(fixedSyncLag(120 * time.Millisecond))
	if d := c.Check(); !d.Allowed {
		t.Fatalf("a normal 120ms lag must be allowed: %+v", d)
	}

	off := NewController(Limits{MaxSyncLagMS: 0}, "")
	off.WatchSyncLag(fixedSyncLag(time.Minute))
	if d := off.Check(); !d.Allowed {
		t.Fatalf("AQUIFER_MAX_SYNC_LAG_MS=0 disables the check: %+v", d)
	}

	// A store that syncs every write never reports a lag.
	plain := NewController(Limits{MaxSyncLagMS: 1000}, "")
	plain.WatchSyncLag(struct{}{})
	if d := plain.Check(); !d.Allowed {
		t.Fatalf("no sync lag reporter: %+v", d)
	}
}
