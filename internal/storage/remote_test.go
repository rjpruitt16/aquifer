// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rjpruitt16/aquifer/internal/jobs"
)

func TestLoadRemoteIdempotencyConfigResultDefaults(t *testing.T) {
	t.Setenv("AQUIFER_REMOTE_IDEMPOTENCY_ENABLED", "true")
	t.Setenv("AQUIFER_VALKEY_URL", "redis://localhost:6379")

	cfg := LoadRemoteConfig()
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
	got, truncated := TruncateStringBytes("abcdef", 3)
	if got != "abc" || !truncated {
		t.Fatalf("expected truncated abc, got %q truncated=%v", got, truncated)
	}

	got, truncated = TruncateStringBytes("abcdef", 0)
	if got != "" || !truncated {
		t.Fatalf("expected empty truncated body for max 0, got %q truncated=%v", got, truncated)
	}

	got, truncated = TruncateStringBytes("abc", 10)
	if got != "abc" || truncated {
		t.Fatalf("expected unchanged body, got %q truncated=%v", got, truncated)
	}
}

func TestNewValkeyRemoteIdempotencyDisabledReturnsNilInterface(t *testing.T) {
	var remote Remote = NewValkeyRemote(RemoteConfig{})
	if remote != nil {
		t.Fatalf("expected disabled Valkey remote idempotency to return a nil interface, got %T", remote)
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

func readRESPArray(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "*")))
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		arg, err := readRESP(r)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	return args, nil
}

func TestValkeyRemoteIdempotencyReusesConnections(t *testing.T) {
	server := startFakeValkey(t)
	remote := NewValkeyRemote(RemoteConfig{
		Enabled: true,
		URL:     "valkey://" + server.ln.Addr().String(),
		Timeout: time.Second,
	}).(*ValkeyRemote)
	t.Cleanup(func() { remote.client.Close() })

	if !remote.Record([]jobs.DrainEvent{{JobID: "job-1", HashKey: "hash-1", Status: jobs.StatusCompleted}}) {
		t.Fatalf("expected record to succeed")
	}
	entry, found := remote.Lookup("hash-1")
	if !found || entry.JobID != "job-1" {
		t.Fatalf("expected recorded entry, got %+v found=%v", entry, found)
	}
	if _, found := remote.Lookup("missing"); found {
		t.Fatalf("expected a missing key to be a miss, not an error")
	}

	for i := 0; i < 200; i++ {
		if _, found := remote.Lookup("hash-1"); !found {
			t.Fatalf("lookup %d: expected a hit", i)
		}
	}

	// 203 commands in a row; one dial per lookup would be 203 connections.
	if conns := server.conns.Load(); conns > remoteIdempotencyMinIdle+1 {
		t.Fatalf("expected the pool to reuse connections (at most %d), got %d", remoteIdempotencyMinIdle+1, conns)
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			remote.Lookup("hash-1")
		}()
	}
	wg.Wait()
}

func TestValkeyRemoteIdempotencyFailsOpenWhenUnreachable(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	remote := NewValkeyRemote(RemoteConfig{
		Enabled: true,
		URL:     "redis://" + addr,
		Timeout: 50 * time.Millisecond,
	}).(*ValkeyRemote)
	t.Cleanup(func() { remote.client.Close() })

	start := time.Now()
	if _, found := remote.Lookup("hash-1"); found {
		t.Fatalf("expected an unreachable Valkey to be a miss")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("expected the lookup timeout to bound the call, took %v", elapsed)
	}
}

// fakeValkey answers GET and SET over RESP and counts the TCP connections
// it accepts, so a test can tell pooled lookups from one dial per lookup.
type fakeValkey struct {
	ln    net.Listener
	conns atomic.Int64
	mu    sync.Mutex
	data  map[string]string
}

func startFakeValkey(t *testing.T) *fakeValkey {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f := &fakeValkey{ln: ln, data: map[string]string{}}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			f.conns.Add(1)
			go f.serve(conn)
		}
	}()
	return f
}

func (f *fakeValkey) serve(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		args, err := readRESPArray(r)
		if err != nil {
			return
		}
		var reply string
		switch strings.ToUpper(args[0]) {
		case "GET":
			f.mu.Lock()
			value, ok := f.data[args[1]]
			f.mu.Unlock()
			if ok {
				reply = fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
			} else {
				reply = "$-1\r\n"
			}
		case "SET":
			f.mu.Lock()
			f.data[args[1]] = args[2]
			f.mu.Unlock()
			reply = "+OK\r\n"
		case "PING":
			reply = "+PONG\r\n"
		default:
			reply = "-ERR unknown command\r\n"
		}
		if _, err := conn.Write([]byte(reply)); err != nil {
			return
		}
	}
}
