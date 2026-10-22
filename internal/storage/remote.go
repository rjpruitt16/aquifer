// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rjpruitt16/aquifer/internal/cluster"
	"github.com/rjpruitt16/aquifer/internal/env"
	"github.com/rjpruitt16/aquifer/internal/jobs"
)

const (
	defaultRemoteIdempotencyTimeoutMS  = 25
	defaultRemoteIdempotencyTTLSeconds = 2 * 60 * 60
	defaultRemoteIdempotencyPrefix     = "aqueduct:idempotency:"
	defaultRemoteResultPrefix          = "aqueduct:result:"
	defaultRemoteResultMaxBytes        = 64 * 1024
	// A pooled connection is dialed once and reused, so the dial gets more
	// room than the per-lookup timeout; a cold dial inside 25ms is what made
	// lookups fail under load when every lookup opened its own connection.
	remoteIdempotencyDialTimeout = time.Second
	remoteIdempotencyMinIdle     = 2
)

type RemoteEntry struct {
	JobID      string      `json:"job_id"`
	Status     jobs.Status `json:"status"`
	RecordedAt int64       `json:"recorded_at"`
	Source     string      `json:"source"`
	ResultKey  string      `json:"result_key,omitempty"`
}

type Remote interface {
	Lookup(hash string) (RemoteEntry, bool)
	Record(events []jobs.DrainEvent) bool
}

type ResultRecorder interface {
	RecordResult(hash string, result jobs.Result) (string, bool)
}

type ResultReader interface {
	LookupResult(hash string) (jobs.Result, bool)
}

type RemoteConfig struct {
	Enabled        bool
	URL            string
	Timeout        time.Duration
	Prefix         string
	TTLSeconds     int64
	ResultEnabled  bool
	ResultPrefix   string
	ResultMaxBytes int64
}

func LoadRemoteConfig() RemoteConfig {
	timeoutMS := env.Int64("AQUIFER_REMOTE_IDEMPOTENCY_TIMEOUT_MS", defaultRemoteIdempotencyTimeoutMS)
	if timeoutMS <= 0 {
		timeoutMS = defaultRemoteIdempotencyTimeoutMS
	}
	ttl := env.Int64("AQUIFER_REMOTE_IDEMPOTENCY_TTL_SECONDS", defaultRemoteIdempotencyTTLSeconds)
	if ttl <= 0 {
		ttl = defaultRemoteIdempotencyTTLSeconds
	}
	prefix := os.Getenv("AQUIFER_REMOTE_IDEMPOTENCY_PREFIX")
	if prefix == "" {
		prefix = defaultRemoteIdempotencyPrefix
	}
	resultPrefix := os.Getenv("AQUIFER_REMOTE_RESULT_PREFIX")
	if resultPrefix == "" {
		resultPrefix = defaultRemoteResultPrefix
	}
	maxBytes := env.Int64("AQUIFER_REMOTE_RESULT_MAX_BYTES", defaultRemoteResultMaxBytes)
	if maxBytes < 0 {
		maxBytes = defaultRemoteResultMaxBytes
	}

	return RemoteConfig{
		Enabled:        env.Bool("AQUIFER_REMOTE_IDEMPOTENCY_ENABLED", false) || strings.EqualFold(os.Getenv("AQUIFER_DRAIN_SINK"), "valkey"),
		URL:            os.Getenv("AQUIFER_VALKEY_URL"),
		Timeout:        time.Duration(timeoutMS) * time.Millisecond,
		Prefix:         prefix,
		TTLSeconds:     ttl,
		ResultEnabled:  env.Bool("AQUIFER_REMOTE_RESULT_ENABLED", false),
		ResultPrefix:   resultPrefix,
		ResultMaxBytes: maxBytes,
	}
}

type ValkeyRemote struct {
	cfg    RemoteConfig
	client *redis.Client
}

func NewValkeyRemote(cfg RemoteConfig) Remote {
	if !cfg.Enabled {
		return nil
	}
	if cfg.URL == "" {
		log.Printf("remote idempotency: enabled but AQUIFER_VALKEY_URL is not set")
		return nil
	}
	if cfg.Prefix == "" {
		cfg.Prefix = defaultRemoteIdempotencyPrefix
	}
	if cfg.ResultPrefix == "" {
		cfg.ResultPrefix = defaultRemoteResultPrefix
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultRemoteIdempotencyTimeoutMS * time.Millisecond
	}
	if cfg.TTLSeconds <= 0 {
		cfg.TTLSeconds = defaultRemoteIdempotencyTTLSeconds
	}
	client, err := newRemoteClient(cfg)
	if err != nil {
		log.Printf("remote idempotency: %v", err)
		return nil
	}
	return &ValkeyRemote{cfg: cfg, client: client}
}

// newRemoteClient builds one pooled client for the life of the
// process. Each lookup used to dial, authenticate and close its own TCP
// connection; at a few hundred jobs per second that filled the kernel's
// TIME_WAIT table and pushed DNS plus dial past the lookup timeout, so the
// cross-node check silently fell back to local-only exactly when load was
// highest.
func newRemoteClient(cfg RemoteConfig) (*redis.Client, error) {
	normalized, err := cluster.NormalizeValkeyURL(cfg.URL)
	if err != nil {
		return nil, err
	}
	opts, err := redis.ParseURL(normalized)
	if err != nil {
		return nil, fmt.Errorf("parse Valkey URL: %w", err)
	}
	opts.DialTimeout = remoteIdempotencyDialTimeout
	opts.ReadTimeout = cfg.Timeout
	opts.WriteTimeout = cfg.Timeout
	opts.MinIdleConns = remoteIdempotencyMinIdle
	opts.MaxRetries = 0
	opts.DisableIdentity = true
	return redis.NewClient(opts), nil
}

func (v *ValkeyRemote) Lookup(hash string) (RemoteEntry, bool) {
	raw, err := v.command("GET", v.cfg.Prefix+hash)
	if err != nil {
		log.Printf("remote idempotency: lookup failed: %v", err)
		return RemoteEntry{}, false
	}
	if raw == "" {
		return RemoteEntry{}, false
	}
	var entry RemoteEntry
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		log.Printf("remote idempotency: invalid entry for %s: %v", hash, err)
		return RemoteEntry{}, false
	}
	if entry.JobID == "" {
		return RemoteEntry{}, false
	}
	return entry, true
}

func (v *ValkeyRemote) Record(events []jobs.DrainEvent) bool {
	for _, event := range events {
		entry := RemoteEntry{
			JobID:      event.JobID,
			Status:     event.Status,
			RecordedAt: event.RecordedAt,
			Source:     "aquifer",
		}
		if v.cfg.ResultEnabled {
			entry.ResultKey = v.cfg.ResultPrefix + event.HashKey
		}
		payload, err := json.Marshal(entry)
		if err != nil {
			log.Printf("remote idempotency: marshal event %d: %v", event.Sequence, err)
			return false
		}
		if _, err := v.command("SET", v.cfg.Prefix+event.HashKey, string(payload), "EX", strconv.FormatInt(v.cfg.TTLSeconds, 10)); err != nil {
			log.Printf("remote idempotency: record event %d failed: %v", event.Sequence, err)
			return false
		}
	}
	return true
}

func (v *ValkeyRemote) RecordResult(hash string, result jobs.Result) (string, bool) {
	if !v.cfg.ResultEnabled {
		return "", false
	}
	key := v.cfg.ResultPrefix + hash
	result.Body, result.BodyTruncated = TruncateStringBytes(result.Body, v.cfg.ResultMaxBytes)
	if result.Source == "" {
		result.Source = "aquifer"
	}
	if result.RecordedAt == 0 {
		result.RecordedAt = time.Now().UnixMilli()
	}
	payload, err := json.Marshal(result)
	if err != nil {
		log.Printf("remote result: marshal job %s: %v", result.JobID, err)
		return "", false
	}
	if _, err := v.command("SET", key, string(payload), "EX", strconv.FormatInt(v.cfg.TTLSeconds, 10)); err != nil {
		log.Printf("remote result: record job %s failed: %v", result.JobID, err)
		return "", false
	}
	return key, true
}

func (v *ValkeyRemote) LookupResult(hash string) (jobs.Result, bool) {
	raw, err := v.command("GET", v.cfg.ResultPrefix+hash)
	if err != nil {
		log.Printf("remote result: lookup failed: %v", err)
		return jobs.Result{}, false
	}
	if raw == "" {
		return jobs.Result{}, false
	}
	var result jobs.Result
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		log.Printf("remote result: invalid entry for %s: %v", hash, err)
		return jobs.Result{}, false
	}
	if result.JobID == "" {
		return jobs.Result{}, false
	}
	return result, true
}

// TruncateStringBytes cuts s to at most maxBytes, reporting whether it did.
func TruncateStringBytes(s string, maxBytes int64) (string, bool) {
	if maxBytes == 0 {
		return "", s != ""
	}
	if maxBytes < 0 || int64(len(s)) <= maxBytes {
		return s, false
	}
	return s[:maxBytes], true
}

func (v *ValkeyRemote) command(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), v.cfg.Timeout)
	defer cancel()
	cmdArgs := make([]any, len(args))
	for i, arg := range args {
		cmdArgs[i] = arg
	}
	reply, err := v.client.Do(ctx, cmdArgs...).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	switch value := reply.(type) {
	case string:
		return value, nil
	case int64:
		return strconv.FormatInt(value, 10), nil
	default:
		return fmt.Sprint(value), nil
	}
}

func writeRESPCommand(w io.Writer, args ...string) error {
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, arg := range args {
		if _, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(arg), arg); err != nil {
			return err
		}
	}
	return nil
}

func readRESP(r *bufio.Reader) (string, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return "", err
	}
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")

	switch prefix {
	case '+':
		return line, nil
	case '-':
		return "", fmt.Errorf("valkey error: %s", line)
	case ':':
		return line, nil
	case '$':
		n, err := strconv.Atoi(line)
		if err != nil {
			return "", err
		}
		if n < 0 {
			return "", nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", err
		}
		return string(buf[:n]), nil
	default:
		return "", fmt.Errorf("unsupported valkey response prefix %q", prefix)
	}
}
