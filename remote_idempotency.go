package aquifer

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultRemoteIdempotencyTimeoutMS  = 25
	defaultRemoteIdempotencyTTLSeconds = 2 * 60 * 60
	defaultRemoteIdempotencyPrefix     = "aqueduct:idempotency:"
	defaultRemoteResultPrefix          = "aqueduct:result:"
	defaultRemoteResultMaxBytes        = 64 * 1024
)

type RemoteIdempotencyEntry struct {
	JobID      string `json:"job_id"`
	Status     Status `json:"status"`
	RecordedAt int64  `json:"recorded_at"`
	Source     string `json:"source"`
	ResultKey  string `json:"result_key,omitempty"`
}

type JobResult struct {
	JobID          string `json:"job_id"`
	Status         Status `json:"status"`
	ResponseStatus int    `json:"response_status"`
	ContentType    string `json:"content_type,omitempty"`
	Body           string `json:"body,omitempty"`
	BodyTruncated  bool   `json:"body_truncated,omitempty"`
	RecordedAt     int64  `json:"recorded_at"`
	Source         string `json:"source"`
}

type RemoteIdempotency interface {
	Lookup(hash string) (RemoteIdempotencyEntry, bool)
	Record(events []DrainEvent) bool
}

type JobResultRecorder interface {
	RecordResult(hash string, result JobResult) (string, bool)
}

type JobResultReader interface {
	LookupResult(hash string) (JobResult, bool)
}

type RemoteIdempotencyConfig struct {
	Enabled        bool
	URL            string
	Timeout        time.Duration
	Prefix         string
	TTLSeconds     int64
	ResultEnabled  bool
	ResultPrefix   string
	ResultMaxBytes int64
}

func LoadRemoteIdempotencyConfig() RemoteIdempotencyConfig {
	timeoutMS := envInt64("AQUIFER_REMOTE_IDEMPOTENCY_TIMEOUT_MS", defaultRemoteIdempotencyTimeoutMS)
	if timeoutMS <= 0 {
		timeoutMS = defaultRemoteIdempotencyTimeoutMS
	}
	ttl := envInt64("AQUIFER_REMOTE_IDEMPOTENCY_TTL_SECONDS", defaultRemoteIdempotencyTTLSeconds)
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
	maxBytes := envInt64("AQUIFER_REMOTE_RESULT_MAX_BYTES", defaultRemoteResultMaxBytes)
	if maxBytes < 0 {
		maxBytes = defaultRemoteResultMaxBytes
	}

	return RemoteIdempotencyConfig{
		Enabled:        envBool("AQUIFER_REMOTE_IDEMPOTENCY_ENABLED", false) || strings.EqualFold(os.Getenv("AQUIFER_DRAIN_SINK"), "valkey"),
		URL:            os.Getenv("AQUIFER_VALKEY_URL"),
		Timeout:        time.Duration(timeoutMS) * time.Millisecond,
		Prefix:         prefix,
		TTLSeconds:     ttl,
		ResultEnabled:  envBool("AQUIFER_REMOTE_RESULT_ENABLED", false),
		ResultPrefix:   resultPrefix,
		ResultMaxBytes: maxBytes,
	}
}

type ValkeyRemoteIdempotency struct {
	cfg RemoteIdempotencyConfig
}

func NewValkeyRemoteIdempotency(cfg RemoteIdempotencyConfig) RemoteIdempotency {
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
	return &ValkeyRemoteIdempotency{cfg: cfg}
}

func (v *ValkeyRemoteIdempotency) Lookup(hash string) (RemoteIdempotencyEntry, bool) {
	raw, err := v.command("GET", v.cfg.Prefix+hash)
	if err != nil {
		log.Printf("remote idempotency: lookup failed: %v", err)
		return RemoteIdempotencyEntry{}, false
	}
	if raw == "" {
		return RemoteIdempotencyEntry{}, false
	}
	var entry RemoteIdempotencyEntry
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		log.Printf("remote idempotency: invalid entry for %s: %v", hash, err)
		return RemoteIdempotencyEntry{}, false
	}
	if entry.JobID == "" {
		return RemoteIdempotencyEntry{}, false
	}
	return entry, true
}

func (v *ValkeyRemoteIdempotency) Record(events []DrainEvent) bool {
	for _, event := range events {
		entry := RemoteIdempotencyEntry{
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

func (v *ValkeyRemoteIdempotency) RecordResult(hash string, result JobResult) (string, bool) {
	if !v.cfg.ResultEnabled {
		return "", false
	}
	key := v.cfg.ResultPrefix + hash
	result.Body, result.BodyTruncated = truncateStringBytes(result.Body, v.cfg.ResultMaxBytes)
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

func (v *ValkeyRemoteIdempotency) LookupResult(hash string) (JobResult, bool) {
	raw, err := v.command("GET", v.cfg.ResultPrefix+hash)
	if err != nil {
		log.Printf("remote result: lookup failed: %v", err)
		return JobResult{}, false
	}
	if raw == "" {
		return JobResult{}, false
	}
	var result JobResult
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		log.Printf("remote result: invalid entry for %s: %v", hash, err)
		return JobResult{}, false
	}
	if result.JobID == "" {
		return JobResult{}, false
	}
	return result, true
}

func truncateStringBytes(s string, maxBytes int64) (string, bool) {
	if maxBytes == 0 {
		return "", s != ""
	}
	if maxBytes < 0 || int64(len(s)) <= maxBytes {
		return s, false
	}
	return s[:maxBytes], true
}

func (v *ValkeyRemoteIdempotency) command(args ...string) (string, error) {
	parsed, err := url.Parse(v.cfg.URL)
	if err != nil {
		return "", err
	}
	if parsed.Scheme != "redis" && parsed.Scheme != "valkey" {
		return "", fmt.Errorf("unsupported Valkey URL scheme %q", parsed.Scheme)
	}
	host := parsed.Host
	if !strings.Contains(host, ":") {
		host += ":6379"
	}

	conn, err := net.DialTimeout("tcp", host, v.cfg.Timeout)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(v.cfg.Timeout))

	reader := bufio.NewReader(conn)
	if password, ok := parsed.User.Password(); ok {
		if username := parsed.User.Username(); username != "" {
			if err := writeRESPCommand(conn, "AUTH", username, password); err != nil {
				return "", err
			}
		} else if err := writeRESPCommand(conn, "AUTH", password); err != nil {
			return "", err
		}
		if _, err := readRESP(reader); err != nil {
			return "", err
		}
	}
	if db := strings.Trim(parsed.Path, "/"); db != "" {
		if err := writeRESPCommand(conn, "SELECT", db); err != nil {
			return "", err
		}
		if _, err := readRESP(reader); err != nil {
			return "", err
		}
	}

	if err := writeRESPCommand(conn, args...); err != nil {
		return "", err
	}
	return readRESP(reader)
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
