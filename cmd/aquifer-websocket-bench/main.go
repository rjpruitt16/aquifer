// aquifer-websocket-bench measures how many aqueduct.v1 WebSocket sessions
// one Aquifer (or EZThrottle Local) instance holds, and how fast it moves
// messages through them. Two modes:
//
//	-backend :6060   run a no-limits backend: acks each command and emits one
//	                 event, so the backend is never the bottleneck
//	(default)        open -connections sessions over -ramp, optionally send
//	                 -rate commands/s on each for -hold, then report JSON
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const webSocketSubprotocol = "aqueduct.v1"

type options struct {
	target         string
	upstream       string
	authorization  string
	sessionPrefix  string
	connections    int
	ramp           time.Duration
	connectTimeout time.Duration
	hold           time.Duration
	rate           float64
	statsURL       string
}

type connectionResult struct {
	conn       *websocket.Conn
	latency    time.Duration
	httpStatus int
	err        error
}

type benchmarkReport struct {
	Attempted           int            `json:"attempted"`
	Connected           int            `json:"connected"`
	Failed              int            `json:"failed"`
	HTTPFailures        map[string]int `json:"http_failures,omitempty"`
	ErrorSamples        []string       `json:"error_samples,omitempty"`
	RampMS              int64          `json:"ramp_ms"`
	HoldMS              int64          `json:"hold_ms"`
	ElapsedMS           int64          `json:"elapsed_ms"`
	ConnectLatencyP50MS float64        `json:"connect_latency_p50_ms"`
	ConnectLatencyP95MS float64        `json:"connect_latency_p95_ms"`
	ConnectLatencyP99MS float64        `json:"connect_latency_p99_ms"`
	Messages            *messageReport `json:"messages,omitempty"`
	TargetBefore        map[string]any `json:"target_before,omitempty"`
	TargetConnected     map[string]any `json:"target_connected,omitempty"`
	TargetAfter         map[string]any `json:"target_after,omitempty"`
}

// messageReport covers the message phase: every connection sends commands
// at -rate per second for -hold. "recorded" is the target confirming the
// command is durable (command_recorded); "event" is the backend's event for
// that command arriving back at the client through the target.
type messageReport struct {
	RatePerConnection float64 `json:"rate_per_connection"`
	Sent              int64   `json:"commands_sent"`
	Recorded          int64   `json:"commands_recorded"`
	Events            int64   `json:"events_received"`
	SendErrors        int64   `json:"send_errors"`
	Disconnects       int64   `json:"disconnects"`
	SentPerSecond     float64 `json:"commands_per_s"`
	EventsPerSecond   float64 `json:"events_per_s"`
	RecordedP50MS     float64 `json:"recorded_p50_ms"`
	RecordedP99MS     float64 `json:"recorded_p99_ms"`
	EventP50MS        float64 `json:"event_round_trip_p50_ms"`
	EventP99MS        float64 `json:"event_round_trip_p99_ms"`
}

func main() {
	var cfg options
	backend := flag.String("backend", "", "serve the benchmark backend on this address (e.g. :6060) instead of running a benchmark")
	flag.StringVar(&cfg.target, "target", "ws://localhost:8080/websocket", "Aquifer WebSocket endpoint")
	flag.StringVar(&cfg.upstream, "upstream", "", "trusted upstream ws:// or wss:// URL")
	flag.StringVar(&cfg.authorization, "authorization", "", "optional Authorization header passed through the gateway boundary")
	flag.StringVar(&cfg.sessionPrefix, "session-prefix", "aquifer-bench", "unique session id prefix")
	flag.IntVar(&cfg.connections, "connections", 1000, "number of client connections to open against this Aquifer instance")
	flag.DurationVar(&cfg.ramp, "ramp", 10*time.Second, "time over which connections are opened")
	flag.DurationVar(&cfg.connectTimeout, "connect-timeout", 30*time.Second, "maximum time for each socket to reach upstream connected state")
	flag.DurationVar(&cfg.hold, "hold", 30*time.Second, "time to hold all successfully connected sockets")
	flag.Float64Var(&cfg.rate, "rate", 0, "commands per second each connection sends while held (0: hold idle)")
	flag.StringVar(&cfg.statsURL, "stats", "", "optional JSON stats URL for the target (e.g. the soak supervisor's /stats), sampled before, once connected, and after")
	flag.Parse()
	raiseFileLimit()

	if *backend != "" {
		serveBackend(*backend)
		return
	}
	if cfg.upstream == "" || cfg.connections <= 0 || cfg.ramp < 0 || cfg.connectTimeout <= 0 || cfg.hold < 0 || cfg.rate < 0 {
		flag.Usage()
		os.Exit(2)
	}

	report := run(cfg)
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if report.Failed > 0 {
		os.Exit(1)
	}
}

func run(cfg options) benchmarkReport {
	started := time.Now()
	report := benchmarkReport{
		Attempted:    cfg.connections,
		HTTPFailures: make(map[string]int),
		RampMS:       cfg.ramp.Milliseconds(),
		HoldMS:       cfg.hold.Milliseconds(),
		TargetBefore: fetchStats(cfg.statsURL),
	}

	results := make(chan connectionResult, cfg.connections)
	interval := time.Duration(0)
	if cfg.connections > 1 {
		interval = cfg.ramp / time.Duration(cfg.connections-1)
	}

	var launches sync.WaitGroup
	for i := 0; i < cfg.connections; i++ {
		launches.Add(1)
		go func(index int) {
			defer launches.Done()
			results <- openConnection(cfg, index)
		}(i)
		if interval > 0 && i+1 < cfg.connections {
			time.Sleep(interval)
		}
	}
	launches.Wait()
	close(results)

	var connections []*websocket.Conn
	var latencies []time.Duration
	for result := range results {
		if result.err != nil {
			report.Failed++
			if result.httpStatus != 0 {
				report.HTTPFailures[strconv.Itoa(result.httpStatus)]++
			}
			if len(report.ErrorSamples) < 5 {
				report.ErrorSamples = append(report.ErrorSamples, result.err.Error())
			}
			continue
		}
		report.Connected++
		connections = append(connections, result.conn)
		latencies = append(latencies, result.latency)
	}
	report.TargetConnected = fetchStats(cfg.statsURL)

	if cfg.rate > 0 && len(connections) > 0 {
		report.Messages = exchange(connections, cfg.rate, cfg.hold, cfg.sessionPrefix)
	} else {
		time.Sleep(cfg.hold)
	}
	for _, conn := range connections {
		deadline := time.Now().Add(time.Second)
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "benchmark complete"), deadline)
		_ = conn.Close()
	}
	time.Sleep(2 * time.Second) // let the target release the sessions before sampling
	report.TargetAfter = fetchStats(cfg.statsURL)

	report.ElapsedMS = time.Since(started).Milliseconds()
	report.ConnectLatencyP50MS = percentileMilliseconds(latencies, 0.50)
	report.ConnectLatencyP95MS = percentileMilliseconds(latencies, 0.95)
	report.ConnectLatencyP99MS = percentileMilliseconds(latencies, 0.99)
	if len(report.HTTPFailures) == 0 {
		report.HTTPFailures = nil
	}
	return report
}

type envelope struct {
	Type      string          `json:"type"`
	State     string          `json:"state,omitempty"`
	MessageID string          `json:"message_id,omitempty"`
	CausedBy  string          `json:"caused_by,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

// exchange sends commands on every connection at rate per second for hold,
// and times command_recorded and the backend's event for each command.
func exchange(conns []*websocket.Conn, rate float64, hold time.Duration, prefix string) *messageReport {
	rep := &messageReport{RatePerConnection: rate}
	var mu sync.Mutex
	var recorded, events []time.Duration
	var wg sync.WaitGroup
	start := time.Now()
	stopSending := start.Add(hold)

	for i, conn := range conns {
		wg.Add(1)
		go func(i int, conn *websocket.Conn) {
			defer wg.Done()
			var sentAt sync.Map // message_id -> time.Time
			done := make(chan struct{})

			go func() {
				defer close(done)
				for {
					_ = conn.SetReadDeadline(time.Now().Add(hold + 15*time.Second))
					var msg envelope
					if err := conn.ReadJSON(&msg); err != nil {
						if time.Now().Before(stopSending) {
							atomic.AddInt64(&rep.Disconnects, 1)
						}
						return
					}
					switch msg.Type {
					case "command_recorded":
						if at, ok := sentAt.Load(msg.MessageID); ok {
							atomic.AddInt64(&rep.Recorded, 1)
							mu.Lock()
							recorded = append(recorded, time.Since(at.(time.Time)))
							mu.Unlock()
						}
					case "event":
						if at, ok := sentAt.Load(msg.CausedBy); ok {
							atomic.AddInt64(&rep.Events, 1)
							mu.Lock()
							events = append(events, time.Since(at.(time.Time)))
							mu.Unlock()
						}
					}
				}
			}()

			// Stagger connections across one interval so sends don't arrive in lockstep.
			interval := time.Duration(float64(time.Second) / rate)
			time.Sleep(time.Duration(i%1000) * interval / 1000)
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for n := 0; time.Now().Before(stopSending); n++ {
				id := fmt.Sprintf("%s-c%d-m%d", prefix, i, n)
				sentAt.Store(id, time.Now())
				if err := conn.WriteJSON(envelope{Type: "command", MessageID: id, Payload: json.RawMessage(`{"action":"bench"}`)}); err != nil {
					atomic.AddInt64(&rep.SendErrors, 1)
					break
				}
				atomic.AddInt64(&rep.Sent, 1)
				select {
				case <-ticker.C:
				case <-done:
					return
				}
			}
			// Give in-flight commands a moment to come back before closing.
			select {
			case <-done:
			case <-time.After(5 * time.Second):
			}
		}(i, conn)
	}
	wg.Wait()

	secs := hold.Seconds()
	rep.SentPerSecond = float64(rep.Sent) / secs
	rep.EventsPerSecond = float64(rep.Events) / secs
	rep.RecordedP50MS, rep.RecordedP99MS = percentileMilliseconds(recorded, .5), percentileMilliseconds(recorded, .99)
	rep.EventP50MS, rep.EventP99MS = percentileMilliseconds(events, .5), percentileMilliseconds(events, .99)
	return rep
}

func openConnection(cfg options, index int) connectionResult {
	started := time.Now()
	endpoint, err := url.Parse(cfg.target)
	if err != nil {
		return connectionResult{err: err}
	}
	query := endpoint.Query()
	query.Set("session_id", fmt.Sprintf("%s-%d", cfg.sessionPrefix, index))
	query.Set("after", "0-0")
	endpoint.RawQuery = query.Encode()

	headers := http.Header{}
	headers.Set("X-Aqueduct-Upstream-URL", cfg.upstream)
	if cfg.authorization != "" {
		headers.Set("Authorization", cfg.authorization)
	}
	dialer := websocket.Dialer{
		HandshakeTimeout: cfg.connectTimeout,
		Subprotocols:     []string{webSocketSubprotocol},
	}
	conn, response, err := dialer.Dial(endpoint.String(), headers)
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
			response.Body.Close()
		}
		return connectionResult{httpStatus: status, err: err}
	}
	if err := conn.SetReadDeadline(time.Now().Add(cfg.connectTimeout)); err != nil {
		conn.Close()
		return connectionResult{err: err}
	}
	for {
		var message struct {
			Type  string `json:"type"`
			State string `json:"state"`
		}
		if err := conn.ReadJSON(&message); err != nil {
			conn.Close()
			return connectionResult{err: err}
		}
		if message.Type == "status" && message.State == "connected" {
			_ = conn.SetReadDeadline(time.Time{})
			return connectionResult{conn: conn, latency: time.Since(started)}
		}
	}
}

// serveBackend is a no-limits aqueduct.v1 backend: it advertises generous
// capacity, then answers each command with an ack and one event.
func serveBackend(addr string) {
	upgrader := websocket.Upgrader{
		Subprotocols:    []string{webSocketSubprotocol},
		CheckOrigin:     func(*http.Request) bool { return true },
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}
	var open atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/socket", func(w http.ResponseWriter, r *http.Request) {
		headers := http.Header{}
		headers.Set("X-Aqueduct-WS-Max-Connections", "1000000")
		headers.Set("X-Aqueduct-WS-Connect-Rps", "100000")
		conn, err := upgrader.Upgrade(w, r, headers)
		if err != nil {
			return
		}
		open.Add(1)
		defer open.Add(-1)
		defer conn.Close()
		for {
			var cmd envelope
			if err := conn.ReadJSON(&cmd); err != nil {
				return
			}
			if cmd.Type != "command" || cmd.MessageID == "" {
				continue
			}
			if conn.WriteJSON(envelope{Type: "ack", MessageID: "ack-" + cmd.MessageID, CausedBy: cmd.MessageID}) != nil ||
				conn.WriteJSON(envelope{Type: "event", MessageID: "event-" + cmd.MessageID, CausedBy: cmd.MessageID, Payload: json.RawMessage(`{"done":true}`)}) != nil {
				return
			}
		}
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"open_backend_sockets":%d}`, open.Load())
	})
	log.Printf("benchmark backend listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

// raiseFileLimit lifts the open-file limit as far as the process may: each
// held session is one descriptor here, and containers often cap at 10,240.
func raiseFileLimit() {
	var rl syscall.Rlimit
	if syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl) != nil {
		return
	}
	want := uint64(1 << 20)
	if raw, err := os.ReadFile("/proc/sys/fs/nr_open"); err == nil {
		if n, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64); err == nil {
			want = n
		}
	}
	if syscall.Setrlimit(syscall.RLIMIT_NOFILE, &syscall.Rlimit{Cur: want, Max: want}) != nil {
		rl.Cur = rl.Max
		syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rl)
	}
}

func fetchStats(statsURL string) map[string]any {
	if statsURL == "" {
		return nil
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Get(statsURL)
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var out map[string]any
	if json.Unmarshal(body, &out) != nil {
		return map[string]any{"error": string(body)}
	}
	return out
}

func percentileMilliseconds(values []time.Duration, quantile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	index := int(float64(len(sorted)-1) * quantile)
	return float64(sorted[index].Microseconds()) / 1000
}
