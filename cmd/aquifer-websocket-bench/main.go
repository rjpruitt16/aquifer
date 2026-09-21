package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"sync"
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
	RampMS              int64          `json:"ramp_ms"`
	HoldMS              int64          `json:"hold_ms"`
	ElapsedMS           int64          `json:"elapsed_ms"`
	ConnectLatencyP50MS float64        `json:"connect_latency_p50_ms"`
	ConnectLatencyP95MS float64        `json:"connect_latency_p95_ms"`
	ConnectLatencyP99MS float64        `json:"connect_latency_p99_ms"`
}

func main() {
	var cfg options
	flag.StringVar(&cfg.target, "target", "ws://localhost:8080/websocket", "Aquifer WebSocket endpoint")
	flag.StringVar(&cfg.upstream, "upstream", "", "trusted upstream ws:// or wss:// URL")
	flag.StringVar(&cfg.authorization, "authorization", "", "optional Authorization header passed through the gateway boundary")
	flag.StringVar(&cfg.sessionPrefix, "session-prefix", "aquifer-bench", "unique session id prefix")
	flag.IntVar(&cfg.connections, "connections", 1000, "number of client connections to open against this Aquifer instance")
	flag.DurationVar(&cfg.ramp, "ramp", 10*time.Second, "time over which connections are opened")
	flag.DurationVar(&cfg.connectTimeout, "connect-timeout", 30*time.Second, "maximum time for each socket to reach upstream connected state")
	flag.DurationVar(&cfg.hold, "hold", 30*time.Second, "time to hold all successfully connected sockets")
	flag.Parse()

	if cfg.upstream == "" || cfg.connections <= 0 || cfg.ramp < 0 || cfg.connectTimeout <= 0 || cfg.hold < 0 {
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

	report := benchmarkReport{
		Attempted:    cfg.connections,
		HTTPFailures: make(map[string]int),
		RampMS:       cfg.ramp.Milliseconds(),
		HoldMS:       cfg.hold.Milliseconds(),
	}
	var connections []*websocket.Conn
	var latencies []time.Duration
	for result := range results {
		if result.err != nil {
			report.Failed++
			if result.httpStatus != 0 {
				report.HTTPFailures[strconv.Itoa(result.httpStatus)]++
			}
			continue
		}
		report.Connected++
		connections = append(connections, result.conn)
		latencies = append(latencies, result.latency)
	}

	time.Sleep(cfg.hold)
	for _, conn := range connections {
		deadline := time.Now().Add(time.Second)
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "benchmark complete"), deadline)
		_ = conn.Close()
	}

	report.ElapsedMS = time.Since(started).Milliseconds()
	report.ConnectLatencyP50MS = percentileMilliseconds(latencies, 0.50)
	report.ConnectLatencyP95MS = percentileMilliseconds(latencies, 0.95)
	report.ConnectLatencyP99MS = percentileMilliseconds(latencies, 0.99)
	if len(report.HTTPFailures) == 0 {
		report.HTTPFailures = nil
	}
	return report
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

func percentileMilliseconds(values []time.Duration, quantile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	index := int(float64(len(sorted)-1) * quantile)
	return float64(sorted[index].Microseconds()) / 1000
}
