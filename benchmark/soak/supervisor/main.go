// supervisor: runs Aquifer for the soak test (see benchmark/soak/README.md).
// It restarts Aquifer whenever it exits and, on a private port, lets the soak
// driver kill it (SIGKILL, the same as `kill -9`), restart the whole machine,
// read process and disk stats, and tail the log. The port has no public Fly
// service, so only machines in the same Fly organization can reach it.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type supervisor struct {
	cmdPath  string
	dataDir  string
	debugURL string
	logFile  string

	mu        sync.Mutex
	proc      *os.Process
	startedAt time.Time
	restarts  int
	kills     int
	lastExit  string
	lines     []string // ring buffer of recent log lines
	next      int
	full      bool
	logOut    *os.File
	logBytes  int64
	bootAt    time.Time
	stopping  bool // shutting down: don't restart aquifer
}

const ringSize = 5000
const maxLogBytes = 64 << 20

func main() {
	listen := flag.String("listen", ":9090", "control listener (private network only)")
	cmdPath := flag.String("cmd", "/app/aquifer", "Aquifer binary")
	dataDir := flag.String("data", "/data", "Aquifer data directory, measured for size and file count")
	debugURL := flag.String("debug", "http://127.0.0.1:6060", "Aquifer's AQUIFER_DEBUG_ADDR, for goroutine and heap stats")
	flag.Parse()

	s := &supervisor{cmdPath: *cmdPath, dataDir: *dataDir, debugURL: *debugURL,
		logFile: filepath.Join(*dataDir, "aquifer.log"), lines: make([]string, ringSize), bootAt: time.Now()}
	s.openLog()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /kill", s.handleKill)
	mux.HandleFunc("POST /reboot", s.handleReboot)
	mux.HandleFunc("GET /stats", s.handleStats)
	mux.HandleFunc("GET /logs", s.handleLogs)
	go func() { log.Fatal(http.ListenAndServe(*listen, mux)) }()
	go s.forwardShutdown()

	for {
		s.runOnce()
		s.mu.Lock()
		stopping := s.stopping
		s.mu.Unlock()
		if stopping {
			select {} // forwardShutdown exits the process
		}
		time.Sleep(time.Second)
	}
}

// forwardShutdown passes the platform's stop signal (a deploy, fly machine
// stop) on to Aquifer as SIGTERM so it drains gracefully, then exits once
// Aquifer has. Without it the supervisor died at once and Aquifer was
// killed hard when the machine stopped.
func (s *supervisor) forwardShutdown() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	got := <-sig
	s.record(fmt.Sprintf("supervisor: %v received, sending SIGTERM to aquifer", got))
	s.mu.Lock()
	s.stopping = true
	p := s.proc
	s.mu.Unlock()
	if p != nil {
		p.Signal(syscall.SIGTERM)
		deadline := time.Now().Add(25 * time.Second)
		for time.Now().Before(deadline) {
			s.mu.Lock()
			running := s.proc != nil
			s.mu.Unlock()
			if !running {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	s.record("supervisor: exiting")
	os.Exit(0)
}

func (s *supervisor) runOnce() {
	cmd := exec.Command(s.cmdPath)
	cmd.Env = os.Environ()
	out, _ := cmd.StdoutPipe()
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		s.record("supervisor: start failed: " + err.Error())
		return
	}
	s.mu.Lock()
	s.proc = cmd.Process
	s.startedAt = time.Now()
	s.mu.Unlock()
	s.record(fmt.Sprintf("supervisor: started aquifer pid %d", cmd.Process.Pid))

	sc := bufio.NewScanner(out)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		s.record(sc.Text())
	}
	err := cmd.Wait()
	s.mu.Lock()
	s.proc = nil
	s.restarts++
	s.lastExit = fmt.Sprintf("%s at %s", exitText(err), time.Now().UTC().Format(time.RFC3339))
	exit := s.lastExit
	s.mu.Unlock()
	s.record("supervisor: aquifer exited: " + exit)
}

func exitText(err error) string {
	if err == nil {
		return "exit 0"
	}
	return err.Error()
}

// record keeps the line in memory for /logs, prints it for `fly logs`, and
// appends it to a capped file on the volume so it survives a machine restart.
func (s *supervisor) record(line string) {
	line = time.Now().UTC().Format("2006-01-02T15:04:05.000Z ") + line
	fmt.Println(line)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lines[s.next] = line
	s.next = (s.next + 1) % ringSize
	if s.next == 0 {
		s.full = true
	}
	if s.logOut != nil {
		n, _ := s.logOut.WriteString(line + "\n")
		s.logBytes += int64(n)
		if s.logBytes > maxLogBytes {
			s.logOut.Close()
			os.Rename(s.logFile, s.logFile+".1")
			s.logOut, _ = os.OpenFile(s.logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
			s.logBytes = 0
		}
	}
}

func (s *supervisor) openLog() {
	f, err := os.OpenFile(s.logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Printf("supervisor: log file: %v", err)
		return
	}
	if st, err := f.Stat(); err == nil {
		s.logBytes = st.Size()
	}
	s.logOut = f
}

func (s *supervisor) handleKill(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	p := s.proc
	s.mu.Unlock()
	if p == nil {
		http.Error(w, "aquifer not running", http.StatusConflict)
		return
	}
	at := time.Now()
	if err := p.Signal(syscall.SIGKILL); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.mu.Lock()
	s.kills++
	s.mu.Unlock()
	s.record(fmt.Sprintf("supervisor: sent SIGKILL to pid %d", p.Pid))
	writeJSON(w, map[string]any{"killed_pid": p.Pid, "at_unix_ms": at.UnixMilli()})
}

// handleReboot exits the supervisor, which ends the machine's main process;
// Fly's restart policy then boots the machine again.
func (s *supervisor) handleReboot(w http.ResponseWriter, r *http.Request) {
	s.record("supervisor: machine restart requested")
	writeJSON(w, map[string]any{"at_unix_ms": time.Now().UnixMilli()})
	go func() {
		time.Sleep(200 * time.Millisecond)
		s.mu.Lock()
		if s.proc != nil {
			s.proc.Signal(syscall.SIGKILL)
		}
		s.mu.Unlock()
		os.Exit(1)
	}()
}

func (s *supervisor) handleLogs(w http.ResponseWriter, r *http.Request) {
	n, _ := strconv.Atoi(r.URL.Query().Get("n"))
	if n <= 0 || n > ringSize {
		n = 500
	}
	grep := r.URL.Query().Get("grep")
	s.mu.Lock()
	var all []string
	if s.full {
		all = append(all, s.lines[s.next:]...)
	}
	all = append(all, s.lines[:s.next]...)
	s.mu.Unlock()
	if grep != "" {
		kept := all[:0:0]
		for _, l := range all {
			if strings.Contains(l, grep) {
				kept = append(kept, l)
			}
		}
		all = kept
	}
	if len(all) > n {
		all = all[len(all)-n:]
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	io.WriteString(w, strings.Join(all, "\n")+"\n")
}

func (s *supervisor) handleStats(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	p := s.proc
	out := map[string]any{
		"restarts":        s.restarts,
		"kills":           s.kills,
		"last_exit":       s.lastExit,
		"supervisor_up_s": int(time.Since(s.bootAt).Seconds()),
		"aquifer_running": p != nil,
	}
	if p != nil {
		out["aquifer_pid"] = p.Pid
		out["aquifer_up_s"] = int(time.Since(s.startedAt).Seconds())
	}
	s.mu.Unlock()

	if p != nil {
		if rss, threads, ok := procStatus(p.Pid); ok {
			out["rss_bytes"] = rss
			out["threads"] = threads
		}
		if fds, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", p.Pid)); err == nil {
			out["open_fds"] = len(fds)
		}
	}
	size, files := dirUsage(s.dataDir)
	out["data_bytes"] = size
	out["data_files"] = files
	if peb, pebFiles := dirUsage(filepath.Join(s.dataDir, "aquifer.db")); pebFiles > 0 {
		out["pebble_bytes"] = peb
		out["pebble_files"] = pebFiles
	}
	var st syscall.Statfs_t
	if syscall.Statfs(s.dataDir, &st) == nil {
		out["volume_free_bytes"] = int64(st.Bavail) * int64(st.Bsize)
		out["volume_total_bytes"] = int64(st.Blocks) * int64(st.Bsize)
	}
	for k, v := range s.debugStats() {
		out[k] = v
	}
	writeJSON(w, out)
}

func procStatus(pid int) (rss int64, threads int, ok bool) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0, 0, false
	}
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		switch f[0] {
		case "VmRSS:":
			kb, _ := strconv.ParseInt(f[1], 10, 64)
			rss = kb * 1024
		case "Threads:":
			threads, _ = strconv.Atoi(f[1])
		}
	}
	return rss, threads, true
}

func dirUsage(root string) (bytes int64, files int) {
	filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if info, err := d.Info(); err == nil {
			bytes += info.Size()
			files++
		}
		return nil
	})
	return bytes, files
}

var (
	goroutineRe = regexp.MustCompile(`goroutine profile: total (\d+)`)
	memStatRe   = regexp.MustCompile(`(?m)^# (HeapAlloc|HeapInuse|HeapSys|HeapObjects|Sys|NumGC) = (\d+)`)
	debugClient = &http.Client{Timeout: 5 * time.Second}
)

// debugStats reads goroutine and heap numbers from Aquifer's pprof listener.
func (s *supervisor) debugStats() map[string]any {
	out := map[string]any{}
	if body, err := get(s.debugURL + "/debug/pprof/goroutine?debug=1"); err == nil {
		if m := goroutineRe.FindStringSubmatch(body); m != nil {
			out["goroutines"], _ = strconv.Atoi(m[1])
		}
	}
	if body, err := get(s.debugURL + "/debug/pprof/heap?debug=1"); err == nil {
		for _, m := range memStatRe.FindAllStringSubmatch(body, -1) {
			v, _ := strconv.ParseInt(m[2], 10, 64)
			out["go_"+strings.ToLower(m[1])] = v
		}
	}
	return out
}

func get(url string) (string, error) {
	resp, err := debugClient.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	return string(b), err
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
