package aquifer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

type Server struct {
	aquifer *Aquifer
}

func NewServer(aquifer *Aquifer) *Server {
	return &Server{aquifer: aquifer}
}

type HTTPAdapter struct {
	addr string
}

func NewHTTPAdapter(addr string) *HTTPAdapter {
	return &HTTPAdapter{addr: addr}
}

func (a *HTTPAdapter) Name() string {
	return "http"
}

func (a *HTTPAdapter) Start(ctx context.Context, aquifer *Aquifer) error {
	server := &http.Server{
		Addr:    a.addr,
		Handler: NewServer(aquifer).Routes(),
	}

	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()

	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", s.createJob)
	mux.HandleFunc("POST /proxy", s.proxyJob)
	mux.HandleFunc("GET /jobs/{id}/stream", s.streamJob)
	mux.HandleFunc("GET /jobs/{id}", s.getJob)
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("POST /pools/{pool_id}/members", s.registerPoolMember)
	mux.HandleFunc("GET /.well-known/l8", s.wellKnownL8)
	mux.HandleFunc("POST /l8/challenge", s.l8Challenge)
	mux.HandleFunc("GET /l8-spec", s.l8Spec)
	return mux
}

// registerPoolMember handles both initial registration and heartbeat
// refresh for a pool member — the same request shape serves both, per
// the API design: re-calling this resets the member's liveness TTL and
// updates its declared capacity.
func (s *Server) registerPoolMember(w http.ResponseWriter, r *http.Request) {
	poolID := r.PathValue("pool_id")

	var req struct {
		MemberID                 string  `json:"member_id"`
		Address                  string  `json:"address"`
		CapacityRPS              float64 `json:"capacity_rps"`
		HeartbeatIntervalSeconds int     `json:"heartbeat_interval_seconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid json", http.StatusBadRequest)
		return
	}

	if err := s.aquifer.RegisterPoolMember(poolID, req.MemberID, req.Address, req.CapacityRPS, req.HeartbeatIntervalSeconds); err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{"status": "registered"})
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	if maxBytes := s.aquifer.MaxBodyBytes(); maxBytes > 0 {
		body = http.MaxBytesReader(w, r.Body, maxBytes)
	}

	var req JobRequest
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			jsonErrorFields(w, "request body too large", http.StatusRequestEntityTooLarge, map[string]any{
				"limit_bytes": maxErr.Limit,
			})
			return
		}
		jsonError(w, "invalid json", http.StatusBadRequest)
		return
	}

	// Account-queue mode is a request header, never part of the JSON body —
	// same X-Aqueduct-*/X-Aquifer-* precedence used for pacing headers
	// elsewhere. Empty means "no opinion," leaving the upstream's current
	// mode unchanged rather than forcing it off for every request that
	// doesn't happen to set this.
	req.AccountQueueMode = pacingHeader(r.Header, "Account-Queue")

	result, err := s.aquifer.Enqueue(req)
	if err != nil {
		var admissionErr *AdmissionRejectedError
		if errors.As(err, &admissionErr) {
			w.Header().Set("Retry-After", fmt.Sprintf("%d", s.aquifer.RetryAfterSeconds()))
			jsonErrorFields(w, err.Error(), http.StatusTooManyRequests, map[string]any{
				"limit_reason": admissionErr.Decision.Reason,
				"limit":        admissionErr.Decision.Limit,
				"current":      admissionErr.Decision.Current,
			})
			return
		}
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if result.Duplicate {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusCreated)
	}
	json.NewEncoder(w).Encode(result)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := s.aquifer.GetJob(id)
	if err != nil {
		jsonError(w, "job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"job_id":     job.ID,
		"status":     job.Status,
		"url":        job.URL,
		"method":     job.Method,
		"created_at": job.CreatedAt,
	})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.aquifer.Health())
}

func (s *Server) wellKnownL8(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.aquifer.L8Metadata(r.Host))
}

func (s *Server) l8Challenge(w http.ResponseWriter, r *http.Request) {
	var req L8ChallengeReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid json", http.StatusBadRequest)
		return
	}
	resp, err := s.aquifer.HandleL8Challenge(req)
	if err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) l8Spec(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
	w.Write([]byte(l8SpecDocument))
}

func (s *Server) streamJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, events, unsubscribe, err := s.aquifer.SubscribeJob(id)
	if err != nil {
		jsonError(w, "job not found", http.StatusNotFound)
		return
	}
	s.streamEvents(w, r, job, events, unsubscribe)
}

// streamEvents is streamJob's actual event loop, extracted so proxy mode's
// fallback path can reuse the identical SSE behavior on the same
// connection ("automatically start streaming") instead of reimplementing
// it. Writes catch-up events, then loops on the events channel, a 30s
// keepalive, and request cancellation, exactly as streamJob always has.
func (s *Server) streamEvents(w http.ResponseWriter, r *http.Request, job *Job, events <-chan SSEEvent, unsubscribe func()) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		jsonError(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	defer unsubscribe()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	// Catchup events for states already passed before client connected
	writeSSE(w, "queued", map[string]any{"job_id": job.ID, "status": "queued"})
	if job.Status == StatusInFlight {
		writeSSE(w, "dispatching", map[string]any{"job_id": job.ID})
	}
	flusher.Flush()

	keepalive := time.NewTicker(30 * time.Second)
	defer keepalive.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return

		case event, ok := <-events:
			if !ok {
				return
			}
			writeSSE(w, event.Event, event.Data)
			flusher.Flush()
			if event.Event == "completed" || event.Event == "failed" {
				return
			}

		case <-keepalive.C:
			fmt.Fprintf(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}

// proxyJob is proxy mode's HTTP entry point: try the upstream directly and
// synchronously; on success, relay its response verbatim; on failure,
// overload, or an already-open circuit breaker, fall back to the same
// durable-queue-and-SSE path createJob/streamJob always use, on this same
// connection.
func (s *Server) proxyJob(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	if maxBytes := s.aquifer.MaxBodyBytes(); maxBytes > 0 {
		body = http.MaxBytesReader(w, r.Body, maxBytes)
	}

	var req JobRequest
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			jsonErrorFields(w, "request body too large", http.StatusRequestEntityTooLarge, map[string]any{
				"limit_bytes": maxErr.Limit,
			})
			return
		}
		jsonError(w, "invalid json", http.StatusBadRequest)
		return
	}
	req.AccountQueueMode = pacingHeader(r.Header, "Account-Queue")

	outcome := s.aquifer.AttemptDirect(r.Context(), req, proxyDirectAttemptTimeout())

	if outcome.Err != nil {
		var admissionErr *AdmissionRejectedError
		if errors.As(outcome.Err, &admissionErr) {
			w.Header().Set("Retry-After", fmt.Sprintf("%d", s.aquifer.RetryAfterSeconds()))
			jsonErrorFields(w, outcome.Err.Error(), http.StatusTooManyRequests, map[string]any{
				"limit_reason": admissionErr.Decision.Reason,
				"limit":        admissionErr.Decision.Limit,
				"current":      admissionErr.Decision.Current,
			})
			return
		}
		jsonError(w, outcome.Err.Error(), http.StatusBadRequest)
		return
	}

	if outcome.Duplicate {
		s.streamExistingJob(w, r, outcome.ExistingJob)
		return
	}

	if outcome.Direct {
		for k, vs := range outcome.Header {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(outcome.Status)
		w.Write(outcome.Body)
		return
	}

	// Fallback: subscribe before dispatching, so a fast completion can't
	// publish before anyone's listening for it.
	_, events, unsubscribe, err := s.aquifer.SubscribeJob(outcome.Job.ID)
	if err != nil {
		jsonError(w, "job not found", http.StatusNotFound)
		return
	}
	s.aquifer.Dispatch(outcome.Job, req.AccountQueueMode)
	s.streamEvents(w, r, outcome.Job, events, unsubscribe)
}

// streamExistingJob handles a proxy-mode request that turned out to be a
// duplicate of one already in flight or already finished. Job never
// persists its upstream response body past the transient SSE/webhook
// payload (see Job), so a duplicate of an already-terminal job has no
// cached response to replay — opening a stream for it would just keepalive
// forever, since its completed/failed event already fired before this
// request ever subscribed. Return its current status synchronously
// instead; only a still-in-flight duplicate gets a real stream.
func (s *Server) streamExistingJob(w http.ResponseWriter, r *http.Request, job *Job) {
	if job.Status == StatusCompleted || job.Status == StatusFailed {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"job_id": job.ID, "status": job.Status, "duplicate": true,
		})
		return
	}
	_, events, unsubscribe, err := s.aquifer.SubscribeJob(job.ID)
	if err != nil {
		jsonError(w, "job not found", http.StatusNotFound)
		return
	}
	s.streamEvents(w, r, job, events, unsubscribe)
}

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// jsonErrorFields writes an error body that also identifies which limit was
// hit, so a caller (or an operator reading logs) knows exactly why a request
// was rejected rather than just that it was.
func jsonErrorFields(w http.ResponseWriter, msg string, code int, fields map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	body := map[string]any{"error": msg}
	for k, v := range fields {
		body[k] = v
	}
	json.NewEncoder(w).Encode(body)
}
