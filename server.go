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
	mux.HandleFunc("GET /jobs/{id}/stream", s.streamJob)
	mux.HandleFunc("GET /jobs/{id}", s.getJob)
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /.well-known/l8", s.wellKnownL8)
	mux.HandleFunc("POST /l8/challenge", s.l8Challenge)
	mux.HandleFunc("GET /l8-spec", s.l8Spec)
	return mux
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var req JobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid json", http.StatusBadRequest)
		return
	}

	result, err := s.aquifer.Enqueue(req)
	if err != nil {
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
	writeSSE(w, "queued", map[string]any{"job_id": id, "status": "queued"})
	if job.Status == StatusInFlight {
		writeSSE(w, "dispatching", map[string]any{"job_id": id})
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

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
