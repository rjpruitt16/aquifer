package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Server struct {
	store    *Store
	registry *Registry
	broker   *Broker
	l8       *L8Registry
}

func NewServer(store *Store, registry *Registry, broker *Broker, l8 *L8Registry) *Server {
	return &Server{store: store, registry: registry, broker: broker, l8: l8}
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

	if msg := req.Validate(); msg != "" {
		jsonError(w, msg, http.StatusBadRequest)
		return
	}

	job := NewJob(&req)

	if existingID, isDuplicate := s.store.CheckOrInsert(job); isDuplicate {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{
			"job_id":    existingID,
			"status":    "queued",
			"duplicate": true,
		})
		return
	}

	s.registry.Enqueue(job)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{
		"job_id": job.ID,
		"status": "queued",
	})
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job := s.store.GetJob(id)
	if job == nil {
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
	json.NewEncoder(w).Encode(map[string]any{
		"status":       "ok",
		"l8_protocol":  "0.1",
		"l8_public_key": s.l8.PubB64,
	})
}

func (s *Server) wellKnownL8(w http.ResponseWriter, r *http.Request) {
	host := r.Host
	if host == "" {
		host = "localhost"
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.l8.Meta(host))
}

func (s *Server) l8Challenge(w http.ResponseWriter, r *http.Request) {
	var req L8ChallengeReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid json", http.StatusBadRequest)
		return
	}
	resp, err := s.l8.HandleChallenge(req)
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
	job := s.store.GetJob(id)
	if job == nil {
		jsonError(w, "job not found", http.StatusNotFound)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		jsonError(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	events, unsubscribe := s.broker.Subscribe(id)
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
