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
}

func NewServer(store *Store, registry *Registry, broker *Broker) *Server {
	return &Server{store: store, registry: registry, broker: broker}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", s.createJob)
	mux.HandleFunc("GET /jobs/{id}/stream", s.streamJob)
	mux.HandleFunc("GET /jobs/{id}", s.getJob)
	mux.HandleFunc("GET /health", s.health)
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
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
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

	s.broker.SetDeliveryMode(id, "stream")
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
			s.broker.SetDeliveryMode(id, "stream_fallback")
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
