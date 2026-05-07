package main

import (
	"encoding/json"
	"net/http"
)

type Server struct {
	store    *Store
	registry *Registry
}

func NewServer(store *Store, registry *Registry) *Server {
	return &Server{store: store, registry: registry}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", s.createJob)
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

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
