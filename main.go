package main

import (
	"log"
	"net/http"
	"os"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "aquifer.db"
	}

	cfg := LoadConfig(os.Getenv("CONFIG_PATH"))

	store := NewStore(dbPath)
	registry := NewRegistry(store, cfg)

	queued := store.GetQueuedJobs()
	if len(queued) > 0 {
		log.Printf("recovering %d queued jobs from %s", len(queued), dbPath)
		for _, job := range queued {
			registry.Enqueue(job)
		}
	}

	server := NewServer(store, registry)

	log.Printf("Aquifer listening on :%s (db: %s)", port, dbPath)
	log.Fatal(http.ListenAndServe(":"+port, server.Routes()))
}
