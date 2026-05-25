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

	l8KeyPath := os.Getenv("L8_KEY_PATH")
	if l8KeyPath == "" {
		l8KeyPath = ".l8-key"
	}
	l8 := NewL8Registry(l8KeyPath, "l8-trust")

	store := NewStore(dbPath)
	broker := NewBroker()
	registry := NewRegistry(store, cfg, broker, l8)

	queued := store.GetQueuedJobs()
	if len(queued) > 0 {
		log.Printf("recovering %d queued jobs from %s", len(queued), dbPath)
		for _, job := range queued {
			registry.Enqueue(job)
		}
	}

	server := NewServer(store, registry, broker, l8)

	log.Printf("Aquifer listening on :%s (db: %s)", port, dbPath)
	log.Fatal(http.ListenAndServe(":"+port, server.Routes()))
}
