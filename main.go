package main

import (
	"context"
	"log"
	"os"
)

func main() {
	adapterName := os.Getenv("AQUIFER_ADAPTER")
	if adapterName == "" {
		adapterName = "http"
	}
	if adapterName == "mcp-stdio" {
		log.SetOutput(os.Stderr)
	}

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
	metrics := NoopMetricsAdapter{}
	registry := NewRegistry(store, cfg, broker, l8, metrics)
	aquifer := NewAquifer(store, registry, broker, l8)

	queued := store.GetQueuedJobs()
	if len(queued) > 0 {
		log.Printf("recovering %d queued jobs from %s", len(queued), dbPath)
		for _, job := range queued {
			registry.Enqueue(job)
		}
	}

	adapter := buildAdapter(adapterName, port)
	if adapter == nil {
		log.Fatalf("unknown AQUIFER_ADAPTER %q (expected http or mcp-stdio)", adapterName)
	}

	if adapter.Name() == "http" {
		log.Printf("Aquifer listening on :%s (db: %s)", port, dbPath)
	} else {
		log.Printf("Aquifer running %s (db: %s)", adapter.Name(), dbPath)
	}
	if err := adapter.Start(context.Background(), aquifer); err != nil {
		log.Fatal(err)
	}
}

func buildAdapter(name, port string) FrameworkAdapter {
	switch name {
	case "http":
		return NewHTTPAdapter(":" + port)
	case "mcp-stdio":
		return NewMCPStdioAdapter(os.Stdin, os.Stdout)
	default:
		return nil
	}
}
