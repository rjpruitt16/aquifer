package main

import (
	"context"
	"log"
	"os"

	"github.com/rjpruitt16/aquifer"
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

	l8KeyPath := os.Getenv("L8_KEY_PATH")
	if l8KeyPath == "" {
		l8KeyPath = ".l8-key"
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

	if err := aquifer.RunAdapter(context.Background(), adapter, aquifer.RuntimeOptions{
		DBPath:     dbPath,
		ConfigPath: os.Getenv("CONFIG_PATH"),
		L8KeyPath:  l8KeyPath,
		Metrics:    aquifer.NoopMetricsAdapter{},
	}); err != nil {
		log.Fatal(err)
	}
}

func buildAdapter(name, port string) aquifer.FrameworkAdapter {
	switch name {
	case "http":
		return aquifer.NewHTTPAdapter(":" + port)
	case "mcp-stdio":
		return aquifer.NewMCPStdioAdapter(os.Stdin, os.Stdout)
	default:
		return nil
	}
}
