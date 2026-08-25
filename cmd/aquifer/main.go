package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/rjpruitt16/aquifer"
	"github.com/rjpruitt16/aquifer/a2aadapter"
)

// Set via -ldflags at build time (see .goreleaser.yaml); "dev" when built
// directly with go build/go run, matching the common Go CLI convention.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "--version" || os.Args[1] == "-version") {
		fmt.Printf("aquifer %s (commit %s, built %s)\n", version, commit, date)
		return
	}

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
		log.Fatalf("unknown AQUIFER_ADAPTER %q (expected http, mcp-stdio, or a2a)", adapterName)
	}

	if adapter.Name() == "http" {
		log.Printf("Aquifer %s listening on :%s (db: %s)", version, port, dbPath)
	} else {
		log.Printf("Aquifer %s running %s (db: %s)", version, adapter.Name(), dbPath)
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
	case "a2a":
		publicURL := os.Getenv("AQUIFER_A2A_PUBLIC_URL")
		if publicURL == "" {
			publicURL = "http://localhost:" + port
		}
		return a2aadapter.NewAdapter(":"+port, publicURL)
	default:
		return nil
	}
}
