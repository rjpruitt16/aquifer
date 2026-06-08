package aquifer

import (
	"net/http"
	"testing"
)

func TestPacingHeaderPrefersAqueduct(t *testing.T) {
	headers := http.Header{}
	headers.Set("X-Aquifer-Rps", "2")
	headers.Set("X-Aqueduct-Rps", "5")

	if got := pacingHeader(headers, "Rps"); got != "5" {
		t.Fatalf("expected Aqueduct header to win, got %q", got)
	}
}

func TestPacingHeaderFallsBackToAquifer(t *testing.T) {
	headers := http.Header{}
	headers.Set("X-Aquifer-Max-Concurrent", "3")

	if got := pacingHeader(headers, "Max-Concurrent"); got != "3" {
		t.Fatalf("expected Aquifer fallback, got %q", got)
	}
}

func TestSetLoadHeaderWritesBothNamespaces(t *testing.T) {
	headers := http.Header{}
	setLoadHeader(headers, "Queue-Depth", "42")

	if got := headers.Get("X-Aqueduct-Queue-Depth"); got != "42" {
		t.Fatalf("expected Aqueduct header, got %q", got)
	}
	if got := headers.Get("X-Aquifer-Queue-Depth"); got != "42" {
		t.Fatalf("expected Aquifer header, got %q", got)
	}
}
