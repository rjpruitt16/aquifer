package aquifer

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

// orcaHeaderName is the header vLLM (and other ORCA-aware backends) use to
// report load, per gRFC A51 / the ORCA native-HTTP format. Verified against
// vLLM's actual source (vllm/entrypoints/serve/utils/orca_metrics.py):
//
//	endpoint-load-metrics: TEXT named_metrics.kv_cache_usage_perc=0.4
//	endpoint-load-metrics: JSON {"named_metrics":{"kv_cache_usage_perc":0.4}}
const orcaHeaderName = "endpoint-load-metrics"

// orcaRequestHeaderName is the request-side header that opts a dispatch
// into getting an ORCA response back at all. Verified against vLLM's
// current source (vllm/entrypoints/openai/chat_completion/api_router.py):
// ORCA reporting is entirely request-driven there -- the backend only
// includes endpoint-load-metrics on the response if the request carried
// this header naming the desired format ("TEXT" or "JSON"). There is no
// server-side startup flag for this in current vLLM; an earlier version
// used one (--orca_formats), but that's not what's deployed today.
const orcaRequestHeaderName = "endpoint-load-metrics-format"

// orcaKVCacheMetric is the one named metric this pacing curve keys off --
// vLLM already normalizes it to a 0-1 utilization fraction (Prometheus
// vllm:kv_cache_usage_perc). vLLM also reports num_requests_waiting, a raw
// count with no inherent scale to pace against without a configured ceiling
// -- left as a natural future extension, not used here.
const orcaKVCacheMetric = "kv_cache_usage_perc"

// orcaRps derives a suggested dispatch rate from an ORCA endpoint-load-metrics
// response header, if present and parseable. Returns nil if there's no ORCA
// header, no kv_cache_usage_perc metric in it, or the reported load is low
// enough that no override is warranted -- callers should keep whatever rate
// is already configured in that case.
//
// This is a fallback signal, not a primary one. Callers should only consult
// it when the response carried no explicit X-Aqueduct-Rps/X-Aquifer-Rps,
// which always takes precedence -- an operator's own explicit pacing
// directive should never be overridden by a best-effort inferred one.
func orcaRps(headers http.Header) *float64 {
	raw := headers.Get(orcaHeaderName)
	if raw == "" {
		return nil
	}

	metrics := parseOrcaHeader(raw)
	if metrics == nil {
		return nil
	}

	util, ok := metrics[orcaKVCacheMetric]
	if !ok {
		return nil
	}

	return orcaLoadToRps(util)
}

// parseOrcaHeader splits the "TEXT ..." / "JSON ..." format prefix from the
// payload and dispatches to the matching parser.
func parseOrcaHeader(raw string) map[string]float64 {
	format, data, found := strings.Cut(raw, " ")
	if !found {
		return nil
	}

	switch strings.ToUpper(format) {
	case "TEXT":
		return parseOrcaText(data)
	case "JSON":
		return parseOrcaJSON(data)
	default:
		return nil
	}
}

// parseOrcaText parses comma-separated "named_metrics.<name>=<value>" pairs,
// e.g. "named_metrics.kv_cache_usage_perc=0.4, named_metrics.num_requests_waiting=3".
func parseOrcaText(data string) map[string]float64 {
	metrics := make(map[string]float64)
	for _, pair := range strings.Split(data, ",") {
		name, valStr, found := strings.Cut(strings.TrimSpace(pair), "=")
		if !found {
			continue
		}
		name = strings.TrimPrefix(strings.TrimSpace(name), "named_metrics.")
		val, err := strconv.ParseFloat(strings.TrimSpace(valStr), 64)
		if err != nil {
			continue
		}
		metrics[name] = val
	}
	if len(metrics) == 0 {
		return nil
	}
	return metrics
}

// parseOrcaJSON parses {"named_metrics": {"<name>": <value>, ...}}.
func parseOrcaJSON(data string) map[string]float64 {
	var parsed struct {
		NamedMetrics map[string]float64 `json:"named_metrics"`
	}
	if err := json.Unmarshal([]byte(data), &parsed); err != nil {
		return nil
	}
	if len(parsed.NamedMetrics) == 0 {
		return nil
	}
	return parsed.NamedMetrics
}

// orcaLoadToRps maps a 0-1 KV-cache utilization fraction onto a dispatch
// rate, mirroring Aquifer's existing philosophy of pacing down gracefully
// rather than cutting off entirely -- even a fully saturated backend still
// gets a slow trickle of dispatch, not zero.
func orcaLoadToRps(util float64) *float64 {
	var rps float64
	switch {
	case util < 0.70:
		return nil // healthy -- no override, let the configured rate stand
	case util < 0.90:
		rps = 2
	case util < 0.97:
		rps = 0.5
	default:
		rps = 0.25
	}
	return &rps
}
