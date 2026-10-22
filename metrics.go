// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"github.com/rjpruitt16/aquifer/internal/dispatch"
	"github.com/rjpruitt16/aquifer/internal/metrics"
)

// The built-in Prometheus adapter lives in internal/metrics. These names
// keep it reachable for embedding applications.
type (
	PrometheusMetrics = metrics.Prometheus
	TargetStats       = metrics.TargetStats
	QueueStat         = metrics.QueueStat
)

// NewPrometheusMetrics reads AQUIFER_METRICS_TENANT_LABELS.
func NewPrometheusMetrics() *PrometheusMetrics { return metrics.NewPrometheus() }

// The metrics hooks the dispatch path calls live in internal/dispatch.
type (
	MetricsAdapter     = dispatch.MetricsAdapter
	NoopMetricsAdapter = dispatch.NoopMetricsAdapter
	JobTimingMetrics   = dispatch.JobTimingMetrics
	DispatchMetrics    = dispatch.DispatchMetrics
	WebhookJobMetrics  = dispatch.WebhookJobMetrics
	AdmissionMetrics   = dispatch.AdmissionMetrics
)
