package main

type MetricsAdapter interface {
	JobQueued(userID, upstream string)
	JobDispatched(userID, upstream string)
	JobCompleted(userID, upstream string, durationMs int64)
	JobFailed(userID, upstream string, reason string)
	WebhookDelivered(url string, attempt int)
	WebhookFailed(url string, attempts int)
	QueueDepth(upstream string, depth int)
	FlowRate(upstream string, rps float64)
}

type NoopMetricsAdapter struct{}

func (NoopMetricsAdapter) JobQueued(userID, upstream string)                      {}
func (NoopMetricsAdapter) JobDispatched(userID, upstream string)                  {}
func (NoopMetricsAdapter) JobCompleted(userID, upstream string, durationMs int64) {}
func (NoopMetricsAdapter) JobFailed(userID, upstream string, reason string)       {}
func (NoopMetricsAdapter) WebhookDelivered(url string, attempt int)               {}
func (NoopMetricsAdapter) WebhookFailed(url string, attempts int)                 {}
func (NoopMetricsAdapter) QueueDepth(upstream string, depth int)                  {}
func (NoopMetricsAdapter) FlowRate(upstream string, rps float64)                  {}

func ensureMetrics(metrics MetricsAdapter) MetricsAdapter {
	if metrics == nil {
		return NoopMetricsAdapter{}
	}
	return metrics
}
