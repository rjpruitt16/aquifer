package aquifer

type MetricsAdapter interface {
	JobQueued(userID, upstream string)
	JobDispatched(userID, upstream string)
	JobCompleted(userID, upstream string, durationMs int64)
	JobFailed(userID, upstream string, reason string)
	WebhookDelivered(url string, attempt int)
	WebhookFailed(url string, attempts int)
	QueueDepth(upstream string, depth int)
	FlowRate(upstream string, rps float64)
	// DrainFlushSucceeded/DrainFlushFailed only ever fire when drain mode
	// is enabled (see drain.go) -- unreached on a deployment that never
	// turns it on.
	DrainFlushSucceeded(instanceKey string, ledgerSize int)
	DrainFlushFailed(instanceKey string, ledgerSize int)
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
func (NoopMetricsAdapter) DrainFlushSucceeded(instanceKey string, ledgerSize int) {}
func (NoopMetricsAdapter) DrainFlushFailed(instanceKey string, ledgerSize int)    {}

func ensureMetrics(metrics MetricsAdapter) MetricsAdapter {
	if metrics == nil {
		return NoopMetricsAdapter{}
	}
	return metrics
}
