package aquifer

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"time"
)

const webhookMaxRetries = 4

// deliverWebhookSync is used only by drain mode (drain.go), where clearing
// the local idempotency ledger must happen only after a confirmed delivery
// -- a synchronous, immediate-fire-with-retry call, distinct from regular
// per-job completion/failure webhooks, which now go through
// Registry.EnqueueWebhook's paced account-queue delivery (account_queue.go)
// instead of firing immediately from here.
func deliverWebhookSync(url string, payload map[string]any, l8 *L8Registry, metrics MetricsAdapter) bool {
	return deliverWebhookSyncContext(context.Background(), url, payload, l8, metrics)
}

func deliverWebhookSyncContext(ctx context.Context, url string, payload map[string]any, l8 *L8Registry, metrics MetricsAdapter) bool {
	return deliverWithRetryContext(ctx, url, payload, 0, l8, ensureMetrics(metrics))
}

func deliverWebhookOnceContext(ctx context.Context, rawURL string, payload map[string]any, l8 *L8Registry, metrics MetricsAdapter) bool {
	metrics = ensureMetrics(metrics)
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, bytes.NewReader(body))
	if err != nil {
		metrics.WebhookFailed(rawURL, 1)
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	if l8 != nil && l8.IsTrusted(rawURL) {
		for key, value := range l8.SignHeaders(body) {
			req.Header.Set(key, value)
		}
	}
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		metrics.WebhookFailed(rawURL, 1)
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		metrics.WebhookFailed(rawURL, 1)
		return false
	}
	metrics.WebhookDelivered(rawURL, 1)
	return true
}

func deliverWithRetry(rawURL string, payload map[string]any, attempt int, l8 *L8Registry, metrics MetricsAdapter) bool {
	return deliverWithRetryContext(context.Background(), rawURL, payload, attempt, l8, metrics)
}

func deliverWithRetryContext(ctx context.Context, rawURL string, payload map[string]any, attempt int, l8 *L8Registry, metrics MetricsAdapter) bool {
	if ctx.Err() != nil {
		return false
	}
	body, _ := json.Marshal(payload)

	if l8 != nil {
		l8.EnsureTrust(rawURL)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, bytes.NewReader(body))
	if err != nil {
		log.Printf("[Webhook] invalid URL %s: %v", rawURL, err)
		metrics.WebhookFailed(rawURL, attempt+1)
		return false
	}
	req.Header.Set("Content-Type", "application/json")

	if l8 != nil && l8.IsTrusted(rawURL) {
		for k, v := range l8.SignHeaders(body) {
			req.Header.Set(k, v)
		}
	}

	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		if attempt < webhookMaxRetries {
			backoff := withJitter(time.Duration(math.Pow(2, float64(attempt))) * time.Second)
			log.Printf("[Webhook] error delivering to %s, retry %d/%d in %s: %v", rawURL, attempt+1, webhookMaxRetries, backoff, err)
			if !sleepBeforeRetryContext(ctx, backoff) {
				return false
			}
			return deliverWithRetryContext(ctx, rawURL, payload, attempt+1, l8, metrics)
		}
		log.Printf("[Webhook] giving up after %d retries for %s: %v", webhookMaxRetries, rawURL, err)
		metrics.WebhookFailed(rawURL, attempt+1)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if attempt < webhookMaxRetries {
			backoff := withJitter(time.Duration(math.Pow(2, float64(attempt))) * time.Second)
			log.Printf("[Webhook] %d from %s, retry %d/%d in %s", resp.StatusCode, rawURL, attempt+1, webhookMaxRetries, backoff)
			if !sleepBeforeRetryContext(ctx, backoff) {
				return false
			}
			return deliverWithRetryContext(ctx, rawURL, payload, attempt+1, l8, metrics)
		}
		log.Printf("[Webhook] giving up after %d retries, last status %d for %s", webhookMaxRetries, resp.StatusCode, rawURL)
		metrics.WebhookFailed(rawURL, attempt+1)
		return false
	}

	metrics.WebhookDelivered(rawURL, attempt+1)
	return true
}
