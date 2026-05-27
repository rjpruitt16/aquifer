package main

import (
	"bytes"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"time"
)

const webhookMaxRetries = 4

func deliverWebhook(url string, payload map[string]any, l8 *L8Registry, metrics MetricsAdapter) {
	deliverWithRetry(url, payload, 0, l8, ensureMetrics(metrics))
}

func deliverWithRetry(rawURL string, payload map[string]any, attempt int, l8 *L8Registry, metrics MetricsAdapter) {
	body, _ := json.Marshal(payload)

	if l8 != nil {
		l8.EnsureTrust(rawURL)
	}

	req, err := http.NewRequest("POST", rawURL, bytes.NewReader(body))
	if err != nil {
		log.Printf("[Webhook] invalid URL %s: %v", rawURL, err)
		metrics.WebhookFailed(rawURL, attempt+1)
		return
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
			backoff := time.Duration(math.Pow(2, float64(attempt))) * time.Second
			log.Printf("[Webhook] error delivering to %s, retry %d/%d in %s: %v", rawURL, attempt+1, webhookMaxRetries, backoff, err)
			time.Sleep(backoff)
			deliverWithRetry(rawURL, payload, attempt+1, l8, metrics)
			return
		}
		log.Printf("[Webhook] giving up after %d retries for %s: %v", webhookMaxRetries, rawURL, err)
		metrics.WebhookFailed(rawURL, attempt+1)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if attempt < webhookMaxRetries {
			backoff := time.Duration(math.Pow(2, float64(attempt))) * time.Second
			log.Printf("[Webhook] %d from %s, retry %d/%d in %s", resp.StatusCode, rawURL, attempt+1, webhookMaxRetries, backoff)
			time.Sleep(backoff)
			deliverWithRetry(rawURL, payload, attempt+1, l8, metrics)
			return
		}
		log.Printf("[Webhook] giving up after %d retries, last status %d for %s", webhookMaxRetries, resp.StatusCode, rawURL)
		metrics.WebhookFailed(rawURL, attempt+1)
		return
	}

	metrics.WebhookDelivered(rawURL, attempt+1)
}
