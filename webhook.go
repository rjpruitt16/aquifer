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

func deliverWebhook(url string, payload map[string]any) {
	deliverWithRetry(url, payload, 0)
}

func deliverWithRetry(url string, payload map[string]any, attempt int) {
	body, _ := json.Marshal(payload)

	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		if attempt < webhookMaxRetries {
			backoff := time.Duration(math.Pow(2, float64(attempt))) * time.Second
			log.Printf("[Webhook] error delivering to %s, retry %d/%d in %s: %v", url, attempt+1, webhookMaxRetries, backoff, err)
			time.Sleep(backoff)
			deliverWithRetry(url, payload, attempt+1)
			return
		}
		log.Printf("[Webhook] giving up after %d retries for %s: %v", webhookMaxRetries, url, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if attempt < webhookMaxRetries {
			backoff := time.Duration(math.Pow(2, float64(attempt))) * time.Second
			log.Printf("[Webhook] %d from %s, retry %d/%d in %s", resp.StatusCode, url, attempt+1, webhookMaxRetries, backoff, )
			time.Sleep(backoff)
			deliverWithRetry(url, payload, attempt+1)
			return
		}
		log.Printf("[Webhook] giving up after %d retries, last status %d for %s", webhookMaxRetries, resp.StatusCode, url)
	}
}
