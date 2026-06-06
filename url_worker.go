package aquifer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"sync"
)

type URLWorker struct {
	mu               sync.Mutex
	domain           string
	rps              float64
	maxConc          int
	accountQueueMode bool
	queues           map[string]*AccountQueue
	store            *Store
	broker           *Broker
	l8               *L8Registry
	metrics          MetricsAdapter
	onIdle           func(string)
}

func NewURLWorker(domain string, rps float64, maxConc int, store *Store, broker *Broker, l8 *L8Registry, metrics MetricsAdapter, onIdle func(string)) *URLWorker {
	return &URLWorker{
		domain:  domain,
		rps:     rps,
		maxConc: maxConc,
		queues:  make(map[string]*AccountQueue),
		store:   store,
		broker:  broker,
		l8:      l8,
		metrics: ensureMetrics(metrics),
		onIdle:  onIdle,
	}
}

func (w *URLWorker) Enqueue(job *Job) {
	w.mu.Lock()
	defer w.mu.Unlock()

	key := sharedKey
	if w.accountQueueMode {
		key = jobQueueKey(job)
	}

	q, ok := w.queues[key]
	if !ok {
		q = NewAccountQueue(key, w.domain, w.rps, w.maxConc, w.store, w.broker, w.l8, w.metrics, func(k string) {
			w.mu.Lock()
			delete(w.queues, k)
			w.mu.Unlock()
		})
		w.queues[key] = q
	}

	q.Enqueue(job)
}

func (w *URLWorker) handleAccountQueueHeader(val string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.accountQueueMode = val == "enabled"
}

const sharedKey = "__shared__"

func jobQueueKey(job *Job) string {
	apiKey := job.Headers["Authorization"]
	if apiKey == "" {
		apiKey = job.Headers["x-api-key"]
	}
	if apiKey == "" {
		apiKey = job.Headers["api-key"]
	}
	raw := fmt.Sprintf("%s:%s", job.UserID, apiKey)
	return sha256String(raw)
}

func domainKey(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	return fmt.Sprintf("%s://%s", u.Scheme, u.Host)
}

func sha256String(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}
