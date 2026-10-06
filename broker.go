package aquifer

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type SSEEvent struct {
	Event string
	Data  map[string]any
}

// Broker is the pub/sub layer for SSE streams.
// Each job gets its own set of subscriber channels.
type Broker struct {
	mu          sync.RWMutex
	subscribers map[string][]chan SSEEvent
	// delivered holds, per job whose final event was just published to live
	// streams, a channel a stream closes once it has written and flushed
	// that event to a still-connected client.
	delivered map[string]chan struct{}
}

func NewBroker() *Broker {
	return &Broker{subscribers: make(map[string][]chan SSEEvent), delivered: make(map[string]chan struct{})}
}

// PublishTerminal publishes a job's final event and reports whether a live
// stream confirmed delivering it, waiting up to wait. It returns false at
// once when nobody is streaming the job.
func (b *Broker) PublishTerminal(jobID string, event SSEEvent, wait time.Duration) bool {
	b.mu.Lock()
	subs := append([]chan SSEEvent(nil), b.subscribers[jobID]...)
	var done chan struct{}
	if len(subs) > 0 {
		done = make(chan struct{})
		b.delivered[jobID] = done
	}
	b.mu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- event:
		default:
		}
	}
	if done == nil {
		return false
	}
	defer func() {
		b.mu.Lock()
		if b.delivered[jobID] == done {
			delete(b.delivered, jobID)
		}
		b.mu.Unlock()
	}()

	select {
	case <-done:
		return true
	case <-time.After(wait):
		return false
	}
}

// ConfirmDelivered is called by a stream after flushing a job's final event
// to a client that was still connected.
func (b *Broker) ConfirmDelivered(jobID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if done := b.delivered[jobID]; done != nil {
		close(done)
		delete(b.delivered, jobID)
	}
}

func (b *Broker) Subscribe(jobID string) (<-chan SSEEvent, func()) {
	ch := make(chan SSEEvent, 10)
	b.mu.Lock()
	b.subscribers[jobID] = append(b.subscribers[jobID], ch)
	b.mu.Unlock()

	return ch, func() {
		b.mu.Lock()
		subs := b.subscribers[jobID]
		for i, s := range subs {
			if s == ch {
				b.subscribers[jobID] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		if len(b.subscribers[jobID]) == 0 {
			delete(b.subscribers, jobID)
		}
		b.mu.Unlock()
		close(ch)
	}
}

func (b *Broker) Publish(jobID string, event SSEEvent) {
	b.mu.RLock()
	subs := b.subscribers[jobID]
	b.mu.RUnlock()

	for _, ch := range subs {
		select {
		case ch <- event:
		default: // never block if the subscriber is slow
		}
	}
}

func writeSSE(w http.ResponseWriter, event string, data map[string]any) error {
	b, _ := json.Marshal(data)
	_, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b)
	return err
}
