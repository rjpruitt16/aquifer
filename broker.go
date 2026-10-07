package aquifer

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
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
}

func NewBroker() *Broker {
	return &Broker{subscribers: make(map[string][]chan SSEEvent)}
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

// HasSubscribers reports whether anyone is streaming jobID's events, so
// callers can skip building events no one will receive.
func (b *Broker) HasSubscribers(jobID string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subscribers[jobID]) > 0
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
