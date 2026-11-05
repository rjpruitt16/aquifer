// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"sync"
	"testing"
)

// TestBrokerPublishWhileUnsubscribing runs Publish against subscribers that
// unsubscribe at the same time. Unsubscribe compacts the subscriber slice in
// place, so Publish must not read that slice after releasing the lock. Run
// with -race.
func TestBrokerPublishWhileUnsubscribing(t *testing.T) {
	b := NewBroker()
	const job = "job-1"
	var wg sync.WaitGroup
	for round := 0; round < 200; round++ {
		unsubs := make([]func(), 0, 8)
		for i := 0; i < 8; i++ {
			_, unsub := b.Subscribe(job)
			unsubs = append(unsubs, unsub)
		}
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				b.Publish(job, SSEEvent{Event: "queued"})
			}
		}()
		go func() {
			defer wg.Done()
			for _, u := range unsubs {
				u()
			}
		}()
		wg.Wait()
	}
}
