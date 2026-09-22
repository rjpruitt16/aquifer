package aquifer

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWebSocketSchedulerEnforcesPerNodeClientLimit(t *testing.T) {
	s := NewWebSocketScheduler(WebSocketSchedulerConfig{MaxClients: 2, MaxUpstreams: 1, MaxWaiting: 1, ConnectRPS: 1000, SlowStartRPS: 1000})
	t.Cleanup(s.Close)

	if err := s.AcquireClient(); err != nil {
		t.Fatalf("acquire first client: %v", err)
	}
	if err := s.AcquireClient(); err != nil {
		t.Fatalf("acquire second client: %v", err)
	}
	if err := s.AcquireClient(); !errors.Is(err, ErrWebSocketClientLimit) {
		t.Fatalf("expected client limit, got %v", err)
	}
	s.ReleaseClient()
	if err := s.AcquireClient(); err != nil {
		t.Fatalf("expected released client slot to be reusable, got %v", err)
	}
}

func TestWebSocketSchedulerQueuesUpstreamsFIFO(t *testing.T) {
	s := NewWebSocketScheduler(WebSocketSchedulerConfig{MaxClients: 4, MaxUpstreams: 1, MaxWaiting: 4, ConnectRPS: 1000, SlowStartRPS: 1000})
	s.jitter = func(d time.Duration) time.Duration { return d }
	t.Cleanup(s.Close)

	first, err := s.EnqueueUpstream()
	if err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	second, err := s.EnqueueUpstream()
	if err != nil {
		t.Fatalf("enqueue second: %v", err)
	}
	if first.Position != 1 || second.Position != 2 {
		t.Fatalf("expected FIFO positions 1 and 2, got %d and %d", first.Position, second.Position)
	}

	releaseFirst, err := first.Wait(context.Background(), nil)
	if err != nil {
		t.Fatalf("wait first: %v", err)
	}
	secondReady := make(chan func(), 1)
	go func() {
		release, waitErr := second.Wait(context.Background(), nil)
		if waitErr == nil {
			secondReady <- release
		}
	}()

	select {
	case <-secondReady:
		t.Fatal("second admission should wait for the first upstream slot")
	case <-time.After(20 * time.Millisecond):
	}

	releaseFirst()
	select {
	case releaseSecond := <-secondReady:
		releaseSecond()
	case <-time.After(time.Second):
		t.Fatal("second admission did not receive the released upstream slot")
	}
}

func TestWebSocketSchedulerBackendCapacityOnlyLowersOperatorCeiling(t *testing.T) {
	s := NewWebSocketScheduler(WebSocketSchedulerConfig{MaxClients: 10, MaxUpstreams: 8, MaxWaiting: 10, ConnectRPS: 20, SlowStartRPS: 20})
	t.Cleanup(s.Close)

	max := 3
	rps := 5.0
	s.UpdateCapacity(&max, &rps)
	snapshot := s.Snapshot()
	if snapshot.EffectiveMax != 3 || snapshot.EffectiveConnectRPS != 5 {
		t.Fatalf("expected backend-lowered capacity, got %+v", snapshot)
	}

	max = 100
	rps = 100
	s.UpdateCapacity(&max, &rps)
	snapshot = s.Snapshot()
	if snapshot.EffectiveMax != 8 || snapshot.EffectiveConnectRPS != 20 {
		t.Fatalf("backend must not raise operator ceilings, got %+v", snapshot)
	}
}

func TestWebSocketSchedulerRejectsWaitingOverflow(t *testing.T) {
	s := NewWebSocketScheduler(WebSocketSchedulerConfig{MaxClients: 2, MaxUpstreams: 1, MaxWaiting: 1, ConnectRPS: 1, SlowStartRPS: 1})
	t.Cleanup(s.Close)

	if _, err := s.EnqueueUpstream(); err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	if _, err := s.EnqueueUpstream(); !errors.Is(err, ErrWebSocketWaitingLimit) {
		t.Fatalf("expected waiting limit, got %v", err)
	}
}

func TestWebSocketSchedulerPublishesLiveQueuePositions(t *testing.T) {
	s := NewWebSocketScheduler(WebSocketSchedulerConfig{MaxClients: 4, MaxUpstreams: 1, MaxWaiting: 4, ConnectRPS: 1000, SlowStartRPS: 1000})
	t.Cleanup(s.Close)

	first, err := s.EnqueueUpstream()
	if err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	releaseFirst, err := first.Wait(context.Background(), nil)
	if err != nil {
		t.Fatalf("wait first: %v", err)
	}

	second, err := s.EnqueueUpstream()
	if err != nil {
		t.Fatalf("enqueue second: %v", err)
	}
	third, err := s.EnqueueUpstream()
	if err != nil {
		t.Fatalf("enqueue third: %v", err)
	}
	secondContext, cancelSecond := context.WithCancel(context.Background())
	positions := make(chan int, 4)
	secondDone := make(chan error, 1)
	go func() {
		_, waitErr := second.Wait(secondContext, nil)
		secondDone <- waitErr
	}()
	thirdReady := make(chan func(), 1)
	go func() {
		release, waitErr := third.Wait(context.Background(), func(position int) error {
			positions <- position
			return nil
		})
		if waitErr == nil {
			thirdReady <- release
		}
	}()

	if position := <-positions; position != 2 {
		t.Fatalf("expected initial third position 2, got %d", position)
	}
	cancelSecond()
	if err := <-secondDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled second waiter, got %v", err)
	}
	select {
	case position := <-positions:
		if position != 1 {
			t.Fatalf("expected third position to advance to 1, got %d", position)
		}
	case <-time.After(time.Second):
		t.Fatal("third waiter did not receive a live queue position update")
	}

	releaseFirst()
	select {
	case releaseThird := <-thirdReady:
		releaseThird()
	case <-time.After(time.Second):
		t.Fatal("third waiter did not receive the released upstream slot")
	}
}

func TestWebSocketSchedulerSlowStartRampsAndResets(t *testing.T) {
	s := NewWebSocketScheduler(WebSocketSchedulerConfig{MaxClients: 10, MaxUpstreams: 10, MaxWaiting: 10, ConnectRPS: 8, SlowStartRPS: 1})
	t.Cleanup(s.Close)

	if got := s.Snapshot().EffectiveConnectRPS; got != 1 {
		t.Fatalf("expected initial slow-start rate 1, got %v", got)
	}
	s.ReportSuccess()
	if got := s.Snapshot().EffectiveConnectRPS; got != 2 {
		t.Fatalf("expected first success to double rate to 2, got %v", got)
	}
	s.ReportSuccess()
	if got := s.Snapshot().EffectiveConnectRPS; got != 4 {
		t.Fatalf("expected second success to double rate to 4, got %v", got)
	}
	advertised := 3.0
	s.UpdateCapacity(nil, &advertised)
	if got := s.Snapshot().EffectiveConnectRPS; got != 3 {
		t.Fatalf("expected backend rate to lower ramp to 3, got %v", got)
	}
	s.ReportFailure()
	snapshot := s.Snapshot()
	if snapshot.CurrentRampRPS != 1 || snapshot.EffectiveConnectRPS != 1 {
		t.Fatalf("expected failure to reset slow start, got %+v", snapshot)
	}
}
