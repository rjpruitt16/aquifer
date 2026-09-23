package main

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

func TestJSONLineAdapterReturnsWhenContextIsCanceled(t *testing.T) {
	reader, writer := io.Pipe()
	t.Cleanup(func() {
		_ = reader.Close()
		_ = writer.Close()
	})
	ctx, cancel := context.WithCancel(context.Background())
	adapter := &JSONLineAdapter{in: reader, out: &bytes.Buffer{}}
	done := make(chan error, 1)
	go func() { done <- adapter.Start(ctx, nil) }()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("adapter cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("adapter did not return after context cancellation")
	}
}
