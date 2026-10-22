// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"testing"
	"time"
)

func waitForWebhook(t *testing.T, ch <-chan map[string]any) map[string]any {
	t.Helper()

	select {
	case payload := <-ch:
		return payload
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for webhook")
	}
	return nil
}
