// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"testing"
)

func TestErrorKindsLeaveOutNumbersAndStayBounded(t *testing.T) {
	a := errorKind(errors.New(`Post "http://10.0.0.1:8080/jobs": stream error: stream ID 7; INTERNAL_ERROR`))
	b := errorKind(errors.New(`Post "http://10.0.0.2:8080/jobs": stream error: stream ID 4093; INTERNAL_ERROR`))
	if a != b {
		t.Fatalf("the same error with different numbers should be one kind: %q vs %q", a, b)
	}

	d := &driver{ledger: openLedger(t.TempDir())}
	for i := range maxErrorKinds * 2 {
		d.clientError(uint64(i), fmt.Sprintf("kind %c%c", 'a'+i%26, 'a'+i/26))
	}
	kinds := d.clientErrorKinds()
	if len(kinds) > maxErrorKinds+1 {
		t.Fatalf("%d kinds kept, want at most %d plus other", len(kinds), maxErrorKinds)
	}
	if kinds["other"] != maxErrorKinds {
		t.Fatalf("other = %d, want %d", kinds["other"], maxErrorKinds)
	}
}
