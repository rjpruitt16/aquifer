// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"net/http"
	"time"

	"github.com/rjpruitt16/aquifer/internal/dispatch"
	"github.com/rjpruitt16/aquifer/internal/env"
	"github.com/rjpruitt16/aquifer/l8"
)

// L8 lives in its own public package, github.com/rjpruitt16/aquifer/l8, so
// other senders and receivers can use it. These names keep code written
// against the root package working.

type (
	L8Registry          = l8.Registry
	L8Meta              = l8.Meta
	L8ChallengeReq      = l8.ChallengeRequest
	L8ChallengeResp     = l8.ChallengeResponse
	SchemaMismatchError = l8.SchemaMismatchError
)

// NewL8Registry is l8.NewRegistry with Aquifer's settings: handshakes share
// the dispatch client's pooled transport, and AQUIFER_L8_SCHEMA_VALIDATION
// turns on request-schema validation.
func NewL8Registry(keyPath, trustDir string) *L8Registry {
	return l8.NewRegistry(keyPath, trustDir,
		l8.WithHTTPClient(&http.Client{Timeout: 5 * time.Second, Transport: dispatch.DispatchClient.Transport}),
		l8.WithSchemaValidation(env.Bool("AQUIFER_L8_SCHEMA_VALIDATION", false)),
	)
}
