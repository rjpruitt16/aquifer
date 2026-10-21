// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

// Package l8 implements L8, a small protocol for trusted webhook delivery
// between a sender (such as Aquifer) and the service that receives its
// webhooks. Spec: https://rjpruitt16.github.io/l8-protocol/
//
// A sender keeps a Registry: its own Ed25519 key, plus the receivers it has
// verified with a challenge handshake.
//
//   - EnsureTrust fetches a receiver's /.well-known/l8 metadata and runs the
//     handshake once, remembering the result on disk.
//   - SealDelivery signs a webhook body for a trusted receiver, and encrypts
//     it first when the receiver advertises EncryptionCapability.
//   - ValidateRequestBody checks a request against the JSON Schemas a
//     receiver advertises (L8 0.2), when WithSchemaValidation is on.
//
// A receiver verifies X-L8-Signature with the sender's public key, then, for
// encrypted deliveries, decrypts with OpenPayload.
package l8
