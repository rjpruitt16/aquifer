// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/rjpruitt16/aquifer/l8"
)

// Aquifer's webhook deliveries are signed, and encrypted for receivers that
// ask for it. These tests drive Aquifer's delivery paths against an L8
// receiver; the protocol itself is tested in the l8 package.

type testReceiver struct {
	*httptest.Server
	signPriv ed25519.PrivateKey
	encPriv  *ecdh.PrivateKey
	encrypt  bool
	got      chan receivedDelivery
}

type receivedDelivery struct {
	header http.Header
	body   []byte
}

func newTestReceiver(t *testing.T, encrypt bool) *testReceiver {
	t.Helper()
	signPub, signPriv, _ := ed25519.GenerateKey(rand.Reader)
	encPriv, _ := ecdh.X25519().GenerateKey(rand.Reader)
	rcv := &testReceiver{signPriv: signPriv, encPriv: encPriv, encrypt: encrypt, got: make(chan receivedDelivery, 1)}
	rcv.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/l8":
			meta := l8.Meta{
				ProtocolVersion:   "0.2",
				ServiceName:       "receiver",
				PublicKey:         base64.StdEncoding.EncodeToString(signPub),
				ChallengeEndpoint: "/l8/challenge",
				Capabilities:      []string{"signed_payloads"},
			}
			if encrypt {
				meta.Capabilities = append(meta.Capabilities, l8.EncryptionCapability)
				meta.EncryptionPublicKey = base64.StdEncoding.EncodeToString(encPriv.PublicKey().Bytes())
			}
			json.NewEncoder(w).Encode(meta)
		case "/l8/challenge":
			var req l8.ChallengeRequest
			json.NewDecoder(r.Body).Decode(&req)
			sig := ed25519.Sign(signPriv, []byte(req.ChallengeID+":"+req.Nonce))
			json.NewEncoder(w).Encode(l8.ChallengeResponse{
				ChallengeID: req.ChallengeID, Nonce: req.Nonce,
				ReceiverSignature: base64.StdEncoding.EncodeToString(sig),
				ReceiverPublicKey: base64.StdEncoding.EncodeToString(signPub),
			})
		default:
			body, _ := io.ReadAll(r.Body)
			rcv.got <- receivedDelivery{header: r.Header.Clone(), body: body}
		}
	}))
	t.Cleanup(rcv.Close)
	return rcv
}

func newTestRegistry(t *testing.T) *l8.Registry {
	t.Helper()
	dir := t.TempDir()
	r := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	t.Cleanup(r.Close)
	return r
}

func verifyL8Signature(t *testing.T, sender ed25519.PublicKey, h http.Header, body []byte) {
	t.Helper()
	sum := sha256.Sum256(body)
	msg := fmt.Sprintf("%s.%s.%s", h.Get("X-L8-Delivery-Id"), h.Get("X-L8-Timestamp"), base64.StdEncoding.EncodeToString(sum[:]))
	sig, _ := base64.StdEncoding.DecodeString(h.Get("X-L8-Signature"))
	if !ed25519.Verify(sender, []byte(msg), sig) {
		t.Fatal("signature does not verify over the bytes received")
	}
}

func TestWebhookDeliveryEncryptedThenSigned(t *testing.T) {
	rcv := newTestReceiver(t, true)
	registry := newTestRegistry(t)
	payload := map[string]any{"job_id": "j1", "status": "completed", "body": "secret"}

	if !deliverWithRetryContext(context.Background(), rcv.URL+"/hook", payload, webhookMaxRetries, registry, NoopMetricsAdapter{}) {
		t.Fatal("delivery failed")
	}
	d := <-rcv.got
	if d.header.Get("X-L8-Encryption") != l8.EncryptionAlgorithm || d.header.Get("Content-Type") != l8.EncryptedContentType {
		t.Fatalf("expected encrypted delivery, got headers %v", d.header)
	}
	if d.header.Get("X-L8-Content-Type") != "application/json" {
		t.Fatalf("original content type not carried: %v", d.header)
	}
	if bytes.Contains(d.body, []byte("secret")) {
		t.Fatal("body travelled in plaintext")
	}
	verifyL8Signature(t, registry.PublicKey(), d.header, d.body)

	ts, _ := strconv.ParseInt(d.header.Get("X-L8-Timestamp"), 10, 64)
	plain, err := l8.OpenPayload(rcv.encPriv, d.body, d.header.Get("X-L8-Ephemeral-Key"), d.header.Get("X-L8-Nonce"), d.header.Get("X-L8-Delivery-Id"), ts)
	if err != nil {
		t.Fatalf("receiver could not decrypt: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(plain, &got); err != nil || got["body"] != "secret" {
		t.Fatalf("decrypted payload mismatch: %s", plain)
	}
}

func TestWebhookDeliveryWithoutEncryptionCapabilityStaysPlaintext(t *testing.T) {
	rcv := newTestReceiver(t, false)
	registry := newTestRegistry(t)

	if !deliverWithRetryContext(context.Background(), rcv.URL+"/hook", map[string]any{"body": "visible"}, webhookMaxRetries, registry, NoopMetricsAdapter{}) {
		t.Fatal("delivery failed")
	}
	d := <-rcv.got
	if d.header.Get("X-L8-Encryption") != "" || !bytes.Contains(d.body, []byte("visible")) {
		t.Fatalf("expected signed plaintext, got %v %s", d.header, d.body)
	}
	verifyL8Signature(t, registry.PublicKey(), d.header, d.body)
}

func TestQueuedWebhookJobIsEncrypted(t *testing.T) {
	rcv := newTestReceiver(t, true)
	registry := newTestRegistry(t)
	job := &Job{ID: "w1", Method: "POST", URL: rcv.URL + "/hook", Body: `{"body":"secret"}`, Headers: map[string]string{"Content-Type": "application/json"}}

	resp, err := makeRequest(context.Background(), job, job.URL, 0, 0, 0, 0, 0, registry)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	d := <-rcv.got
	if d.header.Get("X-L8-Encryption") == "" || bytes.Contains(d.body, []byte("secret")) {
		t.Fatalf("queued webhook delivery was not encrypted: %v %s", d.header, d.body)
	}
	verifyL8Signature(t, registry.PublicKey(), d.header, d.body)
}
