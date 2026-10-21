// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package l8

import (
	"bytes"
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
	"testing"
)

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
			meta := Meta{
				ProtocolVersion:   "0.2",
				ServiceName:       "receiver",
				PublicKey:         base64.StdEncoding.EncodeToString(signPub),
				ChallengeEndpoint: "/l8/challenge",
				Capabilities:      []string{"signed_payloads"},
			}
			if encrypt {
				meta.Capabilities = append(meta.Capabilities, EncryptionCapability)
				meta.EncryptionPublicKey = base64.StdEncoding.EncodeToString(encPriv.PublicKey().Bytes())
			}
			json.NewEncoder(w).Encode(meta)
		case "/l8/challenge":
			var req ChallengeRequest
			json.NewDecoder(r.Body).Decode(&req)
			sig := ed25519.Sign(signPriv, []byte(req.ChallengeID+":"+req.Nonce))
			json.NewEncoder(w).Encode(ChallengeResponse{
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

func newTestRegistry(t *testing.T) *Registry {
	t.Helper()
	dir := t.TempDir()
	r := NewRegistry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
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

func TestL8PayloadRoundTripAndTamperDetection(t *testing.T) {
	receiver, _ := ecdh.X25519().GenerateKey(rand.Reader)
	plaintext := []byte(`{"job_id":"j1","body":"secret"}`)

	sealed, headers, err := sealPayload(receiver.PublicKey(), plaintext, "d1", 100)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sealed, []byte("secret")) {
		t.Fatal("ciphertext leaks plaintext")
	}
	eph, nonce := headers["X-L8-Ephemeral-Key"], headers["X-L8-Nonce"]

	got, err := OpenPayload(receiver, sealed, eph, nonce, "d1", 100)
	if err != nil || !bytes.Equal(got, plaintext) {
		t.Fatalf("round trip failed: %q, %v", got, err)
	}

	tampered := append([]byte{}, sealed...)
	tampered[0] ^= 1
	if _, err := OpenPayload(receiver, tampered, eph, nonce, "d1", 100); err == nil {
		t.Fatal("tampered ciphertext must not decrypt")
	}
	if _, err := OpenPayload(receiver, sealed, eph, nonce, "d2", 100); err == nil {
		t.Fatal("ciphertext must be bound to its delivery id")
	}
	if _, err := OpenPayload(receiver, sealed, eph, nonce, "d1", 101); err == nil {
		t.Fatal("ciphertext must be bound to its timestamp")
	}
	other, _ := ecdh.X25519().GenerateKey(rand.Reader)
	if _, err := OpenPayload(other, sealed, eph, nonce, "d1", 100); err == nil {
		t.Fatal("a different receiver key must not decrypt")
	}
}

func TestTrustReloadKeepsEncryptionKey(t *testing.T) {
	rcv := newTestReceiver(t, true)
	dir := t.TempDir()
	first := NewRegistry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	first.EnsureTrust(rcv.URL)
	first.Close()

	reloaded := NewRegistry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	defer reloaded.Close()
	value, ok := reloaded.trusts.Load(rcv.URL)
	if !ok || value.(*peer).encryption == nil {
		t.Fatal("encryption key not restored from the trust file")
	}
}
