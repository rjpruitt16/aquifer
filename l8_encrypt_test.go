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
)

type l8TestReceiver struct {
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

func newL8TestReceiver(t *testing.T, encrypt bool) *l8TestReceiver {
	t.Helper()
	signPub, signPriv, _ := ed25519.GenerateKey(rand.Reader)
	encPriv, _ := ecdh.X25519().GenerateKey(rand.Reader)
	rcv := &l8TestReceiver{signPriv: signPriv, encPriv: encPriv, encrypt: encrypt, got: make(chan receivedDelivery, 1)}
	rcv.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/l8":
			meta := L8Meta{
				ProtocolVersion:   "0.2",
				ServiceName:       "receiver",
				PublicKey:         base64.StdEncoding.EncodeToString(signPub),
				ChallengeEndpoint: "/l8/challenge",
				Capabilities:      []string{"signed_payloads"},
			}
			if encrypt {
				meta.Capabilities = append(meta.Capabilities, l8EncryptionCapability)
				meta.EncryptionPublicKey = base64.StdEncoding.EncodeToString(encPriv.PublicKey().Bytes())
			}
			json.NewEncoder(w).Encode(meta)
		case "/l8/challenge":
			var req L8ChallengeReq
			json.NewDecoder(r.Body).Decode(&req)
			sig := ed25519.Sign(signPriv, []byte(req.ChallengeID+":"+req.Nonce))
			json.NewEncoder(w).Encode(L8ChallengeResp{
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

func newTestL8Registry(t *testing.T) *L8Registry {
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

func TestL8PayloadRoundTripAndTamperDetection(t *testing.T) {
	receiver, _ := ecdh.X25519().GenerateKey(rand.Reader)
	plaintext := []byte(`{"job_id":"j1","body":"secret"}`)

	sealed, headers, err := sealL8Payload(receiver.PublicKey(), plaintext, "d1", 100)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sealed, []byte("secret")) {
		t.Fatal("ciphertext leaks plaintext")
	}
	eph, nonce := headers["X-L8-Ephemeral-Key"], headers["X-L8-Nonce"]

	got, err := openL8Payload(receiver, sealed, eph, nonce, "d1", 100)
	if err != nil || !bytes.Equal(got, plaintext) {
		t.Fatalf("round trip failed: %q, %v", got, err)
	}

	tampered := append([]byte{}, sealed...)
	tampered[0] ^= 1
	if _, err := openL8Payload(receiver, tampered, eph, nonce, "d1", 100); err == nil {
		t.Fatal("tampered ciphertext must not decrypt")
	}
	if _, err := openL8Payload(receiver, sealed, eph, nonce, "d2", 100); err == nil {
		t.Fatal("ciphertext must be bound to its delivery id")
	}
	if _, err := openL8Payload(receiver, sealed, eph, nonce, "d1", 101); err == nil {
		t.Fatal("ciphertext must be bound to its timestamp")
	}
	other, _ := ecdh.X25519().GenerateKey(rand.Reader)
	if _, err := openL8Payload(other, sealed, eph, nonce, "d1", 100); err == nil {
		t.Fatal("a different receiver key must not decrypt")
	}
}

func TestWebhookDeliveryEncryptedThenSigned(t *testing.T) {
	rcv := newL8TestReceiver(t, true)
	l8 := newTestL8Registry(t)
	payload := map[string]any{"job_id": "j1", "status": "completed", "body": "secret"}

	if !deliverWithRetryContext(context.Background(), rcv.URL+"/hook", payload, webhookMaxRetries, l8, NoopMetricsAdapter{}) {
		t.Fatal("delivery failed")
	}
	d := <-rcv.got
	if d.header.Get("X-L8-Encryption") != l8EncryptionAlgorithm || d.header.Get("Content-Type") != l8EncryptedContentType {
		t.Fatalf("expected encrypted delivery, got headers %v", d.header)
	}
	if d.header.Get("X-L8-Content-Type") != "application/json" {
		t.Fatalf("original content type not carried: %v", d.header)
	}
	if bytes.Contains(d.body, []byte("secret")) {
		t.Fatal("body travelled in plaintext")
	}
	verifyL8Signature(t, l8.publicKey, d.header, d.body)

	ts, _ := strconv.ParseInt(d.header.Get("X-L8-Timestamp"), 10, 64)
	plain, err := openL8Payload(rcv.encPriv, d.body, d.header.Get("X-L8-Ephemeral-Key"), d.header.Get("X-L8-Nonce"), d.header.Get("X-L8-Delivery-Id"), ts)
	if err != nil {
		t.Fatalf("receiver could not decrypt: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(plain, &got); err != nil || got["body"] != "secret" {
		t.Fatalf("decrypted payload mismatch: %s", plain)
	}
}

func TestWebhookDeliveryWithoutEncryptionCapabilityStaysPlaintext(t *testing.T) {
	rcv := newL8TestReceiver(t, false)
	l8 := newTestL8Registry(t)

	if !deliverWithRetryContext(context.Background(), rcv.URL+"/hook", map[string]any{"body": "visible"}, webhookMaxRetries, l8, NoopMetricsAdapter{}) {
		t.Fatal("delivery failed")
	}
	d := <-rcv.got
	if d.header.Get("X-L8-Encryption") != "" || !bytes.Contains(d.body, []byte("visible")) {
		t.Fatalf("expected signed plaintext, got %v %s", d.header, d.body)
	}
	verifyL8Signature(t, l8.publicKey, d.header, d.body)
}

func TestQueuedWebhookJobIsEncrypted(t *testing.T) {
	rcv := newL8TestReceiver(t, true)
	l8 := newTestL8Registry(t)
	job := &Job{ID: "w1", Method: "POST", URL: rcv.URL + "/hook", Body: `{"body":"secret"}`, Headers: map[string]string{"Content-Type": "application/json"}}

	resp, err := makeRequest(context.Background(), job, job.URL, 0, 0, 0, 0, 0, l8)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	d := <-rcv.got
	if d.header.Get("X-L8-Encryption") == "" || bytes.Contains(d.body, []byte("secret")) {
		t.Fatalf("queued webhook delivery was not encrypted: %v %s", d.header, d.body)
	}
	verifyL8Signature(t, l8.publicKey, d.header, d.body)
}

func TestTrustReloadKeepsEncryptionKey(t *testing.T) {
	rcv := newL8TestReceiver(t, true)
	dir := t.TempDir()
	first := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	first.EnsureTrust(rcv.URL)
	first.Close()

	reloaded := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	defer reloaded.Close()
	value, ok := reloaded.trusts.Load(rcv.URL)
	if !ok || value.(*l8Peer).encryption == nil {
		t.Fatal("encryption key not restored from the trust file")
	}
}
