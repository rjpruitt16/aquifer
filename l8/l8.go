// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package l8

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	Version      = "0.2"
	nonceTTL     = 5 * time.Minute
	metaMaxBytes = 1 << 20
)

// ---- wire types ----

type Meta struct {
	ProtocolVersion   string   `json:"protocol_version"`
	ServiceName       string   `json:"service_name"`
	PublicKey         string   `json:"public_key"`
	ChallengeEndpoint string   `json:"challenge_endpoint"`
	SupportedAlgos    []string `json:"supported_algorithms"`
	Capabilities      []string `json:"capabilities"`
	SpecURL           string   `json:"spec_url"`

	SchemaHash     string                     `json:"schema_hash,omitempty"`
	RequestSchemas map[string]json.RawMessage `json:"request_schemas,omitempty"`

	EncryptionPublicKey string `json:"encryption_public_key,omitempty"`
}

type ChallengeRequest struct {
	ChallengeID     string `json:"challenge_id"`
	Nonce           string `json:"nonce"`
	Timestamp       int64  `json:"timestamp"`
	SenderPublicKey string `json:"sender_public_key"`
	Signature       string `json:"signature"`
}

type ChallengeResponse struct {
	ChallengeID       string `json:"challenge_id"`
	Nonce             string `json:"nonce"`
	ReceiverSignature string `json:"receiver_signature"`
	ReceiverPublicKey string `json:"receiver_public_key"`
}

// on-disk trust file — one per trusted domain, named {domain}.json
type trustFile struct {
	Domain          string   `json:"domain"`
	PublicKey       string   `json:"public_key"`
	ValidatedAt     int64    `json:"validated_at"`
	ProtocolVersion string   `json:"protocol_version"`
	Capabilities    []string `json:"capabilities"`

	EncryptionPublicKey string `json:"encryption_public_key,omitempty"`
}

// ---- registry ----

type Registry struct {
	privateKey ed25519.PrivateKey
	publicKey  ed25519.PublicKey
	PubB64     string // exported so server can embed in responses

	trustDir string
	trusts   sync.Map // domain -> *peer (in-memory, loaded from disk on start)
	// notL8 remembers receivers that don't speak L8 (domain -> time.Time of
	// the failed probe) for negativeTTL. Without it every webhook to such
	// a receiver cost an extra GET /.well-known/l8.
	notL8   sync.Map
	nonces  sync.Map // nonce -> time.Time expiry
	schemas *schemaCache

	// client makes the handshake and metadata requests.
	client *http.Client

	closeOnce sync.Once
	stop      chan struct{}
	done      chan struct{}
}

// Option configures a Registry.
type Option func(*options)

type options struct {
	client              *http.Client
	schemaValidation    bool
	schemaMinRefetchAge time.Duration
}

// WithHTTPClient sets the client used for handshakes and metadata requests.
// The default is a client with a 5 second timeout.
func WithHTTPClient(client *http.Client) Option {
	return func(o *options) { o.client = client }
}

// WithSchemaValidation turns on validating request bodies against the
// request schemas a receiver advertises (L8 0.2). Off by default.
func WithSchemaValidation(enabled bool) Option {
	return func(o *options) { o.schemaValidation = enabled }
}

// WithSchemaMinRefetchAge sets how soon a changed X-Aqueduct-Schema-Hash may
// trigger another fetch of a receiver's schemas (default 10 s), so a
// receiver flapping its hash can't cause a fetch per response.
func WithSchemaMinRefetchAge(age time.Duration) Option {
	return func(o *options) { o.schemaMinRefetchAge = age }
}

// NewRegistry loads this sender's Ed25519 key from keyPath (creating it if
// missing) and the trusted receivers saved in trustDir (default "l8-trust").
// Call Close when done.
func NewRegistry(keyPath, trustDir string, opts ...Option) *Registry {
	o := options{client: &http.Client{Timeout: 5 * time.Second}, schemaMinRefetchAge: schemaMinRefetchAge}
	for _, opt := range opts {
		opt(&o)
	}
	pub, priv := loadOrGenKey(keyPath)
	if trustDir == "" {
		trustDir = "l8-trust"
	}
	os.MkdirAll(trustDir, 0700)

	r := &Registry{
		privateKey: priv,
		publicKey:  pub,
		PubB64:     base64.StdEncoding.EncodeToString(pub),
		trustDir:   trustDir,
		client:     o.client,
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
	r.schemas = newSchemaCache(o.schemaValidation, r.fetchMeta)
	r.schemas.minRefetchAge = o.schemaMinRefetchAge
	r.loadTrustsFromDisk()
	go r.sweepNonces()
	return r
}

// PublicKey is this sender's Ed25519 key; receivers verify X-L8-Signature with it.
func (r *Registry) PublicKey() ed25519.PublicKey { return r.publicKey }

func (r *Registry) Close() {
	r.closeOnce.Do(func() {
		close(r.stop)
		<-r.done
	})
}

func loadOrGenKey(path string) (ed25519.PublicKey, ed25519.PrivateKey) {
	if raw := os.Getenv("L8_PRIVATE_KEY"); raw != "" {
		b, err := base64.StdEncoding.DecodeString(raw)
		if err == nil && len(b) == ed25519.PrivateKeySize {
			priv := ed25519.PrivateKey(b)
			pub := priv.Public().(ed25519.PublicKey)
			log.Printf("[L8] loaded key from L8_PRIVATE_KEY (pub: %s...)", base64.StdEncoding.EncodeToString(pub)[:16])
			return pub, priv
		}
		log.Printf("[L8] L8_PRIVATE_KEY is set but invalid — generating new key")
	}

	if path != "" {
		if data, err := os.ReadFile(path); err == nil && len(data) == ed25519.PrivateKeySize {
			priv := ed25519.PrivateKey(data)
			pub := priv.Public().(ed25519.PublicKey)
			log.Printf("[L8] loaded key from %s (pub: %s...)", path, base64.StdEncoding.EncodeToString(pub)[:16])
			return pub, priv
		}
	}

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		log.Fatalf("[L8] generate key: %v", err)
	}
	if path != "" {
		if err := os.WriteFile(path, []byte(priv), 0600); err != nil {
			log.Printf("[L8] warning: could not save key to %s: %v", path, err)
		} else {
			log.Printf("[L8] generated key, saved to %s", path)
		}
	}
	log.Printf("[L8] public key: %s", base64.StdEncoding.EncodeToString(pub))
	return pub, priv
}

func (r *Registry) Meta(host string) Meta {
	return Meta{
		ProtocolVersion:   Version,
		ServiceName:       "aquifer",
		PublicKey:         r.PubB64,
		ChallengeEndpoint: "/l8/challenge",
		SupportedAlgos:    []string{"ed25519"},
		Capabilities:      []string{"signed_payloads"},
		SpecURL:           "https://rjpruitt16.github.io/l8-protocol/spec.json",
	}
}

// HandleChallenge verifies the sender's ownership proof and returns Aquifer's signed response.
func (r *Registry) HandleChallenge(req ChallengeRequest) (*ChallengeResponse, error) {
	age := time.Since(time.Unix(req.Timestamp, 0))
	if age < 0 {
		age = -age
	}
	if age > nonceTTL {
		return nil, fmt.Errorf("timestamp expired")
	}
	if _, used := r.nonces.LoadOrStore(req.Nonce, time.Now().Add(nonceTTL)); used {
		return nil, fmt.Errorf("nonce already used")
	}

	senderPub, err := base64.StdEncoding.DecodeString(req.SenderPublicKey)
	if err != nil || len(senderPub) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("invalid sender_public_key")
	}
	sig, err := base64.StdEncoding.DecodeString(req.Signature)
	if err != nil {
		return nil, fmt.Errorf("invalid signature encoding")
	}
	msg := []byte(req.ChallengeID + ":" + req.Nonce)
	if !ed25519.Verify(ed25519.PublicKey(senderPub), msg, sig) {
		return nil, fmt.Errorf("signature verification failed")
	}

	ourSig := ed25519.Sign(r.privateKey, msg)
	return &ChallengeResponse{
		ChallengeID:       req.ChallengeID,
		Nonce:             req.Nonce,
		ReceiverSignature: base64.StdEncoding.EncodeToString(ourSig),
		ReceiverPublicKey: r.PubB64,
	}, nil
}

// EnsureTrust runs the L8 handshake with the webhook domain if not already trusted.
// Silently does nothing if the receiver doesn't support L8 — delivery still proceeds unsigned.
func (r *Registry) EnsureTrust(webhookURL string) {
	domain := extractDomain(webhookURL)
	if domain == "" {
		return
	}
	if _, ok := r.trusts.Load(domain); ok {
		return
	}
	if at, ok := r.notL8.Load(domain); ok && time.Since(at.(time.Time)) < negativeTTL {
		return
	}

	meta, err := r.fetchMeta(domain)
	if err != nil {
		r.notL8.Store(domain, time.Now())
		return
	}
	r.notL8.Delete(domain)

	challengeID := uuid.New().String()
	nonce := uuid.New().String()
	msg := []byte(challengeID + ":" + nonce)
	sig := ed25519.Sign(r.privateKey, msg)

	challengeURL := domain + meta.ChallengeEndpoint
	if strings.HasPrefix(meta.ChallengeEndpoint, "http") {
		challengeURL = meta.ChallengeEndpoint
	}

	reqBody, _ := json.Marshal(ChallengeRequest{
		ChallengeID:     challengeID,
		Nonce:           nonce,
		Timestamp:       time.Now().Unix(),
		SenderPublicKey: r.PubB64,
		Signature:       base64.StdEncoding.EncodeToString(sig),
	})

	resp, err := r.client.Post(challengeURL, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		log.Printf("[L8] challenge to %s failed: %v", domain, err)
		return
	}
	defer resp.Body.Close()

	var cr ChallengeResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil || cr.ReceiverSignature == "" {
		log.Printf("[L8] invalid challenge response from %s", domain)
		return
	}

	receiverPub, err := base64.StdEncoding.DecodeString(meta.PublicKey)
	if err != nil || len(receiverPub) != ed25519.PublicKeySize {
		log.Printf("[L8] invalid receiver public key from %s", domain)
		return
	}
	receiverSig, err := base64.StdEncoding.DecodeString(cr.ReceiverSignature)
	if err != nil || !ed25519.Verify(ed25519.PublicKey(receiverPub), msg, receiverSig) {
		log.Printf("[L8] receiver %s failed ownership proof", domain)
		return
	}

	r.trusts.Store(domain, &peer{signing: ed25519.PublicKey(receiverPub), encryption: parseEncryptionKey(meta.Capabilities, meta.EncryptionPublicKey)})
	r.saveTrustToDisk(domain, ed25519.PublicKey(receiverPub), meta)
	log.Printf("[L8] trust established with %s", domain)
}

// IsTrusted returns true if the L8 handshake has been completed for this webhook domain.
func (r *Registry) IsTrusted(webhookURL string) bool {
	domain := extractDomain(webhookURL)
	if domain == "" {
		return false
	}
	_, ok := r.trusts.Load(domain)
	return ok
}

// signHeaders returns the X-L8-* signature headers for the exact bytes sent.
func (r *Registry) signHeaders(body []byte, deliveryID string, ts int64) map[string]string {
	h := sha256.Sum256(body)
	msg := fmt.Sprintf("%s.%d.%s", deliveryID, ts, base64.StdEncoding.EncodeToString(h[:]))
	sig := ed25519.Sign(r.privateKey, []byte(msg))
	return map[string]string{
		"X-L8-Delivery-Id": deliveryID,
		"X-L8-Timestamp":   fmt.Sprintf("%d", ts),
		"X-L8-Key-Id":      base64.StdEncoding.EncodeToString(r.publicKey[:8]),
		"X-L8-Signature":   base64.StdEncoding.EncodeToString(sig),
	}
}

// ---- disk persistence ----

func (r *Registry) saveTrustToDisk(domain string, pub ed25519.PublicKey, meta *Meta) {
	tf := trustFile{
		Domain:              domain,
		PublicKey:           base64.StdEncoding.EncodeToString(pub),
		ValidatedAt:         time.Now().Unix(),
		ProtocolVersion:     meta.ProtocolVersion,
		Capabilities:        meta.Capabilities,
		EncryptionPublicKey: meta.EncryptionPublicKey,
	}
	data, _ := json.MarshalIndent(tf, "", "  ")
	filename := filepath.Join(r.trustDir, sanitizeDomain(domain)+".json")
	if err := os.WriteFile(filename, data, 0600); err != nil {
		log.Printf("[L8] could not save trust for %s: %v", domain, err)
	}
}

func (r *Registry) loadTrustsFromDisk() {
	entries, err := os.ReadDir(r.trustDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(r.trustDir, e.Name()))
		if err != nil {
			continue
		}
		var tf trustFile
		if err := json.Unmarshal(data, &tf); err != nil || tf.Domain == "" || tf.PublicKey == "" {
			continue
		}
		pub, err := base64.StdEncoding.DecodeString(tf.PublicKey)
		if err != nil || len(pub) != ed25519.PublicKeySize {
			continue
		}
		r.trusts.Store(tf.Domain, &peer{signing: ed25519.PublicKey(pub), encryption: parseEncryptionKey(tf.Capabilities, tf.EncryptionPublicKey)})
		log.Printf("[L8] loaded trust for %s", tf.Domain)
	}
}

// ---- helpers ----

// negativeTTL is how long a receiver without L8 metadata is left alone
// before Aquifer probes it again (it may have added L8 since).
const negativeTTL = 5 * time.Minute

func (r *Registry) fetchMeta(baseURL string) (*Meta, error) {
	resp, err := r.client.Get(baseURL + "/.well-known/l8")
	if err != nil {
		return nil, err
	}
	// Drain what's left so the connection goes back to the pool.
	defer func() {
		io.Copy(io.Discard, io.LimitReader(resp.Body, metaMaxBytes))
		resp.Body.Close()
	}()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("/.well-known/l8 returned %d", resp.StatusCode)
	}
	var meta Meta
	if err := json.NewDecoder(io.LimitReader(resp.Body, metaMaxBytes)).Decode(&meta); err != nil {
		return nil, err
	}
	if meta.PublicKey == "" || meta.ChallengeEndpoint == "" {
		return nil, fmt.Errorf("incomplete L8 metadata")
	}
	return &meta, nil
}

func extractDomain(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return ""
	}
	return u.Scheme + "://" + u.Host
}

func sanitizeDomain(domain string) string {
	s := strings.TrimPrefix(strings.TrimPrefix(domain, "https://"), "http://")
	return strings.ReplaceAll(s, ":", "_")
}

func (r *Registry) sweepNonces() {
	defer close(r.done)

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.sweep(time.Now())
		case <-r.stop:
			return
		}
	}
}

// sweep drops expired nonces, negative-cache entries and cached schemas.
// Receiver and target domains come from callers' URLs, so without it these
// maps would grow with every distinct host a long-lived node ever sees.
func (r *Registry) sweep(now time.Time) {
	r.nonces.Range(func(k, v any) bool {
		if expiry, ok := v.(time.Time); ok && now.After(expiry) {
			r.nonces.Delete(k)
		}
		return true
	})
	r.notL8.Range(func(k, v any) bool {
		if at, ok := v.(time.Time); ok && now.Sub(at) >= negativeTTL {
			r.notL8.Delete(k)
		}
		return true
	})
	if r.schemas != nil {
		r.schemas.sweep(now)
	}
}

// SpecDocument is served at GET /l8-spec so agents and developers can discover the protocol.
const SpecDocument = `# L8 Protocol — v0.2

L8 is a lightweight challenge-response handshake for trustless webhook delivery.
No shared secrets. No central authority. Ownership is proven once via Ed25519 signatures.
All future deliveries carry signed headers the receiver verifies locally.

## Receiver endpoints (implement these to support L8)

### GET /.well-known/l8

Returns your service identity and public key.

` + "```" + `json
{
  "protocol_version":   "0.2",
  "service_name":       "your-service",
  "public_key":         "<base64 ed25519 public key>",
  "challenge_endpoint": "/l8/challenge",
  "supported_algorithms": ["ed25519"],
  "capabilities":       ["signed_payloads"]
}
` + "```" + `

### POST /l8/challenge

Sender proves it owns its private key. You prove you own yours.

Request from sender:
` + "```" + `json
{
  "challenge_id":      "<uuid>",
  "nonce":             "<uuid>",
  "timestamp":         1740000000,
  "sender_public_key": "<base64 ed25519 public key>",
  "signature":         "<base64 ed25519 sig of 'challenge_id:nonce'>"
}
` + "```" + `

Your response:
` + "```" + `json
{
  "challenge_id":        "<same uuid>",
  "nonce":               "<same nonce>",
  "receiver_signature":  "<base64 ed25519 sig of 'challenge_id:nonce'>",
  "receiver_public_key": "<base64 ed25519 public key>"
}
` + "```" + `

Reject if: timestamp is older than 5 minutes, or nonce has been seen before (replay protection).

## Signed delivery headers

After trust is established, all webhook POST requests include:

` + "```" + `
X-L8-Delivery-Id: <uuid>
X-L8-Timestamp:   <unix seconds>
X-L8-Key-Id:      <base64 first 8 bytes of sender public key>
X-L8-Signature:   <base64 ed25519 sig of '{delivery_id}.{timestamp}.{base64(sha256(body))}'>
` + "```" + `

## Verification

` + "```" + `python
import base64, hashlib
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

def verify_l8(headers, body, sender_public_key_b64):
    pub = Ed25519PublicKey.from_public_bytes(base64.b64decode(sender_public_key_b64))
    delivery_id = headers["X-L8-Delivery-Id"]
    timestamp   = headers["X-L8-Timestamp"]
    body_hash   = base64.b64encode(hashlib.sha256(body).digest()).decode()
    msg = f"{delivery_id}.{timestamp}.{body_hash}".encode()
    sig = base64.b64decode(headers["X-L8-Signature"])
    pub.verify(sig, msg)  # raises InvalidSignature if tampered
` + "```" + `

## Payload encryption (0.2, optional)

Advertise an X25519 key (separate from your Ed25519 signing key) to receive encrypted bodies:

` + "```" + `json
{
  "capabilities":          ["signed_payloads", "encrypted_payloads"],
  "supported_algorithms":  ["ed25519", "x25519-hkdf-sha256-aes256gcm"],
  "encryption_public_key": "<base64 X25519 public key>"
}
` + "```" + `

The sender generates an ephemeral X25519 key per delivery, derives a key with
HKDF-SHA256(ECDH(ephemeral, yours), salt = ephemeral_pub || your_pub, info = "l8/0.2 payload"),
and encrypts with AES-256-GCM using AAD "{delivery_id}.{timestamp}". Extra headers:

` + "```" + `
X-L8-Encryption:    x25519-hkdf-sha256-aes256gcm
X-L8-Ephemeral-Key: <base64 ephemeral X25519 public key>
X-L8-Nonce:         <base64 12-byte nonce>
X-L8-Content-Type:  <original content type>
Content-Type:       application/l8-encrypted
` + "```" + `

X-L8-Signature covers the ciphertext. Verify it first, then decrypt.

## Request schemas (0.2, optional)

An upstream can publish JSON Schemas (draft 2020-12) for its request bodies, keyed by "METHOD /path":

` + "```" + `json
{
  "capabilities":    ["signed_payloads", "request_schemas"],
  "schema_hash":     "sha256:<hex>",
  "request_schemas": { "POST /v1/chat": { "type": "object", "required": ["model"] } }
}
` + "```" + `

schema_hash is required when request_schemas is present; senders compare it for equality only.
Send X-Aqueduct-Schema-Hash on responses so senders notice a change, refetch this document,
and re-run the handshake. Senders cache schemas for at most 10 minutes and never fetch external $refs.

## Key rotation

If signature verification fails, re-fetch the sender's ` + "`/.well-known/l8`" + ` to get the new public key
and retry verification before rejecting. This makes key rotation seamless — no coordination needed.

## Trust cache

Store one file per trusted domain: ` + "`l8-trust/{domain}.json`" + `

` + "```" + `json
{
  "domain":           "https://example.com",
  "public_key":       "<base64>",
  "validated_at":     1740000000,
  "protocol_version": "0.2",
  "capabilities":     ["signed_payloads"]
}
` + "```" + `

To revoke trust with a domain: delete its file. The handshake will re-run on next delivery.

## Graceful degradation

L8 is opt-in. If ` + "`/.well-known/l8`" + ` returns 404, delivery proceeds without signed headers.
Receivers that don't implement L8 are unaffected.
`
