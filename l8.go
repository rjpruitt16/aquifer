package aquifer

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
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
	l8Version  = "0.1"
	l8NonceTTL = 5 * time.Minute
)

// ---- wire types ----

type L8Meta struct {
	ProtocolVersion   string   `json:"protocol_version"`
	ServiceName       string   `json:"service_name"`
	PublicKey         string   `json:"public_key"`
	ChallengeEndpoint string   `json:"challenge_endpoint"`
	SupportedAlgos    []string `json:"supported_algorithms"`
	Capabilities      []string `json:"capabilities"`
	SpecURL           string   `json:"spec_url"`
}

type L8ChallengeReq struct {
	ChallengeID     string `json:"challenge_id"`
	Nonce           string `json:"nonce"`
	Timestamp       int64  `json:"timestamp"`
	SenderPublicKey string `json:"sender_public_key"`
	Signature       string `json:"signature"`
}

type L8ChallengeResp struct {
	ChallengeID       string `json:"challenge_id"`
	Nonce             string `json:"nonce"`
	ReceiverSignature string `json:"receiver_signature"`
	ReceiverPublicKey string `json:"receiver_public_key"`
}

// on-disk trust file — one per trusted domain, named {domain}.json
type l8TrustFile struct {
	Domain          string   `json:"domain"`
	PublicKey       string   `json:"public_key"`
	ValidatedAt     int64    `json:"validated_at"`
	ProtocolVersion string   `json:"protocol_version"`
	Capabilities    []string `json:"capabilities"`
}

// ---- registry ----

type L8Registry struct {
	privateKey ed25519.PrivateKey
	publicKey  ed25519.PublicKey
	PubB64     string // exported so server can embed in responses

	trustDir string
	trusts   sync.Map // domain -> ed25519.PublicKey (in-memory, loaded from disk on start)
	nonces   sync.Map // nonce -> time.Time expiry
}

func NewL8Registry(keyPath, trustDir string) *L8Registry {
	pub, priv := loadOrGenKey(keyPath)
	if trustDir == "" {
		trustDir = "l8-trust"
	}
	os.MkdirAll(trustDir, 0700)

	r := &L8Registry{
		privateKey: priv,
		publicKey:  pub,
		PubB64:     base64.StdEncoding.EncodeToString(pub),
		trustDir:   trustDir,
	}
	r.loadTrustsFromDisk()
	go r.sweepNonces()
	return r
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

func (r *L8Registry) Meta(host string) L8Meta {
	return L8Meta{
		ProtocolVersion:   l8Version,
		ServiceName:       "aquifer",
		PublicKey:         r.PubB64,
		ChallengeEndpoint: "/l8/challenge",
		SupportedAlgos:    []string{"ed25519"},
		Capabilities:      []string{"signed_payloads"},
		SpecURL:           "https://rjpruitt16.github.io/l8-protocol/spec.json",
	}
}

// HandleChallenge verifies the sender's ownership proof and returns Aquifer's signed response.
func (r *L8Registry) HandleChallenge(req L8ChallengeReq) (*L8ChallengeResp, error) {
	age := time.Since(time.Unix(req.Timestamp, 0))
	if age < 0 {
		age = -age
	}
	if age > l8NonceTTL {
		return nil, fmt.Errorf("timestamp expired")
	}
	if _, used := r.nonces.LoadOrStore(req.Nonce, time.Now().Add(l8NonceTTL)); used {
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
	return &L8ChallengeResp{
		ChallengeID:       req.ChallengeID,
		Nonce:             req.Nonce,
		ReceiverSignature: base64.StdEncoding.EncodeToString(ourSig),
		ReceiverPublicKey: r.PubB64,
	}, nil
}

// EnsureTrust runs the L8 handshake with the webhook domain if not already trusted.
// Silently does nothing if the receiver doesn't support L8 — delivery still proceeds unsigned.
func (r *L8Registry) EnsureTrust(webhookURL string) {
	domain := extractDomain(webhookURL)
	if domain == "" {
		return
	}
	if _, ok := r.trusts.Load(domain); ok {
		return
	}

	meta, err := fetchL8Meta(domain)
	if err != nil {
		return
	}

	challengeID := uuid.New().String()
	nonce := uuid.New().String()
	msg := []byte(challengeID + ":" + nonce)
	sig := ed25519.Sign(r.privateKey, msg)

	challengeURL := domain + meta.ChallengeEndpoint
	if strings.HasPrefix(meta.ChallengeEndpoint, "http") {
		challengeURL = meta.ChallengeEndpoint
	}

	reqBody, _ := json.Marshal(L8ChallengeReq{
		ChallengeID:     challengeID,
		Nonce:           nonce,
		Timestamp:       time.Now().Unix(),
		SenderPublicKey: r.PubB64,
		Signature:       base64.StdEncoding.EncodeToString(sig),
	})

	resp, err := (&http.Client{Timeout: 5 * time.Second}).Post(challengeURL, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		log.Printf("[L8] challenge to %s failed: %v", domain, err)
		return
	}
	defer resp.Body.Close()

	var cr L8ChallengeResp
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

	r.trusts.Store(domain, ed25519.PublicKey(receiverPub))
	r.saveTrustToDisk(domain, ed25519.PublicKey(receiverPub), meta)
	log.Printf("[L8] trust established with %s", domain)
}

// IsTrusted returns true if the L8 handshake has been completed for this webhook domain.
func (r *L8Registry) IsTrusted(webhookURL string) bool {
	domain := extractDomain(webhookURL)
	if domain == "" {
		return false
	}
	_, ok := r.trusts.Load(domain)
	return ok
}

// SignHeaders returns X-L8-* headers to attach to an outgoing webhook delivery.
func (r *L8Registry) SignHeaders(body []byte) map[string]string {
	deliveryID := uuid.New().String()
	ts := time.Now().Unix()
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

func (r *L8Registry) saveTrustToDisk(domain string, pub ed25519.PublicKey, meta *L8Meta) {
	tf := l8TrustFile{
		Domain:          domain,
		PublicKey:       base64.StdEncoding.EncodeToString(pub),
		ValidatedAt:     time.Now().Unix(),
		ProtocolVersion: meta.ProtocolVersion,
		Capabilities:    meta.Capabilities,
	}
	data, _ := json.MarshalIndent(tf, "", "  ")
	filename := filepath.Join(r.trustDir, sanitizeDomain(domain)+".json")
	if err := os.WriteFile(filename, data, 0600); err != nil {
		log.Printf("[L8] could not save trust for %s: %v", domain, err)
	}
}

func (r *L8Registry) loadTrustsFromDisk() {
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
		var tf l8TrustFile
		if err := json.Unmarshal(data, &tf); err != nil || tf.Domain == "" || tf.PublicKey == "" {
			continue
		}
		pub, err := base64.StdEncoding.DecodeString(tf.PublicKey)
		if err != nil || len(pub) != ed25519.PublicKeySize {
			continue
		}
		r.trusts.Store(tf.Domain, ed25519.PublicKey(pub))
		log.Printf("[L8] loaded trust for %s", tf.Domain)
	}
}

// ---- helpers ----

func fetchL8Meta(baseURL string) (*L8Meta, error) {
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Get(baseURL + "/.well-known/l8")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("/.well-known/l8 returned %d", resp.StatusCode)
	}
	var meta L8Meta
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
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

func (r *L8Registry) sweepNonces() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		r.nonces.Range(func(k, v any) bool {
			if expiry, ok := v.(time.Time); ok && now.After(expiry) {
				r.nonces.Delete(k)
			}
			return true
		})
	}
}

// l8SpecDocument is served at GET /l8-spec so agents and developers can discover the protocol.
const l8SpecDocument = `# L8 Protocol — v0.1

L8 is a lightweight challenge-response handshake for trustless webhook delivery.
No shared secrets. No central authority. Ownership is proven once via Ed25519 signatures.
All future deliveries carry signed headers the receiver verifies locally.

## Receiver endpoints (implement these to support L8)

### GET /.well-known/l8

Returns your service identity and public key.

` + "```" + `json
{
  "protocol_version":   "0.1",
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
  "protocol_version": "0.1",
  "capabilities":     ["signed_payloads"]
}
` + "```" + `

To revoke trust with a domain: delete its file. The handshake will re-run on next delivery.

## Graceful degradation

L8 is opt-in. If ` + "`/.well-known/l8`" + ` returns 404, delivery proceeds without signed headers.
Receivers that don't implement L8 are unaffected.
`
