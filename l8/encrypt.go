// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package l8

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
)

const (
	// EncryptionAlgorithm is the X-L8-Encryption value for encrypted deliveries.
	EncryptionAlgorithm = "x25519-hkdf-sha256-aes256gcm"
	// EncryptionCapability is what a receiver advertises to get encrypted deliveries.
	EncryptionCapability = "encrypted_payloads"
	// EncryptedContentType is the Content-Type of an encrypted delivery; the
	// original type travels in X-L8-Content-Type.
	EncryptedContentType = "application/l8-encrypted"

	encryptionInfo = "l8/0.2 payload"
)

// peer is what a completed handshake leaves behind for a receiver domain.
// encryption is nil unless the receiver advertised encrypted_payloads.
type peer struct {
	signing    ed25519.PublicKey
	encryption *ecdh.PublicKey
}

// parseEncryptionKey returns the receiver's X25519 key only when it both
// advertises the capability and supplies a valid key; anything else means
// deliveries stay plaintext (signed only), exactly as in L8 0.1.
func parseEncryptionKey(capabilities []string, keyB64 string) *ecdh.PublicKey {
	if keyB64 == "" || !slices.Contains(capabilities, EncryptionCapability) {
		return nil
	}
	raw, err := base64.StdEncoding.DecodeString(keyB64)
	if err != nil {
		return nil
	}
	key, err := ecdh.X25519().NewPublicKey(raw)
	if err != nil {
		return nil
	}
	return key
}

// SealDelivery prepares an outgoing webhook body for an L8-trusted receiver:
// encrypt to the receiver's X25519 key when it advertised one, then sign the
// bytes actually sent (encrypt-then-sign, so the receiver verifies before it
// decrypts). Untrusted receivers get body back unchanged with no headers.
func (r *Registry) SealDelivery(rawURL string, body []byte, contentType string) ([]byte, map[string]string, error) {
	if r == nil {
		return body, nil, nil
	}
	domain := extractDomain(rawURL)
	if domain == "" {
		return body, nil, nil
	}
	value, ok := r.trusts.Load(domain)
	if !ok {
		return body, nil, nil
	}
	peer := value.(*peer)

	deliveryID := uuid.New().String()
	ts := time.Now().Unix()
	headers := map[string]string{}
	if peer.encryption != nil {
		sealed, encHeaders, err := sealPayload(peer.encryption, body, deliveryID, ts)
		if err != nil {
			return nil, nil, fmt.Errorf("l8 encrypt for %s: %w", domain, err)
		}
		body = sealed
		for k, v := range encHeaders {
			headers[k] = v
		}
		headers["X-L8-Content-Type"] = contentType
		headers["Content-Type"] = EncryptedContentType
	}
	for k, v := range r.signHeaders(body, deliveryID, ts) {
		headers[k] = v
	}
	return body, headers, nil
}

func sealPayload(receiver *ecdh.PublicKey, plaintext []byte, deliveryID string, ts int64) ([]byte, map[string]string, error) {
	ephemeral, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	key, err := derivePayloadKey(ephemeral, receiver, ephemeral.PublicKey(), receiver)
	if err != nil {
		return nil, nil, err
	}
	gcm, err := newGCM(key)
	if err != nil {
		return nil, nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, nil, err
	}
	ciphertext := gcm.Seal(nil, nonce, plaintext, payloadAAD(deliveryID, ts))
	return ciphertext, map[string]string{
		"X-L8-Encryption":    EncryptionAlgorithm,
		"X-L8-Ephemeral-Key": base64.StdEncoding.EncodeToString(ephemeral.PublicKey().Bytes()),
		"X-L8-Nonce":         base64.StdEncoding.EncodeToString(nonce),
	}, nil
}

// OpenPayload decrypts an encrypted delivery: the receiver side of the
// scheme. Verify X-L8-Signature over the ciphertext first, then pass the
// receiver's X25519 key, the body, and the X-L8-Ephemeral-Key, X-L8-Nonce,
// X-L8-Delivery-Id and X-L8-Timestamp headers.
func OpenPayload(receiver *ecdh.PrivateKey, ciphertext []byte, ephemeralB64, nonceB64, deliveryID string, ts int64) ([]byte, error) {
	ephRaw, err := base64.StdEncoding.DecodeString(ephemeralB64)
	if err != nil {
		return nil, err
	}
	ephemeral, err := ecdh.X25519().NewPublicKey(ephRaw)
	if err != nil {
		return nil, err
	}
	nonce, err := base64.StdEncoding.DecodeString(nonceB64)
	if err != nil {
		return nil, err
	}
	key, err := derivePayloadKey(receiver, ephemeral, ephemeral, receiver.PublicKey())
	if err != nil {
		return nil, err
	}
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	if len(nonce) != gcm.NonceSize() {
		return nil, fmt.Errorf("nonce must be %d bytes", gcm.NonceSize())
	}
	return gcm.Open(nil, nonce, ciphertext, payloadAAD(deliveryID, ts))
}

// Salt binds the derived key to both public keys, so a ciphertext can't be
// replayed against a different receiver key.
func derivePayloadKey(priv *ecdh.PrivateKey, peer *ecdh.PublicKey, ephemeralPub, receiverPub *ecdh.PublicKey) ([]byte, error) {
	shared, err := priv.ECDH(peer)
	if err != nil {
		return nil, err
	}
	salt := append(append([]byte{}, ephemeralPub.Bytes()...), receiverPub.Bytes()...)
	return hkdf.Key(sha256.New, shared, salt, encryptionInfo, 32)
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func payloadAAD(deliveryID string, ts int64) []byte {
	return fmt.Appendf(nil, "%s.%d", deliveryID, ts)
}
