// Package crypto encrypts secret request fields at rest with AES-256-GCM.
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"crep/internal/domain"
)

// Keyring holds named AES-256 keys. New data is sealed with the primary key; any
// known key can open, which allows rotation.
type Keyring struct {
	primary string
	aeads   map[string]cipher.AEAD
}

// ParseKeyring parses "kid1:base64key,kid2:base64key". The first entry is primary.
func ParseKeyring(spec string) (*Keyring, error) {
	k := &Keyring{aeads: map[string]cipher.AEAD{}}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kid, b64, ok := strings.Cut(part, ":")
		if !ok || kid == "" || len(kid) > 255 {
			return nil, fmt.Errorf("crypto: key entry must be kid:base64key")
		}
		raw, err := base64.StdEncoding.DecodeString(b64)
		if err != nil || len(raw) != 32 {
			return nil, fmt.Errorf("crypto: key %q must be 32 bytes, base64-encoded", kid)
		}
		block, _ := aes.NewCipher(raw)
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, err
		}
		k.aeads[kid] = aead
		if k.primary == "" {
			k.primary = kid
		}
	}
	if k.primary == "" {
		return nil, errors.New("crypto: no keys configured")
	}
	return k, nil
}

// Seal encrypts plaintext. Format: len(kid) | kid | nonce | ciphertext+tag.
func (k *Keyring) Seal(plaintext []byte) ([]byte, error) {
	aead := k.aeads[k.primary]
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	out := make([]byte, 0, 1+len(k.primary)+len(nonce)+len(plaintext)+aead.Overhead())
	out = append(out, byte(len(k.primary)))
	out = append(out, k.primary...)
	out = append(out, nonce...)
	return aead.Seal(out, nonce, plaintext, nil), nil
}

func (k *Keyring) Open(sealed []byte) ([]byte, error) {
	if len(sealed) < 1 {
		return nil, errors.New("crypto: empty ciphertext")
	}
	n := int(sealed[0])
	if len(sealed) < 1+n {
		return nil, errors.New("crypto: truncated ciphertext")
	}
	kid, rest := string(sealed[1:1+n]), sealed[1+n:]
	aead, ok := k.aeads[kid]
	if !ok {
		return nil, fmt.Errorf("crypto: unknown key id %q", kid)
	}
	if len(rest) < aead.NonceSize() {
		return nil, errors.New("crypto: truncated ciphertext")
	}
	return aead.Open(nil, rest[:aead.NonceSize()], rest[aead.NonceSize():], nil)
}

// RequestCodec stores a request's headers and body as one AES-GCM blob (the
// `secret` column). Per-header and body secret flags are kept inside the blob so
// API responses can still redact only what the user marked secret.
type RequestCodec struct{ K *Keyring }

type storedHeader struct {
	N string `json:"n"`
	V string `json:"v"`
	S bool   `json:"s,omitempty"`
}

type storedRequest struct {
	H  []storedHeader `json:"h,omitempty"`
	B  []byte         `json:"b,omitempty"`
	BS bool           `json:"bs,omitempty"`
}

// Encode seals headers and body. It returns nil (store NULL) when both are empty.
func (c RequestCodec) Encode(hs []domain.Header, body []byte, bodySecret bool) ([]byte, error) {
	if len(hs) == 0 && len(body) == 0 && !bodySecret {
		return nil, nil
	}
	in := storedRequest{B: body, BS: bodySecret}
	for _, h := range hs {
		in.H = append(in.H, storedHeader{N: h.Name, V: h.Value, S: h.Secret})
	}
	pt, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	return c.K.Seal(pt)
}

func (c RequestCodec) Decode(sealed []byte) (hs []domain.Header, body []byte, bodySecret bool, err error) {
	if len(sealed) == 0 {
		return nil, nil, false, nil
	}
	pt, err := c.K.Open(sealed)
	if err != nil {
		return nil, nil, false, err
	}
	var in storedRequest
	if err := json.Unmarshal(pt, &in); err != nil {
		return nil, nil, false, err
	}
	for _, h := range in.H {
		hs = append(hs, domain.Header{Name: h.N, Value: h.V, Secret: h.S})
	}
	return hs, in.B, in.BS, nil
}
