package crypto

import (
	"bytes"
	"encoding/base64"
	"testing"

	"crep/internal/domain"
)

func key(b byte) string { return base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{b}, 32)) }

func TestRotationAndCodec(t *testing.T) {
	old, _ := ParseKeyring("k1:" + key(1))
	sealed, err := old.Seal([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := ParseKeyring("k2:" + key(2) + ",k1:" + key(1))
	if err != nil {
		t.Fatal(err)
	}
	if pt, err := rotated.Open(sealed); err != nil || string(pt) != "hello" {
		t.Fatalf("open with rotated ring: %q %v", pt, err)
	}
	sealed[len(sealed)-1] ^= 1
	if _, err := rotated.Open(sealed); err == nil {
		t.Fatal("tampering must fail")
	}

	c := RequestCodec{K: rotated}
	hs := []domain.Header{{Name: "Authorization", Value: "Bearer s3cret", Secret: true}, {Name: "X-Plain", Value: "v"}}
	blob, err := c.Encode(hs, []byte(`{"pw":"x"}`), true)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(blob, []byte("s3cret")) || bytes.Contains(blob, []byte("pw")) || bytes.Contains(blob, []byte("X-Plain")) {
		t.Fatal("request stored in plaintext")
	}
	dh, db, bs, err := c.Decode(blob)
	if err != nil || len(dh) != 2 || dh[0].Value != "Bearer s3cret" || !dh[0].Secret || dh[1].Value != "v" || dh[1].Secret ||
		string(db) != `{"pw":"x"}` || !bs {
		t.Fatalf("roundtrip: %v %+v %q %v", err, dh, db, bs)
	}
	if empty, _ := c.Encode(nil, nil, false); empty != nil {
		t.Fatal("empty request should store NULL")
	}
}

func TestParseKeyringErrors(t *testing.T) {
	for _, s := range []string{"", "nocolon", "k:notbase64!", "k:" + base64.StdEncoding.EncodeToString([]byte("short"))} {
		if _, err := ParseKeyring(s); err == nil {
			t.Errorf("%q: expected error", s)
		}
	}
}
