package httpcaller

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"crep/internal/app/ports"
	"crep/internal/domain"
)

func TestPolicyBlocksNonPublic(t *testing.T) {
	p := Policy{}
	for _, addr := range []string{"127.0.0.1:80", "10.1.2.3:443", "169.254.169.254:80", "192.168.1.1:443",
		"[::1]:443", "[fd00::1]:443", "[::ffff:127.0.0.1]:80", "100.64.0.1:80", "8.8.8.8:22"} {
		if err := p.CheckAddr(addr); !errors.Is(err, ErrBlockedDestination) {
			t.Errorf("%s: expected block, got %v", addr, err)
		}
	}
	if err := p.CheckAddr("8.8.8.8:443"); err != nil {
		t.Errorf("public 443 should pass: %v", err)
	}
	dev := Policy{AllowPrivate: true, DenyCIDRs: []netip.Prefix{netip.MustParsePrefix("169.254.0.0/16")}}
	if err := dev.CheckAddr("127.0.0.1:443"); err != nil {
		t.Errorf("AllowPrivate: %v", err)
	}
	if err := dev.CheckAddr("169.254.169.254:80"); err == nil {
		t.Error("explicit deny must still apply")
	}
}

func TestCheckURL(t *testing.T) {
	p := Policy{}
	bad := []string{"http://localhost/x", "http://127.0.0.1/", "https://[::1]/", "https://example.com:8443/",
		"http://metadata.google.internal/", "gopher://example.com/"}
	for _, u := range bad {
		if !domain.IsValidation(p.CheckURL(u)) {
			t.Errorf("%s: expected rejection", u)
		}
	}
	if err := p.CheckURL("https://example.com/hook"); err != nil {
		t.Error(err)
	}
}

func TestCallBlocksLoopbackAtDialTime(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	port, _ := strconv.Atoi(u.Port())
	c := New(Policy{AllowedPorts: map[int]bool{port: true}})
	res := c.Call(context.Background(), domain.RequestSpec{Method: "GET", URL: srv.URL, Timeout: time.Second}, ports.CallMeta{JobID: "j"})
	if !errors.Is(res.Err, ErrBlockedDestination) {
		t.Fatalf("expected blocked, got %+v", res)
	}
}

func TestCallBlocksRedirectToPrivate(t *testing.T) {
	inner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer inner.Close()
	// The redirecting server is itself private, so allow everything except the inner port.
	outer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, inner.URL, http.StatusFound)
	}))
	defer outer.Close()
	ou, _ := url.Parse(outer.URL)
	op, _ := strconv.Atoi(ou.Port())
	c := New(Policy{AllowPrivate: true, AllowedPorts: map[int]bool{op: true}})
	res := c.Call(context.Background(), domain.RequestSpec{Method: "GET", URL: outer.URL, Timeout: time.Second}, ports.CallMeta{JobID: "j"})
	if !errors.Is(res.Err, ErrBlockedDestination) {
		t.Fatalf("redirect to disallowed port should be blocked, got %+v", res)
	}
}

func TestCallSendsIdempotencyHeaders(t *testing.T) {
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		w.WriteHeader(503)
		_, _ = w.Write([]byte("try later"))
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	port, _ := strconv.Atoi(u.Port())
	c := New(Policy{AllowPrivate: true, AllowedPorts: map[int]bool{port: true}})
	res := c.Call(context.Background(), domain.RequestSpec{Method: "POST", URL: srv.URL, Timeout: time.Second,
		Headers: []domain.Header{{Name: "Authorization", Value: "Bearer x"}}}, ports.CallMeta{JobID: "job-1", TaskID: "t", Attempt: 3})
	if res.StatusCode != 503 || res.BodySnip != "try later" {
		t.Fatalf("%+v", res)
	}
	if got.Get("Idempotency-Key") != "job-1" || got.Get("X-Attempt") != "3" || got.Get("Authorization") != "Bearer x" {
		t.Fatalf("headers: %v", got)
	}
}

func TestCallErrorOmitsURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	port, _ := strconv.Atoi(u.Port())
	c := New(Policy{AllowedPorts: map[int]bool{port: true}})
	res := c.Call(context.Background(), domain.RequestSpec{Method: "GET", URL: srv.URL + "/x?token=s3cret", Timeout: time.Second}, ports.CallMeta{JobID: "j"})
	if res.Err == nil || strings.Contains(res.Err.Error(), "s3cret") || strings.Contains(res.Err.Error(), srv.URL) {
		t.Fatalf("error must not include the URL: %v", res.Err)
	}
	if !errors.Is(res.Err, ErrBlockedDestination) {
		t.Fatalf("unwrapping must keep the cause: %v", res.Err)
	}
}
