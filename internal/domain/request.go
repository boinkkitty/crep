package domain

import (
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	MaxBodyBytes   = 64 << 10
	MaxHeaders     = 50
	MinTimeout     = time.Second
	MaxTimeout     = 5 * time.Minute
	DefaultTimeout = 30 * time.Second
)

// Header is one outbound request header. Secret values are encrypted at rest and redacted on read.
type Header struct {
	Name   string
	Value  string
	Secret bool
}

// RequestSpec is the HTTP request a job performs.
type RequestSpec struct {
	Method     string
	URL        string
	Headers    []Header
	Body       []byte
	BodySecret bool
	Timeout    time.Duration

	// Unreadable is set by storage when the persisted headers/body could not be
	// decoded (e.g. encrypted with a key that is no longer configured). Such a
	// request must not be executed or re-saved.
	Unreadable string
}

var allowedMethods = map[string]bool{
	http.MethodGet: true, http.MethodPost: true, http.MethodPut: true,
	http.MethodPatch: true, http.MethodDelete: true, http.MethodHead: true,
}

// reservedHeaders are set by the executor and may not be supplied by users.
var reservedHeaders = map[string]bool{
	"idempotency-key": true, "x-attempt": true, "x-job-id": true, "x-task-id": true,
	"x-scheduled-for": true, "host": true, "content-length": true,
}

// Normalize fills defaults.
func (r *RequestSpec) Normalize() {
	r.Method = strings.ToUpper(strings.TrimSpace(r.Method))
	if r.Method == "" {
		r.Method = http.MethodPost
	}
	if r.Timeout == 0 {
		r.Timeout = DefaultTimeout
	}
}

// Validate checks structural validity. Destination (SSRF) policy is enforced separately.
func (r RequestSpec) Validate() error {
	if r.Unreadable != "" {
		return invalid("request", "stored request is unreadable; supply a new request")
	}
	if !allowedMethods[r.Method] {
		return invalid("request.method", "unsupported method %q", r.Method)
	}
	u, err := url.Parse(r.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return invalid("request.url", "must be an absolute http or https URL")
	}
	if u.User != nil {
		return invalid("request.url", "must not contain credentials")
	}
	if len(r.URL) > 2048 {
		return invalid("request.url", "too long")
	}
	if len(r.Headers) > MaxHeaders {
		return invalid("request.headers", "at most %d headers", MaxHeaders)
	}
	for _, h := range r.Headers {
		name := strings.TrimSpace(h.Name)
		if name == "" || strings.ContainsAny(name, " :\r\n") {
			return invalid("request.headers", "invalid header name %q", h.Name)
		}
		if strings.ContainsAny(h.Value, "\r\n") {
			return invalid("request.headers", "invalid value for header %q", h.Name)
		}
		if reservedHeaders[strings.ToLower(name)] {
			return invalid("request.headers", "header %q is reserved", h.Name)
		}
	}
	if len(r.Body) > MaxBodyBytes {
		return invalid("request.body", "at most %d bytes", MaxBodyBytes)
	}
	if r.Timeout < MinTimeout || r.Timeout > MaxTimeout {
		return invalid("request.timeout_ms", "must be between %s and %s", MinTimeout, MaxTimeout)
	}
	return nil
}

// Redacted returns a copy with secret values hidden, safe for API responses.
func (r RequestSpec) Redacted() RequestSpec {
	out := r
	out.Headers = make([]Header, len(r.Headers))
	for i, h := range r.Headers {
		if h.Secret {
			h.Value = "***"
		}
		out.Headers[i] = h
	}
	if r.BodySecret {
		out.Body = []byte("***")
	}
	return out
}
