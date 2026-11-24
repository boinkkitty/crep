package httpcaller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"syscall"
	"time"
	"unicode/utf8"

	"crep/internal/app/ports"
	"crep/internal/domain"
)

func invalidURL(msg string) error {
	return &domain.ValidationError{Field: "request.url", Message: msg}
}

// Caller implements ports.HTTPCaller.
type Caller struct {
	client       *http.Client
	maxBodyBytes int64
	userAgent    string
}

// New builds a caller whose dialer enforces policy on every resolved address,
// including those reached via redirects. Environment proxies are ignored so they
// cannot be used to bypass the policy.
func New(policy Policy) *Caller {
	dialer := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
		Control: func(_, address string, _ syscall.RawConn) error {
			return policy.CheckAddr(address)
		},
	}
	tr := &http.Transport{
		Proxy:                 nil,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	client := &http.Client{
		Transport: tr,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return errors.New("too many redirects")
			}
			if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
				return fmt.Errorf("%w: redirect to %s scheme", ErrBlockedDestination, req.URL.Scheme)
			}
			return nil
		},
	}
	return &Caller{client: client, maxBodyBytes: 1 << 20, userAgent: "crep/1"}
}

var _ ports.HTTPCaller = (*Caller)(nil)

func (c *Caller) Call(ctx context.Context, spec domain.RequestSpec, meta ports.CallMeta) ports.CallResult {
	req, err := http.NewRequestWithContext(ctx, spec.Method, spec.URL, bytes.NewReader(spec.Body))
	if err != nil {
		return ports.CallResult{Err: errors.New("invalid request")}
	}
	for _, h := range spec.Headers {
		req.Header.Add(h.Name, h.Value)
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", c.userAgent)
	}
	// Same key on every attempt so receivers can deduplicate (at-least-once delivery).
	req.Header.Set("Idempotency-Key", meta.JobID)
	req.Header.Set("X-Job-Id", meta.JobID)
	req.Header.Set("X-Task-Id", meta.TaskID)
	req.Header.Set("X-Attempt", strconv.Itoa(meta.Attempt))
	req.Header.Set("X-Scheduled-For", meta.ScheduledFor.UTC().Format(time.RFC3339Nano))

	resp, err := c.client.Do(req)
	if err != nil {
		return ports.CallResult{Err: withoutURL(err)}
	}
	defer resp.Body.Close()
	limited := io.LimitReader(resp.Body, c.maxBodyBytes)
	snip := make([]byte, 256)
	n, _ := io.ReadFull(limited, snip)
	_, _ = io.Copy(io.Discard, limited)
	s := snip[:n]
	for len(s) > 0 && !utf8.Valid(s) {
		s = s[:len(s)-1]
	}
	return ports.CallResult{StatusCode: resp.StatusCode, BodySnip: string(s)}
}

// withoutURL drops the URL that net/http puts in transport errors ("Post \"https://..?token=x\": ..."),
// so a query-string secret never ends up in jobs.error or archived history.
func withoutURL(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return fmt.Errorf("%s: %w", ue.Op, ue.Err)
	}
	return err
}
