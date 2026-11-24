package domain

import "time"

// RetryPolicy controls how failed attempts are retried.
type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

// DefaultRetryPolicy is applied when a task does not specify one.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 5, BaseDelay: time.Second, MaxDelay: 5 * time.Minute}
}

// Normalize fills zero fields with defaults.
func (p *RetryPolicy) Normalize() {
	d := DefaultRetryPolicy()
	if p.MaxAttempts == 0 {
		p.MaxAttempts = d.MaxAttempts
	}
	if p.BaseDelay == 0 {
		p.BaseDelay = d.BaseDelay
	}
	if p.MaxDelay == 0 {
		p.MaxDelay = d.MaxDelay
	}
}

func (p RetryPolicy) Validate() error {
	if p.MaxAttempts < 1 || p.MaxAttempts > 50 {
		return invalid("retry.max_attempts", "must be between 1 and 50")
	}
	if p.BaseDelay < 100*time.Millisecond || p.BaseDelay > time.Hour {
		return invalid("retry.base_delay_ms", "must be between 100ms and 1h")
	}
	if p.MaxDelay < p.BaseDelay || p.MaxDelay > 24*time.Hour {
		return invalid("retry.max_delay_ms", "must be between base_delay and 24h")
	}
	return nil
}

// Backoff returns the delay before retrying after the given (1-based) failed attempt,
// using exponential growth capped at MaxDelay with "equal jitter": the delay is
// uniformly distributed in [d/2, d]. rnd must return a value in [0, 1).
func (p RetryPolicy) Backoff(attempt int, rnd func() float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := p.MaxDelay
	if shift := attempt - 1; shift < 40 {
		if exp := p.BaseDelay << shift; exp > 0 && exp < p.MaxDelay {
			d = exp
		}
	}
	half := d / 2
	return half + time.Duration(rnd()*float64(d-half))
}

// Outcome classifies a single attempt.
type Outcome int

const (
	OutcomeSuccess Outcome = iota // 2xx
	OutcomeFailure                // anything else: non-2xx, timeout, transport error; retried
)

// Classify maps an HTTP status (0 when the request failed in transport) to an Outcome.
// Only 2xx succeeds; every other result is retried until attempts run out.
func Classify(statusCode int, transportErr error) Outcome {
	if transportErr == nil && statusCode >= 200 && statusCode < 300 {
		return OutcomeSuccess
	}
	return OutcomeFailure
}
