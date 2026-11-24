package domain

import (
	"errors"
	"testing"
	"time"
)

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestNextCronTimezoneAndDST(t *testing.T) {
	s := Schedule{Type: ScheduleCron, Cron: "30 2 * * *", Timezone: "America/New_York"}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	// 2026-03-08 is US spring-forward: 02:30 local does not exist that day, so that
	// fire is skipped (robfig/cron semantics) and the next one is 03-09 02:30 EDT.
	got, ok := s.NextCron(mustTime(t, "2026-03-07T12:00:00Z"))
	if !ok {
		t.Fatal("no next")
	}
	if want := mustTime(t, "2026-03-09T06:30:00Z"); !got.Equal(want) {
		t.Fatalf("DST gap: got %v want %v", got, want)
	}
	// Normal day: 02:30 EST = 07:30Z.
	got, _ = s.NextCron(mustTime(t, "2026-01-10T00:00:00Z"))
	if want := mustTime(t, "2026-01-10T07:30:00Z"); !got.Equal(want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestScheduleValidation(t *testing.T) {
	cases := []Schedule{
		{Type: "nope"},
		{Type: ScheduleOnce},
		{Type: ScheduleCron, Cron: "not a cron"},
		{Type: ScheduleCron, Cron: "* * * * *", Timezone: "Mars/Olympus"},
	}
	for _, c := range cases {
		if err := c.Validate(); !IsValidation(err) {
			t.Errorf("%+v: expected validation error, got %v", c, err)
		}
	}
}

func TestBackoffBounds(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 10, BaseDelay: time.Second, MaxDelay: 30 * time.Second}
	for attempt, full := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 6: 30 * time.Second, 100: 30 * time.Second} {
		lo, hi := p.Backoff(attempt, func() float64 { return 0 }), p.Backoff(attempt, func() float64 { return 0.999999 })
		if lo != full/2 || hi > full || hi < full*99/100 {
			t.Errorf("attempt %d: lo=%v hi=%v full=%v", attempt, lo, hi, full)
		}
	}
}

func TestClassify(t *testing.T) {
	cases := map[int]Outcome{200: OutcomeSuccess, 204: OutcomeSuccess, 301: OutcomeFailure, 400: OutcomeFailure, 404: OutcomeFailure,
		408: OutcomeFailure, 429: OutcomeFailure, 500: OutcomeFailure, 503: OutcomeFailure, 0: OutcomeFailure}
	for code, want := range cases {
		if got := Classify(code, nil); got != want {
			t.Errorf("%d: got %v want %v", code, got, want)
		}
	}
	if Classify(200, errors.New("x")) != OutcomeFailure {
		t.Error("transport error must be retryable")
	}
}

func newTestTask(kind ScheduleType) Task {
	t := Task{ID: "t1", TenantID: "ten", Name: "n", Schedule: Schedule{Type: kind, Cron: "*/5 * * * *"},
		Request: RequestSpec{URL: "https://example.com/hook"}}
	if kind == ScheduleOnce {
		t.Schedule.RunAt = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	t.Normalize()
	return t
}

func TestTaskPlanAndAdvanceReplaysMissedFires(t *testing.T) {
	task := newTestTask(ScheduleCron)
	if err := task.Validate(); err != nil {
		t.Fatal(err)
	}
	now := mustTime(t, "2026-01-01T00:01:00Z")
	if _, err := task.Plan(now); err != nil {
		t.Fatal(err)
	}
	if !task.NextRunAt.Equal(mustTime(t, "2026-01-01T00:05:00Z")) {
		t.Fatalf("next=%v", task.NextRunAt)
	}
	// Scheduler was down for an hour: every missed fire is still produced, in order.
	task.Advance(*task.NextRunAt)
	if !task.NextRunAt.Equal(mustTime(t, "2026-01-01T00:10:00Z")) {
		t.Fatalf("expected the next missed fire 00:10, got %v", task.NextRunAt)
	}
}

func TestOneShotTasks(t *testing.T) {
	now := time.Now().UTC()
	imm := newTestTask(ScheduleImmediate)
	fire, err := imm.Plan(now)
	if err != nil || fire == nil || imm.Status != TaskCompleted || imm.NextRunAt != nil {
		t.Fatalf("immediate: %v %v %+v", fire, err, imm)
	}
	once := newTestTask(ScheduleOnce)
	if fire, _ := once.Plan(now); fire != nil || once.NextRunAt == nil || once.Status != TaskActive {
		t.Fatalf("once: %+v", once)
	}
	once.Advance(*once.NextRunAt)
	if once.Status != TaskCompleted || once.NextRunAt != nil {
		t.Fatalf("once after advance: %+v", once)
	}
}

func TestPauseResume(t *testing.T) {
	task := newTestTask(ScheduleCron)
	if _, err := task.Plan(time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := task.Resume(time.Now()); !errors.Is(err, ErrConflict) {
		t.Fatal("resume of active should conflict")
	}
	if err := task.Pause(); err != nil || task.Status != TaskPaused {
		t.Fatal(err)
	}
	if _, err := task.Resume(time.Now()); err != nil || task.Status != TaskActive || task.NextRunAt == nil {
		t.Fatal(err)
	}
}

func TestJobLifecycle(t *testing.T) {
	now := mustTime(t, "2026-01-01T00:00:00Z")
	task := newTestTask(ScheduleImmediate)
	task.Retry = RetryPolicy{MaxAttempts: 2, BaseDelay: time.Second, MaxDelay: time.Minute}
	j := NewJob("j1", task, now, now)

	f, err := j.Claim("exec-a", now, 10*time.Second)
	if err != nil || f.Attempt != 1 || j.Status != JobRunning {
		t.Fatalf("claim: %v %+v", err, j)
	}
	if want := now.Add(task.Request.Timeout + 10*time.Second); !j.LeaseExpiresAt.Equal(want) {
		t.Fatalf("lease %v want %v", j.LeaseExpiresAt, want)
	}
	if _, err := j.Claim("exec-b", now, 0); !errors.Is(err, ErrConflict) {
		t.Fatal("double claim must conflict")
	}

	j.RecordOutcome(OutcomeFailure, 503, "boom", now, func() float64 { return 0 })
	if j.Status != JobPending || !j.AvailableAt.Equal(now.Add(500*time.Millisecond)) {
		t.Fatalf("retry: %+v", j)
	}
	if _, err := j.Claim("exec-a", now, 0); err != nil {
		t.Fatal(err)
	}
	j.RecordOutcome(OutcomeFailure, 503, "boom", now, func() float64 { return 0 })
	if j.Status != JobDead || j.FinishedAt == nil {
		t.Fatalf("expected dead: %+v", j)
	}
	if err := j.ManualRetry(now); err != nil || j.Status != JobPending || j.MaxAttempts != 3 || j.Attempt != 2 {
		t.Fatalf("manual retry: %v %+v", err, j)
	}
	if _, err := j.Claim("exec-a", now, 0); err != nil {
		t.Fatal(err)
	}
	j.RecordOutcome(OutcomeSuccess, 200, "", now, nil)
	if j.Status != JobSucceeded || j.Attempt != 3 {
		t.Fatalf("success: %+v", j)
	}
}

func TestClientErrorIsRetried(t *testing.T) {
	now := time.Now()
	j := NewJob("j", newTestTask(ScheduleImmediate), now, now)
	if _, err := j.Claim("e", now, 0); err != nil {
		t.Fatal(err)
	}
	j.RecordOutcome(Classify(404, nil), 404, "not found", now, func() float64 { return 0 })
	if j.Status != JobPending {
		t.Fatalf("404 must be retried, got %s", j.Status)
	}
}

func TestManualRetryGrantsExactlyOneAttempt(t *testing.T) {
	now := time.Now()
	task := newTestTask(ScheduleImmediate)
	task.Retry.MaxAttempts = 3
	j := NewJob("j", task, now, now)
	for i := 0; i < 3; i++ {
		if _, err := j.Claim("e", now, 0); err != nil {
			t.Fatal(err)
		}
		j.RecordOutcome(OutcomeFailure, 500, "boom", now, func() float64 { return 0 })
	}
	if j.Status != JobDead {
		t.Fatalf("want dead, got %s", j.Status)
	}
	for want := 4; want <= 5; want++ {
		if err := j.ManualRetry(now); err != nil {
			t.Fatal(err)
		}
		if _, err := j.Claim("e", now, 0); err != nil {
			t.Fatal(err)
		}
		j.RecordOutcome(OutcomeFailure, 500, "boom", now, func() float64 { return 0 })
		if j.Status != JobDead || j.Attempt != want {
			t.Fatalf("retry should allow one attempt then DLQ: %s attempt=%d", j.Status, j.Attempt)
		}
	}
}

func TestLeaseExpired(t *testing.T) {
	now := time.Now()
	task := newTestTask(ScheduleImmediate)
	task.Retry.MaxAttempts = 1
	j := NewJob("j", task, now, now)
	if _, err := j.Claim("e", now, 0); err != nil {
		t.Fatal(err)
	}
	j.LeaseExpired(now)
	if j.Status != JobDead {
		t.Fatalf("got %s", j.Status)
	}
}

func TestRequestValidation(t *testing.T) {
	r := RequestSpec{URL: "https://user:pw@example.com"}
	r.Normalize()
	if !IsValidation(r.Validate()) {
		t.Error("credentials in URL must be rejected")
	}
	r = RequestSpec{URL: "ftp://example.com"}
	r.Normalize()
	if !IsValidation(r.Validate()) {
		t.Error("ftp must be rejected")
	}
	r = RequestSpec{URL: "https://example.com", Headers: []Header{{Name: "Idempotency-Key", Value: "x"}}}
	r.Normalize()
	if !IsValidation(r.Validate()) {
		t.Error("reserved header must be rejected")
	}
	r = RequestSpec{URL: "https://example.com", Headers: []Header{{Name: "Authorization", Value: "s", Secret: true}}, Body: []byte("b"), BodySecret: true}
	red := r.Redacted()
	if red.Headers[0].Value != "***" || string(red.Body) != "***" || r.Headers[0].Value != "s" {
		t.Error("redaction")
	}
}
