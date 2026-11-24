package domain

import (
	"fmt"
	"time"
)

// JobStatus is the lifecycle status of a job.
type JobStatus string

const (
	JobPending   JobStatus = "pending"
	JobRunning   JobStatus = "running"
	JobSucceeded JobStatus = "succeeded"
	JobDead      JobStatus = "dead" // attempts exhausted (or request unreadable); the DLQ
	JobCancelled JobStatus = "cancelled"
)

// Terminal reports whether no further attempts will be made automatically.
func (s JobStatus) Terminal() bool {
	return s == JobSucceeded || s == JobDead || s == JobCancelled
}

// Job is one planned execution of a task. Its request is a snapshot taken at creation,
// so later task edits do not alter it.
type Job struct {
	ID             string
	TaskID         string
	TenantID       string
	ScheduledFor   time.Time
	Priority       Priority
	Status         JobStatus
	Attempt        int // number of attempts started; also the fencing token
	MaxAttempts    int
	AvailableAt    time.Time // earliest time the next attempt may start
	WorkerID       string
	LeaseExpiresAt *time.Time
	Request        RequestSpec
	Retry          RetryPolicy
	HTTPStatus     int
	Error          string
	CreatedAt      time.Time
	FinishedAt     *time.Time
}

// Attempt records one execution attempt of a job.
type Attempt struct {
	JobID      string
	Attempt    int
	StartedAt  time.Time
	FinishedAt time.Time
	HTTPStatus int
	Error      string
	Duration   time.Duration
}

// NewJob plans a job for task t firing at scheduledFor.
func NewJob(id string, t Task, scheduledFor, now time.Time) Job {
	sf := scheduledFor.UTC().Truncate(time.Millisecond)
	return Job{
		ID:           id,
		TaskID:       t.ID,
		TenantID:     t.TenantID,
		ScheduledFor: sf,
		Priority:     t.Priority,
		Status:       JobPending,
		MaxAttempts:  t.Retry.MaxAttempts,
		AvailableAt:  sf,
		Request:      t.Request,
		Retry:        t.Retry,
		CreatedAt:    now,
	}
}

// StartAt is when the claimed attempt should actually fire.
func (j Job) StartAt() time.Time {
	if j.AvailableAt.After(j.ScheduledFor) {
		return j.AvailableAt
	}
	return j.ScheduledFor
}

// Fence identifies the claim that is allowed to write a job's result.
type Fence struct {
	Attempt int
	Owner   string
}

// Claim transitions a pending job to running under owner's lease and returns the
// fence for the new attempt. The lease covers waiting for the start time, the
// request timeout, and grace.
func (j *Job) Claim(owner string, now time.Time, grace time.Duration) (Fence, error) {
	if j.Status != JobPending {
		return Fence{}, ErrConflict
	}
	start := j.StartAt()
	if now.After(start) {
		start = now
	}
	exp := start.Add(j.Request.Timeout + grace)
	j.Status = JobRunning
	j.Attempt++
	j.WorkerID = owner
	j.LeaseExpiresAt = &exp
	return Fence{Attempt: j.Attempt, Owner: owner}, nil
}

// Release undoes a claim whose attempt never started (e.g. executor shutdown while
// waiting for the start time). The attempt counter is rolled back; fencing stays
// safe because lease owners are unique per executor process.
func (j *Job) Release() {
	j.Status = JobPending
	j.Attempt--
	j.WorkerID = ""
	j.LeaseExpiresAt = nil
}

// Fence returns the fence of the current running attempt.
func (j Job) Fence() Fence { return Fence{Attempt: j.Attempt, Owner: j.WorkerID} }

// RecordOutcome applies the result of the current attempt.
func (j *Job) RecordOutcome(o Outcome, statusCode int, errMsg string, now time.Time, rnd func() float64) {
	j.HTTPStatus = statusCode
	j.Error = errMsg
	j.WorkerID = ""
	j.LeaseExpiresAt = nil
	switch {
	case o == OutcomeSuccess:
		j.finish(JobSucceeded, now)
		j.Error = ""
	case j.Attempt < j.MaxAttempts:
		j.Status = JobPending
		j.AvailableAt = now.Add(j.Retry.Backoff(j.Attempt, rnd))
	default:
		j.finish(JobDead, now)
	}
}

// LeaseExpired handles an executor that disappeared mid-attempt. The attempt counts.
func (j *Job) LeaseExpired(now time.Time) {
	msg := fmt.Sprintf("lease held by %s expired", j.WorkerID)
	j.WorkerID = ""
	j.LeaseExpiresAt = nil
	j.Error = msg
	if j.Attempt < j.MaxAttempts {
		j.Status = JobPending
		j.AvailableAt = now
		return
	}
	j.finish(JobDead, now)
}

// DeadLetter sends a running job straight to the DLQ without retrying, for failures
// no retry can fix (its stored request can't be decrypted).
func (j *Job) DeadLetter(errMsg string, now time.Time) {
	j.HTTPStatus = 0
	j.Error = errMsg
	j.WorkerID = ""
	j.LeaseExpiresAt = nil
	j.finish(JobDead, now)
}

// ManualRetry moves a dead job back to pending with exactly one more attempt. The
// attempt counter is not reset so it remains a monotonic fencing token.
func (j *Job) ManualRetry(now time.Time) error {
	if j.Status != JobDead {
		return ErrConflict
	}
	j.Status = JobPending
	j.MaxAttempts = j.Attempt + 1
	j.AvailableAt = now
	j.FinishedAt = nil
	return nil
}

// Cancel stops a job that has not started.
func (j *Job) Cancel(now time.Time, reason string) error {
	if j.Status != JobPending && j.Status != JobDead {
		return ErrConflict
	}
	j.Error = reason
	j.finish(JobCancelled, now)
	return nil
}

func (j *Job) finish(s JobStatus, now time.Time) {
	j.Status = s
	t := now
	j.FinishedAt = &t
}
