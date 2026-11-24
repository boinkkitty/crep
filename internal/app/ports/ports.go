// Package ports defines the interfaces between application services and adapters.
package ports

import (
	"context"
	"time"

	"crep/internal/domain"
)

// TxManager runs fn in a database transaction. Repositories called with the
// context passed to fn participate in that transaction.
type TxManager interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

// Page is a keyset-paginated result.
type Page[T any] struct {
	Items      []T
	NextCursor string
}

// TaskRepo persists tasks.
type TaskRepo interface {
	// Create inserts a task. It returns domain.ErrDuplicate if the tenant already has a
	// task with the same non-empty IdempotencyKey.
	Create(ctx context.Context, t domain.Task) error
	GetByIdempotencyKey(ctx context.Context, tenantID, key string) (domain.Task, error)
	// Get returns a task, including soft-deleted ones.
	Get(ctx context.Context, tenantID, id string) (domain.Task, error)
	// GetForUpdate locks the row for the current transaction.
	GetForUpdate(ctx context.Context, tenantID, id string) (domain.Task, error)
	// List returns non-deleted tasks, newest first.
	List(ctx context.Context, tenantID, cursor string, limit int) (Page[domain.Task], error)
	// Update overwrites the mutable fields of a task and increments its version.
	Update(ctx context.Context, t domain.Task) error
	// LockDue locks active tasks with next_run_at <= before, skipping rows locked by others.
	LockDue(ctx context.Context, before time.Time, limit int) ([]domain.Task, error)
	// SaveSchedule persists status/next_run_at after the scheduler advanced a task.
	SaveSchedule(ctx context.Context, t domain.Task) error
}

// ClaimOrder selects how claim candidates are ordered.
type ClaimOrder int

const (
	ClaimByPriority ClaimOrder = iota // priority, then available_at
	ClaimOldestDue                    // available_at only; starvation guard
)

// JobRepo persists jobs and their attempts. MySQL's jobs table is the work queue.
type JobRepo interface {
	// Insert creates a job; inserted is false if a job for (task, scheduled_for) already exists.
	Insert(ctx context.Context, j domain.Job) (inserted bool, err error)
	Get(ctx context.Context, tenantID, id string) (domain.Job, error)
	GetForUpdate(ctx context.Context, tenantID, id string) (domain.Job, error)
	// LockClaimable locks pending jobs with available_at <= horizon, skipping locked rows.
	LockClaimable(ctx context.Context, horizon time.Time, order ClaimOrder, limit int) ([]domain.Job, error)
	// MarkClaimed persists the running state of jobs claimed in the current transaction.
	MarkClaimed(ctx context.Context, jobs []domain.Job) error
	// RecordResult writes j's new state only if the row still matches fence and is
	// running. It returns domain.ErrStaleLease otherwise.
	RecordResult(ctx context.Context, j domain.Job, fence domain.Fence) error
	// Save overwrites a job's mutable state; caller must hold the row lock.
	Save(ctx context.Context, j domain.Job) error
	InsertAttempt(ctx context.Context, a domain.Attempt) error
	Attempts(ctx context.Context, jobIDs []string) (map[string][]domain.Attempt, error)
	// LockExpiredLeases locks running jobs whose lease expired before now.
	LockExpiredLeases(ctx context.Context, now time.Time, limit int) ([]domain.Job, error)
	// CancelPending cancels a task's pending jobs.
	CancelPending(ctx context.Context, tenantID, taskID string, now time.Time) (int64, error)
	// ListByTask returns jobs with scheduled_for < before, newest first.
	ListByTask(ctx context.Context, tenantID, taskID string, before time.Time, limit int) ([]domain.Job, error)
	// ListDead returns DLQ entries, most recently failed first.
	ListDead(ctx context.Context, tenantID, cursor string, limit int) (Page[domain.Job], error)
	// ListArchivable returns, oldest-finished first, succeeded/cancelled jobs finished
	// before doneBefore and dead jobs finished before deadBefore.
	ListArchivable(ctx context.Context, doneBefore, deadBefore time.Time, limit int) ([]domain.Job, error)
	// DeleteArchived removes jobs (and their attempts) from the hot store, but only rows
	// still terminal at the same attempt as the archived copy, so a dead job retried
	// in the meantime is kept.
	DeleteArchived(ctx context.Context, jobs []domain.Job) error
	CountDead(ctx context.Context) (int64, error)
}

// ArchivedJob is a finished job with its attempts.
type ArchivedJob struct {
	Job      domain.Job
	Attempts []domain.Attempt
}

// HistoryStore holds archived job history (Cassandra). Writes must be idempotent.
type HistoryStore interface {
	Save(ctx context.Context, jobs []ArchivedJob) error
	Get(ctx context.Context, tenantID, jobID string) (ArchivedJob, error)
	// ListByTask returns jobs with scheduled_for < before and >= notBefore, newest first.
	ListByTask(ctx context.Context, tenantID, taskID string, before, notBefore time.Time, limit int) ([]domain.Job, error)
}

// CallResult is the outcome of one outbound HTTP call.
type CallResult struct {
	StatusCode int
	Err        error  // transport error (DNS, connect, timeout, blocked destination)
	BodySnip   string // start of the response body, for diagnostics on failures
}

// HTTPCaller performs a job's request.
type HTTPCaller interface {
	Call(ctx context.Context, req domain.RequestSpec, meta CallMeta) CallResult
}

// CallMeta carries identifiers sent with each attempt.
type CallMeta struct {
	JobID        string
	TaskID       string
	Attempt      int
	ScheduledFor time.Time // the fire time this job exists for (same on every attempt)
}

// DestinationPolicy validates request URLs at task-creation time (SSRF pre-check).
// The HTTP caller enforces the policy again on every connection.
type DestinationPolicy interface {
	CheckURL(rawURL string) error
}

type Clock interface{ Now() time.Time }

type IDGen interface{ NewID() string }

// Metrics records operational signals. Implementations must be safe for concurrent use.
type Metrics interface {
	JobsPlanned(n int)
	JobsClaimed(n int)
	JobFinished(state domain.JobStatus, attempt int)
	StartLag(d time.Duration)
	StaleResult()
	LeasesRecovered(n int)
	JobsArchived(n int)
}
