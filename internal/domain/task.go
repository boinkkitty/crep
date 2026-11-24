package domain

import (
	"strings"
	"time"
)

// TaskStatus is the lifecycle status of a task.
type TaskStatus string

const (
	TaskActive    TaskStatus = "active"
	TaskPaused    TaskStatus = "paused"
	TaskCompleted TaskStatus = "completed" // one-shot task whose job has been planned
	TaskDeleted   TaskStatus = "deleted"
)

// Task is a user's configuration: endpoint, schedule, priority, request, timeout, retry policy.
type Task struct {
	ID        string
	TenantID  string
	Name      string
	Schedule  Schedule
	Priority  Priority
	Request   RequestSpec
	Retry     RetryPolicy
	Status    TaskStatus
	NextRunAt *time.Time // next fire time the scheduler will plan; nil when none
	// IdempotencyKey is the client's Idempotency-Key from task creation; empty when none.
	IdempotencyKey string
	Version        int64
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// Normalize fills defaults on user input.
func (t *Task) Normalize() {
	t.Name = strings.TrimSpace(t.Name)
	t.Request.Normalize()
	t.Retry.Normalize()
	if t.Schedule.Type == ScheduleOnce {
		t.Schedule.RunAt = t.Schedule.RunAt.UTC().Truncate(time.Millisecond)
	}
}

func (t Task) Validate() error {
	if t.TenantID == "" {
		return invalid("tenant_id", "required")
	}
	if t.Name == "" || len(t.Name) > 255 {
		return invalid("name", "required, at most 255 characters")
	}
	if !t.Priority.Valid() {
		return invalid("priority", "invalid")
	}
	if err := t.Schedule.Validate(); err != nil {
		return err
	}
	if err := t.Request.Validate(); err != nil {
		return err
	}
	return t.Retry.Validate()
}

// Plan sets Status and NextRunAt for a freshly created, resumed, or rescheduled task.
// For immediate tasks it returns the fire time of the job the caller must create now.
func (t *Task) Plan(now time.Time) (immediateFire *time.Time, err error) {
	switch t.Schedule.Type {
	case ScheduleImmediate:
		t.NextRunAt = nil
		t.Status = TaskCompleted
		f := now.UTC().Truncate(time.Millisecond)
		return &f, nil
	case ScheduleOnce:
		r := t.Schedule.RunAt
		t.NextRunAt = &r
		t.Status = TaskActive
	case ScheduleCron:
		n, ok := t.Schedule.NextCron(now)
		if !ok {
			return nil, invalid("schedule.cron", "has no future fire time")
		}
		t.NextRunAt = &n
		t.Status = TaskActive
	}
	return nil, nil
}

// Advance moves a task past the fire time the scheduler just planned.
//
// Missed-fire policy: no run is dropped. The next fire is computed from the previous
// fire, not from now, so after scheduler downtime every missed fire is planned (the
// scheduler caps how many per task per tick; see MaxCatchUpPerTick). Fires missed
// while a task is paused are the exception: Resume replans from now on purpose.
func (t *Task) Advance(fired time.Time) {
	switch t.Schedule.Type {
	case ScheduleCron:
		if n, ok := t.Schedule.NextCron(fired); ok {
			t.NextRunAt = &n
			return
		}
		fallthrough
	default:
		t.NextRunAt = nil
		t.Status = TaskCompleted
	}
}

// Pause stops future planning. Jobs already planned (at most the look-ahead window) still run.
func (t *Task) Pause() error {
	if t.Status != TaskActive {
		return ErrConflict
	}
	t.Status = TaskPaused
	return nil
}

// Resume re-enables planning. Cron fires missed while paused are skipped; a once task
// whose time passed while paused runs promptly.
func (t *Task) Resume(now time.Time) (immediateFire *time.Time, err error) {
	if t.Status != TaskPaused {
		return nil, ErrConflict
	}
	return t.Plan(now)
}

// Delete soft-deletes the task.
func (t *Task) Delete() error {
	if t.Status == TaskDeleted {
		return ErrNotFound
	}
	t.Status = TaskDeleted
	t.NextRunAt = nil
	return nil
}
