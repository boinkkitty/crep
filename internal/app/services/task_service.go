package services

import (
	"context"
	"errors"
	"strings"

	"crep/internal/app/ports"
	"crep/internal/domain"
)

// MaxIdempotencyKeyLen matches tasks.idempotency_key VARCHAR(255).
const MaxIdempotencyKeyLen = 255

// TaskService implements the Task API use cases.
type TaskService struct{ d Deps }

func NewTaskService(d Deps) *TaskService { return &TaskService{d: d} }

// TaskInput carries the user-editable fields of a task.
type TaskInput struct {
	Name     string
	Schedule domain.Schedule
	Priority domain.Priority
	Request  domain.RequestSpec
	Retry    domain.RetryPolicy
}

func (s *TaskService) validate(t *domain.Task) error {
	t.Normalize()
	if err := t.Validate(); err != nil {
		return err
	}
	return s.d.Dest.CheckURL(t.Request.URL)
}

// Create stores a task. An immediate task's first job is written in the same transaction.
func (s *TaskService) Create(ctx context.Context, tenantID string, in TaskInput) (domain.Task, error) {
	t, _, err := s.CreateIdempotent(ctx, tenantID, "", in)
	return t, err
}

// CreateIdempotent is Create with an optional client idempotency key. If the tenant
// already created a task with that key, the stored task is returned with created=false
// and nothing new is written (the new input is not compared with the stored one).
// Keys never expire: they live as long as the task row, including after soft delete.
func (s *TaskService) CreateIdempotent(ctx context.Context, tenantID, key string, in TaskInput) (t domain.Task, created bool, err error) {
	key = strings.TrimSpace(key)
	if len(key) > MaxIdempotencyKeyLen {
		return domain.Task{}, false, &domain.ValidationError{Field: "Idempotency-Key", Message: "at most 255 characters"}
	}
	if key != "" {
		if prev, err := s.d.Tasks.GetByIdempotencyKey(ctx, tenantID, key); err == nil {
			return prev, false, nil
		} else if !errors.Is(err, domain.ErrNotFound) {
			return domain.Task{}, false, err
		}
	}
	now := s.d.Clock.Now()
	t = domain.Task{
		ID: s.d.IDs.NewID(), TenantID: tenantID, Name: in.Name, Schedule: in.Schedule,
		Priority: in.Priority, Request: in.Request, Retry: in.Retry, IdempotencyKey: key,
		Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := s.validate(&t); err != nil {
		return domain.Task{}, false, err
	}
	fire, err := t.Plan(now)
	if err != nil {
		return domain.Task{}, false, err
	}
	err = s.d.Tx.WithinTx(ctx, func(ctx context.Context) error {
		if err := s.d.Tasks.Create(ctx, t); err != nil {
			return err
		}
		return s.planImmediate(ctx, t, fire, now)
	})
	if errors.Is(err, domain.ErrDuplicate) { // lost a race with a concurrent request using the same key
		prev, gerr := s.d.Tasks.GetByIdempotencyKey(ctx, tenantID, key)
		return prev, false, gerr
	}
	if err != nil {
		return domain.Task{}, false, err
	}
	return t, true, nil
}

func (s *TaskService) planImmediate(ctx context.Context, t domain.Task, fire *timeT, now timeT) error {
	if fire == nil {
		return nil
	}
	_, err := s.d.Jobs.Insert(ctx, domain.NewJob(s.d.IDs.NewID(), t, *fire, now))
	if err == nil {
		s.d.Metrics.JobsPlanned(1)
	}
	return err
}

func (s *TaskService) Get(ctx context.Context, tenantID, id string) (domain.Task, error) {
	t, err := s.d.Tasks.Get(ctx, tenantID, id)
	if err == nil && t.Status == domain.TaskDeleted {
		return domain.Task{}, domain.ErrNotFound
	}
	return t, err
}

func (s *TaskService) List(ctx context.Context, tenantID, cursor string, limit int) (ports.Page[domain.Task], error) {
	return s.d.Tasks.List(ctx, tenantID, cursor, clampLimit(limit))
}

// mutate loads a task under a row lock (blocking the scheduler for that row), applies fn, and saves.
func (s *TaskService) mutate(ctx context.Context, tenantID, id string, fn func(ctx context.Context, t *domain.Task, now timeT) error) (domain.Task, error) {
	var out domain.Task
	err := s.d.Tx.WithinTx(ctx, func(ctx context.Context) error {
		t, err := s.d.Tasks.GetForUpdate(ctx, tenantID, id)
		if err != nil {
			return err
		}
		if t.Status == domain.TaskDeleted {
			return domain.ErrNotFound
		}
		now := s.d.Clock.Now()
		if err := fn(ctx, &t, now); err != nil {
			return err
		}
		t.UpdatedAt = now
		t.Version++
		if err := s.d.Tasks.Update(ctx, t); err != nil {
			return err
		}
		out = t
		return nil
	})
	return out, err
}

// Replace overwrites a task's user-editable fields (PUT semantics: in is the whole
// task). ifVersion must equal the task's current version, otherwise
// domain.ErrVersionMismatch is returned and nothing changes (optimistic concurrency, so
// two clients can't silently overwrite each other). Jobs already planned keep their
// request snapshot. The task is replanned only if its schedule changed and it is not
// paused (resume replans).
func (s *TaskService) Replace(ctx context.Context, tenantID, id string, ifVersion int64, in TaskInput) (domain.Task, error) {
	return s.mutate(ctx, tenantID, id, func(ctx context.Context, t *domain.Task, now timeT) error {
		if t.Version != ifVersion {
			return domain.ErrVersionMismatch
		}
		prev := t.Schedule
		t.Name, t.Schedule, t.Priority, t.Request, t.Retry = in.Name, in.Schedule, in.Priority, in.Request, in.Retry
		if err := s.validate(t); err != nil {
			return err
		}
		if t.Schedule.Equal(prev) || t.Status == domain.TaskPaused {
			return nil
		}
		fire, err := t.Plan(now)
		if err != nil {
			return err
		}
		return s.planImmediate(ctx, *t, fire, now)
	})
}

func (s *TaskService) Pause(ctx context.Context, tenantID, id string) (domain.Task, error) {
	return s.mutate(ctx, tenantID, id, func(_ context.Context, t *domain.Task, _ timeT) error { return t.Pause() })
}

func (s *TaskService) Resume(ctx context.Context, tenantID, id string) (domain.Task, error) {
	return s.mutate(ctx, tenantID, id, func(ctx context.Context, t *domain.Task, now timeT) error {
		fire, err := t.Resume(now)
		if err != nil {
			return err
		}
		return s.planImmediate(ctx, *t, fire, now)
	})
}

// Delete soft-deletes a task and cancels its pending jobs.
func (s *TaskService) Delete(ctx context.Context, tenantID, id string) error {
	_, err := s.mutate(ctx, tenantID, id, func(ctx context.Context, t *domain.Task, now timeT) error {
		if err := t.Delete(); err != nil {
			return err
		}
		_, err := s.d.Jobs.CancelPending(ctx, tenantID, id, now)
		return err
	})
	return err
}

func clampLimit(n int) int {
	switch {
	case n <= 0:
		return 50
	case n > 500:
		return 500
	}
	return n
}
