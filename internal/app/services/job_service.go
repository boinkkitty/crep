package services

import (
	"context"
	"errors"
	"sort"
	"time"

	"crep/internal/app/ports"
	"crep/internal/domain"
)

// JobService implements the Job API: status, history, DLQ, manual retry.
type JobService struct{ d Deps }

func NewJobService(d Deps) *JobService { return &JobService{d: d} }

// Get returns a job from MySQL, falling back to archived history.
func (s *JobService) Get(ctx context.Context, tenantID, id string) (domain.Job, []domain.Attempt, error) {
	j, err := s.d.Jobs.Get(ctx, tenantID, id)
	if err == nil {
		atts, err := s.d.Jobs.Attempts(ctx, []string{id})
		return j, atts[id], err
	}
	if !errors.Is(err, domain.ErrNotFound) || s.d.History == nil {
		return domain.Job{}, nil, err
	}
	a, err := s.d.History.Get(ctx, tenantID, id)
	if err != nil {
		return domain.Job{}, nil, err
	}
	return a.Job, a.Attempts, nil
}

// History lists a task's jobs newest first, merging recent MySQL rows with archived
// Cassandra rows. The cursor is the scheduled_for of the last item returned.
func (s *JobService) History(ctx context.Context, tenantID, taskID, cursor string, limit int) (ports.Page[domain.Job], error) {
	limit = clampLimit(limit)
	task, err := s.d.Tasks.Get(ctx, tenantID, taskID)
	if err != nil {
		return pageJobs{}, err
	}
	before := time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
	if cursor != "" {
		if before, err = time.Parse(time.RFC3339Nano, cursor); err != nil {
			return pageJobs{}, &domain.ValidationError{Field: "cursor", Message: "invalid"}
		}
	}
	hot, err := s.d.Jobs.ListByTask(ctx, tenantID, taskID, before, limit)
	if err != nil {
		return pageJobs{}, err
	}
	var cold []domain.Job
	if s.d.History != nil {
		// Earliest possible fire is creation time (one-shot run_at can precede it only by
		// user choice, so allow a generous margin).
		notBefore := task.CreatedAt.AddDate(0, -1, 0)
		if cold, err = s.d.History.ListByTask(ctx, tenantID, taskID, before, notBefore, limit); err != nil {
			return pageJobs{}, err
		}
	}
	items := mergeJobs(hot, cold, limit)
	var next string
	if len(items) == limit {
		next = items[len(items)-1].ScheduledFor.Format(time.RFC3339Nano)
	}
	return pageJobs{Items: items, NextCursor: next}, nil
}

// mergeJobs merges two newest-first lists, de-duplicating jobs present in both
// (possible while the archiver is between copy and delete); the hot copy wins.
func mergeJobs(hot, cold []domain.Job, limit int) []domain.Job {
	seen := make(map[string]bool, len(hot))
	out := make([]domain.Job, 0, len(hot)+len(cold))
	for _, j := range hot {
		seen[j.ID] = true
		out = append(out, j)
	}
	for _, j := range cold {
		if !seen[j.ID] {
			out = append(out, j)
		}
	}
	sort.SliceStable(out, func(a, b int) bool { return out[a].ScheduledFor.After(out[b].ScheduledFor) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

func (s *JobService) ListDLQ(ctx context.Context, tenantID, cursor string, limit int) (ports.Page[domain.Job], error) {
	return s.d.Jobs.ListDead(ctx, tenantID, cursor, clampLimit(limit))
}

// Retry moves a DLQ job back to pending with exactly one more attempt.
func (s *JobService) Retry(ctx context.Context, tenantID, id string) (domain.Job, error) {
	return s.mutateJob(ctx, tenantID, id, func(j *domain.Job, now time.Time) error { return j.ManualRetry(now) })
}

// Discard removes a job from the DLQ by canceling it; it is archived later.
func (s *JobService) Discard(ctx context.Context, tenantID, id string) (domain.Job, error) {
	return s.mutateJob(ctx, tenantID, id, func(j *domain.Job, now time.Time) error {
		if j.Status != domain.JobDead {
			return domain.ErrConflict
		}
		return j.Cancel(now, "discarded from DLQ")
	})
}

func (s *JobService) mutateJob(ctx context.Context, tenantID, id string, fn func(*domain.Job, time.Time) error) (domain.Job, error) {
	var out domain.Job
	err := s.d.Tx.WithinTx(ctx, func(ctx context.Context) error {
		j, err := s.d.Jobs.GetForUpdate(ctx, tenantID, id)
		if err != nil {
			return err
		}
		if err := fn(&j, s.d.Clock.Now()); err != nil {
			return err
		}
		out = j
		return s.d.Jobs.Save(ctx, j)
	})
	return out, err
}
