// Package memory provides in-process implementations of the persistence ports.
// Transactions are serialized with a single mutex. Intended for tests and local experiments.
package memory

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"crep/internal/app/ports"
	"crep/internal/domain"
)

type txKey struct{}

// Store implements ports.TxManager, ports.TaskRepo, ports.JobRepo and ports.HistoryStore.
type Store struct {
	mu       sync.Mutex
	tasks    map[string]domain.Task
	jobs     map[string]domain.Job
	attempts map[string][]domain.Attempt
	history  map[string]ports.ArchivedJob
}

func New() *Store {
	return &Store{tasks: map[string]domain.Task{}, jobs: map[string]domain.Job{},
		attempts: map[string][]domain.Attempt{}, history: map[string]ports.ArchivedJob{}}
}

// Tasks/Jobs/History return views of the store satisfying the individual ports.
func (s *Store) TaskRepo() ports.TaskRepo         { return taskRepo{s} }
func (s *Store) JobRepo() ports.JobRepo           { return jobRepo{s} }
func (s *Store) HistoryStore() ports.HistoryStore { return historyStore{s} }

func (s *Store) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if ctx.Value(txKey{}) != nil {
		return fn(ctx)
	}
	s.mu.Lock()
	tasks, jobs, atts := cloneMap(s.tasks), cloneMap(s.jobs), cloneMap(s.attempts)
	err := fn(context.WithValue(ctx, txKey{}, true))
	if err != nil { // roll back
		s.tasks, s.jobs, s.attempts = tasks, jobs, atts
	}
	s.mu.Unlock()
	return err
}

func cloneMap[K comparable, V any](m map[K]V) map[K]V {
	out := make(map[K]V, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// locked runs fn under the store mutex unless already inside a transaction.
func (s *Store) locked(ctx context.Context, fn func() error) error {
	if ctx.Value(txKey{}) != nil {
		return fn()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn()
}

// Snapshot returns copies of all jobs, for test assertions.
func (s *Store) Snapshot() []domain.Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		out = append(out, j)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ScheduledFor.Before(out[b].ScheduledFor) })
	return out
}

// ---- tasks ----

type taskRepo struct{ s *Store }

func (r taskRepo) Create(ctx context.Context, t domain.Task) error {
	return r.s.locked(ctx, func() error {
		if t.IdempotencyKey != "" {
			if _, err := r.byKey(t.TenantID, t.IdempotencyKey); err == nil {
				return domain.ErrDuplicate
			}
		}
		r.s.tasks[t.ID] = t
		return nil
	})
}

func (r taskRepo) byKey(tenantID, key string) (domain.Task, error) {
	for _, t := range r.s.tasks {
		if t.TenantID == tenantID && t.IdempotencyKey == key {
			return t, nil
		}
	}
	return domain.Task{}, domain.ErrNotFound
}

func (r taskRepo) GetByIdempotencyKey(ctx context.Context, tenantID, key string) (t domain.Task, err error) {
	err = r.s.locked(ctx, func() error { t, err = r.byKey(tenantID, key); return err })
	return
}

func (r taskRepo) Get(ctx context.Context, tenantID, id string) (t domain.Task, err error) {
	err = r.s.locked(ctx, func() error {
		v, ok := r.s.tasks[id]
		if !ok || v.TenantID != tenantID {
			return domain.ErrNotFound
		}
		t = v
		return nil
	})
	return
}

func (r taskRepo) GetForUpdate(ctx context.Context, tenantID, id string) (domain.Task, error) {
	return r.Get(ctx, tenantID, id)
}

func (r taskRepo) List(ctx context.Context, tenantID, cursor string, limit int) (p ports.Page[domain.Task], err error) {
	err = r.s.locked(ctx, func() error {
		var all []domain.Task
		for _, t := range r.s.tasks {
			if t.TenantID == tenantID && t.Status != domain.TaskDeleted {
				all = append(all, t)
			}
		}
		sort.Slice(all, func(a, b int) bool { return all[a].ID > all[b].ID })
		for _, t := range all {
			if cursor != "" && t.ID >= cursor {
				continue
			}
			if len(p.Items) == limit {
				p.NextCursor = p.Items[len(p.Items)-1].ID
				break
			}
			p.Items = append(p.Items, t)
		}
		return nil
	})
	return
}

func (r taskRepo) Update(ctx context.Context, t domain.Task) error {
	return r.s.locked(ctx, func() error { r.s.tasks[t.ID] = t; return nil })
}

func (r taskRepo) LockDue(ctx context.Context, before time.Time, limit int) (out []domain.Task, err error) {
	err = r.s.locked(ctx, func() error {
		for _, t := range r.s.tasks {
			if t.Status == domain.TaskActive && t.NextRunAt != nil && !t.NextRunAt.After(before) {
				out = append(out, t)
			}
		}
		sort.Slice(out, func(a, b int) bool { return out[a].NextRunAt.Before(*out[b].NextRunAt) })
		if len(out) > limit {
			out = out[:limit]
		}
		return nil
	})
	return
}

func (r taskRepo) SaveSchedule(ctx context.Context, t domain.Task) error {
	return r.s.locked(ctx, func() error {
		cur := r.s.tasks[t.ID]
		cur.Status, cur.NextRunAt = t.Status, t.NextRunAt
		r.s.tasks[t.ID] = cur
		return nil
	})
}

// ---- jobs ----

type jobRepo struct{ s *Store }

func (r jobRepo) Insert(ctx context.Context, j domain.Job) (ok bool, err error) {
	err = r.s.locked(ctx, func() error {
		for _, x := range r.s.jobs {
			if x.TaskID == j.TaskID && x.ScheduledFor.Equal(j.ScheduledFor) {
				return nil
			}
		}
		r.s.jobs[j.ID] = j
		ok = true
		return nil
	})
	return
}

func (r jobRepo) Get(ctx context.Context, tenantID, id string) (j domain.Job, err error) {
	err = r.s.locked(ctx, func() error {
		v, ok := r.s.jobs[id]
		if !ok || v.TenantID != tenantID {
			return domain.ErrNotFound
		}
		j = v
		return nil
	})
	return
}

func (r jobRepo) GetForUpdate(ctx context.Context, tenantID, id string) (domain.Job, error) {
	return r.Get(ctx, tenantID, id)
}

func (r jobRepo) LockClaimable(ctx context.Context, horizon time.Time, order ports.ClaimOrder, limit int) (out []domain.Job, err error) {
	err = r.s.locked(ctx, func() error {
		for _, j := range r.s.jobs {
			if j.Status == domain.JobPending && !j.AvailableAt.After(horizon) {
				out = append(out, j)
			}
		}
		sort.Slice(out, func(a, b int) bool {
			if order == ports.ClaimByPriority && out[a].Priority != out[b].Priority {
				return out[a].Priority < out[b].Priority
			}
			return out[a].AvailableAt.Before(out[b].AvailableAt)
		})
		if len(out) > limit {
			out = out[:limit]
		}
		return nil
	})
	return
}

func (r jobRepo) MarkClaimed(ctx context.Context, jobs []domain.Job) error {
	return r.s.locked(ctx, func() error {
		for _, j := range jobs {
			r.s.jobs[j.ID] = j
		}
		return nil
	})
}

func (r jobRepo) RecordResult(ctx context.Context, j domain.Job, f domain.Fence) error {
	return r.s.locked(ctx, func() error {
		cur, ok := r.s.jobs[j.ID]
		if !ok || cur.Status != domain.JobRunning || cur.Attempt != f.Attempt || cur.WorkerID != f.Owner {
			return domain.ErrStaleLease
		}
		r.s.jobs[j.ID] = j
		return nil
	})
}

func (r jobRepo) Save(ctx context.Context, j domain.Job) error {
	return r.s.locked(ctx, func() error { r.s.jobs[j.ID] = j; return nil })
}

func (r jobRepo) InsertAttempt(ctx context.Context, a domain.Attempt) error {
	return r.s.locked(ctx, func() error { r.s.attempts[a.JobID] = append(r.s.attempts[a.JobID], a); return nil })
}

func (r jobRepo) Attempts(ctx context.Context, ids []string) (out map[string][]domain.Attempt, err error) {
	out = map[string][]domain.Attempt{}
	err = r.s.locked(ctx, func() error {
		for _, id := range ids {
			if a := r.s.attempts[id]; len(a) > 0 {
				out[id] = append([]domain.Attempt(nil), a...)
			}
		}
		return nil
	})
	return
}

func (r jobRepo) LockExpiredLeases(ctx context.Context, now time.Time, limit int) (out []domain.Job, err error) {
	err = r.s.locked(ctx, func() error {
		for _, j := range r.s.jobs {
			if j.Status == domain.JobRunning && j.LeaseExpiresAt != nil && j.LeaseExpiresAt.Before(now) && len(out) < limit {
				out = append(out, j)
			}
		}
		return nil
	})
	return
}

func (r jobRepo) CancelPending(ctx context.Context, tenantID, taskID string, now time.Time) (n int64, err error) {
	err = r.s.locked(ctx, func() error {
		for id, j := range r.s.jobs {
			if j.TenantID == tenantID && j.TaskID == taskID && j.Status == domain.JobPending {
				_ = j.Cancel(now, "task deleted")
				r.s.jobs[id] = j
				n++
			}
		}
		return nil
	})
	return
}

func (r jobRepo) ListByTask(ctx context.Context, tenantID, taskID string, before time.Time, limit int) (out []domain.Job, err error) {
	err = r.s.locked(ctx, func() error {
		for _, j := range r.s.jobs {
			if j.TenantID == tenantID && j.TaskID == taskID && j.ScheduledFor.Before(before) {
				out = append(out, j)
			}
		}
		sort.Slice(out, func(a, b int) bool { return out[a].ScheduledFor.After(out[b].ScheduledFor) })
		if len(out) > limit {
			out = out[:limit]
		}
		return nil
	})
	return
}

func (r jobRepo) ListDead(ctx context.Context, tenantID, cursor string, limit int) (p ports.Page[domain.Job], err error) {
	err = r.s.locked(ctx, func() error {
		for _, j := range r.s.jobs {
			if j.TenantID == tenantID && j.Status == domain.JobDead && (cursor == "" || strings.Compare(j.ID, cursor) < 0) {
				p.Items = append(p.Items, j)
			}
		}
		sort.Slice(p.Items, func(a, b int) bool { return p.Items[a].ID > p.Items[b].ID })
		if len(p.Items) > limit {
			p.Items = p.Items[:limit]
			p.NextCursor = p.Items[limit-1].ID
		}
		return nil
	})
	return
}

func (r jobRepo) ListArchivable(ctx context.Context, doneBefore, deadBefore time.Time, limit int) (out []domain.Job, err error) {
	err = r.s.locked(ctx, func() error {
		for _, j := range r.s.jobs {
			if j.FinishedAt == nil {
				continue
			}
			done := (j.Status == domain.JobSucceeded || j.Status == domain.JobCancelled) && j.FinishedAt.Before(doneBefore)
			dead := j.Status == domain.JobDead && j.FinishedAt.Before(deadBefore)
			if done || dead {
				out = append(out, j)
			}
		}
		sort.Slice(out, func(a, b int) bool { return out[a].FinishedAt.Before(*out[b].FinishedAt) })
		if len(out) > limit {
			out = out[:limit]
		}
		return nil
	})
	return
}

func (r jobRepo) DeleteArchived(ctx context.Context, jobs []domain.Job) error {
	return r.s.locked(ctx, func() error {
		for _, a := range jobs {
			if cur, ok := r.s.jobs[a.ID]; ok && cur.Attempt == a.Attempt && cur.Status.Terminal() {
				delete(r.s.jobs, a.ID)
				delete(r.s.attempts, a.ID)
			}
		}
		return nil
	})
}

func (r jobRepo) CountDead(ctx context.Context) (n int64, err error) {
	err = r.s.locked(ctx, func() error {
		for _, j := range r.s.jobs {
			if j.Status == domain.JobDead {
				n++
			}
		}
		return nil
	})
	return
}

// ---- history ----

type historyStore struct{ s *Store }

func (h historyStore) Save(_ context.Context, jobs []ports.ArchivedJob) error {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	for _, a := range jobs {
		h.s.history[a.Job.ID] = a
	}
	return nil
}

func (h historyStore) Get(_ context.Context, tenantID, id string) (ports.ArchivedJob, error) {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	a, ok := h.s.history[id]
	if !ok || a.Job.TenantID != tenantID {
		return ports.ArchivedJob{}, domain.ErrNotFound
	}
	return a, nil
}

func (h historyStore) ListByTask(_ context.Context, tenantID, taskID string, before, notBefore time.Time, limit int) (out []domain.Job, _ error) {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	for _, a := range h.s.history {
		j := a.Job
		if j.TenantID == tenantID && j.TaskID == taskID && j.ScheduledFor.Before(before) && !j.ScheduledFor.Before(notBefore) {
			out = append(out, j)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ScheduledFor.After(out[b].ScheduledFor) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
