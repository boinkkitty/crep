package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"crep/internal/app/ports"
	"crep/internal/domain"
)

type jobRepo struct{ s *Store }

const jobCols = `id, task_id, tenant_id, scheduled_for, priority, status, attempt, max_attempts,
	available_at, worker_id, lease_expires_at, method, url, secret,
	timeout_ms, backoff_base_ms, backoff_max_ms, retry_max_attempts, http_status, error,
	created_at, finished_at`

func (r jobRepo) scan(sc scanner) (domain.Job, error) {
	var (
		j                        domain.Job
		status                   string
		owner, lastErr           sql.NullString
		leaseExp, finished       sql.NullTime
		lastCode                 sql.NullInt64
		secret                   []byte
		timeoutMs, baseMs, maxMs int64
	)
	err := sc.Scan(&j.ID, &j.TaskID, &j.TenantID, &j.ScheduledFor, &j.Priority, &status, &j.Attempt,
		&j.MaxAttempts, &j.AvailableAt, &owner, &leaseExp, &j.Request.Method, &j.Request.URL, &secret,
		&timeoutMs, &baseMs, &maxMs, &j.Retry.MaxAttempts, &lastCode,
		&lastErr, &j.CreatedAt, &finished)
	if errors.Is(err, sql.ErrNoRows) {
		return j, domain.ErrNotFound
	}
	if err != nil {
		return j, err
	}
	j.Status = domain.JobStatus(status)
	j.WorkerID, j.Error = owner.String, lastErr.String
	j.HTTPStatus = int(lastCode.Int64)
	j.LeaseExpiresAt, j.FinishedAt = timePtr(leaseExp), timePtr(finished)
	j.ScheduledFor, j.AvailableAt, j.CreatedAt = j.ScheduledFor.UTC(), j.AvailableAt.UTC(), j.CreatedAt.UTC()
	j.Request.Timeout = dur(timeoutMs)
	j.Retry.BaseDelay, j.Retry.MaxDelay = dur(baseMs), dur(maxMs)
	decodeRequest(r.s.codec, secret, &j.Request)
	return j, nil
}

func (r jobRepo) query(ctx context.Context, q string, args ...any) ([]domain.Job, error) {
	rows, err := r.s.q(ctx).QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Job
	for rows.Next() {
		j, err := r.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (r jobRepo) Insert(ctx context.Context, j domain.Job) (bool, error) {
	if j.Request.Unreadable != "" {
		return false, fmt.Errorf("refusing to plan job with unreadable request: %s", j.Request.Unreadable)
	}
	secret, err := encodeRequest(r.s.codec, j.Request)
	if err != nil {
		return false, err
	}
	_, err = r.s.q(ctx).ExecContext(ctx, `INSERT INTO jobs (`+jobCols+`, updated_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		j.ID, j.TaskID, j.TenantID, j.ScheduledFor, int(j.Priority), string(j.Status), j.Attempt,
		j.MaxAttempts, j.AvailableAt, nullString(j.WorkerID), nullTime(j.LeaseExpiresAt),
		j.Request.Method, j.Request.URL, secret, ms(j.Request.Timeout),
		ms(j.Retry.BaseDelay), ms(j.Retry.MaxDelay), j.Retry.MaxAttempts, nullInt(j.HTTPStatus),
		nullString(j.Error), j.CreatedAt, nullTime(j.FinishedAt), j.CreatedAt)
	if isDuplicate(err) {
		return false, nil // already planned for this (task, scheduled_for)
	}
	return err == nil, err
}

func (r jobRepo) Get(ctx context.Context, tenantID, id string) (domain.Job, error) {
	return r.scan(r.s.q(ctx).QueryRowContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE id = ? AND tenant_id = ?`, id, tenantID))
}

func (r jobRepo) GetForUpdate(ctx context.Context, tenantID, id string) (domain.Job, error) {
	return r.scan(r.s.q(ctx).QueryRowContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE id = ? AND tenant_id = ? FOR UPDATE`, id, tenantID))
}

func (r jobRepo) LockClaimable(ctx context.Context, horizon time.Time, order ports.ClaimOrder, limit int) ([]domain.Job, error) {
	q := `SELECT ` + jobCols + ` FROM jobs FORCE INDEX (idx_jobs_claim_priority)
		WHERE status = 'pending' AND available_at <= ?
		ORDER BY priority, available_at LIMIT ? FOR UPDATE SKIP LOCKED`
	if order == ports.ClaimOldestDue {
		q = `SELECT ` + jobCols + ` FROM jobs FORCE INDEX (idx_jobs_claim_oldest)
		WHERE status = 'pending' AND available_at <= ?
		ORDER BY available_at LIMIT ? FOR UPDATE SKIP LOCKED`
	}
	return r.query(ctx, q, horizon, limit)
}

// MarkClaimed writes all claims of one executor in a single UPDATE. Attempt and lease
// differ per job, so they come from CASE expressions over the values the domain
// computed (keeping Go the single source of truth for the lease formula).
func (r jobRepo) MarkClaimed(ctx context.Context, jobs []domain.Job) error {
	if len(jobs) == 0 {
		return nil
	}
	var attemptCase, leaseCase strings.Builder
	var attemptArgs, leaseArgs, ids []any
	for _, j := range jobs {
		attemptCase.WriteString(" WHEN ? THEN ?")
		attemptArgs = append(attemptArgs, j.ID, j.Attempt)
		leaseCase.WriteString(" WHEN ? THEN ?")
		leaseArgs = append(leaseArgs, j.ID, nullTime(j.LeaseExpiresAt))
		ids = append(ids, j.ID)
	}
	in := strings.TrimSuffix(strings.Repeat("?,", len(jobs)), ",")
	q := `UPDATE jobs SET status='running', worker_id=?,
		attempt = CASE id` + attemptCase.String() + ` END,
		lease_expires_at = CASE id` + leaseCase.String() + ` END,
		updated_at=? WHERE id IN (` + in + `) AND status='pending'`
	args := append([]any{nullString(jobs[0].WorkerID)}, attemptArgs...)
	args = append(args, leaseArgs...)
	args = append(args, r.s.clock.Now())
	args = append(args, ids...)
	res, err := r.s.q(ctx).ExecContext(ctx, q, args...)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != int64(len(jobs)) {
		return fmt.Errorf("claim: updated %d of %d locked jobs", n, len(jobs))
	}
	return nil
}

const saveSet = `status=?, attempt=?, max_attempts=?, available_at=?, worker_id=?, lease_expires_at=?,
	http_status=?, error=?, finished_at=?, updated_at=?`

func (r jobRepo) saveArgs(j domain.Job) []any {
	return []any{string(j.Status), j.Attempt, j.MaxAttempts, j.AvailableAt, nullString(j.WorkerID),
		nullTime(j.LeaseExpiresAt), nullInt(j.HTTPStatus), nullString(j.Error),
		nullTime(j.FinishedAt), r.s.clock.Now()}
}

func (r jobRepo) RecordResult(ctx context.Context, j domain.Job, f domain.Fence) error {
	args := append(r.saveArgs(j), j.ID, f.Attempt, f.Owner)
	res, err := r.s.q(ctx).ExecContext(ctx, `UPDATE jobs SET `+saveSet+`
		WHERE id=? AND status='running' AND attempt=? AND worker_id=?`, args...)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return domain.ErrStaleLease
	}
	return nil
}

func (r jobRepo) Save(ctx context.Context, j domain.Job) error {
	args := append(r.saveArgs(j), j.ID)
	_, err := r.s.q(ctx).ExecContext(ctx, `UPDATE jobs SET `+saveSet+` WHERE id=?`, args...)
	return err
}

func (r jobRepo) InsertAttempt(ctx context.Context, a domain.Attempt) error {
	// Upsert: a retried result write may insert the same attempt twice.
	_, err := r.s.q(ctx).ExecContext(ctx, `INSERT INTO job_attempts
		(job_id, attempt, started_at, finished_at, http_status, error, duration_ms) VALUES (?,?,?,?,?,?,?)
		ON DUPLICATE KEY UPDATE finished_at=VALUES(finished_at), http_status=VALUES(http_status),
		error=VALUES(error), duration_ms=VALUES(duration_ms)`,
		a.JobID, a.Attempt, a.StartedAt, a.FinishedAt, nullInt(a.HTTPStatus), nullString(a.Error), a.Duration.Milliseconds())
	return err
}

func (r jobRepo) Attempts(ctx context.Context, ids []string) (map[string][]domain.Attempt, error) {
	out := map[string][]domain.Attempt{}
	if len(ids) == 0 {
		return out, nil
	}
	in, args := inArgs(ids)
	rows, err := r.s.q(ctx).QueryContext(ctx, `SELECT job_id, attempt, started_at, finished_at, http_status,
		error, duration_ms FROM job_attempts WHERE job_id IN (`+in+`) ORDER BY job_id, attempt`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			a    domain.Attempt
			code sql.NullInt64
			msg  sql.NullString
			dms  int64
		)
		if err := rows.Scan(&a.JobID, &a.Attempt, &a.StartedAt, &a.FinishedAt, &code, &msg, &dms); err != nil {
			return nil, err
		}
		a.StartedAt, a.FinishedAt = a.StartedAt.UTC(), a.FinishedAt.UTC()
		a.HTTPStatus, a.Error, a.Duration = int(code.Int64), msg.String, dur(dms)
		out[a.JobID] = append(out[a.JobID], a)
	}
	return out, rows.Err()
}

func (r jobRepo) LockExpiredLeases(ctx context.Context, now time.Time, limit int) ([]domain.Job, error) {
	return r.query(ctx, `SELECT `+jobCols+` FROM jobs
		WHERE status = 'running' AND lease_expires_at < ?
		ORDER BY lease_expires_at LIMIT ? FOR UPDATE SKIP LOCKED`, now, limit)
}

func (r jobRepo) CancelPending(ctx context.Context, tenantID, taskID string, now time.Time) (int64, error) {
	res, err := r.s.q(ctx).ExecContext(ctx, `UPDATE jobs SET status='cancelled', finished_at=?, updated_at=?,
		error='task deleted' WHERE task_id=? AND tenant_id=? AND status='pending'`, now, now, taskID, tenantID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (r jobRepo) ListByTask(ctx context.Context, tenantID, taskID string, before time.Time, limit int) ([]domain.Job, error) {
	return r.query(ctx, `SELECT `+jobCols+` FROM jobs WHERE task_id = ? AND tenant_id = ? AND scheduled_for < ?
		ORDER BY scheduled_for DESC LIMIT ?`, taskID, tenantID, before, limit)
}

func (r jobRepo) ListDead(ctx context.Context, tenantID, cursor string, limit int) (ports.Page[domain.Job], error) {
	q := `SELECT ` + jobCols + ` FROM jobs WHERE tenant_id = ? AND status = 'dead'`
	args := []any{tenantID}
	if cursor != "" {
		ts, id, err := decodeCursor(cursor)
		if err != nil {
			return ports.Page[domain.Job]{}, err
		}
		q += ` AND (finished_at < ? OR (finished_at = ? AND id < ?))`
		args = append(args, ts, ts, id)
	}
	q += ` ORDER BY finished_at DESC, id DESC LIMIT ?`
	args = append(args, limit+1)
	jobs, err := r.query(ctx, q, args...)
	if err != nil {
		return ports.Page[domain.Job]{}, err
	}
	p := ports.Page[domain.Job]{Items: jobs}
	if len(jobs) > limit {
		p.Items = jobs[:limit]
		last := p.Items[limit-1]
		p.NextCursor = encodeCursor(*last.FinishedAt, last.ID)
	}
	return p, nil
}

// ListArchivable unions one index range per status (idx_jobs_finished) rather than an
// OR, so each branch is a bounded range scan.
func (r jobRepo) ListArchivable(ctx context.Context, doneBefore, deadBefore time.Time, limit int) ([]domain.Job, error) {
	branch := `(SELECT ` + jobCols + ` FROM jobs WHERE status = ? AND finished_at < ? ORDER BY finished_at LIMIT ?)`
	return r.query(ctx, branch+` UNION ALL `+branch+` UNION ALL `+branch+` ORDER BY finished_at LIMIT ?`,
		"succeeded", doneBefore, limit, "cancelled", doneBefore, limit, "dead", deadBefore, limit, limit)
}

func (r jobRepo) DeleteArchived(ctx context.Context, jobs []domain.Job) error {
	if len(jobs) == 0 {
		return nil
	}
	pairs := strings.TrimSuffix(strings.Repeat("(?,?),", len(jobs)), ",")
	args := make([]any, 0, 2*len(jobs))
	ids := make([]string, len(jobs))
	for i, j := range jobs {
		args = append(args, j.ID, j.Attempt)
		ids[i] = j.ID
	}
	in, idArgs := inArgs(ids)
	return r.s.WithinTx(ctx, func(ctx context.Context) error {
		if _, err := r.s.q(ctx).ExecContext(ctx, `DELETE FROM jobs WHERE (id, attempt) IN (`+pairs+`)
			AND status IN ('succeeded','cancelled','dead')`, args...); err != nil {
			return err
		}
		// Attempts go only with jobs that were actually deleted above.
		_, err := r.s.q(ctx).ExecContext(ctx, `DELETE a FROM job_attempts a LEFT JOIN jobs j ON j.id = a.job_id
			WHERE a.job_id IN (`+in+`) AND j.id IS NULL`, idArgs...)
		return err
	})
}

func (r jobRepo) CountDead(ctx context.Context) (int64, error) {
	var n int64
	err := r.s.q(ctx).QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE status = 'dead'`).Scan(&n)
	return n, err
}
