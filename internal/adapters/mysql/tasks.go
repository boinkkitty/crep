package mysql

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"crep/internal/app/ports"
	"crep/internal/domain"
)

type taskRepo struct{ s *Store }

const taskCols = `id, tenant_id, name, schedule_type, run_at, cron_expr, timezone, priority,
	method, url, secret, timeout_ms, max_attempts, backoff_base_ms,
	backoff_max_ms, status, next_run_at, idempotency_key, version, created_at, updated_at`

type scanner interface{ Scan(dest ...any) error }

func (r taskRepo) scan(sc scanner) (domain.Task, error) {
	var (
		t                        domain.Task
		runAt, nextRun           sql.NullTime
		cronExpr, tz, idemKey    sql.NullString
		secret                   []byte
		timeoutMs, baseMs, maxMs int64
		typ, status              string
	)
	err := sc.Scan(&t.ID, &t.TenantID, &t.Name, &typ, &runAt, &cronExpr, &tz, &t.Priority,
		&t.Request.Method, &t.Request.URL, &secret, &timeoutMs,
		&t.Retry.MaxAttempts, &baseMs, &maxMs, &status, &nextRun, &idemKey, &t.Version, &t.CreatedAt, &t.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return t, domain.ErrNotFound
	}
	if err != nil {
		return t, err
	}
	t.Schedule = domain.Schedule{Type: domain.ScheduleType(typ), Cron: cronExpr.String, Timezone: tz.String}
	if runAt.Valid {
		t.Schedule.RunAt = runAt.Time.UTC()
	}
	t.Status = domain.TaskStatus(status)
	t.NextRunAt = timePtr(nextRun)
	t.Request.Timeout = dur(timeoutMs)
	t.Retry.BaseDelay, t.Retry.MaxDelay = dur(baseMs), dur(maxMs)
	t.CreatedAt, t.UpdatedAt = t.CreatedAt.UTC(), t.UpdatedAt.UTC()
	t.IdempotencyKey = idemKey.String
	decodeRequest(r.s.codec, secret, &t.Request)
	return t, nil
}

// tzArg stores UTC when no timezone is given (column is NOT NULL).
func tzArg(t domain.Task) string {
	if t.Schedule.Timezone == "" {
		return "UTC"
	}
	return t.Schedule.Timezone
}

func runAtArg(t domain.Task) sql.NullTime {
	if t.Schedule.Type != domain.ScheduleOnce {
		return sql.NullTime{}
	}
	return nullTime(&t.Schedule.RunAt)
}

func (r taskRepo) Create(ctx context.Context, t domain.Task) error {
	secret, err := encodeRequest(r.s.codec, t.Request)
	if err != nil {
		return err
	}
	_, err = r.s.q(ctx).ExecContext(ctx, `INSERT INTO tasks (`+taskCols+`)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		t.ID, t.TenantID, t.Name, string(t.Schedule.Type), runAtArg(t), nullString(t.Schedule.Cron),
		tzArg(t), int(t.Priority), t.Request.Method, t.Request.URL, secret,
		ms(t.Request.Timeout), t.Retry.MaxAttempts, ms(t.Retry.BaseDelay),
		ms(t.Retry.MaxDelay), string(t.Status), nullTime(t.NextRunAt), nullString(t.IdempotencyKey),
		t.Version, t.CreatedAt, t.UpdatedAt)
	if isDuplicate(err) && t.IdempotencyKey != "" {
		return domain.ErrDuplicate
	}
	return err
}

func (r taskRepo) GetByIdempotencyKey(ctx context.Context, tenantID, key string) (domain.Task, error) {
	return r.scan(r.s.q(ctx).QueryRowContext(ctx, `SELECT `+taskCols+` FROM tasks WHERE tenant_id = ? AND idempotency_key = ?`, tenantID, key))
}

func (r taskRepo) Get(ctx context.Context, tenantID, id string) (domain.Task, error) {
	return r.scan(r.s.q(ctx).QueryRowContext(ctx, `SELECT `+taskCols+` FROM tasks WHERE id = ? AND tenant_id = ?`, id, tenantID))
}

func (r taskRepo) GetForUpdate(ctx context.Context, tenantID, id string) (domain.Task, error) {
	return r.scan(r.s.q(ctx).QueryRowContext(ctx, `SELECT `+taskCols+` FROM tasks WHERE id = ? AND tenant_id = ? FOR UPDATE`, id, tenantID))
}

func (r taskRepo) List(ctx context.Context, tenantID, cursor string, limit int) (ports.Page[domain.Task], error) {
	q := `SELECT ` + taskCols + ` FROM tasks WHERE tenant_id = ? AND status <> 'deleted'`
	args := []any{tenantID}
	if cursor != "" {
		ts, id, err := decodeCursor(cursor)
		if err != nil {
			return ports.Page[domain.Task]{}, err
		}
		q += ` AND (created_at < ? OR (created_at = ? AND id < ?))`
		args = append(args, ts, ts, id)
	}
	q += ` ORDER BY created_at DESC, id DESC LIMIT ?`
	args = append(args, limit+1)
	tasks, err := r.query(ctx, q, args...)
	if err != nil {
		return ports.Page[domain.Task]{}, err
	}
	p := ports.Page[domain.Task]{Items: tasks}
	if len(tasks) > limit {
		p.Items = tasks[:limit]
		last := p.Items[limit-1]
		p.NextCursor = encodeCursor(last.CreatedAt, last.ID)
	}
	return p, nil
}

func (r taskRepo) query(ctx context.Context, q string, args ...any) ([]domain.Task, error) {
	rows, err := r.s.q(ctx).QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Task
	for rows.Next() {
		t, err := r.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (r taskRepo) Update(ctx context.Context, t domain.Task) error {
	// An unreadable request keeps its stored ciphertext (it may become readable
	// again if the old key is restored) instead of being overwritten with blanks.
	secretSQL := "?"
	var secret []byte
	if t.Request.Unreadable != "" {
		secretSQL = "secret"
	} else {
		var err error
		if secret, err = encodeRequest(r.s.codec, t.Request); err != nil {
			return err
		}
	}
	q := `UPDATE tasks SET name=?, schedule_type=?, run_at=?, cron_expr=?,
		timezone=?, priority=?, method=?, url=?, secret=` + secretSQL + `, timeout_ms=?,
		max_attempts=?, backoff_base_ms=?, backoff_max_ms=?, status=?, next_run_at=?, version=?, updated_at=?
		WHERE id=? AND tenant_id=?`
	args := []any{t.Name, string(t.Schedule.Type), runAtArg(t), nullString(t.Schedule.Cron), tzArg(t),
		int(t.Priority), t.Request.Method, t.Request.URL}
	if t.Request.Unreadable == "" {
		args = append(args, secret)
	}
	args = append(args, ms(t.Request.Timeout), t.Retry.MaxAttempts, ms(t.Retry.BaseDelay),
		ms(t.Retry.MaxDelay), string(t.Status), nullTime(t.NextRunAt), t.Version, t.UpdatedAt, t.ID, t.TenantID)
	res, err := r.s.q(ctx).ExecContext(ctx, q, args...)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return domain.ErrNotFound
	}
	return nil
}

func (r taskRepo) LockDue(ctx context.Context, before time.Time, limit int) ([]domain.Task, error) {
	return r.query(ctx, `SELECT `+taskCols+` FROM tasks
		WHERE status = 'active' AND next_run_at <= ?
		ORDER BY next_run_at LIMIT ? FOR UPDATE SKIP LOCKED`, before, limit)
}

func (r taskRepo) SaveSchedule(ctx context.Context, t domain.Task) error {
	_, err := r.s.q(ctx).ExecContext(ctx, `UPDATE tasks SET status=?, next_run_at=? WHERE id=?`,
		string(t.Status), nullTime(t.NextRunAt), t.ID)
	return err
}
