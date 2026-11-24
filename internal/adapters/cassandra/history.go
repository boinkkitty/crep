// Package cassandra implements ports.HistoryStore for archived job history.
package cassandra

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"time"

	"github.com/gocql/gocql"

	"crep/internal/app/ports"
	"crep/internal/domain"
)

// Config selects the cluster and keyspace.
type Config struct {
	Hosts             []string
	Keyspace          string
	ReplicationFactor int
	Consistency       gocql.Consistency
	Username          string
	Password          string
	// TTL expires archived history rows; 0 keeps them forever.
	TTL time.Duration
}

func (c Config) cluster() *gocql.ClusterConfig {
	cl := gocql.NewCluster(c.Hosts...)
	cl.Consistency = c.Consistency
	if cl.Consistency == 0 {
		cl.Consistency = gocql.LocalQuorum
	}
	cl.Timeout = 5 * time.Second
	cl.ConnectTimeout = 10 * time.Second
	if c.Username != "" {
		cl.Authenticator = gocql.PasswordAuthenticator{Username: c.Username, Password: c.Password}
	}
	return cl
}

var keyspaceName = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]{0,47}$`)

// schema is applied on every Open; each statement must be idempotent (IF NOT EXISTS).
var schema = []string{
	`CREATE TABLE IF NOT EXISTS jobs_by_task (
  tenant_id text,
  task_id text,
  bucket int,
  scheduled_for timestamp,
  job_id text,
  status text,
  priority int,
  attempt int,
  max_attempts int,
  method text,
  url text,
  http_status int,
  error text,
  created_at timestamp,
  finished_at timestamp,
  PRIMARY KEY ((tenant_id, task_id, bucket), scheduled_for, job_id)
) WITH CLUSTERING ORDER BY (scheduled_for DESC, job_id ASC)`,
	`CREATE TABLE IF NOT EXISTS job_by_id (
  job_id text PRIMARY KEY,
  tenant_id text,
  task_id text,
  scheduled_for timestamp,
  status text,
  priority int,
  attempt int,
  max_attempts int,
  method text,
  url text,
  http_status int,
  error text,
  created_at timestamp,
  finished_at timestamp
)`,
	`CREATE TABLE IF NOT EXISTS attempts_by_job (
  job_id text,
  attempt int,
  started_at timestamp,
  finished_at timestamp,
  http_status int,
  error text,
  duration_ms bigint,
  PRIMARY KEY (job_id, attempt)
)`,
}

// Store is the Cassandra history store.
type Store struct {
	s   *gocql.Session
	ttl int // seconds; 0 = no expiry
}

// Open connects, creating the keyspace (SimpleStrategy) and tables if missing.
func Open(cfg Config) (*Store, error) {
	if !keyspaceName.MatchString(cfg.Keyspace) {
		return nil, fmt.Errorf("invalid keyspace name %q", cfg.Keyspace)
	}
	rf := cfg.ReplicationFactor
	if rf <= 0 {
		rf = 1
	}
	boot, err := cfg.cluster().CreateSession()
	if err != nil {
		return nil, err
	}
	err = boot.Query(fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d}`, cfg.Keyspace, rf)).Exec()
	boot.Close()
	if err != nil {
		return nil, err
	}
	cl := cfg.cluster()
	cl.Keyspace = cfg.Keyspace
	s, err := cl.CreateSession()
	if err != nil {
		return nil, err
	}
	for _, stmt := range schema {
		if err := s.Query(stmt).Exec(); err != nil {
			s.Close()
			return nil, fmt.Errorf("create schema: %w", err)
		}
	}
	return &Store{s: s, ttl: int(cfg.TTL / time.Second)}, nil
}

func (st *Store) Close() { st.s.Close() }

var _ ports.HistoryStore = (*Store)(nil)

// bucket partitions a task's history by UTC month (yyyymm).
func bucket(t time.Time) int {
	t = t.UTC()
	return t.Year()*100 + int(t.Month())
}

func prevMonth(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, -1, 0)
}

func optTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return *t
}

// archivedURL strips the parts of a URL that can carry credentials (user:password,
// the query string, the fragment), keeping scheme, host and path.
func archivedURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	u.User, u.RawQuery, u.ForceQuery, u.Fragment, u.RawFragment = nil, "", false, "", ""
	return u.String()
}

// Save upserts jobs and attempts with the configured TTL. Request headers and bodies are
// not archived, and URLs lose their userinfo, query string and fragment.
func (st *Store) Save(ctx context.Context, jobs []ports.ArchivedJob) error {
	for _, a := range jobs {
		j := a.Job
		common := []any{j.ScheduledFor, string(j.Status), int(j.Priority), j.Attempt, j.MaxAttempts,
			j.Request.Method, archivedURL(j.Request.URL), j.HTTPStatus, j.Error, j.CreatedAt, optTime(j.FinishedAt)}
		if err := st.s.Query(`INSERT INTO jobs_by_task (tenant_id, task_id, bucket, job_id, scheduled_for, status,
			priority, attempt, max_attempts, method, url, http_status, error, created_at, finished_at)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) USING TTL ?`,
			append(append([]any{j.TenantID, j.TaskID, bucket(j.ScheduledFor), j.ID}, common...), st.ttl)...).WithContext(ctx).Exec(); err != nil {
			return err
		}
		if err := st.s.Query(`INSERT INTO job_by_id (job_id, tenant_id, task_id, scheduled_for, status,
			priority, attempt, max_attempts, method, url, http_status, error, created_at, finished_at)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) USING TTL ?`,
			append(append([]any{j.ID, j.TenantID, j.TaskID}, common...), st.ttl)...).WithContext(ctx).Exec(); err != nil {
			return err
		}
		for _, at := range a.Attempts {
			if err := st.s.Query(`INSERT INTO attempts_by_job (job_id, attempt, started_at, finished_at,
				http_status, error, duration_ms) VALUES (?,?,?,?,?,?,?) USING TTL ?`,
				j.ID, at.Attempt, at.StartedAt, at.FinishedAt, at.HTTPStatus, at.Error, at.Duration.Milliseconds(), st.ttl).
				WithContext(ctx).Exec(); err != nil {
				return err
			}
		}
	}
	return nil
}

func scanJob(sc interface{ Scan(...any) bool }, j *domain.Job) bool {
	var (
		status   string
		prio     int
		finished time.Time
	)
	ok := sc.Scan(&j.ID, &j.TenantID, &j.TaskID, &j.ScheduledFor, &status, &prio, &j.Attempt, &j.MaxAttempts,
		&j.Request.Method, &j.Request.URL, &j.HTTPStatus, &j.Error, &j.CreatedAt, &finished)
	j.Status, j.Priority = domain.JobStatus(status), domain.Priority(prio)
	j.ScheduledFor, j.CreatedAt = j.ScheduledFor.UTC(), j.CreatedAt.UTC()
	if !finished.IsZero() {
		f := finished.UTC()
		j.FinishedAt = &f
	}
	return ok
}

const selectCols = `job_id, tenant_id, task_id, scheduled_for, status, priority, attempt, max_attempts,
	method, url, http_status, error, created_at, finished_at`

func (st *Store) Get(ctx context.Context, tenantID, jobID string) (ports.ArchivedJob, error) {
	var a ports.ArchivedJob
	it := st.s.Query(`SELECT `+selectCols+` FROM job_by_id WHERE job_id = ?`, jobID).WithContext(ctx).Iter()
	found := scanJob(it, &a.Job)
	if err := it.Close(); err != nil {
		return a, err
	}
	if !found || a.Job.TenantID != tenantID {
		return ports.ArchivedJob{}, domain.ErrNotFound
	}
	ait := st.s.Query(`SELECT attempt, started_at, finished_at, http_status, error, duration_ms
		FROM attempts_by_job WHERE job_id = ?`, jobID).WithContext(ctx).Iter()
	for {
		var at domain.Attempt
		var dms int64
		if !ait.Scan(&at.Attempt, &at.StartedAt, &at.FinishedAt, &at.HTTPStatus, &at.Error, &dms) {
			break
		}
		at.JobID, at.Duration = jobID, time.Duration(dms)*time.Millisecond
		at.StartedAt, at.FinishedAt = at.StartedAt.UTC(), at.FinishedAt.UTC()
		a.Attempts = append(a.Attempts, at)
	}
	return a, ait.Close()
}

// ListByTask walks monthly partitions backwards from `before` until limit rows are
// found or notBefore is passed.
func (st *Store) ListByTask(ctx context.Context, tenantID, taskID string, before, notBefore time.Time, limit int) ([]domain.Job, error) {
	if before.Year() > 3000 { // "no cursor" sentinel
		before = time.Now().UTC().AddDate(0, 1, 0)
	}
	var out []domain.Job
	floor := bucket(notBefore)
	for month, n := before, 0; bucket(month) >= floor && len(out) < limit && n < 240; month, n = prevMonth(month), n+1 {
		it := st.s.Query(`SELECT `+selectCols+` FROM jobs_by_task
			WHERE tenant_id = ? AND task_id = ? AND bucket = ? AND scheduled_for < ? LIMIT ?`,
			tenantID, taskID, bucket(month), before, limit-len(out)).WithContext(ctx).Iter()
		for {
			var j domain.Job
			if !scanJob(it, &j) {
				break
			}
			out = append(out, j)
		}
		if err := it.Close(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Ping checks connectivity (readiness).
func (st *Store) Ping(ctx context.Context) error {
	err := st.s.Query(`SELECT now() FROM system.local`).WithContext(ctx).Exec()
	if errors.Is(err, gocql.ErrNotFound) {
		return nil
	}
	return err
}
