// Package mysql implements Crep's persistence ports on MySQL 8, whose jobs table is the work queue.
package mysql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"crep/internal/adapters/crypto"
	"crep/internal/app/ports"
	"crep/internal/domain"
)

// Open connects with the DSN options this adapter relies on: UTC times, parsed
// DATETIMEs, and found-rows semantics for fenced updates.
func Open(dsn string) (*sql.DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	cfg.ClientFoundRows = true
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params["time_zone"] = "'+00:00'"
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)
	return db, nil
}

type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type txKey struct{}

// Store implements ports.TxManager, ports.TaskRepo and ports.JobRepo.
type Store struct {
	db    *sql.DB
	codec crypto.RequestCodec
	clock ports.Clock
}

func NewStore(db *sql.DB, codec crypto.RequestCodec, clock ports.Clock) *Store {
	return &Store{db: db, codec: codec, clock: clock}
}

func (s *Store) TaskRepo() ports.TaskRepo { return taskRepo{s} }
func (s *Store) JobRepo() ports.JobRepo   { return jobRepo{s} }

func (s *Store) q(ctx context.Context) querier {
	if tx, ok := ctx.Value(txKey{}).(*sql.Tx); ok {
		return tx
	}
	return s.db
}

// WithinTx runs fn in a READ COMMITTED transaction, retrying on deadlock or lock-wait
// timeout. Nested calls join the outer transaction.
func (s *Store) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if _, ok := ctx.Value(txKey{}).(*sql.Tx); ok {
		return fn(ctx)
	}
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if err = s.runTx(ctx, fn); err == nil || !retryable(err) || ctx.Err() != nil {
			return err
		}
		time.Sleep(time.Duration(attempt+1) * 20 * time.Millisecond)
	}
	return err
}

func (s *Store) runTx(ctx context.Context, fn func(ctx context.Context) error) (err error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if err = fn(context.WithValue(ctx, txKey{}, tx)); err != nil {
		return err
	}
	return tx.Commit()
}

func mysqlCode(err error) uint16 {
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return me.Number
	}
	return 0
}

func retryable(err error) bool {
	c := mysqlCode(err)
	return c == 1213 || c == 1205 // deadlock, lock wait timeout
}

func isDuplicate(err error) bool { return mysqlCode(err) == 1062 }

// Ping checks connectivity (readiness).
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// ---- helpers ----

func nullTime(t *time.Time) sql.NullTime {
	if t == nil || t.IsZero() {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: t.UTC(), Valid: true}
}

func timePtr(n sql.NullTime) *time.Time {
	if !n.Valid {
		return nil
	}
	t := n.Time.UTC()
	return &t
}

func nullString(s string) sql.NullString { return sql.NullString{String: s, Valid: s != ""} }

func nullInt(n int) sql.NullInt64 { return sql.NullInt64{Int64: int64(n), Valid: n != 0} }

func inArgs(ids []string) (string, []any) {
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	return strings.TrimSuffix(strings.Repeat("?,", len(ids)), ","), args
}

func ms(d time.Duration) int64 { return d.Milliseconds() }

func dur(msv int64) time.Duration { return time.Duration(msv) * time.Millisecond }

// decodeRequest decrypts the stored `secret` blob into r. A failure is recorded in
// r.Unreadable rather than returned, so one bad row cannot block a whole batch.
func decodeRequest(c crypto.RequestCodec, blob []byte, r *domain.RequestSpec) {
	hs, body, bodySecret, err := c.Decode(blob)
	if err != nil {
		r.Unreadable = "request: " + err.Error()
		return
	}
	r.Headers, r.Body, r.BodySecret = hs, body, bodySecret
}

func encodeRequest(c crypto.RequestCodec, r domain.RequestSpec) ([]byte, error) {
	return c.Encode(r.Headers, r.Body, r.BodySecret)
}
