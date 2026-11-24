//go:build integration

// Integration tests against real MySQL and Cassandra. Run with:
//
//	CREP_TEST_MYSQL_DSN='crep:crep@tcp(127.0.0.1:3306)/crep_test' CREP_TEST_CASSANDRA_HOSTS=127.0.0.1 \
//	  go test -tags=integration ./internal/integration/
package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	"crep/internal/adapters/cassandra"
	"crep/internal/adapters/clock"
	"crep/internal/adapters/crypto"
	"crep/internal/adapters/httpcaller"
	"crep/internal/adapters/idgen"
	"crep/internal/adapters/mysql"
	"crep/internal/app/ports"
	"crep/internal/app/services"
	"crep/internal/domain"
)

type countingCaller struct {
	mu    sync.Mutex
	calls map[string]int
	code  atomic.Int32
	delay time.Duration
}

func (c *countingCaller) Call(ctx context.Context, _ domain.RequestSpec, m ports.CallMeta) ports.CallResult {
	if c.delay > 0 {
		select {
		case <-time.After(c.delay):
		case <-ctx.Done():
			return ports.CallResult{Err: ctx.Err()}
		}
	}
	c.mu.Lock()
	c.calls[m.JobID]++
	c.mu.Unlock()
	return ports.CallResult{StatusCode: int(c.code.Load())}
}

type harness struct {
	db     *sql.DB
	store  *mysql.Store
	caller *countingCaller
	deps   services.Deps
}

func setup(t *testing.T, clk ports.Clock) *harness {
	t.Helper()
	dsn := os.Getenv("CREP_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("CREP_TEST_MYSQL_DSN not set")
	}
	// setup wipes the tables, so refuse to touch anything but a dedicated test database.
	if cfg, err := gomysql.ParseDSN(dsn); err != nil || !strings.HasSuffix(cfg.DBName, "_test") {
		t.Fatalf("CREP_TEST_MYSQL_DSN must name a database ending in _test (tables are wiped)")
	}
	db, err := mysql.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	// Tables come from deploy/mysql-init (loaded into crep_test by the mysql container).
	for _, tbl := range []string{"job_attempts", "jobs", "tasks"} {
		if _, err := db.Exec("DELETE FROM " + tbl); err != nil {
			t.Fatal(err)
		}
	}
	keys, _ := crypto.ParseKeyring("k1:" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32)))
	st := mysql.NewStore(db, crypto.RequestCodec{K: keys}, clock.System{})
	cc := &countingCaller{calls: map[string]int{}}
	cc.code.Store(200)
	h := &harness{db: db, store: st, caller: cc}
	h.deps = services.Deps{Tx: st, Tasks: st.TaskRepo(), Jobs: st.JobRepo(), Caller: cc,
		Dest: httpcaller.Policy{}, Clock: clk, IDs: idgen.UUIDv7{}, Metrics: services.NopMetrics{},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	return h
}

func taskInput(kind domain.ScheduleType) services.TaskInput {
	in := services.TaskInput{Name: "it", Schedule: domain.Schedule{Type: kind},
		Request: domain.RequestSpec{Method: "POST", URL: "https://example.com/hook",
			Headers: []domain.Header{{Name: "Authorization", Value: "Bearer very-secret", Secret: true}},
			Body:    []byte(`{"card":"4111"}`), BodySecret: true},
		Retry: domain.RetryPolicy{MaxAttempts: 2, BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second}}
	if kind == domain.ScheduleCron {
		in.Schedule.Cron = "* * * * * *" // every second
	}
	return in
}

func TestTaskRoundTripAndEncryptionAtRest(t *testing.T) {
	h := setup(t, clock.System{})
	ctx := context.Background()
	ts := services.NewTaskService(h.deps)
	task, err := ts.Create(ctx, "acme", taskInput(domain.ScheduleCron))
	if err != nil {
		t.Fatal(err)
	}
	var secret []byte
	if err := h.db.QueryRow("SELECT secret FROM tasks WHERE id = ?", task.ID).Scan(&secret); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(secret, []byte("very-secret")) || bytes.Contains(secret, []byte("4111")) {
		t.Fatal("secrets stored in plaintext")
	}
	got, err := ts.Get(ctx, "acme", task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Request.Headers[0].Value != "Bearer very-secret" || string(got.Request.Body) != `{"card":"4111"}` ||
		got.Schedule.Cron != task.Schedule.Cron || !got.NextRunAt.Equal(*task.NextRunAt) || got.Priority != task.Priority {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}
	if _, err := ts.Get(ctx, "other", task.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("tenant isolation")
	}
	// Pagination.
	for i := 0; i < 4; i++ {
		if _, err := ts.Create(ctx, "acme", taskInput(domain.ScheduleCron)); err != nil {
			t.Fatal(err)
		}
	}
	seen := 0
	cursor := ""
	for {
		p, err := ts.List(ctx, "acme", cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		seen += len(p.Items)
		if p.NextCursor == "" {
			break
		}
		cursor = p.NextCursor
	}
	if seen != 5 {
		t.Fatalf("paged %d tasks", seen)
	}
}

func TestConcurrentSchedulersPlanEachFireOnce(t *testing.T) {
	h := setup(t, clock.System{})
	ctx := context.Background()
	ts := services.NewTaskService(h.deps)
	for i := 0; i < 20; i++ {
		if _, err := ts.Create(ctx, "acme", taskInput(domain.ScheduleCron)); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := services.NewScheduler(h.deps, services.SchedulerConfig{Lookahead: 5 * time.Second, Batch: 7})
			for k := 0; k < 3; k++ {
				if _, err := s.Tick(ctx); err != nil {
					errs <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var total, distinct int
	if err := h.db.QueryRow("SELECT COUNT(*), COUNT(DISTINCT task_id, scheduled_for) FROM jobs").Scan(&total, &distinct); err != nil {
		t.Fatal(err)
	}
	if total == 0 || total != distinct {
		t.Fatalf("total=%d distinct=%d", total, distinct)
	}
	if total < 20*5 { // ~5-6 fires per task in a 5s window
		t.Fatalf("expected ≥100 jobs, got %d", total)
	}
}

func TestConcurrentExecutorsRunEachJobOnce(t *testing.T) {
	h := setup(t, clock.System{})
	ctx := context.Background()
	ts := services.NewTaskService(h.deps)
	const n = 300
	for i := 0; i < n; i++ {
		in := taskInput(domain.ScheduleImmediate)
		in.Priority = domain.Priority(i % 4)
		tenant := []string{"a", "b", "c"}[i%3]
		if _, err := ts.Create(ctx, tenant, in); err != nil {
			t.Fatal(err)
		}
	}
	h.caller.delay = 5 * time.Millisecond
	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		ex := services.NewExecutor(h.deps, services.ExecutorConfig{ID: "exec-" + string(rune('a'+i)),
			Concurrency: 16, PollInterval: 20 * time.Millisecond})
		wg.Add(1)
		go func() { defer wg.Done(); ex.Run(runCtx) }()
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var done int
		if err := h.db.QueryRow("SELECT COUNT(*) FROM jobs WHERE status = 'succeeded'").Scan(&done); err != nil {
			t.Fatal(err)
		}
		if done == n {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	h.caller.mu.Lock()
	defer h.caller.mu.Unlock()
	if len(h.caller.calls) != n {
		t.Fatalf("called %d distinct jobs, want %d", len(h.caller.calls), n)
	}
	for id, c := range h.caller.calls {
		if c != 1 {
			t.Fatalf("job %s called %d times", id, c)
		}
	}
	var attempts int
	if err := h.db.QueryRow("SELECT COUNT(*) FROM job_attempts").Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != n {
		t.Fatalf("attempt rows=%d", attempts)
	}
}

func TestFencingRejectsStaleWriterAndReconcilerRecovers(t *testing.T) {
	clk := clock.NewFake(time.Now().UTC())
	h := setup(t, clk)
	ctx := context.Background()
	ts := services.NewTaskService(h.deps)
	if _, err := ts.Create(ctx, "acme", taskInput(domain.ScheduleImmediate)); err != nil {
		t.Fatal(err)
	}
	jobs := h.deps.Jobs
	// Claim directly as executor "old".
	var claimed domain.Job
	err := h.store.WithinTx(ctx, func(ctx context.Context) error {
		c, err := jobs.LockClaimable(ctx, clk.Now().Add(time.Second), ports.ClaimByPriority, 1)
		if err != nil || len(c) != 1 {
			return errors.New("no claimable job")
		}
		claimed = c[0]
		if _, err := claimed.Claim("old", clk.Now(), time.Second); err != nil {
			t.Fatal(err)
		}
		return jobs.MarkClaimed(ctx, []domain.Job{claimed})
	})
	if err != nil {
		t.Fatal(err)
	}
	oldFence := claimed.Fence()

	// Lease expires; reconciler requeues.
	clk.Advance(time.Hour)
	if n, err := services.NewReconciler(h.deps, services.ReconcilerConfig{}).Tick(ctx); err != nil || n != 1 {
		t.Fatalf("reconcile n=%d err=%v", n, err)
	}
	// A new executor completes attempt 2.
	ex := services.NewExecutor(h.deps, services.ExecutorConfig{ID: "new", Concurrency: 1})
	if n, err := ex.ClaimAndStart(ctx, ctx, 1); err != nil || n != 1 {
		t.Fatalf("claim n=%d err=%v", n, err)
	}
	ex.WaitIdle()

	// The old executor finally reports: must be rejected.
	stale := claimed
	stale.RecordOutcome(domain.OutcomeFailure, 400, "late", clk.Now(), func() float64 { return 0 })
	if err := jobs.RecordResult(ctx, stale, oldFence); !errors.Is(err, domain.ErrStaleLease) {
		t.Fatalf("expected stale lease, got %v", err)
	}
	final, err := jobs.Get(ctx, "acme", claimed.ID)
	if err != nil || final.Status != domain.JobSucceeded || final.Attempt != 2 {
		t.Fatalf("final=%+v err=%v", final, err)
	}
}

func TestRetryToDLQAndManualRetry(t *testing.T) {
	h := setup(t, clock.System{})
	ctx := context.Background()
	h.caller.code.Store(503)
	task, _ := services.NewTaskService(h.deps).Create(ctx, "acme", taskInput(domain.ScheduleImmediate))
	ex := services.NewExecutor(h.deps, services.ExecutorConfig{ID: "e", Concurrency: 2, Lookahead: 2 * time.Second})
	js := services.NewJobService(h.deps)
	var jobID string
	for i := 0; i < 50; i++ {
		if _, err := ex.ClaimAndStart(ctx, ctx, 2); err != nil {
			t.Fatal(err)
		}
		ex.WaitIdle()
		p, _ := js.ListDLQ(ctx, "acme", "", 10)
		if len(p.Items) == 1 {
			jobID = p.Items[0].ID
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if jobID == "" {
		t.Fatal("job never reached DLQ")
	}
	j, atts, _ := js.Get(ctx, "acme", jobID)
	if j.Attempt != 2 || len(atts) != 2 || !strings.Contains(j.Error, "503") {
		t.Fatalf("dead job: %+v attempts=%d", j, len(atts))
	}
	h.caller.code.Store(200)
	if _, err := js.Retry(ctx, "acme", jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := ex.ClaimAndStart(ctx, ctx, 1); err != nil {
		t.Fatal(err)
	}
	ex.WaitIdle()
	j, _, _ = js.Get(ctx, "acme", jobID)
	if j.Status != domain.JobSucceeded || j.Attempt != 3 || j.TaskID != task.ID {
		t.Fatalf("after retry: %+v", j)
	}
}

func TestArchiveToCassandraAndMergedHistory(t *testing.T) {
	hosts := os.Getenv("CREP_TEST_CASSANDRA_HOSTS")
	if hosts == "" {
		t.Skip("CREP_TEST_CASSANDRA_HOSTS not set")
	}
	clk := clock.NewFake(time.Now().UTC().Truncate(time.Second))
	h := setup(t, clk)
	ccfg := cassandra.Config{Hosts: strings.Split(hosts, ","), Keyspace: "crep_test", TTL: time.Hour}
	hist, err := cassandra.Open(ccfg)
	if err != nil {
		t.Fatal(err)
	}
	defer hist.Close()
	h.deps.History = hist
	ctx := context.Background()

	task, _ := services.NewTaskService(h.deps).Create(ctx, "acme", taskInput(domain.ScheduleCron))
	sched := services.NewScheduler(h.deps, services.SchedulerConfig{Lookahead: 10 * time.Second})
	ex := services.NewExecutor(h.deps, services.ExecutorConfig{ID: "e", Concurrency: 32})
	if _, err := sched.Tick(ctx); err != nil { // ~10 jobs
		t.Fatal(err)
	}
	clk.Advance(11 * time.Second)
	if _, err := ex.ClaimAndStart(ctx, ctx, 32); err != nil {
		t.Fatal(err)
	}
	ex.WaitIdle()

	var planned int
	if err := h.db.QueryRow("SELECT COUNT(*) FROM jobs WHERE status='succeeded'").Scan(&planned); err != nil {
		t.Fatal(err)
	}
	if planned < 9 {
		t.Fatalf("succeeded=%d", planned)
	}
	// Pause across the gap: resuming skips fires missed while paused, which keeps the
	// hot set small (an unpaused task would replay ~7200 missed fires here).
	ts := services.NewTaskService(h.deps)
	if _, err := ts.Pause(ctx, "acme", task.ID); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Hour)
	n, err := services.NewArchiver(h.deps, services.ArchiverConfig{Retention: time.Hour, Batch: 4}).Tick(ctx)
	if err != nil || n != planned {
		t.Fatalf("archived %d of %d: %v", n, planned, err)
	}
	var left int
	if err := h.db.QueryRow("SELECT COUNT(*) FROM jobs").Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Fatalf("%d rows left in MySQL", left)
	}
	if _, err := ts.Resume(ctx, "acme", task.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := sched.Tick(ctx); err != nil { // new hot jobs, ~10
		t.Fatal(err)
	}
	var hot int
	if err := h.db.QueryRow("SELECT COUNT(*) FROM jobs").Scan(&hot); err != nil {
		t.Fatal(err)
	}
	if hot > 12 {
		t.Fatalf("resume should skip fires missed while paused, got %d hot jobs", hot)
	}

	js := services.NewJobService(h.deps)
	var all []domain.Job
	cursor := ""
	for {
		p, err := js.History(ctx, "acme", task.ID, cursor, 4)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, p.Items...)
		if p.NextCursor == "" {
			break
		}
		cursor = p.NextCursor
	}
	if len(all) != planned+hot {
		t.Fatalf("history=%d want %d (+%d hot)", len(all), planned+hot, hot)
	}
	for i := 1; i < len(all); i++ {
		if !all[i].ScheduledFor.Before(all[i-1].ScheduledFor) {
			t.Fatal("history not strictly descending")
		}
	}
	last := all[len(all)-1]
	j, atts, err := js.Get(ctx, "acme", last.ID)
	if err != nil || j.Status != domain.JobSucceeded || len(atts) != 1 {
		t.Fatalf("archived get: %+v %d %v", j, len(atts), err)
	}
	if _, _, err := js.Get(ctx, "intruder", last.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("archived job visible to other tenant")
	}
	// Re-archiving the same jobs is idempotent.
	if err := hist.Save(ctx, []ports.ArchivedJob{{Job: j, Attempts: atts}}); err != nil {
		t.Fatal(err)
	}
}

func TestRowsEncryptedWithUnknownKeyDoNotBlockQueue(t *testing.T) {
	h := setup(t, clock.System{})
	ctx := context.Background()

	// Write a task and job with a key the running store does not have.
	otherKeys, _ := crypto.ParseKeyring("gone:" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{9}, 32)))
	other := mysql.NewStore(h.db, crypto.RequestCodec{K: otherKeys}, clock.System{})
	od := h.deps
	od.Tx, od.Tasks, od.Jobs = other, other.TaskRepo(), other.JobRepo()
	orphan, err := services.NewTaskService(od).Create(ctx, "acme", taskInput(domain.ScheduleImmediate))
	if err != nil {
		t.Fatal(err)
	}
	orphanCron, _ := services.NewTaskService(od).Create(ctx, "acme", taskInput(domain.ScheduleCron))
	var before []byte
	if err := h.db.QueryRow("SELECT secret FROM tasks WHERE id=?", orphanCron.ID).Scan(&before); err != nil {
		t.Fatal(err)
	}

	// And a normal job.
	if _, err := services.NewTaskService(h.deps).Create(ctx, "acme", taskInput(domain.ScheduleImmediate)); err != nil {
		t.Fatal(err)
	}

	if _, err := services.NewScheduler(h.deps, services.SchedulerConfig{Lookahead: 2 * time.Second}).Tick(ctx); err != nil {
		t.Fatalf("scheduler blocked: %v", err)
	}
	ex := services.NewExecutor(h.deps, services.ExecutorConfig{ID: "e", Concurrency: 8})
	if _, err := ex.ClaimAndStart(ctx, ctx, 8); err != nil {
		t.Fatalf("claim blocked: %v", err)
	}
	ex.WaitIdle()

	var states []string
	rows, err := h.db.Query("SELECT CONCAT(task_id = ?, ':', status) FROM jobs ORDER BY 1", orphan.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		states = append(states, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(states, ",") != "0:succeeded,1:dead" {
		t.Fatalf("states=%v (want good job succeeded, orphan dead-lettered)", states)
	}
	var status string
	var after []byte
	if err := h.db.QueryRow("SELECT status, secret FROM tasks WHERE id=?", orphanCron.ID).Scan(&status, &after); err != nil {
		t.Fatal(err)
	}
	if status != "paused" || !bytes.Equal(before, after) {
		t.Fatalf("orphan cron task: status=%s ciphertext preserved=%v", status, bytes.Equal(before, after))
	}
	// Pausing/resuming via the API must also keep the ciphertext.
	ts := services.NewTaskService(h.deps)
	if _, err := ts.Resume(ctx, "acme", orphanCron.ID); err != nil {
		t.Fatal(err)
	}
	if err := h.db.QueryRow("SELECT secret FROM tasks WHERE id=?", orphanCron.ID).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("ciphertext overwritten by task update")
	}
	// A PUT always carries a new request, which makes the task readable again.
	cur, _ := h.store.TaskRepo().Get(ctx, "acme", orphanCron.ID)
	if _, err := ts.Replace(ctx, "acme", orphanCron.ID, cur.Version, taskInput(domain.ScheduleCron)); err != nil {
		t.Fatal(err)
	}
	if got, err := ts.Get(ctx, "acme", orphanCron.ID); err != nil || got.Request.Unreadable != "" {
		t.Fatalf("after replace: %v %q", err, got.Request.Unreadable)
	}
}

func TestIdempotencyKeyIsUniquePerTenant(t *testing.T) {
	h := setup(t, clock.System{})
	ctx := context.Background()
	ts := services.NewTaskService(h.deps)
	first, created, err := ts.CreateIdempotent(ctx, "acme", "req-1", taskInput(domain.ScheduleCron))
	if err != nil || !created {
		t.Fatalf("first: %v %v", created, err)
	}
	again, created, err := ts.CreateIdempotent(ctx, "acme", "req-1", taskInput(domain.ScheduleCron))
	if err != nil || created || again.ID != first.ID || again.IdempotencyKey != "req-1" {
		t.Fatalf("replay: %v %+v %v", created, again, err)
	}
	// The unique index itself (as when two requests race past the lookup).
	dup := first
	dup.ID = "00000000-0000-0000-0000-000000000001"
	if err := h.store.TaskRepo().Create(ctx, dup); !errors.Is(err, domain.ErrDuplicate) {
		t.Fatalf("duplicate insert: %v", err)
	}
	if _, created, err := ts.CreateIdempotent(ctx, "other", "req-1", taskInput(domain.ScheduleCron)); err != nil || !created {
		t.Fatalf("other tenant: %v %v", created, err)
	}
	var n int
	if err := h.db.QueryRow("SELECT COUNT(*) FROM tasks WHERE idempotency_key = 'req-1'").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("rows with key: %d", n)
	}
}

// Covers the MySQL-specific archive SQL: the UNION ALL listing per status and the
// (id, attempt) guard on delete, which must keep a dead job retried after listing.
func TestArchiveDeadAfterWindowWithAttemptGuard(t *testing.T) {
	clk := clock.NewFake(time.Now().UTC().Truncate(time.Millisecond))
	h := setup(t, clk)
	ctx := context.Background()
	ts := services.NewTaskService(h.deps)
	h.caller.code.Store(500)
	for i := 0; i < 2; i++ {
		in := taskInput(domain.ScheduleImmediate)
		in.Retry.MaxAttempts = 1 // dead after one attempt
		if _, err := ts.Create(ctx, "acme", in); err != nil {
			t.Fatal(err)
		}
	}
	ex := services.NewExecutor(h.deps, services.ExecutorConfig{ID: "e", Concurrency: 4})
	if n, err := ex.ClaimAndStart(ctx, ctx, 4); err != nil || n != 2 {
		t.Fatalf("claim n=%d err=%v", n, err)
	}
	ex.WaitIdle()

	clk.Advance(2 * time.Hour) // past the 1h retention, inside the 14d DLQ window
	listed, err := h.deps.Jobs.ListArchivable(ctx, clk.Now().Add(-time.Hour), clk.Now().Add(-14*24*time.Hour), 10)
	if err != nil || len(listed) != 0 {
		t.Fatalf("dead jobs listed inside DLQ window: %d %v", len(listed), err)
	}
	clk.Advance(15 * 24 * time.Hour)
	listed, err = h.deps.Jobs.ListArchivable(ctx, clk.Now().Add(-time.Hour), clk.Now().Add(-14*24*time.Hour), 10)
	if err != nil || len(listed) != 2 {
		t.Fatalf("listed %d dead jobs, want 2: %v", len(listed), err)
	}
	// One of them is retried (attempt goes 1 -> 2) before the archiver deletes.
	h.caller.code.Store(500)
	if _, err := services.NewJobService(h.deps).Retry(ctx, "acme", listed[0].ID); err != nil {
		t.Fatal(err)
	}
	if n, err := ex.ClaimAndStart(ctx, ctx, 1); err != nil || n != 1 {
		t.Fatalf("reclaim n=%d err=%v", n, err)
	}
	ex.WaitIdle()
	if err := h.deps.Jobs.DeleteArchived(ctx, listed); err != nil {
		t.Fatal(err)
	}
	var jobsLeft, attemptsLeft int
	if err := h.db.QueryRow("SELECT COUNT(*) FROM jobs").Scan(&jobsLeft); err != nil {
		t.Fatal(err)
	}
	if err := h.db.QueryRow("SELECT COUNT(*) FROM job_attempts").Scan(&attemptsLeft); err != nil {
		t.Fatal(err)
	}
	if jobsLeft != 1 || attemptsLeft != 2 {
		t.Fatalf("jobs=%d attempts=%d; want the retried job and its 2 attempts kept", jobsLeft, attemptsLeft)
	}
}
