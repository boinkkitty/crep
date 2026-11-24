package services_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"crep/internal/adapters/clock"
	"crep/internal/adapters/httpcaller"
	"crep/internal/adapters/idgen"
	"crep/internal/adapters/memory"
	"crep/internal/app/ports"
	"crep/internal/app/services"
	"crep/internal/domain"
)

type fakeCaller struct {
	mu    sync.Mutex
	codes []int // status per call, last repeats
	calls []ports.CallMeta
	block chan struct{}
}

func (f *fakeCaller) Call(ctx context.Context, _ domain.RequestSpec, m ports.CallMeta) ports.CallResult {
	if f.block != nil {
		<-f.block
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, m)
	code := f.codes[min(len(f.calls)-1, len(f.codes)-1)]
	return ports.CallResult{StatusCode: code}
}

type env struct {
	store  *memory.Store
	clock  *clock.Fake
	caller *fakeCaller
	deps   services.Deps
	tasks  *services.TaskService
	jobs   *services.JobService
}

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func newEnv(codes ...int) *env {
	if len(codes) == 0 {
		codes = []int{200}
	}
	st := memory.New()
	e := &env{store: st, clock: clock.NewFake(t0), caller: &fakeCaller{codes: codes}}
	e.deps = services.Deps{
		Tx: st, Tasks: st.TaskRepo(), Jobs: st.JobRepo(), History: st.HistoryStore(),
		Caller: e.caller, Dest: httpcaller.Policy{}, Clock: e.clock, IDs: idgen.UUIDv7{},
		Metrics: services.NopMetrics{}, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	e.tasks = services.NewTaskService(e.deps)
	e.jobs = services.NewJobService(e.deps)
	return e
}

func (e *env) executor(id string) *services.Executor {
	return services.NewExecutor(e.deps, services.ExecutorConfig{ID: id, Concurrency: 8, LeaseGrace: 5 * time.Second})
}

func (e *env) runOnce(t *testing.T, ex *services.Executor) int {
	t.Helper()
	n, err := ex.ClaimAndStart(context.Background(), context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	ex.WaitIdle()
	return n
}

func input(kind domain.ScheduleType) services.TaskInput {
	in := services.TaskInput{Name: "hook", Schedule: domain.Schedule{Type: kind},
		Request: domain.RequestSpec{URL: "https://example.com/hook"},
		Retry:   domain.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Second, MaxDelay: time.Minute}}
	switch kind {
	case domain.ScheduleCron:
		in.Schedule.Cron = "*/10 * * * * *" // every 10 seconds
	case domain.ScheduleOnce:
		in.Schedule.RunAt = t0.Add(time.Minute)
	}
	return in
}

func TestImmediateTaskCreatesJobInSameTx(t *testing.T) {
	e := newEnv()
	task, err := e.tasks.Create(context.Background(), "ten", input(domain.ScheduleImmediate))
	if err != nil {
		t.Fatal(err)
	}
	jobs := e.store.Snapshot()
	if len(jobs) != 1 || jobs[0].TaskID != task.ID || !jobs[0].ScheduledFor.Equal(t0) || task.Status != domain.TaskCompleted {
		t.Fatalf("jobs=%+v task=%+v", jobs, task)
	}
	if n := e.runOnce(t, e.executor("a")); n != 1 {
		t.Fatalf("claimed %d", n)
	}
	if j := e.store.Snapshot()[0]; j.Status != domain.JobSucceeded || j.Attempt != 1 {
		t.Fatalf("%+v", j)
	}
	if e.caller.calls[0].JobID != jobs[0].ID {
		t.Fatal("idempotency key must be the job id")
	}
}

func TestCreateRejectsPrivateDestination(t *testing.T) {
	e := newEnv()
	in := input(domain.ScheduleImmediate)
	in.Request.URL = "http://169.254.169.254/latest/meta-data"
	if _, err := e.tasks.Create(context.Background(), "ten", in); !domain.IsValidation(err) {
		t.Fatalf("expected validation error, got %v", err)
	}
	if len(e.store.Snapshot()) != 0 {
		t.Fatal("no job may be created")
	}
}

func TestSchedulerPlansWindowOnceAcrossInstances(t *testing.T) {
	e := newEnv()
	if _, err := e.tasks.Create(context.Background(), "ten", input(domain.ScheduleCron)); err != nil {
		t.Fatal(err)
	}
	s1 := services.NewScheduler(e.deps, services.SchedulerConfig{})
	s2 := services.NewScheduler(e.deps, services.SchedulerConfig{})
	n1, _ := s1.Tick(context.Background())
	n2, _ := s2.Tick(context.Background())
	// Fires at :10, :20, :30 are within 30s of t0.
	if n1 != 3 || n2 != 0 || len(e.store.Snapshot()) != 3 {
		t.Fatalf("n1=%d n2=%d jobs=%d", n1, n2, len(e.store.Snapshot()))
	}
	e.clock.Advance(10 * time.Second)
	if n, _ := s2.Tick(context.Background()); n != 1 {
		t.Fatalf("next window planned %d", n)
	}
}

func TestPausedTaskIsNotPlanned(t *testing.T) {
	e := newEnv()
	task, _ := e.tasks.Create(context.Background(), "ten", input(domain.ScheduleCron))
	if _, err := e.tasks.Pause(context.Background(), "ten", task.ID); err != nil {
		t.Fatal(err)
	}
	s := services.NewScheduler(e.deps, services.SchedulerConfig{})
	if n, _ := s.Tick(context.Background()); n != 0 {
		t.Fatalf("planned %d while paused", n)
	}
	e.clock.Advance(time.Hour)
	if _, err := e.tasks.Resume(context.Background(), "ten", task.ID); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.Tick(context.Background()); n != 3 {
		t.Fatalf("after resume planned %d (missed fires must be skipped)", n)
	}
}

func TestDeleteCancelsPendingJobs(t *testing.T) {
	e := newEnv()
	task, _ := e.tasks.Create(context.Background(), "ten", input(domain.ScheduleCron))
	if _, err := services.NewScheduler(e.deps, services.SchedulerConfig{}).Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := e.tasks.Delete(context.Background(), "ten", task.ID); err != nil {
		t.Fatal(err)
	}
	for _, j := range e.store.Snapshot() {
		if j.Status != domain.JobCancelled {
			t.Fatalf("job %s is %s", j.ID, j.Status)
		}
	}
	if _, err := e.tasks.Get(context.Background(), "ten", task.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("deleted task must be hidden")
	}
}

func TestTenantIsolation(t *testing.T) {
	e := newEnv()
	task, _ := e.tasks.Create(context.Background(), "a", input(domain.ScheduleCron))
	if _, err := e.tasks.Get(context.Background(), "b", task.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("tenant b must not see tenant a's task")
	}
	if err := e.tasks.Delete(context.Background(), "b", task.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("tenant b must not delete tenant a's task")
	}
}

func TestRetryBackoffThenDLQThenManualRetry(t *testing.T) {
	e := newEnv(500, 500, 500, 200)
	if _, err := e.tasks.Create(context.Background(), "ten", input(domain.ScheduleImmediate)); err != nil {
		t.Fatal(err)
	}
	ex := e.executor("a")
	for i := 0; i < 3; i++ {
		if n := e.runOnce(t, ex); n != 1 {
			t.Fatalf("attempt %d: claimed %d", i+1, n)
		}
		e.clock.Advance(2 * time.Minute) // past any backoff
	}
	j := e.store.Snapshot()[0]
	if j.Status != domain.JobDead || j.Attempt != 3 {
		t.Fatalf("expected dead after 3 attempts: %+v", j)
	}
	dlq, _ := e.jobs.ListDLQ(context.Background(), "ten", "", 10)
	if len(dlq.Items) != 1 {
		t.Fatalf("dlq=%d", len(dlq.Items))
	}
	if _, err := e.jobs.Retry(context.Background(), "ten", j.ID); err != nil {
		t.Fatal(err)
	}
	e.runOnce(t, ex)
	j, atts, _ := e.jobs.Get(context.Background(), "ten", j.ID)
	if j.Status != domain.JobSucceeded || j.Attempt != 4 || len(atts) != 4 {
		t.Fatalf("after manual retry: %+v attempts=%d", j, len(atts))
	}
}

func TestBackoffDelaysNextClaim(t *testing.T) {
	e := newEnv(503, 200)
	if _, err := e.tasks.Create(context.Background(), "ten", input(domain.ScheduleImmediate)); err != nil {
		t.Fatal(err)
	}
	ex := e.executor("a")
	e.runOnce(t, ex)
	// Backoff for attempt 1 (base 1s) is in [0.5s, 1s].
	j := e.store.Snapshot()[0]
	if j.Status != domain.JobPending || j.AvailableAt.Before(t0.Add(500*time.Millisecond)) || j.AvailableAt.After(t0.Add(time.Second)) {
		t.Fatalf("backoff: %+v", j)
	}
}

func TestStaleExecutorResultIsFenced(t *testing.T) {
	e := newEnv(200)
	e.caller.block = make(chan struct{})
	if _, err := e.tasks.Create(context.Background(), "ten", input(domain.ScheduleImmediate)); err != nil {
		t.Fatal(err)
	}
	slow := e.executor("slow")
	if n, _ := slow.ClaimAndStart(context.Background(), context.Background(), 1); n != 1 {
		t.Fatal("claim")
	}
	// Executor "slow" hangs past its lease; reconciler requeues, another executor finishes.
	e.clock.Advance(time.Hour)
	rec := services.NewReconciler(e.deps, services.ReconcilerConfig{})
	if n, err := rec.Tick(context.Background()); n != 1 || err != nil {
		t.Fatalf("reconciled %d %v", n, err)
	}
	j := e.store.Snapshot()[0]
	if j.Status != domain.JobPending || j.Attempt != 1 {
		t.Fatalf("after reconcile: %+v", j)
	}
	fast := e.executor("fast")
	fastDone := make(chan struct{})
	go func() { e.runOnce(t, fast); close(fastDone) }()
	e.caller.block <- struct{}{} // whichever call gets it first
	e.caller.block <- struct{}{}
	<-fastDone
	slow.WaitIdle()
	j = e.store.Snapshot()[0]
	if j.Status != domain.JobSucceeded || j.Attempt != 2 || j.WorkerID != "" {
		t.Fatalf("final: %+v", j)
	}
	_, atts, _ := e.jobs.Get(context.Background(), "ten", j.ID)
	if len(atts) != 2 { // lease-expired attempt 1 + successful attempt 2; slow's write dropped
		t.Fatalf("attempts=%+v", atts)
	}
}

func TestReconcilerDeadLettersWhenAttemptsExhausted(t *testing.T) {
	e := newEnv()
	in := input(domain.ScheduleImmediate)
	in.Retry.MaxAttempts = 1
	e.caller.block = make(chan struct{})
	if _, err := e.tasks.Create(context.Background(), "ten", in); err != nil {
		t.Fatal(err)
	}
	ex := e.executor("a")
	if _, err := ex.ClaimAndStart(context.Background(), context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(time.Hour)
	if _, err := services.NewReconciler(e.deps, services.ReconcilerConfig{}).Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(e.caller.block)
	ex.WaitIdle()
	if j := e.store.Snapshot()[0]; j.Status != domain.JobDead {
		t.Fatalf("%+v", j)
	}
}

func TestPriorityOrderAndStarvationGuard(t *testing.T) {
	e := newEnv()
	ctx := context.Background()
	low := input(domain.ScheduleImmediate)
	low.Priority = domain.PriorityLow
	if _, err := e.tasks.Create(ctx, "ten", low); err != nil { // older, low priority
		t.Fatal(err)
	}
	e.clock.Advance(time.Second)
	for i := 0; i < 10; i++ {
		hi := input(domain.ScheduleImmediate)
		hi.Priority = domain.PriorityCritical
		if _, err := e.tasks.Create(ctx, "ten", hi); err != nil {
			t.Fatal(err)
		}
	}
	ex := services.NewExecutor(e.deps, services.ExecutorConfig{ID: "a", Concurrency: 1, OldestDueEvery: 3})
	lowDoneAfter := func() bool {
		for _, j := range e.store.Snapshot() {
			if j.Status == domain.JobSucceeded && j.Priority == domain.PriorityLow {
				return true
			}
		}
		return false
	}
	for i := 1; i <= 3; i++ {
		if _, err := ex.ClaimAndStart(ctx, ctx, 1); err != nil {
			t.Fatal(err)
		}
		ex.WaitIdle()
		// Claims 1-2 go by priority (critical); claim 3 is oldest-due (the low job).
		if got, want := lowDoneAfter(), i == 3; got != want {
			t.Fatalf("after claim %d: low job done=%v, want %v", i, got, want)
		}
	}
}

func TestPickFair(t *testing.T) {
	mk := func(tenants ...string) []domain.Job {
		out := make([]domain.Job, len(tenants))
		for i, tn := range tenants {
			out[i] = domain.Job{ID: string(rune('a' + i)), TenantID: tn}
		}
		return out
	}
	// Tenant A floods the queue; B has one job at the end. Capacity 4 → share 2 each.
	got := services.PickFair(mk("A", "A", "A", "A", "A", "B"), nil, 4, 4, 4)
	count := map[string]int{}
	for _, j := range got {
		count[j.TenantID]++
	}
	if len(got) != 4 || count["B"] != 1 || count["A"] != 3 {
		t.Fatalf("got %v", count)
	}
	// Work-conserving: single tenant may use all slots up to the hard limit.
	if got := services.PickFair(mk("A", "A", "A"), nil, 3, 4, 2); len(got) != 2 {
		t.Fatalf("hard limit: %d", len(got))
	}
	// Busy tenants count against their share.
	got = services.PickFair(mk("A", "A", "B"), map[string]int{"A": 2}, 2, 4, 4)
	if len(got) != 2 || got[0].TenantID != "B" && got[1].TenantID != "B" {
		t.Fatalf("busy: %+v", got)
	}
}

func TestArchiverAndHistoryMerge(t *testing.T) {
	e := newEnv()
	ctx := context.Background()
	task, _ := e.tasks.Create(ctx, "ten", input(domain.ScheduleCron))
	sched := services.NewScheduler(e.deps, services.SchedulerConfig{})
	ex := e.executor("a")
	for i := 0; i < 4; i++ { // 4 windows of 30s -> 12 jobs
		if _, err := sched.Tick(ctx); err != nil {
			t.Fatal(err)
		}
		e.clock.Advance(30 * time.Second)
		e.runOnce(t, ex)
	}
	all := e.store.Snapshot()
	if len(all) != 12 {
		t.Fatalf("jobs=%d", len(all))
	}
	e.clock.Advance(2 * time.Hour)
	arch := services.NewArchiver(e.deps, services.ArchiverConfig{Retention: time.Hour, Batch: 5})
	n, err := arch.Tick(ctx)
	if err != nil || n != 12 || len(e.store.Snapshot()) != 0 {
		t.Fatalf("archived %d err=%v remaining=%d", n, err, len(e.store.Snapshot()))
	}
	// New hot job plus 12 archived ones, paginated across both stores.
	if _, err := sched.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	var seen []domain.Job
	cursor := ""
	for {
		p, err := e.jobs.History(ctx, "ten", task.ID, cursor, 5)
		if err != nil {
			t.Fatal(err)
		}
		seen = append(seen, p.Items...)
		if p.NextCursor == "" {
			break
		}
		cursor = p.NextCursor
	}
	// 12 archived + 723 hot: the 720 fires missed during the 2h gap (every 10s) are
	// all replayed, then 3 in the window.
	if len(seen) != 735 {
		t.Fatalf("history len=%d", len(seen))
	}
	for i := 1; i < len(seen); i++ {
		if !seen[i].ScheduledFor.Before(seen[i-1].ScheduledFor) {
			t.Fatal("history must be strictly newest-first")
		}
	}
	// Archived job is still retrievable by ID.
	if _, atts, err := e.jobs.Get(ctx, "ten", all[0].ID); err != nil || len(atts) != 1 {
		t.Fatalf("get archived: %v %d", err, len(atts))
	}
}

func TestExecutorWaitsForDueTimeAndShutsDownCleanly(t *testing.T) {
	e := newEnv()
	ctx, cancel := context.WithCancel(context.Background())
	in := input(domain.ScheduleOnce)
	in.Schedule.RunAt = t0.Add(time.Second)
	if _, err := e.tasks.Create(ctx, "ten", in); err != nil {
		t.Fatal(err)
	}
	if _, err := services.NewScheduler(e.deps, services.SchedulerConfig{}).Tick(ctx); err != nil {
		t.Fatal(err)
	}
	ex := services.NewExecutor(e.deps, services.ExecutorConfig{ID: "a", Concurrency: 1, PollInterval: 10 * time.Millisecond})
	done := make(chan struct{})
	go func() { ex.Run(ctx); close(done) }()
	time.Sleep(100 * time.Millisecond) // claimed (within lookahead), now waiting ~1s
	cancel()
	<-done
	j := e.store.Snapshot()[0]
	if j.Status != domain.JobPending || j.Attempt != 0 || len(e.caller.calls) != 0 {
		t.Fatalf("job should be released untouched on shutdown: %+v", j)
	}
}

func TestUnreadableRequestsDoNotBlockTheQueue(t *testing.T) {
	e := newEnv()
	ctx := context.Background()
	good, _ := e.tasks.Create(ctx, "ten", input(domain.ScheduleImmediate))

	// Simulate rows storage could not decrypt (e.g. key removed from the keyring).
	badTask := domain.Task{ID: "bad-task", TenantID: "ten", Name: "bad", Status: domain.TaskActive,
		Schedule: domain.Schedule{Type: domain.ScheduleCron, Cron: "* * * * * *"},
		Request:  domain.RequestSpec{Method: "POST", URL: "https://example.com", Timeout: time.Second, Unreadable: "unknown key id"},
		Retry:    domain.DefaultRetryPolicy()}
	next := t0
	badTask.NextRunAt = &next
	if err := e.deps.Tasks.Create(ctx, badTask); err != nil {
		t.Fatal(err)
	}
	badJob := domain.NewJob("bad-job", badTask, t0, t0)
	if _, err := e.deps.Jobs.Insert(ctx, badJob); err != nil {
		t.Fatal(err)
	}

	if _, err := services.NewScheduler(e.deps, services.SchedulerConfig{}).Tick(ctx); err != nil {
		t.Fatalf("scheduler must not fail on an unreadable task: %v", err)
	}
	if bt, _ := e.deps.Tasks.Get(ctx, "ten", "bad-task"); bt.Status != domain.TaskPaused {
		t.Fatalf("unreadable task should be paused, got %s", bt.Status)
	}

	e.runOnce(t, e.executor("a"))
	bj, _ := e.deps.Jobs.Get(ctx, "ten", "bad-job")
	if bj.Status != domain.JobDead || !strings.Contains(bj.Error, "unreadable") {
		t.Fatalf("unreadable job should be dead-lettered: %+v", bj)
	}
	for _, c := range e.caller.calls {
		if c.JobID == "bad-job" {
			t.Fatal("unreadable job must not be sent")
		}
	}
	page, _ := e.jobs.History(ctx, "ten", good.ID, "", 10)
	if len(page.Items) != 1 || page.Items[0].Status != domain.JobSucceeded {
		t.Fatalf("good job must still run: %+v", page.Items)
	}
}

func TestCreateIdempotencyKeyReplays(t *testing.T) {
	e := newEnv()
	ctx := context.Background()
	first, created, err := e.tasks.CreateIdempotent(ctx, "ten", "k1", input(domain.ScheduleImmediate))
	if err != nil || !created {
		t.Fatalf("first: %v %v", created, err)
	}
	again, created, err := e.tasks.CreateIdempotent(ctx, "ten", "k1", input(domain.ScheduleImmediate))
	if err != nil || created || again.ID != first.ID {
		t.Fatalf("replay: created=%v id=%s want %s err=%v", created, again.ID, first.ID, err)
	}
	if n := len(e.store.Snapshot()); n != 1 {
		t.Fatalf("replay planned another job: %d jobs", n)
	}
	other, created, err := e.tasks.CreateIdempotent(ctx, "other", "k1", input(domain.ScheduleImmediate))
	if err != nil || !created || other.ID == first.ID {
		t.Fatalf("keys must be per tenant: %v %v", created, err)
	}
	if _, _, err := e.tasks.CreateIdempotent(ctx, "ten", strings.Repeat("x", 256), input(domain.ScheduleCron)); !domain.IsValidation(err) {
		t.Fatalf("long key: %v", err)
	}
}

func TestSchedulerReplaysEveryMissedFireInCappedBatches(t *testing.T) {
	e := newEnv()
	ctx := context.Background()
	in := input(domain.ScheduleCron)
	in.Schedule.Cron = "* * * * * *" // every second
	if _, err := e.tasks.Create(ctx, "ten", in); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(time.Hour) // scheduler down for an hour
	sched := services.NewScheduler(e.deps, services.SchedulerConfig{Lookahead: 10 * time.Second})
	n, err := sched.Tick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// 3600 missed + 10 in the look-ahead window, planned in batches of MaxCatchUpPerTick.
	if n < 3600 || n > 3612 {
		t.Fatalf("planned %d, want every missed fire", n)
	}
	jobs := e.store.Snapshot()
	for i := 1; i < len(jobs); i++ {
		if d := jobs[i].ScheduledFor.Sub(jobs[i-1].ScheduledFor); d != time.Second {
			t.Fatalf("gap of %v between fires %d and %d", d, i-1, i)
		}
	}
	if again, _ := sched.Tick(ctx); again != 0 {
		t.Fatalf("second tick planned %d duplicates", again)
	}
}

func TestDeadJobsStayRetryableThenArchive(t *testing.T) {
	e := newEnv(500, 500, 500)
	ctx := context.Background()
	if _, err := e.tasks.Create(ctx, "ten", input(domain.ScheduleImmediate)); err != nil {
		t.Fatal(err)
	}
	ex := e.executor("a")
	for i := 0; i < 3; i++ {
		e.runOnce(t, ex)
		e.clock.Advance(2 * time.Minute)
	}
	j := e.store.Snapshot()[0]
	if j.Status != domain.JobDead {
		t.Fatalf("want dead: %+v", j)
	}
	arch := services.NewArchiver(e.deps, services.ArchiverConfig{Retention: time.Hour, DeadRetention: 14 * 24 * time.Hour})

	e.clock.Advance(13 * 24 * time.Hour) // still inside the DLQ window
	if n, _ := arch.Tick(ctx); n != 0 {
		t.Fatalf("archived %d dead jobs inside the DLQ window", n)
	}
	e.clock.Advance(2 * 24 * time.Hour)
	if n, err := arch.Tick(ctx); err != nil || n != 1 || len(e.store.Snapshot()) != 0 {
		t.Fatalf("archived %d err=%v remaining=%d", n, err, len(e.store.Snapshot()))
	}
	got, atts, err := e.jobs.Get(ctx, "ten", j.ID) // now served from history
	if err != nil || got.Status != domain.JobDead || len(atts) != 3 {
		t.Fatalf("archived dead job: %v %+v attempts=%d", err, got, len(atts))
	}
	if _, err := e.jobs.Retry(ctx, "ten", j.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("retry after the DLQ window should be not found, got %v", err)
	}
}

func TestArchiverKeepsJobRetriedAfterListing(t *testing.T) {
	e := newEnv(500, 500, 500)
	ctx := context.Background()
	if _, err := e.tasks.Create(ctx, "ten", input(domain.ScheduleImmediate)); err != nil {
		t.Fatal(err)
	}
	ex := e.executor("a")
	for i := 0; i < 3; i++ {
		e.runOnce(t, ex)
		e.clock.Advance(2 * time.Minute)
	}
	stale := e.store.Snapshot()[0] // what the archiver listed
	if _, err := e.jobs.Retry(ctx, "ten", stale.ID); err != nil {
		t.Fatal(err)
	}
	e.runOnce(t, ex) // a fresh attempt, dead again but at attempt 4
	if err := e.deps.Jobs.DeleteArchived(ctx, []domain.Job{stale}); err != nil {
		t.Fatal(err)
	}
	if len(e.store.Snapshot()) != 1 {
		t.Fatal("a job retried after being listed must not be deleted")
	}
}
