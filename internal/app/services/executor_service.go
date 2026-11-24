package services

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"crep/internal/app/ports"
	"crep/internal/domain"
)

// ExecutorConfig tunes an executor process.
type ExecutorConfig struct {
	ID             string        // unique per process; used as lease owner
	Concurrency    int           // max in-flight jobs
	PerTenantLimit int           // hard cap of in-flight jobs per tenant (0 = Concurrency)
	Lookahead      time.Duration // claim jobs due within this window, then wait for their start time
	LeaseGrace     time.Duration // lease = start + request timeout + grace
	PollInterval   time.Duration // idle wait between empty claims
	OldestDueEvery int           // every Nth claim ignores priority (starvation guard)
	Overfetch      int           // candidates locked per free slot, for fair selection
	DrainTimeout   time.Duration // on shutdown, how long in-flight calls may finish
}

func (c *ExecutorConfig) defaults() {
	if c.Concurrency <= 0 {
		c.Concurrency = 64
	}
	if c.PerTenantLimit <= 0 || c.PerTenantLimit > c.Concurrency {
		c.PerTenantLimit = c.Concurrency
	}
	if c.Lookahead == 0 {
		c.Lookahead = 2 * time.Second
	}
	if c.LeaseGrace == 0 {
		c.LeaseGrace = 30 * time.Second
	}
	if c.PollInterval == 0 {
		c.PollInterval = 500 * time.Millisecond
	}
	if c.OldestDueEvery <= 0 {
		c.OldestDueEvery = 5
	}
	if c.Overfetch <= 0 {
		c.Overfetch = 4
	}
	if c.DrainTimeout == 0 {
		c.DrainTimeout = 30 * time.Second
	}
}

// Executor claims jobs from the MySQL queue, performs their HTTP calls, and records results.
type Executor struct {
	d   Deps
	cfg ExecutorConfig

	mu        sync.Mutex
	inflight  int
	perTenant map[string]int
	claims    uint64
	freed     chan struct{} // signalled when a worker finishes
	wg        sync.WaitGroup
	stopping  chan struct{}
}

func NewExecutor(d Deps, cfg ExecutorConfig) *Executor {
	cfg.defaults()
	return &Executor{d: d, cfg: cfg, perTenant: map[string]int{}, freed: make(chan struct{}, 1), stopping: make(chan struct{})}
}

// Run claims and executes jobs until ctx is cancelled, then drains in-flight work.
func (e *Executor) Run(ctx context.Context) {
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()

	for ctx.Err() == nil {
		free := e.free()
		if free == 0 {
			e.wait(ctx, 0)
			continue
		}
		n, err := e.ClaimAndStart(ctx, workCtx, free)
		if err != nil && ctx.Err() == nil {
			e.d.Log.Error("claim failed", "err", err)
		}
		if n < free {
			e.wait(ctx, e.cfg.PollInterval)
		}
	}

	close(e.stopping)
	done := make(chan struct{})
	go func() { e.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(e.cfg.DrainTimeout):
		e.d.Log.Warn("drain timeout; abandoning in-flight calls to lease recovery")
		cancelWork()
		<-done
	}
}

func (e *Executor) wait(ctx context.Context, d time.Duration) {
	var timer <-chan time.Time
	if d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		timer = t.C
	}
	select {
	case <-ctx.Done():
	case <-e.freed:
	case <-timer:
	}
}

func (e *Executor) free() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.cfg.Concurrency - e.inflight
}

// ClaimAndStart claims up to want jobs and starts a worker for each. Exposed for tests.
func (e *Executor) ClaimAndStart(ctx, workCtx context.Context, want int) (int, error) {
	jobs, err := e.claim(ctx, want)
	if err != nil {
		return 0, err
	}
	for _, j := range jobs {
		e.start(workCtx, j)
	}
	return len(jobs), nil
}

func (e *Executor) claim(ctx context.Context, want int) ([]domain.Job, error) {
	e.mu.Lock()
	e.claims++
	order := ports.ClaimByPriority
	if e.claims%uint64(e.cfg.OldestDueEvery) == 0 {
		order = ports.ClaimOldestDue
	}
	busy := make(map[string]int, len(e.perTenant))
	for k, v := range e.perTenant {
		busy[k] = v
	}
	e.mu.Unlock()

	var chosen []domain.Job
	err := e.d.Tx.WithinTx(ctx, func(ctx context.Context) error {
		now := e.d.Clock.Now()
		cands, err := e.d.Jobs.LockClaimable(ctx, now.Add(e.cfg.Lookahead), order, want*e.cfg.Overfetch)
		if err != nil {
			return err
		}
		chosen = PickFair(cands, busy, want, e.cfg.Concurrency, e.cfg.PerTenantLimit)
		for i := range chosen {
			if _, err := chosen[i].Claim(e.cfg.ID, now, e.cfg.LeaseGrace); err != nil {
				return err
			}
		}
		if len(chosen) == 0 {
			return nil
		}
		return e.d.Jobs.MarkClaimed(ctx, chosen)
	})
	if err != nil {
		return nil, err
	}
	if len(chosen) > 0 {
		e.d.Metrics.JobsClaimed(len(chosen))
	}
	return chosen, nil
}

// PickFair selects up to want jobs from candidates (already in claim order) while
// sharing capacity across tenants. Pass 1 respects each tenant's fair share
// (capacity / active tenants); pass 2 fills leftover slots up to the hard per-tenant
// limit so capacity is never left idle. Order within candidates (priority) is kept.
func PickFair(cands []domain.Job, busy map[string]int, want, capacity, perTenantLimit int) []domain.Job {
	if want <= 0 || len(cands) == 0 {
		return nil
	}
	active := map[string]bool{}
	for t, n := range busy {
		if n > 0 {
			active[t] = true
		}
	}
	for _, c := range cands {
		active[c.TenantID] = true
	}
	share := capacity / len(active)
	if share < 1 {
		share = 1
	}
	if share > perTenantLimit {
		share = perTenantLimit
	}

	taken := make(map[string]int)
	used := make([]bool, len(cands))
	var out []domain.Job
	for _, limit := range []int{share, perTenantLimit} {
		for i, c := range cands {
			if len(out) == want {
				return out
			}
			if used[i] || busy[c.TenantID]+taken[c.TenantID] >= limit {
				continue
			}
			used[i] = true
			taken[c.TenantID]++
			out = append(out, c)
		}
	}
	return out
}

func (e *Executor) start(workCtx context.Context, j domain.Job) {
	e.mu.Lock()
	e.inflight++
	e.perTenant[j.TenantID]++
	e.mu.Unlock()
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer e.finish(j.TenantID)
		e.execute(workCtx, j)
	}()
}

func (e *Executor) finish(tenant string) {
	e.mu.Lock()
	e.inflight--
	if e.perTenant[tenant]--; e.perTenant[tenant] <= 0 {
		delete(e.perTenant, tenant)
	}
	e.mu.Unlock()
	select {
	case e.freed <- struct{}{}:
	default:
	}
}

func (e *Executor) execute(ctx context.Context, j domain.Job) {
	log := e.d.Log.With("job_id", j.ID, "task_id", j.TaskID, "attempt", j.Attempt)
	fence := j.Fence()

	// Wait for the due time; on shutdown, hand the job back without consuming an attempt.
	if d := j.StartAt().Sub(e.d.Clock.Now()); d > 0 {
		t := time.NewTimer(d)
		select {
		case <-t.C:
		case <-e.stopping:
			t.Stop()
			j.Release()
			if err := e.writeResult(ctx, j, fence, nil); err != nil && !errors.Is(err, domain.ErrStaleLease) {
				log.Error("release on shutdown failed", "err", err)
			}
			return
		case <-ctx.Done():
			t.Stop()
			return
		}
	}

	if j.Request.Unreadable != "" {
		now := e.d.Clock.Now()
		msg := "stored request is unreadable: " + j.Request.Unreadable
		j.DeadLetter(msg, now)
		att := &domain.Attempt{JobID: j.ID, Attempt: fence.Attempt, StartedAt: now, FinishedAt: now, Error: msg}
		if err := e.writeResult(ctx, j, fence, att); err != nil {
			log.Error("dead-lettering unreadable job failed", "err", err)
		} else {
			e.d.Metrics.JobFinished(j.Status, j.Attempt)
			log.Error("dead-lettered job with unreadable request", "reason", j.Request.Unreadable)
		}
		return
	}

	started := e.d.Clock.Now()
	if j.Attempt == 1 {
		e.d.Metrics.StartLag(started.Sub(j.ScheduledFor))
	}
	callCtx, cancel := context.WithTimeout(ctx, j.Request.Timeout)
	res := e.d.Caller.Call(callCtx, j.Request, ports.CallMeta{JobID: j.ID, TaskID: j.TaskID, Attempt: j.Attempt, ScheduledFor: j.ScheduledFor})
	cancel()
	finished := e.d.Clock.Now()

	errMsg := ""
	switch {
	case res.Err != nil:
		errMsg = res.Err.Error()
	case res.StatusCode < 200 || res.StatusCode >= 300:
		errMsg = fmt.Sprintf("HTTP %d: %s", res.StatusCode, res.BodySnip)
	}
	if len(errMsg) > 1000 {
		errMsg = errMsg[:1000]
	}
	outcome := domain.Classify(res.StatusCode, res.Err)
	j.RecordOutcome(outcome, res.StatusCode, errMsg, finished, rand.Float64)

	att := &domain.Attempt{JobID: j.ID, Attempt: fence.Attempt, StartedAt: started, FinishedAt: finished,
		HTTPStatus: res.StatusCode, Error: errMsg, Duration: finished.Sub(started)}
	err := e.writeResult(ctx, j, fence, att)
	switch {
	case errors.Is(err, domain.ErrStaleLease):
		e.d.Metrics.StaleResult()
		log.Warn("discarding stale result; lease was lost")
	case err != nil:
		log.Error("recording result failed; reconciler will retry after lease expiry", "err", err)
	default:
		if j.Status.Terminal() {
			e.d.Metrics.JobFinished(j.Status, j.Attempt)
		}
		log.Debug("attempt finished", "state", j.Status, "status", res.StatusCode)
	}
}

// writeResult persists the fenced state change (and attempt row) with a few retries
// on transient database errors.
func (e *Executor) writeResult(ctx context.Context, j domain.Job, fence domain.Fence, att *domain.Attempt) error {
	var err error
	for i := 0; i < 3; i++ {
		err = e.d.Tx.WithinTx(ctx, func(ctx context.Context) error {
			if err := e.d.Jobs.RecordResult(ctx, j, fence); err != nil {
				return err
			}
			if att == nil {
				return nil
			}
			return e.d.Jobs.InsertAttempt(ctx, *att)
		})
		if err == nil || errors.Is(err, domain.ErrStaleLease) || ctx.Err() != nil {
			return err
		}
		time.Sleep(time.Duration(i+1) * 200 * time.Millisecond)
	}
	return err
}

// WaitIdle blocks until all started workers have finished. Intended for tests.
func (e *Executor) WaitIdle() { e.wg.Wait() }
