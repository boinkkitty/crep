package services

import (
	"context"
	"time"

	"crep/internal/domain"
)

// SchedulerConfig tunes the scheduler.
type SchedulerConfig struct {
	Lookahead time.Duration // plan jobs due within this window (30s)
	Batch     int
	Interval  time.Duration
}

// Scheduler turns upcoming task fires into pending jobs. Multiple instances may run:
// tasks are locked with SKIP LOCKED and jobs are unique per (task, scheduled_for).
type Scheduler struct {
	d   Deps
	cfg SchedulerConfig
}

func NewScheduler(d Deps, cfg SchedulerConfig) *Scheduler {
	if cfg.Lookahead == 0 {
		cfg.Lookahead = 30 * time.Second
	}
	if cfg.Batch == 0 {
		cfg.Batch = 200
	}
	if cfg.Interval == 0 {
		cfg.Interval = time.Second
	}
	return &Scheduler{d: d, cfg: cfg}
}

func (s *Scheduler) Run(ctx context.Context) {
	RunLoop(ctx, s.d.Log, "scheduler", s.cfg.Interval, func(ctx context.Context) error {
		_, err := s.Tick(ctx)
		return err
	})
}

// Tick plans every fire due within the look-ahead window and returns the number of jobs created.
func (s *Scheduler) Tick(ctx context.Context) (int, error) {
	total := 0
	for ctx.Err() == nil {
		n, more, err := s.tickBatch(ctx)
		total += n
		if err != nil || !more {
			return total, err
		}
	}
	return total, ctx.Err()
}

// MaxCatchUpPerTick bounds the jobs one task can create per batch after an outage.
const MaxCatchUpPerTick = 1000

func (s *Scheduler) tickBatch(ctx context.Context) (planned int, more bool, err error) {
	err = s.d.Tx.WithinTx(ctx, func(ctx context.Context) error {
		planned = 0
		now := s.d.Clock.Now()
		tasks, err := s.d.Tasks.LockDue(ctx, now.Add(s.cfg.Lookahead), s.cfg.Batch)
		if err != nil {
			return err
		}
		more = len(tasks) == s.cfg.Batch
		for _, t := range tasks {
			if t.Request.Unreadable != "" {
				s.d.Log.Error("pausing task with unreadable request", "task_id", t.ID, "reason", t.Request.Unreadable)
				_ = t.Pause()
				if err := s.d.Tasks.SaveSchedule(ctx, t); err != nil {
					return err
				}
				continue
			}
			// A task may have several fires inside the window (e.g. every 5s), and after
			// downtime every missed fire is planned too, capped per tick so one task
			// can't monopolise a transaction; the rest follow on the next batch.
			for i := 0; t.Status == domain.TaskActive && t.NextRunAt != nil && !t.NextRunAt.After(now.Add(s.cfg.Lookahead)); i++ {
				if i == MaxCatchUpPerTick {
					more = true
					break
				}
				fire := *t.NextRunAt
				ok, err := s.d.Jobs.Insert(ctx, domain.NewJob(s.d.IDs.NewID(), t, fire, now))
				if err != nil {
					return err
				}
				if ok {
					planned++
				}
				t.Advance(fire)
			}
			if err := s.d.Tasks.SaveSchedule(ctx, t); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil && planned > 0 {
		s.d.Metrics.JobsPlanned(planned)
	}
	return planned, more, err
}
