package services

import (
	"context"
	"time"

	"crep/internal/app/ports"
)

type ArchiverConfig struct {
	Retention     time.Duration // how long succeeded/cancelled jobs stay in MySQL (1h)
	DeadRetention time.Duration // how long dead jobs stay in the DLQ, retryable (14d)
	Batch         int
	Interval      time.Duration
}

// Archiver copies finished jobs to the history store and then deletes the hot rows.
// History writes are idempotent, so a crash between the two steps only causes a re-copy.
// Dead (DLQ) jobs stay in MySQL for DeadRetention so they can be retried, then move too.
type Archiver struct {
	d   Deps
	cfg ArchiverConfig
}

func NewArchiver(d Deps, cfg ArchiverConfig) *Archiver {
	if cfg.Retention == 0 {
		cfg.Retention = time.Hour
	}
	if cfg.DeadRetention == 0 {
		cfg.DeadRetention = 14 * 24 * time.Hour
	}
	if cfg.Batch == 0 {
		cfg.Batch = 500
	}
	if cfg.Interval == 0 {
		cfg.Interval = 30 * time.Second
	}
	return &Archiver{d: d, cfg: cfg}
}

func (a *Archiver) Run(ctx context.Context) {
	RunLoop(ctx, a.d.Log, "archiver", a.cfg.Interval, func(ctx context.Context) error {
		_, err := a.Tick(ctx)
		return err
	})
}

func (a *Archiver) Tick(ctx context.Context) (int, error) {
	total := 0
	for ctx.Err() == nil {
		now := a.d.Clock.Now()
		jobs, err := a.d.Jobs.ListArchivable(ctx, now.Add(-a.cfg.Retention), now.Add(-a.cfg.DeadRetention), a.cfg.Batch)
		if err != nil || len(jobs) == 0 {
			return total, err
		}
		ids := make([]string, len(jobs))
		for i, j := range jobs {
			ids[i] = j.ID
		}
		atts, err := a.d.Jobs.Attempts(ctx, ids)
		if err != nil {
			return total, err
		}
		batch := make([]ports.ArchivedJob, len(jobs))
		for i, j := range jobs {
			batch[i] = ports.ArchivedJob{Job: j, Attempts: atts[j.ID]}
		}
		if err := a.d.History.Save(ctx, batch); err != nil {
			return total, err
		}
		if err := a.d.Jobs.DeleteArchived(ctx, jobs); err != nil {
			return total, err
		}
		a.d.Metrics.JobsArchived(len(jobs))
		total += len(jobs)
		if len(jobs) < a.cfg.Batch {
			return total, nil
		}
	}
	return total, ctx.Err()
}
