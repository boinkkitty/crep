package services

import (
	"context"
	"errors"
	"time"

	"crep/internal/domain"
)

type ReconcilerConfig struct {
	Batch    int
	Interval time.Duration
}

// Reconciler requeues (or dead-letters) running jobs whose executor lease expired.
type Reconciler struct {
	d   Deps
	cfg ReconcilerConfig
}

func NewReconciler(d Deps, cfg ReconcilerConfig) *Reconciler {
	if cfg.Batch == 0 {
		cfg.Batch = 200
	}
	if cfg.Interval == 0 {
		cfg.Interval = 5 * time.Second
	}
	return &Reconciler{d: d, cfg: cfg}
}

func (r *Reconciler) Run(ctx context.Context) {
	RunLoop(ctx, r.d.Log, "reconciler", r.cfg.Interval, func(ctx context.Context) error {
		_, err := r.Tick(ctx)
		return err
	})
}

func (r *Reconciler) Tick(ctx context.Context) (int, error) {
	total := 0
	for ctx.Err() == nil {
		var n int
		var dead []domain.Job
		err := r.d.Tx.WithinTx(ctx, func(ctx context.Context) error {
			n, dead = 0, nil
			now := r.d.Clock.Now()
			jobs, err := r.d.Jobs.LockExpiredLeases(ctx, now, r.cfg.Batch)
			if err != nil {
				return err
			}
			for _, j := range jobs {
				fence := j.Fence()
				j.LeaseExpired(now)
				if err := r.d.Jobs.RecordResult(ctx, j, fence); err != nil {
					if errors.Is(err, domain.ErrStaleLease) {
						continue
					}
					return err
				}
				if err := r.d.Jobs.InsertAttempt(ctx, domain.Attempt{
					JobID: j.ID, Attempt: fence.Attempt, StartedAt: now, FinishedAt: now, Error: j.Error,
				}); err != nil {
					return err
				}
				n++
				if j.Status.Terminal() {
					dead = append(dead, j)
				}
			}
			return nil
		})
		if err != nil {
			return total, err
		}
		for _, j := range dead {
			r.d.Metrics.JobFinished(j.Status, j.Attempt)
		}
		if n > 0 {
			r.d.Metrics.LeasesRecovered(n)
			r.d.Log.Warn("recovered expired leases", "count", n)
		}
		total += n
		if n < r.cfg.Batch {
			return total, nil
		}
	}
	return total, ctx.Err()
}
