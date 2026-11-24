// Package services implements Crep's use cases on top of the ports.
package services

import (
	"context"
	"log/slog"
	"time"

	"crep/internal/app/ports"
	"crep/internal/domain"
)

// Deps bundles the ports shared by services.
type Deps struct {
	Tx      ports.TxManager
	Tasks   ports.TaskRepo
	Jobs    ports.JobRepo
	History ports.HistoryStore
	Caller  ports.HTTPCaller
	Dest    ports.DestinationPolicy
	Clock   ports.Clock
	IDs     ports.IDGen
	Metrics ports.Metrics
	Log     *slog.Logger
}

// RunLoop calls tick every interval until ctx is cancelled. Errors are logged.
func RunLoop(ctx context.Context, log *slog.Logger, name string, interval time.Duration, tick func(context.Context) error) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if err := tick(ctx); err != nil && ctx.Err() == nil {
			log.Error("loop tick failed", "loop", name, "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// NopMetrics discards all metrics.
type NopMetrics struct{}

func (NopMetrics) JobsPlanned(int)                   {}
func (NopMetrics) JobsClaimed(int)                   {}
func (NopMetrics) JobFinished(domain.JobStatus, int) {}
func (NopMetrics) StartLag(time.Duration)            {}
func (NopMetrics) StaleResult()                      {}
func (NopMetrics) LeasesRecovered(int)               {}
func (NopMetrics) JobsArchived(int)                  {}
