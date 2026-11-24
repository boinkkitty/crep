// Package metrics implements ports.Metrics with Prometheus.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crep/internal/domain"
)

type Prom struct {
	planned, claimed, stale, recovered, archived prometheus.Counter
	finished                                     *prometheus.CounterVec
	attempts                                     prometheus.Histogram
	lag                                          prometheus.Histogram
}

func NewProm(reg prometheus.Registerer) *Prom {
	c := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Namespace: "crep", Name: name, Help: help})
	}
	p := &Prom{
		planned:   c("jobs_planned_total", "Jobs created by the scheduler or Task API."),
		claimed:   c("jobs_claimed_total", "Job attempts claimed by executors."),
		stale:     c("stale_results_total", "Results dropped because the executor lost its lease."),
		recovered: c("leases_recovered_total", "Expired leases handled by the reconciler."),
		archived:  c("jobs_archived_total", "Jobs moved to the history store."),
		finished: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "crep", Name: "jobs_finished_total",
			Help: "Jobs reaching a terminal status."}, []string{"status"}),
		attempts: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "crep", Name: "job_attempts",
			Help: "Attempts used by finished jobs.", Buckets: []float64{1, 2, 3, 5, 8, 13, 21}}),
		lag: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "crep", Name: "start_lag_seconds",
			Help: "Delay between scheduled time and first attempt start.", Buckets: prometheus.ExponentialBuckets(0.01, 2, 14)}),
	}
	reg.MustRegister(p.planned, p.claimed, p.stale, p.recovered, p.archived, p.finished, p.attempts, p.lag)
	return p
}

func (p *Prom) JobsPlanned(n int)     { p.planned.Add(float64(n)) }
func (p *Prom) JobsClaimed(n int)     { p.claimed.Add(float64(n)) }
func (p *Prom) StaleResult()          { p.stale.Inc() }
func (p *Prom) LeasesRecovered(n int) { p.recovered.Add(float64(n)) }
func (p *Prom) JobsArchived(n int)    { p.archived.Add(float64(n)) }
func (p *Prom) StartLag(d time.Duration) {
	if d < 0 {
		d = 0
	}
	p.lag.Observe(d.Seconds())
}
func (p *Prom) JobFinished(s domain.JobStatus, attempt int) {
	p.finished.WithLabelValues(string(s)).Inc()
	p.attempts.Observe(float64(attempt))
}

// DLQGauge exposes the DLQ size, sampled on scrape.
func DLQGauge(reg prometheus.Registerer, count func() (int64, error)) {
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "crep", Name: "dlq_jobs",
		Help: "Jobs currently in the dead letter queue."}, func() float64 {
		n, err := count()
		if err != nil {
			return -1
		}
		return float64(n)
	}))
}
