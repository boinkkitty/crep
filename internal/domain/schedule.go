package domain

import (
	"time"

	"github.com/robfig/cron/v3"
)

// ScheduleType selects how a task's jobs are planned.
type ScheduleType string

const (
	ScheduleImmediate ScheduleType = "immediate" // one job, created with the task
	ScheduleOnce      ScheduleType = "once"      // one job at RunAt
	ScheduleCron      ScheduleType = "cron"      // recurring, by cron expression in Timezone
)

// Schedule describes when a task runs.
type Schedule struct {
	Type     ScheduleType
	RunAt    time.Time // once
	Cron     string    // cron
	Timezone string    // cron; IANA name, defaults to UTC
}

// Standard 5-field cron plus optional leading seconds field and descriptors (@hourly, @every 5m, ...).
var cronParser = cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// MinCronInterval is the shortest permitted interval between recurring fires.
const MinCronInterval = time.Second

func (s Schedule) Validate() error {
	switch s.Type {
	case ScheduleImmediate:
		return nil
	case ScheduleOnce:
		if s.RunAt.IsZero() {
			return invalid("schedule.run_at", "required for once schedules")
		}
		return nil
	case ScheduleCron:
		if _, err := s.location(); err != nil {
			return invalid("schedule.timezone", "unknown timezone %q", s.Timezone)
		}
		if _, err := cronParser.Parse(s.Cron); err != nil {
			return invalid("schedule.cron", "invalid cron expression: %v", err)
		}
		return nil
	default:
		return invalid("schedule.type", "must be immediate, once, or cron")
	}
}

// Equal reports whether two schedules plan the same fires. An empty timezone means UTC.
func (s Schedule) Equal(o Schedule) bool {
	tz := func(z string) string {
		if z == "" {
			return "UTC"
		}
		return z
	}
	return s.Type == o.Type && s.Cron == o.Cron && tz(s.Timezone) == tz(o.Timezone) && s.RunAt.Equal(o.RunAt)
}

func (s Schedule) location() (*time.Location, error) {
	if s.Timezone == "" {
		return time.UTC, nil
	}
	return time.LoadLocation(s.Timezone)
}

// NextCron returns the first cron fire time strictly after `after`, evaluated in the
// schedule's timezone and returned in UTC. Local times that do not exist because of
// a DST transition are skipped. ok is false for non-cron schedules or
// expressions with no future fire time.
func (s Schedule) NextCron(after time.Time) (next time.Time, ok bool) {
	if s.Type != ScheduleCron {
		return time.Time{}, false
	}
	loc, err := s.location()
	if err != nil {
		return time.Time{}, false
	}
	sched, err := cronParser.Parse(s.Cron)
	if err != nil {
		return time.Time{}, false
	}
	n := sched.Next(after.In(loc))
	if n.IsZero() {
		return time.Time{}, false
	}
	return n.UTC(), true
}
