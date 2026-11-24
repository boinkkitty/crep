package httpapi

import (
	"time"

	"crep/internal/app/services"
	"crep/internal/domain"
)

type scheduleDTO struct {
	Type     string     `json:"type"`
	RunAt    *time.Time `json:"run_at,omitempty"`
	Cron     string     `json:"cron,omitempty"`
	Timezone string     `json:"timezone,omitempty"`
}

type headerDTO struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Secret bool   `json:"secret,omitempty"`
}

type requestDTO struct {
	Method     string      `json:"method,omitempty"`
	URL        string      `json:"url"`
	Headers    []headerDTO `json:"headers,omitempty"`
	Body       string      `json:"body,omitempty"`
	BodySecret bool        `json:"body_secret,omitempty"`
	TimeoutMs  int64       `json:"timeout_ms,omitempty"`
}

type retryDTO struct {
	MaxAttempts int   `json:"max_attempts,omitempty"`
	BaseDelayMs int64 `json:"base_delay_ms,omitempty"`
	MaxDelayMs  int64 `json:"max_delay_ms,omitempty"`
}

type taskRequest struct {
	Name     *string      `json:"name"`
	Schedule *scheduleDTO `json:"schedule"`
	Priority *string      `json:"priority"`
	Request  *requestDTO  `json:"request"`
	Retry    *retryDTO    `json:"retry"`
}

type taskResponse struct {
	ID        string      `json:"id"`
	Name      string      `json:"name"`
	Schedule  scheduleDTO `json:"schedule"`
	Priority  string      `json:"priority"`
	Request   requestDTO  `json:"request"`
	Retry     retryDTO    `json:"retry"`
	Status    string      `json:"status"`
	NextRunAt *time.Time  `json:"next_run_at"`
	Version   int64       `json:"version"`
	CreatedAt time.Time   `json:"created_at"`
	UpdatedAt time.Time   `json:"updated_at"`
}

type attemptResponse struct {
	Attempt    int       `json:"attempt"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	HTTPStatus int       `json:"http_status,omitempty"`
	Error      string    `json:"error,omitempty"`
	DurationMs int64     `json:"duration_ms"`
}

type jobResponse struct {
	ID           string            `json:"id"`
	TaskID       string            `json:"task_id"`
	Status       string            `json:"status"`
	Priority     string            `json:"priority"`
	ScheduledFor time.Time         `json:"scheduled_for"`
	AvailableAt  time.Time         `json:"available_at,omitzero"`
	Attempt      int               `json:"attempt"`
	MaxAttempts  int               `json:"max_attempts"`
	Method       string            `json:"method"`
	URL          string            `json:"url"`
	HTTPStatus   int               `json:"http_status,omitempty"`
	Error        string            `json:"error,omitempty"`
	CreatedAt    time.Time         `json:"created_at"`
	FinishedAt   *time.Time        `json:"finished_at"`
	Attempts     []attemptResponse `json:"attempts,omitempty"`
}

type pageResponse[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
}

func (s *scheduleDTO) toDomain() domain.Schedule {
	out := domain.Schedule{Type: domain.ScheduleType(s.Type), Cron: s.Cron, Timezone: s.Timezone}
	if s.RunAt != nil {
		out.RunAt = *s.RunAt
	}
	return out
}

func (r *requestDTO) toDomain() domain.RequestSpec {
	out := domain.RequestSpec{Method: r.Method, URL: r.URL, BodySecret: r.BodySecret,
		Timeout: time.Duration(r.TimeoutMs) * time.Millisecond}
	if r.Body != "" {
		out.Body = []byte(r.Body)
	}
	for _, h := range r.Headers {
		out.Headers = append(out.Headers, domain.Header{Name: h.Name, Value: h.Value, Secret: h.Secret})
	}
	return out
}

func (r *retryDTO) toDomain() domain.RetryPolicy {
	return domain.RetryPolicy{MaxAttempts: r.MaxAttempts,
		BaseDelay: time.Duration(r.BaseDelayMs) * time.Millisecond,
		MaxDelay:  time.Duration(r.MaxDelayMs) * time.Millisecond}
}

func (t taskRequest) toInput() (services.TaskInput, error) {
	if t.Name == nil || t.Schedule == nil || t.Request == nil {
		return services.TaskInput{}, &domain.ValidationError{Message: "name, schedule and request are required"}
	}
	in := services.TaskInput{Name: *t.Name, Schedule: t.Schedule.toDomain(), Request: t.Request.toDomain()}
	var err error
	if t.Priority != nil {
		if in.Priority, err = domain.ParsePriority(*t.Priority); err != nil {
			return in, err
		}
	} else {
		in.Priority = domain.PriorityNormal
	}
	if t.Retry != nil {
		in.Retry = t.Retry.toDomain()
	}
	return in, nil
}

func toTaskResponse(t domain.Task) taskResponse {
	req := t.Request.Redacted()
	out := taskResponse{
		ID: t.ID, Name: t.Name, Priority: t.Priority.String(), Status: string(t.Status),
		NextRunAt: t.NextRunAt, Version: t.Version, CreatedAt: t.CreatedAt, UpdatedAt: t.UpdatedAt,
		Schedule: scheduleDTO{Type: string(t.Schedule.Type), Cron: t.Schedule.Cron, Timezone: t.Schedule.Timezone},
		Request: requestDTO{Method: req.Method, URL: req.URL, Body: string(req.Body), BodySecret: req.BodySecret,
			TimeoutMs: req.Timeout.Milliseconds()},
		Retry: retryDTO{MaxAttempts: t.Retry.MaxAttempts, BaseDelayMs: t.Retry.BaseDelay.Milliseconds(),
			MaxDelayMs: t.Retry.MaxDelay.Milliseconds()},
	}
	if t.Schedule.Type == domain.ScheduleOnce {
		r := t.Schedule.RunAt
		out.Schedule.RunAt = &r
	}
	for _, h := range req.Headers {
		out.Request.Headers = append(out.Request.Headers, headerDTO{Name: h.Name, Value: h.Value, Secret: h.Secret})
	}
	return out
}

func toJobResponse(j domain.Job, atts []domain.Attempt) jobResponse {
	out := jobResponse{
		ID: j.ID, TaskID: j.TaskID, Status: string(j.Status), Priority: j.Priority.String(),
		ScheduledFor: j.ScheduledFor, Attempt: j.Attempt, MaxAttempts: j.MaxAttempts,
		Method: j.Request.Method, URL: j.Request.URL, HTTPStatus: j.HTTPStatus,
		Error: j.Error, CreatedAt: j.CreatedAt, FinishedAt: j.FinishedAt,
	}
	if j.Status == domain.JobPending {
		out.AvailableAt = j.AvailableAt
	}
	for _, a := range atts {
		out.Attempts = append(out.Attempts, attemptResponse{Attempt: a.Attempt, StartedAt: a.StartedAt,
			FinishedAt: a.FinishedAt, HTTPStatus: a.HTTPStatus, Error: a.Error, DurationMs: a.Duration.Milliseconds()})
	}
	return out
}
