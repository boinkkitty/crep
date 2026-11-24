package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"crep/internal/app/services"
	"crep/internal/domain"
)

// API wires HTTP routes to application services.
type API struct {
	Tasks *services.TaskService // nil disables Task API routes
	Jobs  *services.JobService  // nil disables Job API routes
	Auth  Authenticator
	Log   *slog.Logger
}

// Handler returns the routed handler with auth, recovery and access logging.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	if a.Tasks != nil {
		mux.HandleFunc("POST /v1/tasks", a.createTask)
		mux.HandleFunc("GET /v1/tasks", a.listTasks)
		mux.HandleFunc("GET /v1/tasks/{id}", a.getTask)
		mux.HandleFunc("PUT /v1/tasks/{id}", a.replaceTask)
		mux.HandleFunc("DELETE /v1/tasks/{id}", a.deleteTask)
		mux.HandleFunc("POST /v1/tasks/{id}/pause", a.pauseTask)
		mux.HandleFunc("POST /v1/tasks/{id}/resume", a.resumeTask)
	}
	if a.Jobs != nil {
		mux.HandleFunc("GET /v1/tasks/{id}/jobs", a.taskHistory)
		mux.HandleFunc("GET /v1/jobs/{id}", a.getJob)
		mux.HandleFunc("POST /v1/jobs/{id}/retry", a.retryJob)
		mux.HandleFunc("GET /v1/dlq", a.listDLQ)
		mux.HandleFunc("DELETE /v1/dlq/{id}", a.discardJob)
	}
	return a.logging(recoverer(requireAuth(a.Auth, mux), a.Log))
}

// ---- tasks ----

func (a *API) createTask(w http.ResponseWriter, r *http.Request) {
	var req taskRequest
	if !decode(w, r, &req) {
		return
	}
	in, err := req.toInput()
	if err != nil {
		a.fail(w, r, err)
		return
	}
	t, created, err := a.Tasks.CreateIdempotent(r.Context(), tenantFrom(r.Context()), r.Header.Get("Idempotency-Key"), in)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/tasks/"+t.ID)
	setETag(w, t)
	status := http.StatusCreated
	if !created {
		status = http.StatusOK // replay of an earlier request with the same Idempotency-Key
	}
	writeJSON(w, status, toTaskResponse(t))
}

func (a *API) listTasks(w http.ResponseWriter, r *http.Request) {
	p, err := a.Tasks.List(r.Context(), tenantFrom(r.Context()), r.URL.Query().Get("cursor"), limitParam(r))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	out := pageResponse[taskResponse]{Items: []taskResponse{}, NextCursor: p.NextCursor}
	for _, t := range p.Items {
		out.Items = append(out.Items, toTaskResponse(t))
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *API) getTask(w http.ResponseWriter, r *http.Request) {
	t, err := a.Tasks.Get(r.Context(), tenantFrom(r.Context()), r.PathValue("id"))
	a.respondTask(w, r, t, err)
}

// replaceTask is a full replace: the body has the same shape as create, and omitted
// optional fields (priority, retry, ...) reset to their defaults. It requires
// If-Match with the version the client last saw (the ETag of GET), so concurrent
// edits fail with 412 instead of silently overwriting each other.
func (a *API) replaceTask(w http.ResponseWriter, r *http.Request) {
	ifVersion, ok := parseIfMatch(r.Header.Get("If-Match"))
	if !ok {
		writeError(w, http.StatusPreconditionRequired, "precondition_required",
			`If-Match header with the task version is required (e.g. If-Match: "3"; see the ETag of GET)`)
		return
	}
	var req taskRequest
	if !decode(w, r, &req) {
		return
	}
	in, err := req.toInput()
	if err != nil {
		a.fail(w, r, err)
		return
	}
	t, err := a.Tasks.Replace(r.Context(), tenantFrom(r.Context()), r.PathValue("id"), ifVersion, in)
	a.respondTask(w, r, t, err)
}

func (a *API) pauseTask(w http.ResponseWriter, r *http.Request) {
	t, err := a.Tasks.Pause(r.Context(), tenantFrom(r.Context()), r.PathValue("id"))
	a.respondTask(w, r, t, err)
}

func (a *API) resumeTask(w http.ResponseWriter, r *http.Request) {
	t, err := a.Tasks.Resume(r.Context(), tenantFrom(r.Context()), r.PathValue("id"))
	a.respondTask(w, r, t, err)
}

func (a *API) deleteTask(w http.ResponseWriter, r *http.Request) {
	if err := a.Tasks.Delete(r.Context(), tenantFrom(r.Context()), r.PathValue("id")); err != nil {
		a.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) respondTask(w http.ResponseWriter, r *http.Request, t domain.Task, err error) {
	if err != nil {
		a.fail(w, r, err)
		return
	}
	setETag(w, t)
	writeJSON(w, http.StatusOK, toTaskResponse(t))
}

// ---- jobs ----

func (a *API) taskHistory(w http.ResponseWriter, r *http.Request) {
	p, err := a.Jobs.History(r.Context(), tenantFrom(r.Context()), r.PathValue("id"), r.URL.Query().Get("cursor"), limitParam(r))
	a.respondJobs(w, r, p.Items, p.NextCursor, err)
}

func (a *API) listDLQ(w http.ResponseWriter, r *http.Request) {
	p, err := a.Jobs.ListDLQ(r.Context(), tenantFrom(r.Context()), r.URL.Query().Get("cursor"), limitParam(r))
	a.respondJobs(w, r, p.Items, p.NextCursor, err)
}

func (a *API) getJob(w http.ResponseWriter, r *http.Request) {
	j, atts, err := a.Jobs.Get(r.Context(), tenantFrom(r.Context()), r.PathValue("id"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toJobResponse(j, atts))
}

func (a *API) retryJob(w http.ResponseWriter, r *http.Request) {
	j, err := a.Jobs.Retry(r.Context(), tenantFrom(r.Context()), r.PathValue("id"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, toJobResponse(j, nil))
}

func (a *API) discardJob(w http.ResponseWriter, r *http.Request) {
	if _, err := a.Jobs.Discard(r.Context(), tenantFrom(r.Context()), r.PathValue("id")); err != nil {
		a.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) respondJobs(w http.ResponseWriter, r *http.Request, jobs []domain.Job, next string, err error) {
	if err != nil {
		a.fail(w, r, err)
		return
	}
	out := pageResponse[jobResponse]{Items: []jobResponse{}, NextCursor: next}
	for _, j := range jobs {
		out.Items = append(out.Items, toJobResponse(j, nil))
	}
	writeJSON(w, http.StatusOK, out)
}

// ---- plumbing ----

const maxRequestBytes = 256 << 10

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return false
	}
	return true
}

// setETag exposes the task version so clients can send it back as If-Match.
func setETag(w http.ResponseWriter, t domain.Task) {
	w.Header().Set("ETag", `"`+strconv.FormatInt(t.Version, 10)+`"`)
}

// parseIfMatch accepts `"3"`, `W/"3"` or `3`.
func parseIfMatch(h string) (int64, bool) {
	h = strings.Trim(strings.TrimPrefix(strings.TrimSpace(h), "W/"), `"`)
	v, err := strconv.ParseInt(h, 10, 64)
	return v, err == nil && v > 0
}

func limitParam(r *http.Request) int {
	n, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	return n
}

func (a *API) fail(w http.ResponseWriter, r *http.Request, err error) {
	var v *domain.ValidationError
	switch {
	case errors.As(err, &v):
		writeError(w, http.StatusBadRequest, "invalid_argument", v.Error())
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "not found")
	case errors.Is(err, domain.ErrVersionMismatch):
		writeError(w, http.StatusPreconditionFailed, "version_mismatch", "task was changed since your version; GET it and retry")
	case errors.Is(err, domain.ErrConflict):
		writeError(w, http.StatusConflict, "conflict", "operation not allowed in the current state")
	default:
		a.Log.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error")
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": msg}})
}

func recoverer(next http.Handler, log *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if p := recover(); p != nil {
				if e, ok := p.(error); ok && errors.Is(e, http.ErrAbortHandler) {
					panic(p)
				}
				log.Error("panic in handler", "panic", p, "path", r.URL.Path)
				writeError(w, http.StatusInternalServerError, "internal", "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) { s.status = code; s.ResponseWriter.WriteHeader(code) }

func (a *API) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: 200}
		next.ServeHTTP(rec, r)
		a.Log.Info("http", "method", r.Method, "path", r.URL.Path, "status", rec.status, "dur_ms", time.Since(start).Milliseconds())
	})
}
