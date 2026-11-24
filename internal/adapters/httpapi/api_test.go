package httpapi_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"crep/internal/adapters/clock"
	"crep/internal/adapters/httpapi"
	"crep/internal/adapters/httpcaller"
	"crep/internal/adapters/idgen"
	"crep/internal/adapters/memory"
	"crep/internal/app/services"
)

var secret = []byte("0123456789abcdef0123456789abcdef")

func token(t *testing.T, tenant string) string {
	s, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"tenant_id": tenant, "exp": time.Now().Add(time.Hour).Unix(),
	}).SignedString(secret)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func newServer(t *testing.T) *httptest.Server {
	st := memory.New()
	d := services.Deps{Tx: st, Tasks: st.TaskRepo(), Jobs: st.JobRepo(), History: st.HistoryStore(),
		Dest: httpcaller.Policy{}, Clock: clock.System{}, IDs: idgen.UUIDv7{}, Metrics: services.NopMetrics{},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	auth, err := httpapi.NewJWTAuth(httpapi.JWTConfig{HMACSecret: secret})
	if err != nil {
		t.Fatal(err)
	}
	api := &httpapi.API{Tasks: services.NewTaskService(d), Jobs: services.NewJobService(d), Auth: auth, Log: d.Log}
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	return srv
}

func do(t *testing.T, srv *httptest.Server, tok, method, path, body string) (int, map[string]any) {
	t.Helper()
	return doH(t, srv, tok, method, path, body, nil)
}

func doH(t *testing.T, srv *httptest.Server, tok, method, path, body string, hdr map[string]string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("bad json %q", raw)
		}
	}
	return resp.StatusCode, out
}

func TestTaskLifecycleOverHTTP(t *testing.T) {
	srv := newServer(t)
	a := token(t, "tenant-a")

	if code, _ := do(t, srv, "", "GET", "/v1/tasks", ""); code != 401 {
		t.Fatalf("no token: %d", code)
	}
	if code, _ := do(t, srv, "garbage", "GET", "/v1/tasks", ""); code != 401 {
		t.Fatalf("bad token: %d", code)
	}

	code, task := do(t, srv, a, "POST", "/v1/tasks", `{
		"name": "nightly",
		"schedule": {"type": "cron", "cron": "0 3 * * *", "timezone": "Europe/Berlin"},
		"priority": "high",
		"request": {"method": "POST", "url": "https://example.com/hook",
			"headers": [{"name": "Authorization", "value": "Bearer top-secret", "secret": true}],
			"body": "{\"x\":1}", "timeout_ms": 5000},
		"retry": {"max_attempts": 3}
	}`)
	if code != 201 {
		t.Fatalf("create: %d %v", code, task)
	}
	id := task["id"].(string)
	hdr := task["request"].(map[string]any)["headers"].([]any)[0].(map[string]any)
	if hdr["value"] != "***" {
		t.Fatalf("secret header leaked: %v", hdr)
	}
	if task["priority"] != "high" || task["status"] != "active" || task["next_run_at"] == nil {
		t.Fatalf("task: %v", task)
	}

	if code, _ := do(t, srv, token(t, "tenant-b"), "GET", "/v1/tasks/"+id, ""); code != 404 {
		t.Fatalf("cross-tenant read: %d", code)
	}

	put := `{
		"name": "nightly",
		"schedule": {"type": "cron", "cron": "0 3 * * *", "timezone": "Europe/Berlin"},
		"priority": "low",
		"request": {"method": "POST", "url": "https://example.com/hook"}
	}`
	if code, _ := do(t, srv, a, "PUT", "/v1/tasks/"+id, put); code != 428 {
		t.Fatalf("put without If-Match: %d", code)
	}
	if code, _ := doH(t, srv, a, "PUT", "/v1/tasks/"+id, put, map[string]string{"If-Match": `"7"`}); code != 412 {
		t.Fatalf("put with stale If-Match: %d", code)
	}
	code, upd := doH(t, srv, a, "PUT", "/v1/tasks/"+id, put, map[string]string{"If-Match": `"1"`})
	if code != 200 || upd["priority"] != "low" || upd["version"].(float64) != 2 || upd["next_run_at"] != task["next_run_at"] {
		t.Fatalf("put: %d %v", code, upd)
	}
	if hs := upd["request"].(map[string]any)["headers"]; hs != nil {
		t.Fatalf("put must replace headers, got %v", hs)
	}
	if code, _ := doH(t, srv, a, "PUT", "/v1/tasks/"+id, `{"priority": "low"}`, map[string]string{"If-Match": `"2"`}); code != 400 {
		t.Fatalf("partial put: %d", code)
	}
	if code, p := do(t, srv, a, "POST", "/v1/tasks/"+id+"/pause", ""); code != 200 || p["status"] != "paused" {
		t.Fatalf("pause: %d %v", code, p)
	}
	if code, _ := do(t, srv, a, "POST", "/v1/tasks/"+id+"/pause", ""); code != 409 {
		t.Fatalf("double pause: %d", code)
	}
	if code, p := do(t, srv, a, "POST", "/v1/tasks/"+id+"/resume", ""); code != 200 || p["status"] != "active" {
		t.Fatalf("resume: %d %v", code, p)
	}
	if code, l := do(t, srv, a, "GET", "/v1/tasks", ""); code != 200 || len(l["items"].([]any)) != 1 {
		t.Fatalf("list: %d %v", code, l)
	}
	if code, _ := do(t, srv, a, "DELETE", "/v1/tasks/"+id, ""); code != 204 {
		t.Fatalf("delete: %d", code)
	}
	if code, _ := do(t, srv, a, "GET", "/v1/tasks/"+id, ""); code != 404 {
		t.Fatalf("get deleted: %d", code)
	}
}

func TestValidationErrors(t *testing.T) {
	srv := newServer(t)
	a := token(t, "t")
	cases := []string{
		`{"name":"x","schedule":{"type":"cron","cron":"nope"},"request":{"url":"https://example.com"}}`,
		`{"name":"x","schedule":{"type":"immediate"},"request":{"url":"http://10.0.0.5/admin"}}`,
		`{"name":"x","schedule":{"type":"immediate"},"request":{"url":"https://example.com"},"priority":"urgent"}`,
		`{"name":"x","schedule":{"type":"immediate"},"request":{"url":"https://example.com"},"bogus":1}`,
		`{"schedule":{"type":"immediate"}}`,
	}
	for _, body := range cases {
		if code, resp := do(t, srv, a, "POST", "/v1/tasks", body); code != 400 {
			t.Errorf("%s: %d %v", body, code, resp)
		}
	}
}

func TestImmediateTaskVisibleInJobAPI(t *testing.T) {
	srv := newServer(t)
	a := token(t, "t")
	_, task := do(t, srv, a, "POST", "/v1/tasks", `{"name":"now","schedule":{"type":"immediate"},"request":{"url":"https://example.com"}}`)
	id := task["id"].(string)
	code, hist := do(t, srv, a, "GET", "/v1/tasks/"+id+"/jobs", "")
	items := hist["items"].([]any)
	if code != 200 || len(items) != 1 {
		t.Fatalf("history: %d %v", code, hist)
	}
	jobID := items[0].(map[string]any)["id"].(string)
	if code, j := do(t, srv, a, "GET", "/v1/jobs/"+jobID, ""); code != 200 || j["status"] != "pending" {
		t.Fatalf("job: %d %v", code, j)
	}
	if code, _ := do(t, srv, a, "POST", "/v1/jobs/"+jobID+"/retry", ""); code != 409 {
		t.Fatalf("retry of pending job must conflict: %d", code)
	}
	if code, d := do(t, srv, a, "GET", "/v1/dlq", ""); code != 200 || len(d["items"].([]any)) != 0 {
		t.Fatalf("dlq: %d %v", code, d)
	}
}
