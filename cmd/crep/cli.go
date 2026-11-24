package main

// CLI client for the Task and Job APIs, so day-to-day use doesn't need curl.
//
//	crep token <tenant> [-ttl 24h]
//	crep task create [flags]
//	crep task update <id> [flags] [-version n]
//	crep task list [-limit n] [-cursor c]
//	crep task get|pause|resume|delete <id>
//	crep job get|retry|discard <id>
//	crep job history <task-id> [-limit n] [-cursor c]
//	crep job dlq [-limit n] [-cursor c]
//
// Env: CREP_TOKEN (required), CREP_TASK_API (default http://localhost:8080),
// CREP_JOB_API (default http://localhost:8081). Responses are printed as indented JSON
// on stdout; HTTP errors also exit 1 with the status on stderr.

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const cliUsage = `CLI (env CREP_TOKEN, CREP_TASK_API, CREP_JOB_API):
  crep token <tenant> [-ttl 24h]        mint a dev JWT (needs CREP_JWT_SECRET)
  crep task create [flags]              -h for flags
  crep task update <id> [flags]         full replace, same flags as create, plus -version
  crep task list [-limit n] [-cursor c]
  crep task get|pause|resume|delete <id>
  crep job get|retry|discard <id>
  crep job history <task-id> [-limit n] [-cursor c]
  crep job dlq [-limit n] [-cursor c]
`

func taskAPI() string { return strings.TrimRight(envOr("CREP_TASK_API", "http://localhost:8080"), "/") }
func jobAPI() string  { return strings.TrimRight(envOr("CREP_JOB_API", "http://localhost:8081"), "/") }

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// splitID takes a leading positional id (flags are parsed after it).
func splitID(args []string) (string, []string) {
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		return args[0], args[1:]
	}
	return "", args
}

func parseFlags(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	return nil
}

func taskCmd(sub string, args []string) error {
	id, rest := splitID(args)
	switch sub {
	case "create":
		if id != "" {
			return fmt.Errorf("unexpected argument %q (task create takes only flags)", id)
		}
		body, idem, _, err := taskBody("task create", rest, true)
		if err != nil {
			return err
		}
		hdr := map[string]string{}
		if idem != "" {
			hdr["Idempotency-Key"] = idem
		}
		_, err = call("POST", taskAPI(), "/v1/tasks", body, hdr, true)
		return err
	case "update":
		if id == "" {
			return errors.New("usage: crep task update <id> [flags]")
		}
		body, _, version, err := taskBody("task update", rest, false)
		if err != nil {
			return err
		}
		if version == 0 {
			// No -version: use the current one. This still fails if someone else edits
			// between this GET and the PUT, but not if they edited before you looked.
			h, err := call("GET", taskAPI(), "/v1/tasks/"+url.PathEscape(id), nil, nil, false)
			if err != nil {
				return err
			}
			if version, _ = strconv.ParseInt(strings.Trim(h.Get("ETag"), `"`), 10, 64); version == 0 {
				return errors.New("server sent no ETag; pass -version")
			}
		}
		_, err = call("PUT", taskAPI(), "/v1/tasks/"+url.PathEscape(id), body,
			map[string]string{"If-Match": strconv.Quote(strconv.FormatInt(version, 10))}, true)
		return err
	case "list":
		q, err := pageFlags("task list", args)
		if err != nil {
			return err
		}
		_, err = call("GET", taskAPI(), "/v1/tasks"+q, nil, nil, true)
		return err
	case "get", "pause", "resume", "delete":
		if id == "" || len(rest) > 0 {
			return fmt.Errorf("usage: crep task %s <id>", sub)
		}
		p := "/v1/tasks/" + url.PathEscape(id)
		method := map[string]string{"get": "GET", "pause": "POST", "resume": "POST", "delete": "DELETE"}[sub]
		if sub == "pause" || sub == "resume" {
			p += "/" + sub
		}
		_, err := call(method, taskAPI(), p, nil, nil, true)
		return err
	}
	fmt.Fprint(os.Stderr, cliUsage)
	return fmt.Errorf("unknown task command %q", sub)
}

func jobCmd(sub string, args []string) error {
	id, rest := splitID(args)
	switch sub {
	case "get", "retry", "discard":
		if id == "" || len(rest) > 0 {
			return fmt.Errorf("usage: crep job %s <id>", sub)
		}
		method, p := "GET", "/v1/jobs/"+url.PathEscape(id)
		switch sub {
		case "retry":
			method, p = "POST", p+"/retry"
		case "discard":
			method, p = "DELETE", "/v1/dlq/"+url.PathEscape(id)
		}
		_, err := call(method, jobAPI(), p, nil, nil, true)
		return err
	case "history":
		if id == "" {
			return errors.New("usage: crep job history <task-id> [-limit n] [-cursor c]")
		}
		q, err := pageFlags("job history", rest)
		if err != nil {
			return err
		}
		_, err = call("GET", jobAPI(), "/v1/tasks/"+url.PathEscape(id)+"/jobs"+q, nil, nil, true)
		return err
	case "dlq":
		q, err := pageFlags("job dlq", args)
		if err != nil {
			return err
		}
		_, err = call("GET", jobAPI(), "/v1/dlq"+q, nil, nil, true)
		return err
	}
	fmt.Fprint(os.Stderr, cliUsage)
	return fmt.Errorf("unknown job command %q", sub)
}

func pageFlags(name string, args []string) (string, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	limit := fs.Int("limit", 0, "page size (server default 50, max 500)")
	cursor := fs.String("cursor", "", "next_cursor from the previous page")
	if err := parseFlags(fs, args); err != nil {
		return "", err
	}
	q := url.Values{}
	if *limit > 0 {
		q.Set("limit", strconv.Itoa(*limit))
	}
	if *cursor != "" {
		q.Set("cursor", *cursor)
	}
	if len(q) == 0 {
		return "", nil
	}
	return "?" + q.Encode(), nil
}

// headerList is a repeatable "Name: value" flag; order is kept.
type headerList struct {
	secret bool
	out    *[]map[string]any
}

func (h headerList) String() string { return "" }
func (h headerList) Set(v string) error {
	name, value, ok := strings.Cut(v, ":")
	if !ok || strings.TrimSpace(name) == "" {
		return fmt.Errorf(`want "Name: value", got %q`, v)
	}
	m := map[string]any{"name": strings.TrimSpace(name), "value": strings.TrimSpace(value)}
	if h.secret {
		m["secret"] = true
	}
	*h.out = append(*h.out, m)
	return nil
}

// taskBody builds the create/PUT JSON body from flags. Zero numeric flags are left out
// so the server applies its defaults; since update is a full replace, leaving a flag
// out resets that field.
func taskBody(name string, args []string, create bool) (body map[string]any, idem string, version int64, err error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	taskName := fs.String("name", "", "task name (required)")
	target := fs.String("url", "", "endpoint to call (required)")
	method := fs.String("method", "POST", "HTTP method")
	reqBody := fs.String("body", "", "request body")
	bodySecret := fs.Bool("body-secret", false, "show the body as *** in API responses")
	var headers []map[string]any
	fs.Var(headerList{out: &headers}, "header", `request header "Name: value", repeatable`)
	fs.Var(headerList{secret: true, out: &headers}, "secret-header", `like -header, but shown as *** in API responses`)
	cronExpr := fs.String("cron", "", `cron expression, e.g. "0 9 * * *" (optional leading seconds field)`)
	at := fs.String("at", "", "run once at this RFC 3339 time")
	tz := fs.String("tz", "", "IANA timezone for -cron (default UTC)")
	prio := fs.String("priority", "normal", "critical|high|normal|low")
	timeout := fs.Int("timeout-ms", 0, "per-attempt timeout (server default 30000)")
	attempts := fs.Int("max-attempts", 0, "attempts before the DLQ (server default 5)")
	baseDelay := fs.Int("base-delay-ms", 0, "first retry backoff, doubling per attempt (server default 1000)")
	maxDelay := fs.Int("max-delay-ms", 0, "backoff cap (server default 300000)")
	var idemFlag *string
	var versionFlag *int64
	if create {
		idemFlag = fs.String("idempotency-key", "", "makes create safe to repeat")
	} else {
		versionFlag = fs.Int64("version", 0, "expected task version (If-Match); default: the current version")
	}
	if err := parseFlags(fs, args); err != nil {
		return nil, "", 0, err
	}
	if *cronExpr != "" && *at != "" {
		return nil, "", 0, errors.New("set -cron or -at, not both")
	}

	schedule := map[string]any{"type": "immediate"}
	switch {
	case *cronExpr != "":
		schedule = map[string]any{"type": "cron", "cron": *cronExpr}
		if *tz != "" {
			schedule["timezone"] = *tz
		}
	case *at != "":
		if _, err := time.Parse(time.RFC3339, *at); err != nil {
			return nil, "", 0, fmt.Errorf("-at: want RFC 3339, e.g. 2026-12-24T18:00:00Z")
		}
		schedule = map[string]any{"type": "once", "run_at": *at}
	}
	request := map[string]any{"method": *method, "url": *target}
	if len(headers) > 0 {
		request["headers"] = headers
	}
	if *reqBody != "" {
		request["body"] = *reqBody
	}
	if *bodySecret {
		request["body_secret"] = true
	}
	if *timeout > 0 {
		request["timeout_ms"] = *timeout
	}
	retry := map[string]any{}
	for k, v := range map[string]int{"max_attempts": *attempts, "base_delay_ms": *baseDelay, "max_delay_ms": *maxDelay} {
		if v > 0 {
			retry[k] = v
		}
	}
	body = map[string]any{"name": *taskName, "schedule": schedule, "priority": *prio, "request": request}
	if len(retry) > 0 {
		body["retry"] = retry
	}
	if idemFlag != nil {
		idem = *idemFlag
	}
	if versionFlag != nil {
		version = *versionFlag
	}
	return body, idem, version, nil
}

var cliClient = &http.Client{Timeout: 30 * time.Second}

// call sends one API request. With printBody, the response body is written to stdout
// (indented when JSON). Any HTTP status >= 400 is returned as an error.
func call(method, base, path string, body any, hdr map[string]string, printBody bool) (http.Header, error) {
	tok := os.Getenv("CREP_TOKEN")
	if tok == "" {
		return nil, errors.New("CREP_TOKEN is required (export CREP_TOKEN=$(crep token <tenant>))")
	}
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, base+path, rd) //nolint:gosec,noctx // G704: the CLI calls the API URL its user configured
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := cliClient.Do(req) //nolint:gosec // G704: same as above
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	if printBody || resp.StatusCode >= 400 {
		var pretty bytes.Buffer
		if json.Indent(&pretty, out, "", "  ") == nil {
			out = pretty.Bytes()
		}
		if s := strings.TrimSpace(string(out)); s != "" {
			fmt.Println(s)
		}
	}
	if resp.StatusCode >= 400 {
		return resp.Header, errors.New(resp.Status)
	}
	return resp.Header, nil
}
