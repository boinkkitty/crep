# Crep: Distributed Cron Service

Crep schedules HTTP requests. You create a **task** that runs immediately, once at a
given time, or on a cron expression in a timezone. Each planned run is a **job**.
A job keeps a copy of the task's request at the moment it is planned, so later task
edits don't change it. The MySQL `jobs` table is the work queue (there is no message
broker). Cassandra stores archived history.

## Layout (hexagonal)

```
cmd/crep/                 one binary: serve <service>, keygen, token
internal/domain/          pure business rules: Task, Job status machine, Schedule, RetryPolicy, Priority
internal/app/ports/       interfaces: TaskRepo, JobRepo, HistoryStore, TxManager, HTTPCaller, Clock, ...
internal/app/services/    use cases: TaskService, Scheduler, Executor, Reconciler, Archiver, JobService
internal/adapters/
  mysql/                  repos + READ COMMITTED TxManager (the queue)
  cassandra/              HistoryStore
  httpapi/                inbound REST (Task API + Job API), JWT auth
  httpcaller/             outbound HTTP with SSRF policy
  crypto/                 AES-256-GCM keyring (rotation) + request codec
  memory/                 in-process store for tests
  metrics/, clock/, idgen/
deploy/mysql-init/        MySQL schema, loaded by docker-entrypoint-initdb.d
```

Dependencies point inward: the domain imports nothing from the app or adapter layers.
Services depend only on ports, and `cmd/crep` wires up the adapters.

## Services

Each process runs exactly one service: `crep serve <service>`.

| Service | What it does |
| --- | --- |
| `task-api` | `POST/GET /v1/tasks`, `GET/PUT/DELETE /v1/tasks/{id}`, `POST /v1/tasks/{id}/pause\|resume`. `PUT` is a full replace and requires `If-Match: "<version>"` (the `ETag` of `GET`): `428` without it, `412` if stale |
| `job-api` | `GET /v1/jobs/{id}`, `GET /v1/tasks/{id}/jobs` (MySQL and Cassandra merged), `GET /v1/dlq`, `POST /v1/jobs/{id}/retry`, `DELETE /v1/dlq/{id}` |
| `scheduler` | Every 1s, locks due tasks (`FOR UPDATE SKIP LOCKED`) and inserts jobs for fires within 30s, including every fire missed during downtime |
| `executor` | Claims jobs, waits until they are due, calls the endpoint, records a fenced result |
| `reconciler` | Every 5s, requeues or dead-letters `running` jobs whose lease has expired |
| `archiver` | Copies finished jobs to Cassandra, then deletes them from MySQL: succeeded/cancelled after 1h, dead after 14 days (the DLQ retry window). Cassandra rows expire after 30 days |

Every process also serves `/healthz`, `/readyz` and `/metrics` on `CREP_OPS_ADDR` (default `:9090`).

## Correctness guarantees

- **Each fire is planned once.** `UNIQUE(task_id, scheduled_for)` means two schedulers
  racing on the same fire both succeed, and the second insert does nothing.
- **Each claim goes to one executor.** Claims use `SELECT … FOR UPDATE SKIP LOCKED`.
- **Fencing.** A claim increments `attempt` and sets `worker_id`. A result is only
  written `WHERE status='running' AND attempt=? AND worker_id=?`, so an executor
  that lost its lease cannot overwrite a newer attempt.
- **Idempotent task creation.** `POST /v1/tasks` accepts an `Idempotency-Key` header
  (≤255 chars). A repeat with the same key for the same tenant returns the original
  task with `200` instead of creating another (`201`). Keys don't expire and the
  repeated body is not compared with the original.
- **At-least-once delivery.** Every attempt sends the same `Idempotency-Key` (the
  job ID), plus `X-Attempt`, `X-Job-Id`, `X-Task-Id` and `X-Scheduled-For` (RFC 3339, UTC; the same on
  every attempt, so receivers can tell a retry from a new run).
- **Retries.** Only 2xx succeeds. Everything else (any non-2xx status, timeouts,
  transport errors) is retried with exponential backoff with jitter, between d/2 and d,
  until `max_attempts` runs out; then the job goes to the DLQ. A manual retry grants
  exactly one more attempt, with the same job ID and Idempotency-Key.
- **Missed fires.** No run is dropped: after scheduler downtime every missed fire is
  planned (oldest first, at most 1,000 per task per batch) and runs immediately. The
  exception is a paused task: resuming it skips the fires missed while it was paused,
  on purpose. Local times that don't exist because of a DST change are skipped.
- **Priorities and starvation.** Claims go in order critical > high > normal > low.
  Every 5th claim is taken oldest-due first, ignoring priority.
- **Tenant fairness.** Each executor splits its capacity evenly across the tenants
  that have work, keeping priority order within that split. Slots no tenant needs
  are handed out up to `CREP_EXECUTOR_TENANT_LIMIT`.
- **Graceful shutdown.** An executor stops claiming, returns jobs it hasn't started
  yet (without using up an attempt), and gives in-flight calls `CREP_DRAIN_TIMEOUT`
  to finish.

## Security

- **Auth.** A JWT bearer token is required (HS256 secret, or an RS/ES public key).
  The tenant comes from the `tenant_id` claim, and every query is scoped to that tenant.
- **Encryption at rest.** All request headers and the body are stored together as one
  AES-256-GCM blob (the `secret` column). Header values marked `secret: true` and bodies
  marked `body_secret: true` are additionally shown as `***` in API responses. Rotate keys with `CREP_ENCRYPTION_KEYS=new:…,old:…`; the first key is used
  for new data. Keep old keys until every row that uses them has been archived.
- **Archived history holds no secrets.** Cassandra never gets headers or bodies, and
  archived URLs lose their `user:password@`, query string and fragment. Call errors
  stored on jobs never include the URL.
- **Unreadable rows don't stall the queue.** If a stored request can't be decrypted
  (for example, its key was removed), the scheduler pauses that task and the executor
  sends its jobs to the DLQ without calling the endpoint. Other work keeps flowing.
  The stored ciphertext is never overwritten, so restoring the key makes the task
  usable again. Editing such a task requires supplying a new `request`.
- **SSRF protection.** URLs are checked when a task is created. The dialer also
  checks every *resolved* IP, including on redirects. Loopback, RFC1918, link-local
  and metadata (169.254/16), CGNAT, ULA and multicast addresses are blocked. Ports
  are limited by `CREP_ALLOWED_PORTS` (default 80,443), and environment proxies are
  ignored.

## Configuration

These environment variables are read in `internal/config`:

| Variable | Default |
| --- | --- |
| `CREP_MYSQL_DSN` | `crep:crep@tcp(127.0.0.1:3306)/crep` |
| `CREP_CASSANDRA_HOSTS`, `CREP_CASSANDRA_KEYSPACE`, `CREP_CASSANDRA_RF` | `127.0.0.1`, `crep`, `1` |
| `CREP_ENCRYPTION_KEYS` | required (generate with `crep keygen`) |
| `CREP_JWT_SECRET` or `CREP_JWT_PUBLIC_KEY_FILE`; `CREP_JWT_ISSUER`, `CREP_JWT_AUDIENCE`, `CREP_JWT_TENANT_CLAIM` | required |
| `CREP_HTTP_ADDR`, `CREP_OPS_ADDR` | `:8080`, `:9090` |
| `CREP_ALLOWED_PORTS`, `CREP_ALLOW_PRIVATE_DESTINATIONS`, `CREP_DENY_CIDRS` | `80,443`, `false`, none |
| `CREP_EXECUTOR_CONCURRENCY`, `CREP_EXECUTOR_TENANT_LIMIT`, `CREP_LEASE_GRACE`, `CREP_DRAIN_TIMEOUT` | `64`, = concurrency, `30s`, `30s` |
| `CREP_SCHEDULER_LOOKAHEAD`, `CREP_ARCHIVE_RETENTION`, `CREP_ARCHIVE_DEAD_AFTER` | `30s`, `1h`, `336h` |
| `CREP_HISTORY_TTL` (Cassandra row TTL; `0` keeps history forever) | `720h` |

## Running locally

```sh
docker compose -f deploy/docker-compose.yml up -d --build   # MySQL (schema from deploy/mysql-init), Cassandra, 6 services, echo target
# If 3306/9042 are taken: CREP_MYSQL_HOST_PORT=3307 CREP_CASSANDRA_HOST_PORT=9043 docker compose ...
# To make the echo target fail, send header {"name": "x-set-response-status-code", "value": "503"}.

go build -o bin/crep ./cmd/crep                              # the same binary is the CLI
export CREP_JWT_SECRET=dev-secret-change-me-dev-secret-change-me
export CREP_TOKEN=$(bin/crep token acme)                     # dev JWT for tenant "acme"
# CREP_TASK_API / CREP_JOB_API default to http://localhost:8080 / :8081
```

```sh
# Every 10 seconds, Berlin time, with a secret header
bin/crep task create -name ping -url http://echo:8080/hook -cron "*/10 * * * * *" -tz Europe/Berlin \
  -priority high -secret-header "Authorization: Bearer xyz" -body '{"hello":1}' -idempotency-key ping-1

# Once at a given time; or now (no -cron/-at)
bin/crep task create -name reminder -url http://echo:8080/remind -at 2026-12-24T18:00:00Z
bin/crep task create -name doomed -url http://echo:8080/fail -header "x-set-response-status-code: 503" -max-attempts 2

bin/crep task list
bin/crep task update <task-id> -name ping -url http://echo:8080/hook -cron "*/30 * * * * *"   # full replace
bin/crep job history <task-id> -limit 20
bin/crep job dlq
bin/crep job retry|discard <job-id>
bin/crep task pause|resume|delete <task-id>
```

`crep task create -h` lists every flag. Output is the API's JSON. HTTP errors exit 1.
`task update` is a full replace, so a flag you leave out goes back to its default. It sends
`If-Match` with the task's current version, or with `-version n` if you want the update
to fail when someone else changed the task after you read it.

Schedule types are `immediate`, `once` (with `run_at`, RFC 3339), and `cron`. Cron
accepts 5 fields, an optional leading seconds field, or descriptors such as `@hourly`.

## Tests

```sh
go test -race ./...                                   # domain, services (in-memory), adapters, HTTP API
CREP_TEST_MYSQL_DSN='crep:crep@tcp(127.0.0.1:3306)/crep_test' CREP_TEST_CASSANDRA_HOSTS=127.0.0.1 \
  go test -tags=integration -p 1 ./internal/integration/
```

The integration suite wipes its tables, so it refuses to run unless the database name
ends in `_test`. The compose MySQL creates `crep_test` on first start. It checks the
following against real MySQL 8 and Cassandra 5:

- 4 concurrent schedulers plan each fire exactly once.
- 4 executors × 300 jobs call each endpoint exactly once.
- A stale writer is fenced out after the reconciler recovers its job.
- A failing job retries, lands in the DLQ, and succeeds on manual retry.
- Dead jobs are archived only after the DLQ window, and one retried after being
  listed is kept.
- Idempotency keys are unique per tenant.
- Archived and live history merge correctly when paging.
- Secrets are not stored in plaintext.
- Rows encrypted with an unknown key are paused or dead-lettered without blocking
  other jobs, and their ciphertext is preserved.

### CI

`.github/workflows/ci.yml` runs on every push and PR:

| Job | What it checks |
| --- | --- |
| Lint | `go mod tidy` is clean, `go vet` (incl. `integration` tag), golangci-lint v2 (`.golangci.yml`: revive — golint's successor — staticcheck, errcheck, gosec, errorlint, …), gofmt/goimports |
| govulncheck | Known vulnerabilities reachable from our code, stdlib included |
| Unit tests | `go test -race` with coverage |
| Integration | The suite above against MySQL + Cassandra from `deploy/docker-compose.yml` |
| Container image | Multi-arch (amd64/arm64) build with SBOM + provenance; on `main` and `v*` tags it is pushed to `ghcr.io/<owner>/crep` and signed with cosign (keyless) |

The Go toolchain is pinned by the `toolchain` line in `go.mod`; bump it (and the
Dockerfile base image) when govulncheck reports a standard-library fix. Run the same
checks locally with `golangci-lint run ./...` and `go run golang.org/x/vuln/cmd/govulncheck@latest ./...`.

## Known limitations

- Lease and backoff timing uses each process's wall clock, so keep hosts NTP-synced.
- Tenant fairness is enforced within each executor, not across the cluster.
- Pausing a task does not cancel jobs already planned inside the 30s window.
  Deleting a task does cancel them.
- If a task's schedule is edited, jobs already planned keep their original fire times.
