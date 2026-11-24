-- Loaded by the mysql container on first start (docker-entrypoint-initdb.d) into $MYSQL_DATABASE.
-- Runs only when the data volume is empty; `docker compose down -v` to re-run. All times are UTC.

CREATE TABLE IF NOT EXISTS tasks (
  id              CHAR(36)      NOT NULL,
  tenant_id       VARCHAR(128)  NOT NULL,
  name            VARCHAR(255)  NOT NULL,
  schedule_type   ENUM('immediate', 'once', 'cron') NOT NULL,
  run_at          DATETIME(6)   NULL,                        -- once only
  cron_expr       VARCHAR(255)  NULL,                        -- cron only
  timezone        VARCHAR(64)   NOT NULL DEFAULT 'UTC',      -- IANA name; cron is evaluated in it
  priority        TINYINT       NOT NULL,                    -- 0 critical .. 3 low
  method          VARCHAR(10)   NOT NULL,
  url             VARCHAR(2048) NOT NULL,
  secret          MEDIUMBLOB    NULL,                        -- AES-256-GCM(JSON headers + body); NULL when both empty
  timeout_ms      INT           NOT NULL,
  max_attempts    INT           NOT NULL,
  backoff_base_ms INT           NOT NULL,
  backoff_max_ms  INT           NOT NULL,
  status          ENUM('active', 'paused', 'completed', 'deleted') NOT NULL,
  next_run_at     DATETIME(6)   NULL,
  idempotency_key VARCHAR(255)  NULL,                        -- client Idempotency-Key on create
  version         BIGINT        NOT NULL,
  created_at      DATETIME(6)   NOT NULL,
  updated_at      DATETIME(6)   NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_tasks_idem (tenant_id, idempotency_key),     -- NULLs don't collide
  KEY idx_tasks_due (status, next_run_at),
  KEY idx_tasks_tenant (tenant_id, created_at, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- The hot table and the work queue. Each row snapshots the task's request at planning time.
CREATE TABLE IF NOT EXISTS jobs (
  id                 CHAR(36)      NOT NULL,                 -- also sent as the outbound Idempotency-Key
  task_id            CHAR(36)      NOT NULL,
  tenant_id          VARCHAR(128)  NOT NULL,
  scheduled_for      DATETIME(6)   NOT NULL,                 -- the fire time this job exists for; never changes
  priority           TINYINT       NOT NULL,
  status             ENUM('pending', 'running', 'succeeded', 'dead', 'cancelled') NOT NULL,
  attempt            INT           NOT NULL DEFAULT 0,       -- attempts started; fencing token
  max_attempts       INT           NOT NULL,
  available_at       DATETIME(6)   NOT NULL,                 -- earliest next attempt; moves with backoff
  worker_id          VARCHAR(128)  NULL,                     -- executor holding the lease
  lease_expires_at   DATETIME(6)   NULL,
  method             VARCHAR(10)   NOT NULL,
  url                VARCHAR(2048) NOT NULL,
  secret             MEDIUMBLOB    NULL,                     -- AES-256-GCM(JSON headers + body)
  timeout_ms         INT           NOT NULL,
  backoff_base_ms    INT           NOT NULL,
  backoff_max_ms     INT           NOT NULL,
  retry_max_attempts INT           NOT NULL,
  http_status        INT           NULL,                     -- of the latest attempt
  error              VARCHAR(1024) NULL,                     -- of the latest attempt
  created_at         DATETIME(6)   NOT NULL,
  updated_at         DATETIME(6)   NOT NULL,
  finished_at        DATETIME(6)   NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_jobs_task_fire (task_id, scheduled_for),     -- scheduler idempotency
  KEY idx_jobs_claim_priority (status, priority, available_at),
  KEY idx_jobs_claim_oldest (status, available_at),          -- anti-starvation lane
  KEY idx_jobs_lease (status, lease_expires_at),             -- reconciler
  KEY idx_jobs_finished (status, finished_at),               -- archiver
  KEY idx_jobs_tenant_status (tenant_id, status, finished_at, id)  -- DLQ paging
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- One row per HTTP attempt of a job; jobs only keeps the latest result.
CREATE TABLE IF NOT EXISTS job_attempts (
  job_id       CHAR(36)      NOT NULL,
  attempt      INT           NOT NULL,
  started_at   DATETIME(6)   NOT NULL,
  finished_at  DATETIME(6)   NOT NULL,
  http_status  INT           NULL,
  error        VARCHAR(1024) NULL,
  duration_ms  BIGINT        NOT NULL,
  PRIMARY KEY (job_id, attempt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
