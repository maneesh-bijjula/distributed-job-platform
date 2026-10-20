# Job metrics and operational visibility

The API exports a global view of the PostgreSQL lifecycle state, including work performed by non-web worker containers. A separate scheduler refreshes the database snapshot and probes RabbitMQ every five seconds by default. Prometheus scrapes every five seconds. Allow both intervals before expecting a new transition on a dashboard.

## Start the local profile

From the repository root:

```sh
python3 scripts/init-auth.py
docker compose -p job-platform up --build -d --wait api worker
python3 scripts/init-observability.py
docker compose -p job-platform --profile observability up -d --wait prometheus grafana
python3 scripts/observability-smoke.py --report target/observability-smoke.json
```

The profile pins Prometheus `v3.15.0` and Grafana `13.2.4`. Prometheus is at `http://localhost:9090`; Grafana is at `http://localhost:3001/d/job-platform`. Override `PROMETHEUS_PORT` or `GRAFANA_PORT` in `.env` when needed, then pass the corresponding URL to the verification scripts. `JOB_METRICS_REFRESH_MS` accepts 1000–60000, default 5000. Adjust freshness alert thresholds if using a longer interval.

Grafana's username is `admin`. Its generated password is stored in `.observability/grafana-admin-password`; read it privately for sign-in. The directory is mode `0700`, files are `0600`, and both Git and Docker build context exclude them. The initializer retains a valid existing scrape key and password. It replaces a revoked/unknown scrape key only after authenticating an operator. The dedicated `METRICS` role can scrape and read its own identity, but cannot use job, dead-letter, administration, or info endpoints. Revocation is checked against PostgreSQL on every scrape.

Prometheus reads the token from a mounted file, not an embedded configuration value or URL. Grafana reads its initial password from a mounted file. These local containers run as UID 0 with only `DAC_OVERRIDE`, all other capabilities dropped, and no-new-privileges, to read host-owned private files on Docker's Linux filesystem. Deployment-specific secret ownership and container hardening remain part of deployment preparation.

Grafana stores users in its persistent volume. Changing the initialization password file does not reset an existing Grafana account. Use Grafana's account administration to rotate an existing password and keep the private file aligned for the verification script. To rotate the scrape token, revoke its key ID from `.observability/metrics-key.json`, rerun the initializer, and recreate Prometheus to ensure its file mount reads the replacement. Do not copy the operator token into the scrape file.

Stop monitoring without deleting application or monitoring data:

```sh
docker compose -p job-platform --profile observability stop prometheus grafana
```

## Counter and timing definitions

V7 starts durable rollups at migration time. Lifecycle updates and their rollup deltas commit in the same PostgreSQL transaction; failed transactions and idempotent replays do not increment counters. API/worker restarts preserve these totals. Earlier history is retained but is not backfilled into counters or invented timing samples. State gauges include all current rows, including legacy jobs.

| Prometheus metric | Definition |
| --- | --- |
| `job_platform_jobs_accepted_total{type}` | Committed new jobs, including linked manual/dead-letter retries; an idempotency replay adds nothing. |
| `job_platform_jobs_started_total{type}` | Committed attempt starts; a retried job can start more than once. |
| `job_platform_jobs_succeeded_total{type}` | Jobs that committed success. |
| `job_platform_jobs_failed_total{type}` | Jobs that terminally failed: permanent failure, timeout, retry exhaustion, or worker recovery exhaustion. Retryable attempt failure alone does not increment this. |
| `job_platform_jobs_cancelled_total{type}` | First committed cancellation, queued or running. |
| `job_platform_jobs_retried_total{type,mode}` | Automatic retry scheduling or a new linked manual retry request. `mode` is `automatic` or `manual`; idempotency replays add nothing. |
| `job_platform_jobs_timed_out_total{type}` | Terminal execution deadline expiry; a subset of failed jobs. |
| `job_platform_jobs_retry_exhausted_total{type}` | Jobs whose handler attempt budget was exhausted. |
| `job_platform_worker_recoveries_total{type}` | Expired-lease requeues, excluding exhausted recoveries. |
| `job_platform_worker_recovery_exhausted_total{type}` | Terminal worker-loss recovery exhaustion. |
| `job_platform_queue_delay_seconds_count`, `_sum` | Attempt queue delay from its latest eligibility time to claim. Initial future scheduling and automatic retry backoff are excluded. A lost-worker requeue starts a new eligibility interval. Legacy jobs without recorded eligibility do not create a queue timing sample. |
| `job_platform_execution_duration_seconds_count`, `_sum` | Attempt wall time from claim to logical completion, by `type` and terminal attempt `outcome`: `SUCCEEDED`, `FAILED`, `CANCELLED`, `TIMED_OUT`, or `ABANDONED`. |

Execution duration includes time until a lost attempt is recognized as abandoned and until a cancellation/timeout is recorded. It does not measure CPU time or promise that non-cooperative handler code stopped at that instant. A retryable failure records a failed **attempt** duration while leaving its **job** nonterminal.

The dashboard calculates means from rates of sums divided by rates of counts, for example:

```promql
sum by (type) (rate(job_platform_queue_delay_seconds_sum[5m]))
/
sum by (type) (rate(job_platform_queue_delay_seconds_count[5m]))
```

With no samples in the window, a mean can be absent/NaN. There are no histogram buckets or p95/p99 claims. Use the later load-testing phase for workload-specific performance evidence.

## Backlogs, dependencies, and freshness

| Metric | Meaning |
| --- | --- |
| `job_platform_jobs{type,status}` | Current durable job counts. |
| `job_platform_outbox_entries{state}` | Outbox entries `due` for delivery or `scheduled` in the future. |
| `job_platform_outbox_oldest_due_age_seconds` | Age of the oldest due outbox entry; future scheduling/backoff contributes zero. |
| `job_platform_retry_jobs{state}` | Queued automatic retries `waiting` for backoff or `due` after backoff. Ordinary queued jobs are not retry backlog. |
| `job_platform_queue_oldest_due_age_seconds` | Longest wait among all eligible queued jobs since eligibility; future eligibility contributes zero. Legacy rows fall back to existing creation/schedule/retry timestamps. |
| `job_platform_expired_leases` | Running attempts with expired leases awaiting a durable recovery transition. |
| `job_platform_dead_letters{state}` | Persistent inbox records by `OPEN`, `RECOVERED`, or `DISCARDED`. |
| `job_platform_broker_messages{queue}` | Passive RabbitMQ ready-message counts, combining current and legacy ready queues, or dead queue depth. Excludes unacknowledged in-flight deliveries. Unavailable depth is NaN, not zero. |
| `job_platform_snapshot_up`, `job_platform_snapshot_age_seconds` | Success of the last DB refresh and age of the last successful snapshot. Failed refreshes retain previous job values. |
| `job_platform_dependency_up{dependency}` | Last DB snapshot or broker probe success (`postgres` / `rabbitmq`). |
| `job_platform_broker_snapshot_age_seconds` | Age of the last successful queue probe. |
| `job_platform_operation_failures_total{operation}` | Process-local failures in `outbox_dispatch`, `execution_recovery`, `snapshot_refresh`, or `broker_probe`; these counters reset on process restart. |

Custom labels are bounded enums. Job IDs, attempt IDs, owner IDs, keys, inputs, and exception messages do not become metric labels. Job IDs and attempt IDs are available in structured lifecycle logs for correlation.

**Scrape one API for global totals.** Multiple API instances export the same durable counters and state gauges. Summing their exports would double count work; the provided target is only `api:8080`. Instance-local JVM/HTTP/operation metrics describe that API process. The worker is not an HTTP scrape target; its lifecycle outcomes enter the shared rollups. Multi-instance monitoring topology and load/retention costs need deployment-specific review.

## Probes and outages

| Situation | Liveness | Readiness | Expected behavior |
| --- | --- | --- | --- |
| Healthy | 200 | 200 | Admission, dispatch, execution, and scrapes proceed. |
| RabbitMQ stopped | 200 | 200 | New jobs can commit to PostgreSQL; the outbox retains undelivered work. Broker metrics report unavailable, and the broker alert fires after 30 seconds. Work drains after restoration. |
| PostgreSQL stopped | 200 | 503 | Authentication fails closed with 503; no new work is acknowledged as accepted. Authenticated Prometheus scrapes also fail, making `up{job="job-platform"}` zero and triggering the unavailable-scrape alert. Existing stored work remains for restoration. |

Probe paths are `/actuator/health/liveness` and `/actuator/health/readiness`. Neither requires a credential or returns component details. Readiness includes PostgreSQL and application readiness, deliberately excluding RabbitMQ because durable admission can continue during a broker outage. Aggregate `/actuator/health` may still be DOWN when RabbitMQ is unavailable. Compose checks the readiness endpoint.

The snapshot-staleness metrics help when refresh fails but scraping still works. They cannot replace Prometheus's own `up` signal when database-backed authentication prevents the scrape. Do not interpret a missing scrape or NaN broker depth as a healthy empty queue.

## Logs and alerts

API/worker console output uses Spring Boot ECS JSON. Committed lifecycle logs contain `event=job_transition`, `job_id`, optional `attempt_id`, `job_type`, `transition`, and `job_status`. Rolled-back transitions are not logged as committed. Application operational failures log fixed messages and bounded `error_kind`, with job/attempt IDs where available. Raw payloads, tokens, handler exception messages, and stack traces are excluded from these logs. Built-in framework messages may still include operational addresses; restrict log access appropriately.

The provisioned 17-panel dashboard shows real Prometheus data. Example alerts are in `monitoring/prometheus/alerts.yml`: scrape unavailable, stale DB snapshot, broker unavailable, dispatch delayed, workers delayed, expired leases, open dead letters, and terminal failures. They are evaluated locally and visible in Prometheus's Alerts view; no Alertmanager or external notification destination is configured. Thresholds are illustrative and need tuning against an observed workload.

Validate configuration and deterministic alert behavior:

```sh
docker compose -p job-platform --profile observability run --rm --no-deps --entrypoint /bin/promtool prometheus check config /etc/prometheus/prometheus.yml
docker compose -p job-platform --profile observability run --rm --no-deps -w /etc/prometheus --entrypoint /bin/promtool prometheus test rules alerts.test.yml
```

Verify live values, scrape permissions, credential/payload exclusion, every provisioned panel expression, and an actual Grafana data-source response:

```sh
python3 scripts/observability-smoke.py --report target/observability-smoke.json
```

This creates three jobs (two successes and one cancelled scheduled source), verifies a linked manual retry, and leaves them available for inspection. It requires an operator key through the existing private credential helper.

The following check **temporarily stops only this Compose project's RabbitMQ, restores it, then stops/restores PostgreSQL**. Run it when interrupting local work is acceptable. It never deletes volumes, restores stopped services in `finally` blocks, verifies the actual firing alerts, and checks that a job accepted during broker downtime finishes afterward:

```sh
python3 scripts/observability-outage-smoke.py --report target/observability-outages.json
```

CI runs the full 245-test suite and both monitoring checks against its packaged application. Integration coverage includes exact lifecycle/timing semantics, transaction and metric-flush rollback, suspended transactions, mixed-type concurrent batches, restart persistence, bounded tags, sanitized correlated logs, metric snapshot failure, and scrape-key scope/revocation. These operational checks establish the tested behavior, not production availability or a throughput benchmark.
