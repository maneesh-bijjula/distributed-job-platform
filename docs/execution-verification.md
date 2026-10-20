# Execution tracking and recovery verification

The platform is covered by **122 unit tests and 141 integration tests**, eighteen Python harness/safety tests, and packaged Docker workflows. Integration tests use isolated PostgreSQL and RabbitMQ services.

| Behavior | Verification |
| --- | --- |
| Visible, durable execution | A claim commits `RUNNING`, an attempt, and `STARTED` before the handler starts; the handler observes no active database transaction. |
| Duplicate delivery | Concurrent claims create one attempt. Active and terminal jobs reject duplicate claims. |
| Heartbeat renewal | A cooperative handler runs beyond its initial lease while heartbeats extend that lease without moving its execution deadline. |
| Heartbeat failure | Failed renewals cannot extend the local lease; the handler stops at a checkpoint and the attempt is recovered as abandoned. |
| Worker loss | Tests commit a claim, then expire its lease to simulate a missing worker. Recovery advances the generation and creates durable outbox work. |
| End-to-end recovery | The real scheduler, PostgreSQL outbox, RabbitMQ publisher, listener, and replacement worker complete a recovered generation-one execution. |
| Stale workers | An expired or forged claim cannot renew, fail, or finish the active replacement attempt. Late results cannot overwrite a newer result. |
| Running cancellation | An HTTP request cancels a paused cooperative handler; cancellation is durable before the handler exits. A non-cooperative paused handler's late result is also rejected. |
| Execution deadline | A real short deadline stops a cooperative handler. Results arriving after expiry fail even before the recovery scan runs. Recovery queue wait does not reset the deadline. |
| Bounded recovery | Repeated lease loss reaches the configured recovery limit and produces a durable failure. |
| Retry timing and limits | Transient handler failures persist capped exponential backoff; early/stale deliveries cannot claim them. Actual broker delivery completes after two automatic retries; exhaustion creates one inbox entry. |
| Backoff restart | Two application contexts are started and stopped against the same test database; the replacement retains `retryAt`, the fixed deadline, and completes the second attempt. |
| Failure classification | Known transient dependency failures retry; permanent handler failures immediately fail with sanitized attempt outcomes. Lifecycle storage failures are not misclassified as handler failures. |
| Retry cancellation and rollback | Cancellation fences a waiting retry. Audit/inbox storage failures roll back the attempt, job state, and outbox changes. Terminal failures remove pending outbox work. |
| Dead-letter capture | Bounded manual broker collection commits a sanitized record before acknowledgement; database failure requeues it, and repeated capture retains its original resolution. |
| Dead-letter recovery | Concurrent requests create one resolution and one linked child or one generation advance. Key conflicts, stale generations, schedule/backoff preservation, running/successful conflicts, and audit rollback are tested. |
| Malformed messages | Invalid payloads, invalid generation headers, and unknown job IDs remain inspectable; recovery rejects them, while audited discard is idempotent. |
| Owner isolation | Real HTTP requests reject other-owner reads, cancellation, retry, results, event history, and attempts with `404`; listings are owner filtered, JSON cannot select an owner, and operator role does not bypass the job API. |
| Keys and roles | Real bearer keys authenticate, ordinary keys cannot use any admin/dead-letter operation, key storage contains only hashes, metadata never returns tokens/hashes, revocation is idempotent, and bootstrap never resurrects revoked keys. |
| Shared quotas | Two real API instances share one database owner quota: 20 concurrent submissions admit exactly 5 and reject 15. Multiple keys share the limit, new windows reset it, replays are free, and creation rollback also rolls back admission. |
| Access audit | Owner/key administration commits with actor audit; audit failure rolls back key issuance. Operator dead-letter recovery retains the source owner and records its actor key. |
| Authentication failure | Missing/invalid/revoked credentials return `401`, insufficient privileges return `403`, auth storage failure returns sanitized `503`, and public health remains available without exposing configuration. |
| Concurrent recovery | Multiple recovery passes recover a lost attempt once, with one generation advance. |
| Transactional audit | Failure to persist a completion or recovery event rolls back that transition and its accompanying attempt/outbox changes. |
| Scheduling and legacy delivery | An early legacy message cannot start a future job. UUID-only generation-zero messages still execute eligible legacy jobs; they cannot claim a recovered generation. |
| Upgrade | Automated migrations preserve earlier jobs, results, active attempts, deadlines, and outbox generations. The execution upgrade retained eight jobs and eleven history records. The retry-policy upgrade retained all 25 existing jobs, 62 history records, and 15 attempts with unchanged checksums of their original fields; no old attempts or dead letters were fabricated. |
| Ownership upgrade | The packaged V6 upgrade retained all 29 jobs, 75 history records, 17 attempts, and 2 dead-letter records with matching checksums of their original fields. All earlier jobs belong to the legacy owner; only keys for that owner can use the ordinary job API for them. |
| Durable observability | Lifecycle counters and attempt timings commit with job state. Replays, duplicate delivery, and rollbacks do not inflate totals. Metric-flush failure rolls back lifecycle/audit/outbox/admission changes. Restart preserves totals; V7 does not invent earlier samples. |
| Concurrent metric updates | Mixed-type lifecycle batches and suspended/new transactions preserve committed counts without inverse metric-row locks. |
| Timing semantics | Queue samples exclude future scheduling and retry backoff. Attempt samples include retryable failures, abandoned attempts, cancellation, timeout, and success; terminal job failures are counted separately. |
| Safe metric export | Fixed enum labels, no IDs/inputs/keys, retained snapshots with explicit failure/freshness indicators, and a restricted/revocable METRICS role. |
| Correlated logging | Committed lifecycle logs include job/attempt IDs and omit inputs, keys, and private exception messages. Rolled-back transitions produce no committed-transition log. |
| Monitoring profile | Promtool validates configuration and alert cases. The packaged smoke executes all 17 provisioned panel expressions and retrieves actual job data through Grafana's data source. |
| Real dependency outages | The packaged check physically stops/restores this Compose project's broker and database, verifies actual firing alerts, public probes, fail-closed authentication, durable admission during broker downtime, and completion after restoration. |
| Actual worker termination | The isolated Phase 5 runner kills a controlled worker after a committed claim, then verifies an abandoned attempt, a new delivery generation, one correct success, and no missing job. |
| Resumed stale process | A frozen non-cooperative fixture worker resumes after replacement completion. Its real final write is rejected, with correlated `completion_rejected` evidence and an unchanged successful replacement result. |
| Load accounting and timing | The isolated load harness reconciles every diagnostic key, job, attempt, and lifecycle event; independently checks all results; and records raw latency distributions, duplicates/errors, container limits, resource samples, and exact source/image identity. |
| Priority and prefetch | A finite urgent burst overtakes older queued work before it drains; delivered low-priority messages can delay later urgent work with higher prefetch. Continuous-traffic fairness is not claimed. |
| Capacity controls | Startup rejects unsafe bounds. A real integration check verifies custom batch limits preserve remaining outbox work and pool configuration reaches Hikari. |
| Observability upgrade | The packaged V7 upgrade preserved 37 jobs, 96 events, 20 attempts, 2 outbox entries, 4 dead letters, 4 key records, owners, access audit, and admission records with matching checksums of their original fields. |
| Fresh deployment | The separate release profile starts with random file-mounted credentials and empty volumes, exposes only a loopback API, and passes packaged lifecycle and access-control workflows. |
| Graceful worker shutdown | SIGTERM during an eight-second fixture execution permits its one successful attempt before normal JVM termination. Prefetched work is returned rather than drained during shutdown. |
| Backup and restore | Offline custom-format dumps are restored into a scratch database and into empty replacement deployment volumes; every public table and sequence fingerprint matches before startup. Nonempty targets and online maintenance are refused. |
| Lost broker deliveries | Offline reconciliation recreates missing outbox work only for queued jobs, preserves future schedules and is repeatable; the restored worker verifies original job results without duplicate attempts. |
| Failed rollout | An intentionally failing application startup leaves the schema/data unchanged; restoring the compatible image recovers operation. Database migrations are not automatically reversed. |

Run the automated suite:

```sh
./scripts/test.sh
```

Run the packaged workflow:

```sh
docker compose up --build -d --wait
python3 scripts/smoke.py
python3 scripts/dead-letter-smoke.py
python3 scripts/auth-smoke.py
```

The [observability guide](observability.md) includes the optional monitoring profile, live metric/dashboard check, and controlled outage verification. CI runs these checks too. The automated Java suite totals **263 tests** (122 unit and 141 integration), plus eighteen Python harness/safety tests. The [performance guide](performance.md) covers the additional packaged load/fault workflows and recorded evidence. The [deployment guide](deployment.md) and [recovery runbook](recovery.md) describe the repeatable release drill.

The smoke check creates a scheduled job, cancels it, creates and replays a linked manual retry, waits for the real worker result, and checks attempt persistence, lifecycle history, pagination, and conflict responses. The generated jobs remain available for inspection.

The Java suite uses deterministic lease-expiry simulation. The separate Phase 5 fault runner also physically kills/freezes disposable worker processes using an explicitly identified delay fixture; the fixture is excluded from capacity measurements. The guarantees concern the recorded durable job state and fenced result writes. Arbitrary custom code can ignore cooperative cancellation, and external side effects require their own idempotency strategy. Recorded throughput belongs to its measured environment/workload and does not establish production availability or universal capacity.
