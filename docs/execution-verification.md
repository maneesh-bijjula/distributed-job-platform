# Execution tracking and recovery verification

The execution model is covered by **92 unit tests and 94 integration tests**, plus two packaged Docker smoke checks. Integration tests use isolated PostgreSQL and RabbitMQ services.

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
| Concurrent recovery | Multiple recovery passes recover a lost attempt once, with one generation advance. |
| Transactional audit | Failure to persist a completion or recovery event rolls back that transition and its accompanying attempt/outbox changes. |
| Scheduling and legacy delivery | An early legacy message cannot start a future job. UUID-only generation-zero messages still execute eligible legacy jobs; they cannot claim a recovered generation. |
| Upgrade | Automated migrations preserve earlier jobs, results, active attempts, deadlines, and outbox generations. The execution upgrade retained eight jobs and eleven history records. The retry-policy upgrade retained all 25 existing jobs, 62 history records, and 15 attempts with unchanged checksums of their original fields; no old attempts or dead letters were fabricated. |

Run the automated suite:

```sh
./scripts/test.sh
```

Run the packaged workflow:

```sh
docker compose up --build -d --wait
python3 scripts/smoke.py
python3 scripts/dead-letter-smoke.py
```

The smoke check creates a scheduled job, cancels it, creates and replays a linked manual retry, waits for the real worker result, and checks attempt persistence, lifecycle history, pagination, and conflict responses. The generated jobs remain available for inspection.

Worker-loss scenarios use deterministic lease-expiry simulation; they do not claim that these tests physically terminate a production process. The guarantees concern durable job state and fenced result writes. Arbitrary custom code can ignore cooperative cancellation and continue running until its worker process is stopped, and external side effects require their own idempotency strategy. These checks do not establish a throughput benchmark or production availability.
