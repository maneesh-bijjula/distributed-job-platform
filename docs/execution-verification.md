# Execution tracking and recovery verification

The execution model is covered by **72 unit tests and 60 integration tests**, plus a packaged Docker smoke check. Integration tests use isolated PostgreSQL and RabbitMQ services.

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
| Concurrent recovery | Multiple recovery passes recover a lost attempt once, with one generation advance. |
| Transactional audit | Failure to persist a completion or recovery event rolls back that transition and its accompanying attempt/outbox changes. |
| Scheduling and legacy delivery | An early legacy message cannot start a future job. UUID-only generation-zero messages still execute eligible legacy jobs; they cannot claim a recovered generation. |
| Upgrade | Automated migrations preserve earlier jobs and results. The packaged upgrade retained all eight existing local jobs and eleven history records with unchanged checksums; no legacy attempts were fabricated. |

Run the automated suite:

```sh
./scripts/test.sh
```

Run the packaged workflow:

```sh
docker compose up --build -d --wait
python3 scripts/smoke.py
```

The smoke check creates a scheduled job, cancels it, creates and replays a linked manual retry, waits for the real worker result, and checks attempt persistence, lifecycle history, pagination, and conflict responses. The generated jobs remain available for inspection.

Worker-loss scenarios use deterministic lease-expiry simulation; they do not claim that these tests physically terminate a production process. The guarantees concern durable job state and fenced result writes. Arbitrary custom code can ignore cooperative cancellation and continue running until its worker process is stopped, and external side effects require their own idempotency strategy. These checks do not establish a throughput benchmark or production availability.
