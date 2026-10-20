# Load, resilience, and tuning

This phase supplies repeatable measurements and actual process/dependency failure checks. A successful run proves the recorded jobs and scenarios were verified on the recorded configuration. It does not establish a universal capacity, availability, or arbitrary-side-effect guarantee.

## Isolation and prerequisites

Use Python 3.9+ and Docker Compose 2.24.4+. `compose.load.yaml` uses Docker's `!override` tag for the second API port. The runner always uses the separate `job-platform-load` project, database `jobs_load_test`, labeled test-only containers/volumes, and fixed loopback ports **18080, 18081, 18082**. It refuses foreign container/volume labels before destructive operations. SQL inspections use a read-only connection and UUID-validated owner identifiers.

Each run starts with fresh load-only volumes and cleans them afterward, including unfreezing paused workers. Ordinary `job-platform` volumes and credentials are not used. Generated load credentials live in ignored, mode-0600 `.loadtest.env`; they are never passed as CLI arguments or recorded in results. Leave the ports available and avoid simultaneous load runners. For comparable local results, stop unrelated heavy work and record any remaining activity.

The local profile caps each API/worker at 512 MiB, PostgreSQL/RabbitMQ at 384 MiB, each API/broker at one CPU, and each worker at 0.75 CPU. PostgreSQL defaults to one CPU; `--database-cpus 2` supports a matched follow-up. These are test constraints, not deployment recommendations. The runner records Docker's available CPU/memory, architecture, image IDs, source hashes, exact command, settings, resource samples, and OOM checks. Allow enough Docker memory for two APIs and the selected worker count. A container OOM invalidates the run.

If a terminal is forcibly closed during a fault scenario, clean the isolated resources with:

```sh
python3 scripts/clean-load-stack.py
```

## Reproduce a workload

```sh
python3 scripts/load-test.py --output reports/phase5/baseline --jobs 300 --clients 16 --workers 1
python3 scripts/load-test.py --output reports/phase5/tuned --jobs 300 --clients 16 --workers 1 --batch 50 --dispatch-ms 250 --prefetch 1 --pool 6 --skip-build
```

Every run creates a dedicated owner with an explicit **10000-new-jobs-per-minute** quota. The ordinary owner default remains 120; benchmark admission settings do not silently change application users. Request concurrency is bounded, and this is a finite closed-loop workload, not an open-loop saturation/soak test.

The seeded workload rotates unmodified `WORD_COUNT`, `SHA256`, and `UPPERCASE` handlers with ASCII payloads of **128, 4096, and 16000 bytes**. A warm-up runs before measurement. One in five jobs is scheduled two seconds ahead by default; `--schedule-seconds 0` removes scheduled wait. An additional 20% of requests replay existing idempotency keys across the two APIs. Network-ambiguous and retryable failures retain the exact same key/body and can fail over to the other API. Authentication/validation errors fail the run instead of being hidden by retry. Per-request attempts, error responses, logical retries, and replay latencies remain visible in the raw ledger.

For a controlled worker-count comparison, preload the broker while warmed workers are paused, then release them:

```sh
python3 scripts/load-test.py --output reports/phase5/backlog-1 --jobs 10000 --clients 24 --workers 1 --batch 50 --dispatch-ms 250 --prefetch 1 --pool 6 --schedule-seconds 0 --mode backlog --skip-build
python3 scripts/load-test.py --output reports/phase5/backlog-4 --jobs 10000 --clients 24 --workers 4 --batch 50 --dispatch-ms 250 --prefetch 1 --pool 6 --schedule-seconds 0 --mode backlog --skip-build
```

Backlog drain throughput is distinct from end-to-end submission throughput. It excludes admission/preloading time and includes worker release and terminal-state observation. Queue delay in this mode includes the intentional preload wait, so it is not an ordinary interactive latency estimate. Warmed workers must all register consumers before the test begins. The one-to-four ratio describes the measured whole system with additional worker CPU; it is not assumed to be linear or 3.4x.

The output directory must be new; earlier evidence is never overwritten. Outputs include:

| File | Evidence |
| --- | --- |
| `environment.json` | Git/source identity, image IDs, Docker hardware limits, container limits, settings, and command. A dirty working tree is explicitly recorded. |
| `report.json` | Final success after cleanup, accepted-job accounting, throughput definitions, nearest-rank p50/p95/p99/max, request errors/retries, duplicate effects, and worker use. |
| `measured/requests.jsonl.gz` | Every HTTP attempt: API index, diagnostic key/job ID, payload size/type, HTTP status, latency, and time; no credentials or payload text. |
| `measured/jobs.jsonl.gz`, `attempts.jsonl.gz`, `events.jsonl.gz` | Every accepted job, attempt, and grouped lifecycle event. Results are checked against independently computed oracles before payload content is removed. |
| `resource-samples.jsonl.gz`, `*-logs.jsonl.gz` | Container resource samples and credential-checked structured application logs. |
| `container-final-state.json`, `failed.json` | OOM/cleanup evidence and failure diagnostics when applicable. |

Queue delay is measured from database eligibility, excluding scheduling and retry backoff. Attempt execution is wall time through its committed outcome. Job end-to-end latency includes scheduling, queueing, attempts, and recovery. Percentiles come from raw per-job/attempt samples; they do not come from Prometheus's count/sum timers. End-to-end completion rates include the polling lag (at least 250 ms between polls), making these observed rates conservative. HTTP success latencies, new-admission latencies, replays, all-attempt latencies, and logical submission including retries are reported separately.

Every expected key must resolve to exactly one job; every accepted job must succeed with the correct result, one committed success, and complete lifecycle history. A healthy workload must have exactly one attempt per job. Lost-worker scenarios can have abandoned attempts but only one successful attempt. Unknown or missing work causes failure rather than disappearing from the denominator.

## Real resilience and priority scenarios

```sh
python3 scripts/resilience-test.py --output reports/phase5/resilience --skip-build
```

This runs two actual API instances and workers, then exercises:

- Concurrent submissions and cross-API idempotency replays.
- A stopped/restored RabbitMQ container, durable admission while down, and outbox completion afterward.
- A stopped/restored PostgreSQL container, previously accepted scheduled work, failed readiness/authentication, and keyed submissions retried through restoration.
- A full API/worker stop/restart with pending jobs and stable replay IDs.
- Extra broker deliveries, verifying a single successful attempt for each job.
- An actual worker `SIGKILL` after a committed claim, followed by abandoned-attempt recovery and generation advancement.
- A frozen worker whose replacement completes first; resuming the old process must produce a correlated `completion_rejected` log and no overwritten result.
- A finite high-priority burst ahead of older low-priority work, followed by successful low-priority drain.
- A prefetch scenario demonstrating that already delivered low-priority work can delay later urgent work.
- Shared admission limits across both APIs, including free idempotency replay when the quota is full.

Fault timing uses **a separate image** built from `Dockerfile.loadtest`, with a bounded 100–2000 ms non-cooperative handler delay. This makes a committed claim observable before killing/freezing a worker and ensures a resumed handler reaches the real final-write guard. Its configuration/class is absent from the production JAR/image. All claims, leases, transactions, broker deliveries, recovery schedulers, generation checks, and result writes use the application code. Fixture runs are explicitly flagged and excluded from capacity measurements.

Strict priority offers no aging or reserved low-priority capacity. A finite burst draining successfully does not prove fairness under continuously replenished high-priority traffic. Prefetch 1 reduces work reserved ahead of later urgent messages; it does not interrupt an executing handler or establish a strict global completion order across multiple consumers. Applications requiring stronger fairness should isolate traffic classes or add a separately specified aging policy.

## Tuning controls

| Environment variable | Default | Valid range |
| --- | --- | --- |
| `JOB_DISPATCH_BATCH_SIZE` | 20 | 1–100 |
| `JOB_DISPATCH_DELAY_MS` | 1000 | 50–60000 ms |
| `JOB_DB_POOL_SIZE` | 10 per process | 2–50 |
| `WORKER_CONCURRENCY` | 2 per process | 1–32 |
| `JOB_WORKER_PREFETCH` | 10 per consumer | 1–250 |

Startup validates these bounds. The relay uses the configured bounded batch with publisher confirms and `SKIP LOCKED`; batching never changes delivery to an at-most-once policy. Increasing dispatcher capacity can remove idle polling delay, but larger batches hold more outbox locks/connections and repeated polls cost database work. Pool sizes must be budgeted across every API and worker; they are not a global connection cap. Quota limits should follow measured capacity and acceptable backlog rather than being raised to hide overload.

The optional `compose.tuned.yaml` profile provides the settings evaluated for short-text jobs: relay batch 50 / poll 250 ms, pool 6, worker concurrency 2, prefetch 1. Use it only after reviewing the recorded results and workload tradeoffs:

```sh
docker compose -f compose.yaml -f compose.tuned.yaml up --build -d --wait api worker
```

The base defaults remain available for comparison. Production capacity depends on handler cost, database/broker storage, network, container limits, admission behavior, and other workloads.

## CI and measured results

CI checks harness arithmetic/isolation/retry guards, the complete Java suite, existing packaged and monitoring checks, a small 120-job two-worker workload, and the real resilience scenarios. It uploads raw reports and invokes guarded cleanup even after failures. CI asserts correctness and accounting, not a hardware-dependent speed threshold.

See [recorded measurements](benchmark-results.md) for the actual environment, results, interpretation, and project-claim wording. The original resume's 500+ jobs/sec, 3.4x scaling, and zero loss across 100 failure tests are not inferred from these smaller checks; only measurements explicitly recorded there support numerical claims.

References: [Docker merge/override semantics](https://docs.docker.com/reference/compose-file/merge/), [Spring AMQP consumer and prefetch behavior](https://docs.spring.io/spring-amqp/reference/amqp/receiving-messages/async-consumer.html).
