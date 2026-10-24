# Bulk admission, contention profiling and capacity follow-up

Recorded October 21, 2026. Runtime candidate: `1929ab9a943d0442da9ba880c88cbc992e9b577c`. This increment adds bounded bulk admission, transactional metrics striping, pipelined confirmed outbox publishing, and repeatable PostgreSQL profiling. It retains the prior execution queries after an alternative query refactor regressed in the matched comparison.

## Delivered behavior

- `POST /api/v1/jobs/batch` accepts 1–100 distinct keyed items within the existing 128 KiB body limit. Each item has its own transaction, quota charge and replay identity. Mixed results use HTTP `207`; transient/unconfirmed items can be retried with their original keys. A cooperative five-second budget stops starting further transactions, while allowing an in-flight transaction to finish. See the [API contract](api.md#bulk-submission).
- Process-local `job_platform_batch_items_total{status}` counters expose item outcomes even when the envelope is `207`. Status labels are bounded; no key, owner, job ID or input text becomes a label. Each scraped API reports its own batch outcomes.
- Durable lifecycle rollups use 32 bounded internal stripes keyed by job UUID. The exporter sums them into the same metric names/labels; metric writes remain part of each lifecycle transaction. Existing counts/timings migrate into stripe zero unchanged. Stripe count is bounded independently of job volume.
- The outbox publishes a bounded batch on one scoped RabbitMQ channel, pipelines sends, then verifies every correlated acknowledgement and mandatory-routing result within one confirmation budget. Only then can one generation-qualified SQL deletion remove the locked batch. Partial confirmation, a return, a timeout or a crash leaves the transaction eligible for redelivery. Delivery remains at least once, with existing execution fencing.
- `scripts/profile-load.py` profiles the guarded disposable load stack with PostgreSQL statement statistics and active-session wait samples. It sanitizes SQL literals/comments, retains full job accounting, records observer overhead and refuses to call a profile complete when sampling fails. Ordinary load runs have no profiling overhead. `--warmup-jobs` records an explicit verified unmeasured warm-up while preserving the original default of 12.

No handler implementation, deadline, owner quota default, fsync or synchronous-commit guarantee was weakened for these results.

## Method and final capacity cases

Docker Desktop, ARM64, eight available virtual CPUs and approximately 3.83 GiB memory. Production handlers alternate WORD_COUNT/SHA256/UPPERCASE over seeded, compressible ASCII texts of 128/4096/16000 bytes. A dedicated owner permits 10000 new jobs per database minute. Twenty-four clients use two APIs and add 20% keyed replays. Every expected ID, result, successful attempt and submitted/success event is reconciled. See each raw environment file for image IDs, source hashes, runtime/container limits and the exact command.

The four final cases use 10000 measured jobs, **1000 separately verified warm-up jobs**, no future scheduling, pool 6, prefetch 1, two consumers per worker, dispatch batch 100 / polling 50 ms, and two PostgreSQL CPUs. API containers each have one CPU / 512 MiB, workers 0.75 CPU / 512 MiB each, PostgreSQL 384 MiB and RabbitMQ one CPU / 384 MiB. This is a finite closed-loop local workload, not a sustained open-loop production capacity guarantee. The main project stack was stopped with volumes retained; unrelated application containers remained present on the shared host.

| Scope | Workers | Completed jobs/sec | Queue p95 ms | Attempt p95 ms |
| --- | ---: | ---: | ---: | ---: |
| End to end, including admission | 1 | 293.79 | 13019.32 | 6.93 |
| End to end, including admission | 4 | 285.28 | 156.77 | 16.55 |
| Preloaded broker drain, excluding admission | 1 | 687.43 | 17906.82 | 2.45 |
| Preloaded broker drain, excluding admission | 4 | 798.62 | 21355.28 | 11.44 |

All **40000/40000 final measured jobs** completed correctly with exactly one successful attempt, no duplicate lifecycle success and no HTTP/transport errors. These are single trials per final case, not a repeated-median or confidence-interval claim. Queue delay in a preloaded drain includes the intentional preload wait and must not be interpreted as interactive service latency.

Four-worker scaling was approximately **0.97x end to end** and **1.16x for preloaded drain**. End-to-end and drain rates are different measures. The new observations exceed 500 jobs/sec **only for preloaded drain**; the original 500+ end-to-end and 3.4x scaling targets remain unachieved. The earlier [Phase 5 results](benchmark-results.md) retain their original environment, warm-up and scheduling conditions; direct attribution of differences to code alone would be unsupported.

## Profiling and rejected experiments

Before changing production code, the 10000-job profiled four-worker/one-DB-CPU baseline recorded 23.10 seconds of cumulative counter-upsert SQL execution time and 32 sampled blocked sessions. With metrics-only striping, the counter-upsert total fell to 2.61 seconds and sampled blocked sessions to two. However, overall profiled drain rate fell from 285.52 to 223.75 jobs/sec. Reduced contention alone did not establish a throughput improvement. Samples are sparse correlated observations and not exact fractions of wall time.

The experiment that coalesced event/metric reads and joined claim checks passed correctness tests but regressed in the measured comparison. It was removed before the final candidate. The original claim-token, generation, row-lock and final-write checks remain. An initial numeric conversion issue in that experimental path was fixed during integration testing; it is absent from the selected implementation. A separate timing test crossed a 619.77-second log gap and hit the overall deadline; the uninterrupted rerun passed without loosening its assertion.

| Intermediate case | Source | DB CPUs | Profiled | Warm-up | Jobs | Drain jobs/sec | Disposition |
| --- | --- | ---: | --- | ---: | ---: | ---: | --- |
| Exploratory baseline | `296e792` runtime, profiler later committed in `d8dcead` | 1 | Yes | 100 | 3000 | 258.79 | Diagnostic; dirty harness recorded |
| Clean baseline | `d8dcead` | 1 | Yes | 100 | 10000 | 285.52 | Baseline |
| Metrics-only | `b58dd1c` | 1 | Yes | 100 | 10000 | 223.75 | Counter contention reduced; throughput did not improve |
| Combined query experiment | `b8ffe8c` | 1 | Yes | 100 | 10000 | 107.12 | Query changes rejected |
| Combined query experiment | `b8ffe8c` | 2 | No | 12 | 10000 | 213.78 | Query changes rejected |
| Previous application image control | `d8dcead` source; same runtime files as `296e792` | 2 | No | 12 | 10000 | 633.91 | Matched current-host control |
| Rebuilt metrics-only control | `b58dd1c` | 2 | No | 12 | 10000 | 652.17 | Close to control; no strong speedup inferred from one trial |

These seven completed experimental cases account for **63000/63000 measured jobs**. Combined with the four final cases, the eleven completed measurements verify **103000 jobs**, plus **4436 separately verified warm-up jobs**. One additional attempted control run selected an unavailable intermediate image, was explicitly marked invalid and interrupted with guarded cleanup, and is excluded from timing conclusions. Its invalid/failure record is retained rather than represented as a successful trial. No failed correctness case was substituted into a successful benchmark denominator.

## Reproduce and operate

```sh
# Profile the database, including observer overhead.
python3 scripts/profile-load.py --output reports/advanced/new-profile --jobs 10000 --workers 4

# Explicit warmed end-to-end capacity case; repeat with --workers 4 for its pair.
python3 scripts/load-test.py --output reports/advanced/new-pipeline-1 \
  --jobs 10000 --warmup-jobs 1000 --clients 24 --workers 1 \
  --batch 100 --dispatch-ms 50 --prefetch 1 --pool 6 --database-cpus 2 \
  --schedule-seconds 0 --mode pipeline

# Reuse that exact built candidate for its preloaded-drain pair.
python3 scripts/load-test.py --output reports/advanced/new-backlog-1 \
  --jobs 10000 --warmup-jobs 1000 --clients 24 --workers 1 \
  --batch 100 --dispatch-ms 50 --prefetch 1 --pool 6 --database-cpus 2 \
  --schedule-seconds 0 --mode backlog --skip-build

python3 scripts/batch-smoke.py
```

Use a new output directory every time and retain unsuccessful/invalid runs. Compare the same scope, warm-up, resources and workload. Profiles and isolated test volumes are cleaned through the existing label/database guards. None of these commands targets ordinary job-platform volumes. Capacity and fault-fixture runs remain distinct.

**V8 upgrade:** stop every older API and worker before starting this version and take a verified backup. The migration preserves metric values but changes their conflict keys; older binaries cannot write V8 rollups. Recover a failed rollout with an image compatible with V8, or restore the pre-upgrade backup into an empty replacement while traffic remains closed. Downgrading the application alone does not reverse the migration. See [deployment](deployment.md) and [recovery](recovery.md).

Validation includes 303 Java tests (155 unit, 148 integration), 33 Python tests locally. CI is configured to exercise five packaged API workflows, monitoring, actual outages, bounded load, real resilience scenarios, standalone deployment/restore and five-fault regression. Final CI and a repeated 100-fault campaign remain release gates for this increment. Hosting remains an operator choice; a public domain/server was not selected for this increment.

Implementation references: [PostgreSQL 17 statement statistics](https://www.postgresql.org/docs/17/pgstatstatements.html), [PostgreSQL active-session wait information](https://www.postgresql.org/docs/17/monitoring-stats.html), [Spring AMQP confirms and scoped channels](https://docs.spring.io/spring-amqp/reference/amqp/template.html).
