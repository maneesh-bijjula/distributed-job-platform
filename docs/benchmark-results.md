# Recorded load and resilience results

Recorded October 19, 2026. **All 42400 measured jobs across twelve load runs succeeded with correct results, complete lifecycle accounting, and exactly one successful attempt per job.** The repeated 300-job pipeline workload improved from a median **34.50 to 97.81 completed jobs/second (2.84x)** with the evaluated profile. Extra workers did not improve this short-text workload under the tested limits.

The [machine-readable index](evidence/phase5/index.json), per-run request/job/attempt/event ledgers, resource samples, environment records, and [SHA-256 manifest](evidence/phase5/sha256.json) are committed beside this document. Results were checked against independent oracles before payload content was removed. Source hashes were verified against each recorded Git revision. All twelve runs used the same application image; the later revision added the test-only database CPU parameter.

## Environment and method

- Docker Desktop 29.5.3, ARM64, eight available virtual CPUs, approximately 3.83 GiB Docker memory.
- Two APIs: each 512 MiB / one CPU. Each worker: 512 MiB / 0.75 CPU, two consumers. RabbitMQ: 384 MiB / one CPU. PostgreSQL: 384 MiB, one CPU unless marked two below.
- Java 21, Spring Boot 3.5.16, PostgreSQL 17, RabbitMQ 4.1. Exact image IDs/digests, commands, runtime versions, and source identity are in each `environment.json`.
- One dedicated owner per workload, quota 10000 new jobs/minute. Sixteen clients for 300-job runs; twenty-four for 10000-job runs. An extra 20% of requests replayed existing keys. No HTTP/transport errors, incorrect results, or duplicate successful attempts occurred in the load runs.
- Round-robin word count, SHA-256, and uppercase jobs; ASCII payloads of 128, 4096, and 16000 bytes. Payloads repeat seeded text and are compressible; these results do not represent arbitrary data or expensive/custom handlers.
- Twelve unmeasured warm-up jobs per run. This is a finite workload, not a steady-state saturation or long-duration soak result. Baseline/tuned pipeline settings each have three repetitions; other cases have one recorded run each.
- The ordinary project stack and test containers were stopped. Low-activity unrelated CloudDrive application containers remained on Docker; background activity was not completely eliminated.
- Capacity runs used actual application persistence, publisher confirms, claims, and fenced completion. The delayed fault-test fixture was excluded.

## Finite pipeline workload

Each run submitted 300 unique jobs plus 60 replays; one in five jobs was scheduled two seconds ahead. Completion rate includes submission through observed terminal state. Queue timing excludes intended scheduling wait. Percentiles use nearest-rank values from raw samples.

| Run | Workers | Completed jobs/s | Queue p95 ms | Attempt p95 ms |
| --- | ---: | ---: | ---: | ---: |
| [Baseline 1](evidence/phase5/baseline-1/report.json) | 1 | 36.98 | 5801.72 | 61.27 |
| [Baseline 2](evidence/phase5/baseline-2/report.json) | 1 | 34.44 | 5284.16 | 60.10 |
| [Baseline 3](evidence/phase5/baseline-3/report.json) | 1 | 34.50 | 6254.09 | 59.10 |
| [Tuned 1](evidence/phase5/tuned-1/report.json) | 1 | 97.00 | 809.90 | 42.40 |
| [Tuned 2](evidence/phase5/tuned-2/report.json) | 1 | 97.81 | 630.73 | 54.56 |
| [Tuned 3](evidence/phase5/tuned-3/report.json) | 1 | 99.45 | 698.90 | 56.25 |
| [Relay settings only](evidence/phase5/relay-only/report.json) | 1 | 116.47 | 489.07 | 56.38 |
| [Tuned, four workers](evidence/phase5/pipeline-4-workers/report.json) | 4 | 62.06 | 650.13 | 79.61 |

Baseline: relay batch 20 / poll 1000 ms, pool 10, prefetch 10. Tuned: batch 50 / poll 250 ms, pool 6, prefetch 1. The relay-only run retained pool 10 and prefetch 10.

The median of the three per-run queue p95 values fell from **5801.72 to 698.90 ms**, approximately **88% lower**. This compares per-run p95 values; it is not a pooled percentile or confidence interval. The results support the combined profile's improvement over base settings, without isolating each setting's causal contribution.

The single relay-only trial was faster. Prefetch 1 is selected for responsiveness to later urgent work, supported by separate priority tests, while pool 6 lowers the connection budget. The profile is an explicit tradeoff, not a maximum-throughput claim.

## Preloaded broker drain

Warmed workers were paused while 10000 unique jobs plus 2000 replays were admitted and fully dispatched, then released. There was no future scheduling. Drain rate excludes preloading/admission; raw queue delay includes that intentional wait and is not ordinary user-facing latency.

| Run | DB CPU budget | Workers | Drain jobs/s | Attempt p95 ms |
| --- | ---: | ---: | ---: | ---: |
| [One worker](evidence/phase5/backlog-1-worker/report.json) | 1 | 1 | 443.05 | 3.89 |
| [Four workers](evidence/phase5/backlog-4-workers/report.json) | 1 | 4 | 338.78 | 62.47 |
| [One worker, CPU follow-up](evidence/phase5/db2-backlog-1-workers/report.json) | 2 | 1 | 445.49 | 3.65 |
| [Four workers, CPU follow-up](evidence/phase5/db2-backlog-4-workers/report.json) | 2 | 4 | 321.13 | 42.27 |

One-to-four-worker rate ratios were approximately **0.76x with one DB CPU** and **0.72x with two**. All four worker processes handled jobs. Additional workers consumed resources without improving these recorded rates.

During the one-CPU four-worker drain, PostgreSQL averaged approximately 100% of one CPU across seven resource samples; workers averaged approximately 33–38% each, below their 75% quotas. This suggested shared database pressure, so a matched two-CPU experiment was performed. It did not improve throughput. CPU allocation alone therefore does not explain or resolve the limit. Query/lock contention, JVM warm-up, and shared persistence costs need further profiling before claiming a specific root cause or benefit from more workers.

For this measured short-text workload, use **one worker with two consumers**, bounded pools, and the evaluated relay settings. At four workers, pool 6 budgets 36 connections across two APIs and four workers, versus 60 at pool 10. Extra pools/workers should follow measured benefit. The ordinary 120-jobs/minute owner default remains unchanged; benchmark quotas are explicit test-owner settings.

## Resilience and project claims

The [recorded resilience report](evidence/phase5/resilience/report.json) accounts for **194/194 jobs** through concurrent APIs/workers, extra deliveries, broker/database interruption, application restart, actual worker termination, and stale-process resumption. The [actual rejected completion log](evidence/phase5/resilience/stale-completion-rejected.json) correlates the old attempt after its replacement succeeded. Priority/prefetch and shared admission checks also pass. Each listed scenario executes once per suite run; these are not 100 independent failure trials or an arbitrary-side-effect guarantee.

These measurements do **not** establish 500+ end-to-end jobs/sec or 3.4x one-to-four-worker scaling. The best recorded backlog drain was 445.49 jobs/sec under its stated limits. Backlog drain and end-to-end rates are different measurements. Sustained production capacity, arbitrary handlers, larger deployments, and continuous-traffic fairness remain outside these observations.

Evidence-matching project wording:

- Built a Java/Spring Boot job platform with PostgreSQL transactional outbox, RabbitMQ leased workers, idempotent APIs, retries, dead-letter recovery, and Prometheus/Grafana monitoring.
- Verified 42400 synthetic benchmark jobs; improved median finite-workload completion throughput from 34.5 to 97.8 jobs/sec with measured dispatch/prefetch/pool settings. Tested real worker/dependency recovery and stale-result fencing with 194/194 scenario jobs accounted for.

Implemented technologies include Java, Spring Boot, PostgreSQL, RabbitMQ, Docker, Prometheus, and Grafana.
