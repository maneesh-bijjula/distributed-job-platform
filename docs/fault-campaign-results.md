# Recorded 100-fault campaign

Recorded October 20, 2026, on local Docker Desktop. **All 100 planned fault trials
passed: 800/800 accepted jobs succeeded, with zero observed lost jobs, incorrect
results or duplicate successful attempts.** The run completed in 31 minutes
27 seconds, including startup and recovery; elapsed time is not a capacity metric.

| Injected fault | Trials | Jobs verified | Additional verification |
| --- | ---: | ---: | --- |
| Worker SIGKILL after claim | 20 | 160 | Original attempt abandoned; higher-generation replacement succeeds |
| Worker pause/resume after claim | 20 | 160 | Replacement finishes first; all 20 resumed stale completions are rejected |
| RabbitMQ SIGKILL | 20 | 160 | 80 publisher-confirmed jobs survive; 80 more admitted while down drain from the outbox |
| PostgreSQL SIGKILL | 20 | 160 | Acknowledged queued jobs survive database crash recovery |
| API SIGKILL | 20 | 160 | Surviving API preserves all accepted IDs on keyed replay |
| **Total** | **100** | **800** | **80 hard process crashes and 20 worker freezes** |

Every trial used a fresh owner/workload and eight unique jobs. The seeded plan
(20261020) declared all 100 trials before execution. Fault order and outage hold
times were seeded; real container startup and lease recovery added to outage time.
The shared database was not reset between trials. The runner stopped consumers
between trials and reconciled each workload before proceeding. It never skipped,
replaced or retried a failed trial. There were no failed trials in this run.

## Accounting and audit

A separate offline audit checked the observed process states for all
100 injections, all 800 final job IDs, request/replay identity, attempts, lifecycle
events, per-trial outcomes and stale-rejection logs. It found:

- 800 successful attempts and 40 abandoned attempts, exactly matching the worker
  kill/freeze scenarios; no additional attempt outcomes.
- Exactly one committed `SUBMITTED` and `SUCCEEDED` event per accepted job.
- 1000 submission request records, including diagnostic replays, plus 960 recovery
  replay records. Every replay retained its original accepted job ID.
- All 70 recorded source SHA-256 fingerprints matching Git revision
  `92bc48a14276b8b94555cdc0473ac71679679491`; the run began from a clean working tree.
- 871 raw evidence files checked and packaged with individual checksums. No generated
  credential values were found in plaintext or decompressed evidence.
- Successful dead-queue, OOM and cleanup checks; disposable test resources removed.

The application handlers and persistence/recovery code were unchanged. Worker-loss
timing alone used the separate 3000 ms delayed fixture. Database/broker/API trials
used production handlers with queued work; they do not target every instruction
inside a result transaction. The original local application volumes were retained.

## Evidence and reproduction

- [Evidence overview](evidence/fault-campaign-100/README.md)
- [Final report](evidence/fault-campaign-100/report.json)
- [Environment/source/image identity](evidence/fault-campaign-100/environment.json)
- [Predeclared plan](evidence/fault-campaign-100/plan.json)
- [Offline audit and raw-file checksums](evidence/fault-campaign-100/audit.json)
- [Compressed complete raw evidence](evidence/fault-campaign-100/raw-evidence.tar.gz)
- [Method and rerun instructions](fault-campaign.md)

## Scope and supported wording

These are 100 sequential fault trials across **five repeated fault kinds** on one
Docker host, with fresh workloads on shared persistent services. They are not
100 distinct fault modes or statistically independent environments. The test
excludes destructive volume/host loss, corruption, multi-node partitions, sustained
overload and arbitrary external side effects. Earlier throughput results are separate.

Supported concise wording:

> Verified zero observed job loss across 100 injected worker, broker, database and
> API failure trials, with all 800 accepted jobs completing correctly and exactly
> one successful attempt per job.
