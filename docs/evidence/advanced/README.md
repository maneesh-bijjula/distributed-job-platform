# Follow-up capacity and release evidence

The runtime candidate is `1929ab9a943d0442da9ba880c88cbc992e9b577c`. Later commits publish documentation/evidence; every report retains its own source revision and image identity. Read [methods, results and limits](../../advanced-performance.md) before using any number.

## Capacity

[The offline audit](capacity/audit.json) reconciles eleven completed cases: **103000 measured jobs and 4436 separately verified warm-up jobs**. It checks every ledger ID, accepted keyed response, successful attempt, submitted/success event, no-error count, source SHA-256 and archive member checksum. The live harness independently checked stored results against deterministic handler oracles before removing payload/result content from the exported ledgers; the offline audit does not recompute omitted result values.

`capacity/raw-evidence.tar.gz` contains original reports, environments, request/job/attempt/event ledgers, worker/API lifecycle logs, resource samples, sanitized PostgreSQL statement/wait profiles and final container states. `capacity/audit.json` lists each member's SHA-256 and byte size. Per-case reports/environments and SQL summaries are also available directly in the corresponding directory. No database dump or private configuration is included. Decompressed contents were checked against generated local/deployment/monitoring credentials and bearer-token patterns before publication.

The first 3000-job exploratory profile recorded a dirty harness: runtime files match `296e792`, and its new `scripts/profile-load.py` hash matches the later `d8dcead` commit. The audit records that exact file-specific provenance override without rewriting the original environment. All other completed cases began from clean revisions. The `measurement-plan.json` preserves an intermediate proposal; some cases were superseded after the query experiment regressed. The actual per-case environments/reports define what was run. The repeat policy was not triggered because no end-to-end case reached 500 jobs/sec.

The invalid `control-striped-db2-4` attempt selected an unavailable intermediate image. Its original invalid/failure records and partial artifacts are retained and excluded from timing and completed-job denominators. The correctly rebuilt comparison is `control-striped-db2-4-rebuilt`. Negative performance results remain published; the regressing query changes were removed from the final runtime.

The four final cases are single trials per configuration with 1000 warm-ups, not repeated medians: end-to-end 293.79/s (one worker) and 285.28/s (four), preloaded drain 687.43/s and 798.62/s respectively. They imply 0.97x / 1.16x scaling, not 3.4x. Queue drain excludes admission. The original 500+ end-to-end target remains unachieved.

## Upgrade and test records

[java-tests.json](java-tests.json) summarizes the selected runtime's **303 passing tests** (155 unit, 148 integration), with no failures/errors/skips. Full CI reports are uploaded by GitHub Actions. [local-upgrade.json](local-upgrade.json) records the actual original-volume V7-to-V8 check: private backup restored to scratch, all 12 existing tables and two sequences preserved, existing rollups retained in stripe zero. Backup files, credentials and original row contents remain private. [Five packaged API workflows](local-api-workflows.json), [17 live dashboard queries](local-observability.json), [monitoring configuration/rules](local-monitoring-config.json) and [bulk-item metric labels](local-batch-metrics.json) also pass against the upgraded local image.

## Completed failure revalidation

The fresh [100-trial campaign](fault-100/README.md) passed **100/100 observed injections and 800/800 correct accepted jobs**, with zero observed loss or duplicate successes. Its [report](fault-100/report.json), [environment](fault-100/environment.json), [fixed pre-run plan](fault-100/plan.json), [independent audit](fault-100/audit.json) and [complete raw archive](fault-100/raw-evidence.tar.gz) are published. The audit checks 871 files and 75 source hashes, 800 successful/40 abandoned attempts, 1000 submission records, 960 recovery replays and all 20 stale completions rejected after replacement success. It began from clean revision `8337a335d3a624e9a5ee0b9ba675229f05fd1b4b`, using the unchanged selected runtime image. [Implementation CI and downloaded-report verification](implementation-ci.json) are a separate Linux five-fault regression, not part of the local 100-trial count.

## Interrupted failure campaign

[interrupted-fault-run/interrupted.json](interrupted-fault-run/interrupted.json) preserves the prior run that stopped after 16 completed trials while starting trial 17. It has no completed campaign verdict and is excluded from full-campaign claims. Its complete original artifacts remain in `interrupted-fault-run/raw-evidence.tar.gz`. The audit identifies its truncated resource-sampling gzip stream and scans its available decoded prefix; no damaged bytes were silently repaired or represented as complete. A fresh campaign starts from a new predeclared plan and empty isolated database, without splicing in partial trials.

`SHA256SUMS` covers published files (excluding itself). Hashes detect changes, but are not a third-party signature or hardware attestation. Historical Phase 5/6 and the original 100-trial evidence remain unchanged in their own directories.
