# Scoped release review

The six implementation phases cover execution tracking/recovery, retries and
dead-letter operations, ownership/access control, observability, measured load and
resilience, and deployment/recovery preparation. The deliverable is a working
single-host job platform with repeatable verification and documented operating
limits. Public hosting is a separate deployment decision.

## Acceptance and evidence

| Area | Release check |
| --- | --- |
| Application correctness | 279 passing Java tests: 138 unit, 141 integration; no failures, errors or skipped tests (263 at the recorded Phase 6 release) |
| Harness and recovery safety | 29 Python tests for accounting, isolation, replay identity, offline guards, manifest/archive integrity, safe restore refusal and fault-campaign denominators (18 at the recorded Phase 6 release) |
| Packaged workflows | Cancellation/retry/results/attempts/history; dead-letter collection/recovery; owner isolation/quotas/revocation |
| Operations visibility | Promtool config/rules, 17 live dashboard expressions, real dependency outage checks |
| Load and failure behavior | Bounded CI load and real resilience suite; [recorded larger comparisons](benchmark-results.md) retain their original source/image identity |
| Fresh deployment | Standalone Compose profile, random mounted secrets, private dependencies, loopback API, packaged lifecycle/access workflows |
| Graceful shutdown | SIGTERM during an eight-second separate fixture execution, one committed success, normal JVM termination |
| Failed startup | Deliberately failing application entrypoint; unchanged database; restored compatible image passes readiness/job checks |
| Database restore | Custom archive restored to scratch database and empty replacement volumes; all 12 public tables and two sequences match before startup |
| Broker data loss | Queued jobs reconstructed through missing outbox entries, repeat maintenance is a no-op, original results and future schedules preserved |
| Repeated process/dependency faults | Separate [100-trial campaign](fault-campaign-results.md): 800/800 jobs successful, 80 observed SIGKILLs and 20 worker freezes; complete audited evidence |

The [deployment guide](deployment.md) supplies reproducible commands and TLS
guidance. The [recovery runbook](recovery.md) specifies maintenance boundaries,
backup content, upgrade ordering, restore validation and incident checks.
[Architecture and demo notes](project-walkthrough.md) describe the system and
evidence-backed wording. CI uploads reports from all workflows as its `test-reports`
artifact. Credentials, database archives and private diagnostics are excluded from
that artifact and from the repository.

The [Phase 6 evidence](evidence/phase6/README.md) records the tested revision, image
identity, source SHA-256 values, scenario outcomes and restore counts. It describes
local timings only; a small local restore is not a production recovery-time target.

## Operating boundaries

- One PostgreSQL node and one RabbitMQ node; persistent volumes, not high availability.
- At-least-once delivery and guarded durable results. External side effects need
  handler-level idempotency; non-cooperative arbitrary code cannot be rolled back.
- API readiness includes PostgreSQL and intentionally excludes RabbitMQ. Confirm
  actual worker progress before opening traffic.
- Grace periods are bounded. Longer jobs can require lease recovery after shutdown.
- Maintenance upgrades stop all application instances. Older binaries must not run
  against an incompatible schema; migrations are not automatically reversed.
- Local private backup/restore tooling is supplied. Off-host backup, encryption,
  scheduling, independent retention and secret management belong to the target environment.
- Jobs, audit/history, dead letters and submission idempotency are retained indefinitely.
  There is no automatic data purge or hosted retention service.
- TLS termination is documented with a proxy template. No public certificate,
  cloud host, managed database, external alert receiver or public endpoint is provisioned.
- Measurements do not establish 500+ jobs/sec end-to-end, 3.4× worker scaling,
  statistically independent fault environments, a security audit or an availability SLA.

Future changes can extend this scope based on observed needs: profile shared
persistence costs, tune another workload, implement reviewed retention, or deploy
to a chosen host. These are separate increments; the recorded results remain the
source for performance claims.
