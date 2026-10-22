# Project 1 completion review

Reviewed October 21, 2026 against the six-phase implementation plan. The agreed **single-host platform scope is implemented**. This review follows the API, authentication/admission, job state machine, claim/lease/finalization, outbox delivery, dead-letter recovery, metrics, migrations, deployment and recovery paths, and reconciles them with automated and recorded runtime evidence.

## Acceptance checklist

| Capability | Status | Verification |
| --- | --- | --- |
| Three text handlers, durable submission, results and paginated listing | Complete | `JobProcessorTest`, `JobControllerTest`, `PersistenceIT`, packaged lifecycle smoke |
| Scheduling, priority, duplicate delivery and transactional confirmed outbox | Complete | `SchedulingIT`, `PipelineIT`, `DispatchCapacityIT`; real priority/prefetch and broker-outage scenarios |
| Phase 1: short claims, attempts, heartbeats, fixed deadline, cancellation and fenced crash recovery | Complete | `ExecutionIT`, `LifecycleIT`, `JobExecutorTest`; killed and frozen/resumed workers with actual stale-final-write rejection |
| Phase 2: bounded classified retries/backoff and persistent dead-letter operations | Complete | `RetryIT`, `DeadLetterIT`, packaged dead-letter smoke; restart during backoff and concurrent action tests |
| Phase 3: hashed bearer keys, roles, owner isolation, revocation, shared transactional quota and audit | Complete | `AccessIT`, `KeyTokensTest`, packaged auth smoke; concurrent API instances and rolled-back admission |
| Phase 4: transactional lifecycle counters/timings, bounded metrics, safe structured logs, dashboard and alerts | Complete | `ObservabilityIT`; 17 live dashboard queries, promtool checks and real database/broker outages |
| Phase 5: repeatable workload accounting, load tests, resilience and measured tuning | Complete | [Benchmark results](benchmark-results.md); 42400 verified jobs across 12 measured load runs and 194-job resilience suite |
| Phase 6: standalone deployment, graceful termination, upgrade/recovery guidance, checked backup/restore and broker repair | Complete | `ShutdownConfigurationTest`, Python recovery safety tests; six actual deployment/recovery scenarios in `release-smoke.py` |
| Follow-up: 100 actual injected failures with zero observed accepted-job loss | Complete | [Campaign evidence](fault-campaign-results.md): 800/800 correct successful jobs across 100 predeclared sequential trials, five repeated fault types |
| Final hardening: bounded streaming input, unambiguous JSON, valid Unicode and recovery preflight | Complete | `RequestBoundaryTest`, `RequestBodyLimitFilterTest`, `test_deploy.py`, real HTTP `api-boundary-smoke.py` |
| Usable API and operator documentation | Complete | [API reference](api.md), [walkthrough](project-walkthrough.md), [deployment](deployment.md), [recovery](recovery.md) |

No planned implementation phase remains. Adding unrelated infrastructure or handlers would be a new increment; PostgreSQL already provides durable scheduling, idempotency, quota and lifecycle coordination without Redis.

## Final corrections

1. The Tomcat form-size setting did not limit JSON input. An authorization-ordered filter now caps mutation bodies at 128 KiB before JSON decoding, including streams without a content length. Both exact-limit success and over-limit rejection are exercised through a real packaged HTTP server with chunked transfer. The cap still accommodates the most escaped representation of a maximum-length valid text field.
2. Duplicate JSON fields and trailing JSON values could be silently accepted. Both now return `400`, making the admitted request unambiguous.
3. Escaped isolated UTF-16 surrogates could pass text/name validation and reach storage. Validation now rejects these before persistence while retaining supplementary Unicode characters and exact UTF-8 handler semantics.
4. Recovery previously validated the dump checksum before restoring, but a missing or malformed fingerprint could fail only after database mutation. Manifest format, table/sequence fingerprint structure, names, counts and digests are now checked before any restore SQL. Historical format-1 backups remain supported. Symlinked archive/manifest files and symlinked or publicly accessible backup destinations are refused. Existing private error files have their permissions restored to `0600` before diagnostics are written; missing configuration has an actionable error.
5. The API reference now covers every implemented endpoint, roles, bodies, response fields, paging, replay semantics, quotas, recovery actions, status handling and wire limits. The real HTTP boundary check is part of ordinary CI.

The new request-boundary tests were run against the previous implementation first and reproduced the missing validation. After the fixes, local verification passed **279 Java tests (138 unit, 141 integration)**, **29 Python tests**, and all four packaged API workflows. GitHub CI additionally checks the image build, monitoring/outages, bounded load, real resilience, deployment/restore and five actual fault-injection regression trials. The final commit's Actions result is the release gate; see the repository's linked CI badge/run.

## Deployment and evidence boundaries

This is a tested single-host release with internal PostgreSQL/RabbitMQ networking, private file-mounted secrets, a loopback API, non-root/read-only JVM containers, bounded resources, graceful termination and verified recovery procedures. A public host, working TLS certificate/proxy, external alert delivery, off-host backup storage, credential custody and environment-specific capacity are operator deployment work. The provided local profile is not a high-availability cluster.

Database access in the bundled profile uses its dedicated PostgreSQL `jobs` database/cluster account, also used for Flyway and offline recovery. It is privileged within that isolated PostgreSQL container. A deployment with stricter database trust boundaries should separate migration/restore administration from a restricted runtime account and test its grants. The profile does not claim independent database administrator/runtime credential isolation. Broker administration, operator keys, Docker access, private deployment files and backup archives are trusted administrative capabilities.

Data/history/idempotency records have indefinite retention. No automatic deletion is introduced because it would change audit and replay guarantees. Operators must budget storage and choose a separately reviewed retention policy before long-lived workloads require it. Offset pages can change under concurrent writes. Priority is not a guarantee against starvation under unlimited incoming higher-priority traffic. Arbitrary handler side effects require their own idempotency; the delivery contract is at least once, with fenced database outcomes.

The 100-fault campaign demonstrates the recorded five fault types and workload, not every possible failure. Host/volume destruction, corruption, multi-host partitions and sustained overload were outside it. The full historical campaign and throughput reports retain their original source revisions; this final HTTP/recovery hardening does not relabel those old measurements as a new benchmark run. CI repeats a bounded five-fault regression on the final code.

The original **500+ jobs/sec end-to-end** and **3.4x worker scaling** targets remain unachieved. The best recorded preloaded drain was **445.49 jobs/sec**, excluding admission; four workers were slower in that workload. Repeated tuning improved the mixed-workload median **34.50 → 97.81 jobs/sec (2.84x)**. See the workload and hardware qualifiers in [benchmark results](benchmark-results.md). The original resume is unchanged.

Completion means the defined features, recovery paths, documentation and release checks are delivered. It does not constitute a guarantee of no defects or a certification for an arbitrary production environment.
