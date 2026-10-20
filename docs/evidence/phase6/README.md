# Phase 6 release evidence

Recorded October 20, 2026, from implementation revision
`ccdadc2ba765a71537691849ea238f312b0d1a70` with a clean working tree.

- [Release report](release-report.json): six real deployment/recovery scenarios,
  actual application image identity and 66 source SHA-256 fingerprints.
- [Java test summary](java-tests.json): 122 unit and 141 integration tests, with
  zero failures/errors/skips; only class names/counts are exported from JUnit XML.
- [Checksums](SHA256SUMS): integrity of the committed evidence files.

Every reported source fingerprint was checked against the recorded Git revision
before publication. The implementation image used Java 21, PostgreSQL 17 and
RabbitMQ 4.1 on local Docker Desktop. Private backup manifests retain the actual
database/broker image IDs. Runtime log scanning found no generated deployment
credential values. The archived database, bootstrap key, passwords and unfiltered
private diagnostics are excluded from this directory.

The restore compared all 12 public tables and both sequences before starting the
replacement application: 12 jobs, 27 history events, five attempts, one resolved
dead letter, three outbox entries, four key records, three owners, access audit,
admission state, seven migration records and lifecycle metrics. Completed results,
submission replay identity, terminal failure/dead-letter history and future
scheduling were checked again through the API after restore. Missing deliveries
were reconstructed without changing job identity or deadline budgets.

The eight-second handler is an explicitly separate fixture absent from the
production JAR/image. The worker finished one successful attempt before exiting
on SIGTERM (status 143); stop took approximately 8.34 s. The local empty-volume
recovery time is a diagnostic for this tiny fixture, not an RTO commitment or a
production-capacity result. CI repeats the same drill and uploads its own reports.

The disposable `job-platform-release-test` containers, networks and volumes were
removed afterward. Persistent development/release resources are outside its
cleanup scope. See [deployment](../../deployment.md),
[recovery](../../recovery.md) and [release review](../../release-review.md).
