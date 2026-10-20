# Backup and incident recovery

The deployment helper selects only the separate `job-platform-release` project.
Commands below run from the repository root. Keep traffic closed during offline
operations and stop **every** application instance using the database, including
instances outside Compose. The helper checks its own API/worker containers and
serializes helper commands with a POSIX lock; it cannot discover an application
running on another host. Do not use direct Compose commands concurrently with
maintenance. No command here promises exactly-once external effects or zero data loss
after losing an unbacked-up database.

## Create and prove a backup

```sh
python3 scripts/deploy.py stop
python3 scripts/deploy.py backup
# Substitute the directory printed by backup, not the database.dump filename.
python3 scripts/deploy.py verify-backup BACKUP_DIRECTORY
python3 scripts/deploy.py up
```

Backup uses PostgreSQL 17's custom-format `pg_dump`, excluding ownership and grants.
It records a SHA-256 archive checksum, row counts and ordered row-hash fingerprints
for every public table, and all public sequence values/called flags. Before/after
fingerprints must match with applications stopped. The manifest is written only
after a successful stable dump. A backup directory missing its manifest is incomplete.
The checksum detects accidental changes, not a maliciously replaced manifest.

Verification creates the fixed scratch database `jobs_restore_verification`, restores
in a single transaction, compares every fingerprint and sequence, then drops only
that scratch database. If it already exists, creation fails and the helper leaves it
untouched. After interruption, inspect that database before manually removing it;
never guess which database contains the application. Verification requires the
application processes stopped and uses the PostgreSQL container's local administrator.

Dump/manifest files are 0600 in a private 0700 backup directory. They contain job
inputs/results and access/audit records, including API-key hashes; treat them as
sensitive. Credentials, server-wide roles, TLS certificates, Docker images, broker
data, and monitoring history are **not** in the PostgreSQL archive. Privately back
up the deployment configuration/secrets and retain the matching release image.
Use encrypted, independently accessible off-host storage and test its retrieval.
The repository supplies the local dump/restore tools, not an off-host scheduler or
encryption/key-management service. Choose backup frequency/retention for the actual
data-loss budget; RPO is the age of the last usable snapshot and RTO includes
infrastructure, restore, reconciliation, and validation time.

## Restore a lost database or failed migration

Do not restore onto live data or beneath a broker with messages from a different
database timeline. Preserve the failed deployment for investigation, shut it down,
and provision a **new, empty** Docker host or replacement environment. Keep the
previous host fenced off from the replacement database, broker, and incoming traffic.
Do not delete the old volumes to make room for an experiment.

1. Retrieve the verified backup, matching application image, private deployment
   directory and secrets. Preserve directory/file permissions. A clean replacement
   host may retain the same Compose project name; Docker volumes must be empty.
2. Restore the private configuration into `.deployment/job-platform-release/`.
   **Do not run `init` again** when restoring existing credentials. Restored API-key
   hashes expect the original tokens. If original tokens are unavailable, use the
   reviewed operator/bootstrap recovery path before opening traffic.
3. Start only the dependencies:

   ```sh
   python3 scripts/deploy.py infrastructure
   python3 scripts/deploy.py restore BACKUP_DIRECTORY
   python3 scripts/deploy.py reconcile-outbox
   python3 scripts/deploy.py up
   python3 scripts/deploy.py status
   ```

4. Require the restore fingerprint comparison to pass **before** application startup.
   `restore` refuses a target containing existing user relations; it never runs
   `--clean` or drops application data. PostgreSQL restores in one transaction; on
   failure keep traffic closed and inspect the private `last-error.txt`. Only restore
   trusted archives: a database archive can contain executable SQL.
5. Confirm readiness and authenticated job access, preserved completed results,
   idempotent replay, pending work drain, expected schedule/backoff timing and
   attempt outcomes. Review any restored `RUNNING` work as described below. Run the
   packaged workflow, then reopen traffic after application-owner reconciliation.

Recreating a PostgreSQL user with the same name and the configured new server
password does not change the restored API keys; those are application records.
If the backup's image identifier is a mutable local tag, verify the recorded image
ID against the retained image before using it. PostgreSQL major upgrades are not
part of this restore workflow; use PostgreSQL 17 and its matching tools.

Restored jobs accepted **after** the snapshot are absent. A restored running attempt
may already have produced an external side effect before the failure. The old worker
must remain fenced off. On restart, the API recovery scheduler processes expired
leases, increments generations and applies attempt/deadline limits. It may complete
recovery, fail an expired job, or exhaust its policy. Do not manually mark all running
jobs successful or reset their attempt/deadline budgets. Coordinate external side
effects through handler-specific idempotency and reconciliation.

## Broker outage versus broker data loss

For a temporary broker outage, keep its persistent volume. Readiness can remain
healthy while accepted work accumulates in PostgreSQL's outbox. Restore broker
availability, check connections and queues, and confirm the outbox drains. Existing
publisher confirms, generation checks and job/attempt ownership handle duplicate
messages. Do not purge queues or recreate the broker to fix a connection problem.

If the broker's data was actually lost, already-dispatched queued jobs may have no
outbox entry. To reconstruct those deliveries from the current database:

```sh
python3 scripts/deploy.py stop
# Repair/replace the failed broker and retain the current PostgreSQL data.
python3 scripts/deploy.py infrastructure
python3 scripts/deploy.py reconcile-outbox
python3 scripts/deploy.py up
```

Offline reconciliation inserts missing outbox rows for `QUEUED` jobs only. It
preserves IDs, priority, generation, schedules, backoff and deadline budgets;
existing outbox entries are left intact. Terminal and running jobs are untouched.
Running jobs use lease recovery after API startup. A second reconciliation before
startup inserts nothing. Old broker messages can coexist with reconstructed work;
duplicates remain subject to the normal worker claim guards. A private timestamped
operator report records the count. This is a local administrator maintenance
operation, not an owner-facing API or a replacement for the application audit trail.

The release drill verifies both lost deliveries (purging only its disposable test
queue) and full database/broker volume replacement from a verified archive. It
requires original job IDs/results, one successful attempt per recovered queued job,
preserved submission idempotency and a future schedule remaining queued.

## Dead-letter incident

First inspect sanitized logs and fix the handler/dependency/configuration failure.
Use a valid OPERATOR key and the authenticated dead-letter endpoints documented in
the [API guide](../README.md#dead-letter-operations). `GET` inspects PostgreSQL only.
Explicit `POST .../dead-letters/collect?limit=20` captures bounded broker messages
and acknowledges each only after its persistent record commits. It removes raw
bodies from the broker after capture; only hashes/lengths and sanitized metadata
remain in PostgreSQL.

Recover only records with a known eligible job and use a new deliberate idempotency
key per action; replay that same key after an ambiguous response. Queued redrive
preserves schedule/backoff/deadline. Failed/cancelled recovery creates a linked job
under the original owner and charges admission. Malformed/unknown bodies cannot be
executed; inspect and discard them explicitly. Discard does not cancel a related
job. Do not bulk-clear the inbox or RabbitMQ dead queue to hide an alert. Recovery
and discard are audited with the authenticated operator key.

## Retention and storage

Job inputs/results, attempt/event history, access audit, dead-letter records and
submission idempotency are retained indefinitely by this version. There is no
automatic job purge, archival service or expiry API. This deliberately preserves
replay semantics and referenced history. RabbitMQ dead letters have no automatic
expiry. Docker JSON application logs rotate; Prometheus's optional development
profile retains seven days, and it is not part of the release backup.

Monitor volume free space, database/table sizes, oldest queued/outbox ages, open
dead letters, worker progress and snapshot freshness. Useful read-only database
inspection inside the deployment's PostgreSQL container:

```sql
SELECT pg_size_pretty(pg_database_size(current_database()));
SELECT relname, pg_size_pretty(pg_total_relation_size(relid))
FROM pg_catalog.pg_statio_user_tables ORDER BY pg_total_relation_size(relid) DESC;
SELECT status,count(*) FROM jobs GROUP BY status;
SELECT count(*),min(available_at) FROM job_outbox;
SELECT state,count(*) FROM dead_letters GROUP BY state;
```

Choose a real retention policy before sustained hosted use. A future purge must
account for retry links, job/attempt/event/dead-letter foreign keys, access audit,
metrics and promised idempotency duration. Do not issue a naive age-based `DELETE`
or `TRUNCATE`; neither is supplied as an operations shortcut. Preserve verified
backups before any reviewed retention change. These tools do not implement
data-subject erasure, legal retention rules or an archival product.

## Incident checklist

| Observation | Check and action |
| --- | --- |
| API unavailable | Process state, readiness, PostgreSQL connectivity, image/config changes and sanitized logs; keep traffic closed during recovery |
| Readiness 503, liveness 200 | PostgreSQL availability/authentication; requests fail closed while the DB is unavailable |
| Healthy API, growing outbox | Broker health/credentials, confirm failures, relay-enabled API; restore dependency and observe drain |
| Empty outbox, queued jobs not progressing | Future schedule/backoff, worker consumers, broker queue depth, dead letters; use offline reconciliation only for proven message loss |
| Expired running attempts | At least one dispatcher-enabled API, lease/deadline budgets, recovery limits, DB latency, worker health |
| Old worker reports rejected completion | Correlate attempt/generation with replacement; rejection protects current results; investigate lease expiry |
| Repeated permanent failures | Handler validation/code; fix cause before deliberate dead-letter recovery |
| Disk pressure | Database/broker/log sizes and backup location; preserve evidence and expand/reduce traffic; do not delete active volumes |

See [observability](observability.md) for metric definitions and alerts. Global
snapshot metrics should be scraped from one designated API. The release profile
does not deploy a monitoring server or external alert receiver; attach a private
collector with a restricted METRICS key if the target environment requires it.

References: [PostgreSQL dump](https://www.postgresql.org/docs/17/app-pgdump.html),
[restore](https://www.postgresql.org/docs/17/app-pgrestore.html), and
[RabbitMQ backup limits](https://www.rabbitmq.com/docs/backup).
