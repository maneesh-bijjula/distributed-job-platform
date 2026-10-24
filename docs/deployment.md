# Single-host deployment

The [completion review](completion-review.md) records the final feature checklist and deployment trust boundaries. JSON bodies are capped at 128 KiB by the application as well as the example proxy; see the [API reference](api.md).

For the [V8 metrics upgrade](advanced-performance.md#reproduce-and-operate), stop every older API/worker and back up first. V8 preserves counts/timings while changing their write keys; a pre-V8 application image cannot be rolled back onto that schema. Use a compatible image for rollout recovery or a verified pre-upgrade backup into an empty replacement database.

This profile packages the scoped platform for a Docker host. It has one PostgreSQL
server, one RabbitMQ node, one API/dispatcher and one worker. It is not a highly
available deployment. All persistent application data resides on the host's Docker
volumes. External hosting, DNS, certificate issuance, and public exposure are operator
decisions; these instructions create a loopback-only endpoint.

## Fresh installation

Requirements: Docker Engine/Desktop with Compose v2, Python **3.11+**, Git, and
enough available memory for four services (2 GiB configured container limits plus
the Docker host/OS). Scripts support Linux and macOS; they use POSIX file locks.
Port 8088 must be available. Run commands from the repository root.

```sh
git clone https://github.com/maneesh-bijjula/distributed-job-platform.git
cd distributed-job-platform
git rev-parse HEAD
# Build the checked-out, tested revision; choose a unique tag for this revision.
docker build -t job-platform:release-REVISION .
python3 scripts/deploy.py init --image job-platform:release-REVISION
python3 scripts/deploy.py up
python3 scripts/deploy.py status
curl --fail http://127.0.0.1:8088/actuator/health/readiness
```

Replace `REVISION` with the recorded Git revision. For a registry deployment, build
and publish through your own authenticated release process, then use its immutable
`repository@sha256:...` reference. This repository does not publish an image or
provision a cloud account. A local tag can be overwritten: retain the image ID and
an exported image or registry digest with each release.

`compose.deploy.yaml` is a **standalone** file. The helper always selects project
`job-platform-release` and passes that file explicitly. Do not merge it with the
development `compose.yaml`. Development volumes and `.env` are separate. The
disposable release drill uses only `job-platform-release-test` and port 18088.

Before accepting traffic, submit a job and verify its result. A running worker
container has no HTTP readiness endpoint; API readiness alone does not prove that
workers consume messages. The packaged workflow verifies this directly:

```sh
# A subshell scopes the operator credential to this one verification command.
(
  export JOB_API_KEY="$(cat .deployment/job-platform-release/secrets/bootstrap-key)"
  python3 scripts/smoke.py --base-url http://127.0.0.1:8088
)
```

The smoke creates two diagnostic jobs. Keep shell tracing off when handling keys.
Use the bootstrap operator to provision owner-scoped USER keys for clients; do not
distribute the operator key. See [access controls](../README.md#access-keys-and-ownership).

## Configuration and secrets

`init` creates `.deployment/job-platform-release/deploy.env` (0600), a private
directory and `secrets/` (0700), and random database, broker, and bootstrap credentials.
It refuses to overwrite existing configuration. No plaintext secrets are placed in
Compose environment variables, Git, the Docker build context, or the helper's
stdout. Spring reads mounted config-tree secrets. PostgreSQL reads its password
file; RabbitMQ reads its private configuration file. The worker receives only the
database and broker secrets.

Local Compose uses bind-mounted secret files and retains host ownership/modes. The
individual secret files are 0444 so the non-root JVM can read its mounts; the 0700
parent directories prevent other host users from traversing to those files.
This is file-based secret delivery, not an encrypted secret store. Host root and
users with Docker access can read them. Keep the deployment directory on a private,
encrypted disk; use the target environment's secret manager for a hosted service.

Edit the private `deploy.env` for non-secret configuration. Supported values are
plain `NAME=value` lines, with no shell expressions or quoting:

| Setting | Default | Meaning |
| --- | --- | --- |
| `APP_IMAGE` | Required at initialization | Versioned tag or digest; `latest` refused |
| `DEPLOY_API_PORT` | 8088 | Loopback host port, 1024–65535 |
| `POSTGRES_IMAGE` | `postgres:17-alpine` | Pin a tested PostgreSQL 17 digest for reproducibility |
| `RABBITMQ_IMAGE` | `rabbitmq:4.1-management-alpine` | Pin a tested RabbitMQ 4.1 digest |
| `JOB_DB_POOL_SIZE` | 6 | Connections per JVM, validated 2–50 |
| `WORKER_CONCURRENCY` | 2 | Consumers per worker, validated 1–32 |
| `JOB_WORKER_PREFETCH` | 1 | Messages per consumer, validated 1–250 |
| `JOB_DISPATCH_BATCH_SIZE` | 50 | Outbox batch, validated 1–100 |
| `JOB_DISPATCH_DELAY_MS` | 250 | Dispatch polling, validated 50–60000 ms |
| `JOB_LEASE_SECONDS` | 15 | Renewable lease, 3–300 seconds |
| `JOB_HEARTBEAT_MILLIS` | 2000 | At most one third of the lease, 100–10000 ms |
| `JOB_TIMEOUT_SECONDS` | 30 | Default per-job execution budget, 1–3600 seconds |

To obtain already pulled infrastructure digests, use `docker image inspect
postgres:17-alpine --format '{{index .RepoDigests 0}}'` and the equivalent RabbitMQ
command. Set the corresponding image values before the first `up`, and record
them with the release. Do not switch database major versions against an existing
volume. Database/broker image upgrades need their own compatibility review and backup.

Changing a secret file does **not** rotate an initialized database or broker user's
password. Perform password changes through the respective server, update the
matching mounted files, and restart all clients during a maintenance window. For
API-key rotation, create and privately retain a new OPERATOR key, verify it, then
revoke the old key through the API. Update/remove its bootstrap file deliberately;
restart never resurrects a revoked bootstrap key. Preserve at least one valid operator.

The JVM runs as `jobs`, with a read-only filesystem, a bounded temporary filesystem,
all Linux capabilities dropped and `no-new-privileges`. Only the API publishes a
host port, always on loopback. Database/broker use an internal Docker network.
Java services have 512 MiB limits and one CPU each; dependency services have
512 MiB and one CPU each. Docker JSON logs rotate at 3 × 10 MiB per service.
These are starting limits, not capacity guarantees. Review [measurements](benchmark-results.md).

## Readiness and shutdown

Public liveness checks the process. Readiness checks application state and PostgreSQL;
RabbitMQ is deliberately excluded because the transactional outbox allows durable
admission while the broker is down. HTTP authentication still fails closed on a
database outage. Compose waits for API readiness on startup; runtime health failure
does not automatically restart a container. `unless-stopped` restarts exited
processes, not unhealthy running processes. Monitor readiness and worker progress.

SIGTERM closes HTTP admission gracefully. Spring's lifecycle phase budget is 40 s.
The Rabbit listener allows 35 s for current execution, then returns prefetched work
rather than beginning to drain it. The Docker grace period is 90 s. Lease monitors
remain available while listener shutdown waits. The release drill sends SIGTERM
during a real eight-second execution in a separate fixture image and requires one
successful attempt and normal termination (exit 0 or the JVM's SIGTERM status 143,
never SIGKILL 137). The production handlers have no test delay hook.

These budgets do not guarantee completion of a handler that can run for 3600 s.
Work exceeding the grace period can be redelivered/recovered through renewable
leases and fenced writes. External side effects still require handler-level
idempotency. Uncooperative code cannot be forcibly rolled back. To stop application
processes while retaining the dependencies and all data:

```sh
python3 scripts/deploy.py stop
```

## TLS and networking

For access from another machine, put an operator-managed reverse proxy on the host
in front of `127.0.0.1:8088`. [The nginx template](../deploy/nginx.conf.example)
shows HTTPS termination, a bounded request body, bearer header forwarding, and
blocking public actuator access. Replace the domain and certificate paths, run
`nginx -t`, then test authenticated requests over HTTPS before opening traffic.
Manage renewal and firewall rules on that host. Do not send bearer keys over public
HTTP. Do not publish PostgreSQL, RabbitMQ management, Prometheus, or Grafana ports.
The example is guidance, not a provisioned/tested public TLS endpoint. Internal
database/broker traffic is plaintext on the single host's private Docker network;
multi-host transport needs a separate TLS configuration.

Forwarded headers are ignored by the application by default. If a later feature
needs client IP/origin reconstruction, configure trusted proxies explicitly; do
not accept arbitrary caller-supplied forwarding headers.

## Safe upgrades and failed rollouts

Use a maintenance window; this profile does not implement rolling or zero-downtime
migrations. Keep the old checked-out sources, image digest, private configuration,
and secrets available. Gate external traffic at the proxy before stopping services.

1. Build/test the new image before the window. Read migration changes and the
   release notes; establish whether the old binary supports the resulting schema.
2. Close traffic. `python3 scripts/deploy.py stop` stops **all** API/worker containers
   in this deployment. Ensure no other host/process writes to this database.
3. Run `python3 scripts/deploy.py backup`, then `verify-backup BACKUP_DIRECTORY`.
   Copy the verified backup, configuration, secrets, and old image identity to
   encrypted off-host storage. Keep broker data intact for an ordinary upgrade.
4. Change only `APP_IMAGE` to the new tested image. Start it with `deploy.py up`.
   Flyway validates/applies migrations automatically; no worker or old API may run
   concurrently with an incompatible migration. Both services must use the same
   release. V6 owner-scoped idempotency and V7 metric updates require coordinated
   application upgrades. Phase 6 itself adds no database migration.
5. Check `status`, readiness, structured logs, a submitted job/result, attempts and
   backlog drain. Reopen traffic only after these checks pass.

If image/startup fails **without changing the schema**, stop the failed containers,
restore the recorded old `APP_IMAGE`, run `up`, and repeat checks. The release drill
exercises this case with an intentionally failing application entrypoint.

If the schema changed or a migration partially failed, keep traffic closed. Do not
edit Flyway history, call `repair` to hide a failure, or assume an older binary is
compatible. Choose a reviewed forward fix, or restore the pre-upgrade backup into
an **empty replacement deployment** using [the recovery runbook](recovery.md).
Do not copy an old database snapshot underneath a live or newer broker/application.
Restore may lose jobs accepted after that snapshot and may repeat external side
effects. Reconcile those with the application owner before reopening traffic.

## Repeat the release drill

```sh
docker build -t job-platform:local .
docker build -f Dockerfile.loadtest -t job-platform:load-controlled .
python3 -m unittest discover -s scripts -p 'test_*.py' -v
python3 scripts/release-smoke.py --output reports/phase6/release
```

Use a new output directory per run. The drill deletes only its own disposable
`job-platform-release-test` volumes, including a deliberate empty-volume restore.
Archives, credentials, and error diagnostics remain private under ignored
`.deployment/`; public JSON reports contain no payloads or credentials. After an
interrupted run, `python3 scripts/release-smoke.py --cleanup-only` removes that
test stack. It cannot select the ordinary or persistent release deployment.

Implementation references: [Spring config-tree secrets](https://docs.spring.io/spring-boot/reference/features/external-config.html#features.external-config.files.configtree),
[Spring graceful shutdown](https://docs.spring.io/spring-boot/reference/web/graceful-shutdown.html),
[Spring AMQP shutdown behavior](https://docs.spring.io/spring-amqp/reference/amqp/containerAttributes.html),
[RabbitMQ configuration](https://www.rabbitmq.com/docs/configure).
