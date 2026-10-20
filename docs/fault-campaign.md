# Repeated real fault injection

The campaign runs a predeclared, seeded sequence of actual Docker process failures.
Every trial has fresh jobs/owners/idempotency keys and must finish reconciliation
before the next trial begins. The services and database are shared across trials;
these are not statistically independent environments or 100 distinct failure modes.

## Method

The default is 100 trials: 20 of each fault kind, with eight new jobs per trial.
Types rotate through word count, SHA-256 and uppercase; ASCII payload sizes are
128, 4096 and 16000 bytes. Every job has a 180-second execution budget. Requests
also replay diagnostic keys; post-recovery replays must return the original IDs.

| Actual injected fault | What is verified |
| --- | --- |
| Worker SIGKILL after claim | Exit 137, abandoned original attempt, higher-generation replacement, correct result |
| Worker pause after claim | Docker reports paused, replacement succeeds before resumption, actual stale final write is rejected |
| RabbitMQ SIGKILL | Publisher-confirmed queued work survives the crash; additional jobs admitted while down remain in the outbox and finish afterward |
| PostgreSQL SIGKILL | Previously acknowledged pending jobs survive database crash recovery and finish with their original IDs/results |
| API SIGKILL | A second API replays every accepted key while the first is down; restarting the killed API preserves work and identity |

Database and API crashes happen with acknowledged queued work and consumers stopped;
they do not deliberately crash a database mid-result-commit. Worker kill/pause trials
use a separate 3000 ms non-cooperative delay fixture to make the claimed execution
observable. Production handlers/JARs are unchanged. There is no fixture delay in
the broker/database/API trials. Actual application claims, leases, transactions,
publisher confirms, recovery schedulers and fenced writes are used throughout.

SIGKILL must be observed as a stopped container with exit 137 and no OOM flag;
pause must be observed as a running, paused process. Each fault event is written
to a flushed/fsynced append-only journal before recovery. Graceful restarts,
healthy load tests, priority ordering and admission-limit cases do not count as
campaign fault injections. Outage durations are seeded between 0.25 and 1 second
before restart; total unavailability additionally includes actual service startup.
The frozen worker remains paused until the replacement has completed, beyond its
six-second lease. No database clock or lease timestamp is manually changed.

## Run

Use the same isolation/prerequisites as the [load guide](performance.md): separate
project `job-platform-load`, labeled disposable volumes, `jobs_load_test`, loopback
ports 18080–18082, and private generated `.loadtest.env`. Do not run multiple load,
resilience or campaign processes against this shared test project simultaneously.
Keep enough Docker memory available; an OOM invalidates the run.

```sh
# Pilot each real fault once before a full campaign.
python3 scripts/fault_campaign.py --trials 5 --output reports/fault-campaign/pilot
# New output directory, same built images, 20 trials per fault kind.
python3 scripts/fault_campaign.py --trials 100 --skip-build --output reports/fault-campaign/full
```

Counts must be multiples of five, from 5 through 100. `--seed` defaults to 20261020;
`--jobs-per-trial` defaults to eight and is bounded to 4–40. A smaller run cannot
claim 100 failures. Seeds fix the fault order/workload descriptors, not OS timing,
UUIDs or process scheduling. The runner stops on the first failed trial; it never
retries that trial until it passes or silently drops it from the denominator.
Earlier raw failure reports remain separate when a new diagnostic run is started.

Ordinary CI runs five trials, one per kind, as a regression check. The separate
**Full fault injection campaign** workflow can run all 100 manually on GitHub
Actions with its own 60-minute budget and evidence artifact. This is not a scheduled
automation. The normal application stack is outside the harness cleanup scope.

## Evidence and success criteria

- `plan.json` declares the schedule, workload count and fault-kind denominator.
- `campaign.jsonl.gz` retains trial start, requested/observed injection, target
  container/actual state, accepted IDs, and successful recovery events.
- `trial-NNN.json` records each outcome, per-kind details and old/replacement
  attempts for worker loss. Stale rejection logs retain the correlated old attempt ID.
- Every workload retains request, job, attempt, lifecycle and recovery-replay ledgers.
  Result values are checked against Python oracles before payloads are removed.
- `population.jsonl.gz` reconciles the entire fresh database to all expected job IDs:
  every job succeeded, with exactly one successful attempt and one submitted/success
  event. Duplicate/missing IDs, terminal failure and incomplete accounting fail the run.
- `environment.json` records source revision/hashes, image identities, resource limits,
  fixture settings and the declared plan's hash. Inputs/credentials are excluded.
- `report.json` is successful only after all planned trials, exact job accounting,
  source-stability checks, no unresolved broker dead letters, OOM checks and cleanup.
  `failed.json` and partial journals preserve diagnostics when the campaign stops.

The test excludes destructive volume/host loss, multi-node partitions, corruption,
arbitrary external side effects, sustained overload and long-duration availability.
Zero observed loss in the campaign supports only its recorded failure model and
workload. Fault-trial elapsed times include process startup/recovery and are not
throughput measurements. Earlier throughput evidence retains its original scope.
