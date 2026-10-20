"""Seeded, fail-fast campaign of real faults on the disposable load stack.

Five kinds of actual process failure, equal repetition counts, fresh workloads
and complete reconciliation per trial. A failed/incomplete trial is never replaced
or removed from the denominator. This measures bounded recovery, not throughput.
"""
from collections import Counter
import argparse
import hashlib
import json
import os
import random
import time

from load_support import (ROOT, Stack, Workload, compressed_rows, identifier,
                          now, specs, wait, write_json)

KINDS = ("worker_sigkill", "worker_pause_resume", "broker_sigkill", "database_sigkill", "api_sigkill")


def trial_plan(count, seed):
    if count < 5 or count > 100 or count % len(KINDS):
        raise ValueError("Trial count must be a multiple of five between 5 and 100")
    generator = random.Random(seed)
    kinds = list(KINDS) * (count // len(KINDS))
    generator.shuffle(kinds)
    return [{"trial": i + 1, "kind": kind, "outageSeconds": round(generator.uniform(.25, 1), 3),
             "workloadSeed": generator.randrange(1, 2 ** 31)} for i, kind in enumerate(kinds)]


def verified_fault(kind, state):
    if kind not in KINDS:
        raise ValueError("Unknown fault kind")
    if state.get("OOMKilled"):
        raise AssertionError("An OOM is an unplanned failure, not a valid campaign injection")
    if kind == "worker_pause_resume":
        if not state.get("Running") or not state.get("Paused"):
            raise AssertionError("Worker pause was not observed")
    elif state.get("Running") or state.get("ExitCode") != 137:
        raise AssertionError("Requested SIGKILL was not observed as exit 137")
    return {key: state.get(key) for key in ("Status", "Running", "Paused", "ExitCode", "OOMKilled", "StartedAt", "FinishedAt")}


def verify_population(expected, rows):
    expected = list(expected)
    if len(expected) != len(set(expected)):
        raise AssertionError("Two accepted submissions unexpectedly share a job ID")
    expected = set(expected)
    observed = [row["id"] for row in rows]
    if len(observed) != len(set(observed)) or set(observed) != expected:
        raise AssertionError("Accepted job IDs are missing, duplicated, or unexpected")
    if any(row["status"] != "SUCCEEDED" or row["successful_attempts"] != 1 or
           row["submitted_events"] != 1 or row["succeeded_events"] != 1 for row in rows):
        raise AssertionError("A job failed or lacks exactly one successful attempt and lifecycle transition")


class Campaign:
    def __init__(self, stack, plan, jobs=8):
        if jobs < 4 or jobs > 40:
            raise ValueError("Jobs per trial must be between 4 and 40")
        self.stack, self.plan, self.jobs = stack, plan, jobs
        self.completed, self.injections, self.expected = [], [], []
        self.journal = (stack.output / "campaign.jsonl").open("x")
        self.started_at, self.started = now(), time.monotonic()

    def record(self, event, **fields):
        row = {"at": now(), "event": event, **fields}
        self.journal.write(json.dumps(row, allow_nan=False) + "\n")
        self.journal.flush()
        os.fsync(self.journal.fileno())
        return row

    def stopped_workers(self):
        self.stack.compose("--profile", "fault", "stop", "worker", "controlled-worker")

    def normal_worker(self):
        self.stack.env.update(LOAD_CONCURRENCY="2", LOAD_PREFETCH="1")
        self.stack.up("worker")

    def controlled_worker(self):
        self.stack.env.update(LOAD_CONCURRENCY="1", LOAD_PREFETCH="1", LOAD_HANDLER_DELAY_MS="3000")
        self.stack.up("controlled-worker")

    def work(self, name, count, seed):
        return Workload(self.stack, name, specs(count, seed=seed), 4, duplicates=max(1, count // 4))

    def attempts(self, work):
        return self.stack.sql("SELECT coalesce(json_agg(row_to_json(r)),'[]') FROM ("
                              "SELECT a.id,a.job_id,a.status,a.generation,a.started_at,a.finished_at "
                              "FROM job_attempts a JOIN jobs j ON j.id=a.job_id WHERE j.owner_id='" +
                              identifier(work.owner) + "' ORDER BY a.started_at,a.id) r")

    def claim(self, work):
        result = []
        def running():
            result[:] = [row for row in self.attempts(work) if row["status"] == "RUNNING"]
            return bool(result)
        wait(running, "Worker never committed an execution claim before injection", timeout=60, interval=.05)
        return result[0]

    def inject(self, trial, service, jobs, claim=None):
        containers = self.stack.ids(service)
        if len(containers) != 1:
            raise AssertionError("Expected exactly one target container")
        container = containers[0]
        before = self.stack.inspect(container)
        if not before["State"]["Running"] or before["State"].get("Paused"):
            raise AssertionError("Fault target was not healthy/running before injection")
        started = self.record("injection_started", trial=trial["trial"], kind=trial["kind"],
                              containerId=container, containerName=before["Name"], acceptedJobIds=list(jobs),
                              claim=claim)
        self.stack.container_action("pause" if trial["kind"] == "worker_pause_resume" else "kill", container)
        state = verified_fault(trial["kind"], self.stack.inspect(container)["State"])
        proof = self.record("injection_observed", trial=trial["trial"], kind=trial["kind"],
                            containerId=container, requestedAt=started["at"], state=state)
        self.injections.append(proof)
        time.sleep(trial["outageSeconds"])
        return container

    def replay(self, work, api_index=None, phase="post-recovery"):
        rows = []
        for item in work.items:
            server = api_index if api_index is not None else (item["index"] + 1) % len(work.clients)
            client = work.clients[server]
            client.reset()
            status, value, _ = client.request("/api/v1/jobs", "POST", item["body"], item["key"])
            if status != 200 or value.get("id") != work.ids[item["index"]]:
                raise AssertionError("A recovered idempotency key did not preserve its accepted job ID")
            rows.append({"jobId": value["id"], "idempotencyKey": item["key"], "apiIndex": server,
                         "httpStatus": status, "at": now()})
        compressed_rows(work.directory / (phase + "-replays.jsonl.gz"), rows)

    def finish(self, work, fixture=False):
        work.await_terminal(timeout=150)
        report = work.evidence(allow_abandoned=True)
        report.update(fixtureUsed=fixture, capacityMeasurement=False,
                      completionMeasurement="Fault trial; includes process startup/outage/recovery, not a capacity benchmark")
        write_json(work.directory / "report.json", report)
        self.expected.extend(work.ids.values())
        return report

    def trial(self, trial):
        index, kind = trial["trial"], trial["kind"]
        self.record("trial_started", **trial)
        self.stopped_workers()
        name = f"trial-{index:03d}-{kind}"
        reports, extra = [], {}
        if kind in ("worker_sigkill", "worker_pause_resume"):
            work = self.work(name, self.jobs, trial["workloadSeed"])
            self.controlled_worker()
            work.submit()
            claim = self.claim(work)
            # Re-check ownership immediately before the Docker action; no fabricated claim timestamps.
            if not any(row["id"] == claim["id"] and row["status"] == "RUNNING" for row in self.attempts(work)):
                raise AssertionError("Target claim finished before failure could be injected")
            container = self.inject(trial, "controlled-worker", work.ids.values(), claim)
            try:
                self.normal_worker()
                work.await_terminal(timeout=150)
                outcomes = self.attempts(work)
                old = next(row for row in outcomes if row["id"] == claim["id"])
                replacement = [row for row in outcomes if row["job_id"] == claim["job_id"] and row["status"] == "SUCCEEDED"]
                if old["status"] != "ABANDONED" or len(replacement) != 1 or replacement[0]["generation"] <= old["generation"]:
                    raise AssertionError("The lost execution was not recovered under a new generation")
                extra.update(abandonedAttempt=old, replacementAttempt=replacement[0])
                if kind == "worker_pause_resume":
                    self.stack.container_action("unpause", container)
                    def rejected():
                        return any(row.get("event") == "completion_rejected" and row.get("attempt_id") == claim["id"]
                                   for row in self.stack.logs(container))
                    wait(rejected, "Resumed stale execution never reached the rejected final-write guard", timeout=30)
                    extra["staleCompletionRejected"] = True
                    write_json(work.directory / "stale-completion.json", {
                        "attemptId": claim["id"], "logs": [row for row in self.stack.logs(container)
                            if row.get("event") == "completion_rejected" and row.get("attempt_id") == claim["id"]]})
                self.replay(work)
                reports.append(self.finish(work, fixture=True))
            finally:
                if self.stack.inspect(container)["State"].get("Paused"):
                    self.stack.container_action("unpause", container)
                self.stack.compose("--profile", "fault", "stop", "controlled-worker")
        elif kind == "broker_sigkill":
            before = self.work(name + "-before", self.jobs // 2, trial["workloadSeed"])
            during = self.work(name + "-during", self.jobs - len(before.items), trial["workloadSeed"] + 1)
            before.submit()
            wait(lambda: self.stack.sql("SELECT to_json(count(*)) FROM job_outbox o JOIN jobs j ON j.id=o.job_id "
                                       "WHERE j.owner_id='" + identifier(before.owner) + "'") == 0,
                 "Pre-crash accepted jobs were not publisher-confirmed")
            self.inject(trial, "rabbitmq", before.ids.values())
            during.submit()
            if self.stack.sql("SELECT to_json(count(*)) FROM job_outbox o JOIN jobs j ON j.id=o.job_id "
                              "WHERE j.owner_id='" + identifier(during.owner) + "'") != len(during.items):
                raise AssertionError("Broker-outage admissions were not all retained in the transactional outbox")
            extra.update(confirmedBeforeCrash=len(before.items), durablyAdmittedWhileDown=len(during.items))
            self.stack.up("rabbitmq")
            self.normal_worker()
            for work in (before, during):
                reports.append(self.finish(work))
                self.replay(work)
        else:
            work = self.work(name, self.jobs, trial["workloadSeed"])
            work.submit()
            if work.state() != {"total": self.jobs, "terminal": 0}:
                raise AssertionError("Failure trial needs accepted pending jobs before injection")
            if kind == "database_sigkill":
                self.inject(trial, "postgres", work.ids.values())
                self.stack.up("postgres")
            else:
                service = "api" if index % 2 else "api2"
                survivor = 1 if service == "api" else 0
                self.inject(trial, service, work.ids.values())
                self.replay(work, survivor, phase="surviving-api")
                extra["survivingApiIndex"] = survivor
                self.stack.up(service)
                for client in self.stack.clients:
                    client.reset()
            self.normal_worker()
            reports.append(self.finish(work))
            self.replay(work)
        count = sum(row["accountedFor"] for row in reports)
        if count != self.jobs:
            raise AssertionError("Trial did not account for every expected job")
        result = {**trial, "passed": True, "acceptedJobs": count, "accountedJobs": count,
                  "lostJobs": 0, "incorrectResults": 0, "duplicateSuccessfulAttempts": 0,
                  "workloads": [row["name"] for row in reports],
                  "attemptOutcomes": dict(sum((Counter(row["attemptOutcomes"]) for row in reports), Counter())), **extra}
        write_json(self.stack.output / (f"trial-{index:03d}.json"), result)
        self.completed.append(result)
        self.record("trial_passed", trial=index, kind=kind, acceptedJobs=count)
        print(f"PASS {index}/{len(self.plan)}: {kind}; {count}/{count} accepted jobs verified", flush=True)

    def population(self):
        return self.stack.sql("""SELECT coalesce(json_agg(row_to_json(r)),'[]') FROM (
            SELECT j.id,j.status,
                (SELECT count(*) FROM job_attempts a WHERE a.job_id=j.id AND a.status='SUCCEEDED') successful_attempts,
                (SELECT count(*) FROM job_events e WHERE e.job_id=j.id AND e.event_type='SUBMITTED') submitted_events,
                (SELECT count(*) FROM job_events e WHERE e.job_id=j.id AND e.event_type='SUCCEEDED') succeeded_events
            FROM jobs j ORDER BY j.id) r""")

    def run(self, build=True):
        summary = {"passed": False, "startedAt": self.started_at, "plannedTrials": len(self.plan),
                   "jobsPerTrial": self.jobs, "faultKinds": dict(Counter(row["kind"] for row in self.plan))}
        write_json(self.stack.output / "plan.json", {**summary, "plan": self.plan})
        try:
            self.stack.start(build=build)
            sources = [ROOT / "scripts/fault_campaign.py", ROOT / "scripts/test_fault_campaign.py"]
            environment = json.loads((self.stack.output / "environment.json").read_text())
            environment["sourceSha256"].update({str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources})
            environment["campaign"] = {"fixtureDelayMs": 3000, "fixtureConsumers": 1, "replacementConsumers": 2,
                                       "trialPlanSha256": hashlib.sha256((self.stack.output / "plan.json").read_bytes()).hexdigest()}
            write_json(self.stack.output / "environment.json", environment)
            for trial in self.plan:
                self.trial(trial)
            population = self.population()
            verify_population(self.expected, population)
            if len(self.injections) != len(self.plan) or len(self.completed) != len(self.plan):
                raise AssertionError("Every planned trial must have one observed injection and verified recovery")
            wait(lambda: self.stack.broker_queue("jobs.dead").get("messages", -1) == 0,
                 "Unresolved broker dead letters remain", timeout=30)
            compressed_rows(self.stack.output / "population.jsonl.gz", population)
            # Sources must stay unchanged throughout the campaign.
            for path, digest in environment["sourceSha256"].items():
                if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != digest:
                    raise AssertionError("Test source changed during the campaign")
            summary.update(passed=True, completedTrials=len(self.completed), observedInjections=len(self.injections),
                           acceptedJobs=len(self.expected), accountedJobs=len(population), lostJobs=0,
                           incorrectResults=0, duplicateSuccessfulAttempts=0,
                           elapsedSeconds=time.monotonic() - self.started, trials=self.completed,
                           scope="Sequential fault trials with fresh workloads on shared durable services; five repeated fault kinds, not 100 distinct failure modes or statistically independent environments. No host/volume loss or external-side-effect guarantee.",
                           faultFixture="Separate 3000 ms delayed worker only for kill/pause timing; no production handler changes or throughput claims")
        except BaseException as error:
            summary.update(completedTrials=len(self.completed), observedInjections=len(self.injections),
                           errorType=type(error).__name__, error=str(error))
            write_json(self.stack.output / "failed.json", summary)
            raise
        finally:
            try:
                self.stack.close()
                summary["cleanupVerified"] = True
            finally:
                self.journal.close()
                with (self.stack.output / "campaign.jsonl").open() as stream:
                    compressed_rows(self.stack.output / "campaign.jsonl.gz", (json.loads(line) for line in stream))
                (self.stack.output / "campaign.jsonl").unlink()
                # Do not mark a successful campaign until cleanup/OOM checks also passed.
                summary["passed"] = summary["passed"] and summary.get("cleanupVerified", False)
                write_json(self.stack.output / "report.json", summary)
        return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--trials", type=int, default=100)
    parser.add_argument("--seed", type=int, default=20261020)
    parser.add_argument("--jobs-per-trial", type=int, default=8)
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()
    plan = trial_plan(args.trials, args.seed)
    if not 4 <= args.jobs_per_trial <= 40:
        parser.error("Jobs per trial must be between 4 and 40")
    stack = Stack(args.output, batch=50, delay=250, prefetch=1, pool=6, concurrency=2, workers=1)
    summary = Campaign(stack, plan, args.jobs_per_trial).run(build=not args.skip_build)
    print(f"PASS: {summary['completedTrials']} actual fault injections; {summary['accountedJobs']} accepted jobs verified; zero observed loss.")


if __name__ == "__main__":
    main()
