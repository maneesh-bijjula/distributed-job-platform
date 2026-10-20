"""Real, bounded failure scenarios on the dedicated job-platform-load Compose stack.

The controlled worker image adds a synthetic, non-cooperative delay only for
deterministic fault injection. It is never used for capacity measurements.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
import time
from load_support import Client, Stack, Workload, compressed_rows, expected_result, owner_clients, specs, wait, write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()
    stack = Stack(args.output, batch=50, delay=250, prefetch=1, pool=6, concurrency=1, workers=2)
    reports, injections = [], []

    def work(name, count, duplicates=0):
        return Workload(stack, name, specs(count, sizes=(128, 4096)), 8, duplicates=duplicates)

    def finish(job, fixture=False, abandoned=False):
        job.await_terminal(timeout=150)
        report = job.evidence(allow_abandoned=abandoned)
        report["fixtureUsed"] = fixture
        write_json(job.directory / "report.json", report)
        reports.append(report)
        return report

    def outbox_empty(job):
        wait(lambda: stack.sql("SELECT to_json(count(*)) FROM job_outbox o JOIN jobs j ON j.id=o.job_id WHERE j.owner_id='" + job.owner + "'") == 0,
             "Scenario outbox did not drain")

    def attempts(job):
        return stack.sql("SELECT coalesce(json_agg(row_to_json(r)),'[]'::json) FROM (SELECT a.id,a.job_id,a.status,a.generation,a.started_at,a.worker_id FROM job_attempts a JOIN jobs j ON j.id=a.job_id WHERE j.owner_id='" + job.owner + "' ORDER BY a.started_at,a.id) r")

    def first_claim(job):
        result = []
        def claimed():
            result[:] = attempts(job)
            return result and result[0]["status"] == "RUNNING"
        wait(claimed, "Controlled worker never claimed the diagnostic job", interval=.05)
        return result[0]

    def controlled(delay=2000, prefetch=1):
        stack.env.update(LOAD_HANDLER_DELAY_MS=str(delay), LOAD_PREFETCH=str(prefetch), LOAD_CONCURRENCY="1")
        stack.up("controlled-worker")

    def normal():
        stack.compose("--profile", "fault", "stop", "controlled-worker")
        stack.compose("up", "-d", "--scale", "worker=2", "worker")

    try:
        stack.start(build=not args.skip_build)
        print("Checking concurrent APIs/workers and cross-API idempotency replays.", flush=True)
        concurrent = work("concurrent-duplicates", 80, duplicates=80)
        concurrent.submit()
        report = finish(concurrent)
        assert report["workerProcessesUsed"] >= 2
        assert report["httpStatuses"].get("200", 0) == 80
        assert report["apiRequests"].get("0", 0) > 0 and report["apiRequests"].get("1", 0) > 0

        print("Checking broker downtime: durable admission and outbox restoration.", flush=True)
        stack.compose("stop", "rabbitmq")
        broker = work("broker-outage", 24, duplicates=12)
        broker.submit()
        assert broker.state() == {"total": 24, "terminal": 0}
        assert stack.sql("SELECT to_json(count(*)) FROM job_outbox o JOIN jobs j ON j.id=o.job_id WHERE j.owner_id='" + broker.owner + "'") == 24
        for client in (Client(18080, None), Client(18081, None)):
            assert client.request("/actuator/health/readiness")[0] == 200
        injections.append({"fault": "broker_stop_restore", "acceptedWhileDown": 24, "outboxPreserved": 24})
        stack.up("rabbitmq")
        finish(broker)

        print("Checking database downtime: accepted schedules persist and ambiguous requests retry safely.", flush=True)
        before_db = work("database-before", 6)
        schedule = (datetime.now(timezone.utc) + timedelta(seconds=25)).isoformat()
        for item in before_db.items:
            item["body"]["scheduledAt"] = schedule
        before_db.submit()
        during_db = work("database-during", 12, duplicates=6)  # Provision the owner before interrupting storage.
        stack.compose("stop", "postgres")
        public = Client(18080, None)
        assert public.request("/actuator/health/liveness")[0] == 200
        assert public.request("/actuator/health/readiness")[0] == 503
        assert stack.clients[0].request("/api/v1/jobs")[0] == 503
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(during_db.submit)
            wait(lambda: any(row["httpStatus"] in (0, 503) for row in during_db.responses),
                 "No submission actually observed database downtime", timeout=35, interval=.1)
            stack.up("postgres")
            pending.result(timeout=100)
        assert any(row["httpStatus"] in (0, 503) for row in during_db.responses), "No submission actually overlapped database downtime"
        finish(before_db)
        finish(during_db, abandoned=True)
        injections.append({"fault": "database_stop_restore", "previouslyAccepted": 6, "retriedUniqueSubmissions": 12,
                           "readiness": 503, "liveness": 200, "authenticatedApi": 503})

        print("Checking a full API/worker restart with pending accepted jobs.", flush=True)
        restart = work("application-restart", 12, duplicates=12)
        schedule = (datetime.now(timezone.utc) + timedelta(seconds=12)).isoformat()
        for item in restart.items:
            item["body"]["scheduledAt"] = schedule
        restart.submit()
        assert restart.state()["terminal"] == 0
        stack.compose("stop", "api", "api2", "worker")
        stack.compose("up", "-d", "--wait", "--wait-timeout", "150", "--scale", "worker=2", "api", "api2", "worker", timeout=210)
        for client in stack.clients:
            client.reset()  # The old keep-alive sockets belong to the stopped API processes.
        for item in restart.items:
            status, replay, _ = restart.clients[1].request("/api/v1/jobs", "POST", item["body"], item["key"])
            assert status == 200 and replay["id"] == restart.ids[item["index"]]
        finish(restart)
        injections.append({"fault": "application_stop_restart", "acceptedIdsPreserved": 12})

        print("Checking repeated broker deliveries produce one successful attempt per job.", flush=True)
        stack.compose("stop", "worker")
        duplicate = work("duplicate-delivery", 20, duplicates=20)
        duplicate.submit(); outbox_empty(duplicate)
        for job_id in duplicate.ids.values():
            stack.broker_publish(job_id); stack.broker_publish(job_id)
        normal()
        finish(duplicate)
        injections.append({"fault": "duplicate_broker_delivery", "extraMessages": 40, "jobs": 20})

        print("Killing an actual worker after its execution claim commits.", flush=True)
        stack.compose("stop", "worker")
        controlled()
        killed = work("worker-killed", 1)
        killed.submit(); claim = first_claim(killed)
        container = stack.ids("controlled-worker")[0]
        stack.container_action("kill", container)
        injections.append({"fault": "worker_sigkill", "attemptId": claim["id"], "jobId": claim["job_id"]})
        stack.up("controlled-worker")
        report = finish(killed, fixture=True, abandoned=True)
        assert report["attemptOutcomes"] == {"ABANDONED": 1, "SUCCEEDED": 1}
        assert [attempt["generation"] for attempt in attempts(killed)] == [0, 1]
        stack.compose("--profile", "fault", "stop", "controlled-worker")

        print("Freezing a claimed worker, completing recovery elsewhere, and rejecting its resumed stale result.", flush=True)
        controlled()
        stale = work("stale-worker-resumed", 1)
        stale.submit(); old_claim = first_claim(stale)
        frozen = stack.ids("controlled-worker")[0]
        stack.container_action("pause", frozen)
        replacement = None
        try:
            replacement = stack.compose("--profile", "fault", "run", "-d", "--no-deps", "--name", PROJECT_REPLACEMENT, "controlled-worker").strip().splitlines()[-1]
            report = finish(stale, fixture=True, abandoned=True)
            assert report["attemptOutcomes"] == {"ABANDONED": 1, "SUCCEEDED": 1}
            stack.container_action("unpause", frozen)
            def rejected():
                return any(row.get("event") == "completion_rejected" and row.get("attempt_id") == old_claim["id"] for row in stack.logs(frozen))
            wait(rejected, "Resumed worker did not reach and fail the stale final-write guard", timeout=30)
            assert len([attempt for attempt in attempts(stale) if attempt["status"] == "SUCCEEDED"]) == 1
            injections.append({"fault": "worker_pause_resume", "attemptId": old_claim["id"], "replacementSucceededBeforeResume": True,
                               "staleFinalWriteRejected": True})
        finally:
            if stack.inspect(frozen)["State"].get("Paused"):
                stack.container_action("unpause", frozen)
            if replacement:
                stack.container_action("rm", replacement)
        stack.compose("--profile", "fault", "stop", "controlled-worker")

        print("Checking a finite priority burst overtakes older low-priority work and then drains it.", flush=True)
        low = work("priority-low", 4); low.submit(); outbox_empty(low)
        high = work("priority-high", 16)
        for item in high.items:
            item["body"]["priority"] = 9
        high.submit(); outbox_empty(high)
        controlled(delay=100, prefetch=1)
        finish(low, fixture=True); finish(high, fixture=True)
        assert max(row["started_at"] for row in attempts(high)) < min(row["started_at"] for row in attempts(low))
        injections.append({"fault": "finite_priority_burst", "highFirst": 16, "olderLowEventuallyCompleted": 4,
                           "continuousHighTrafficCanStarveLow": True})
        stack.compose("--profile", "fault", "stop", "controlled-worker")

        print("Checking prefetch can delay urgent work already behind delivered low-priority messages.", flush=True)
        prefetched = work("prefetched-low", 10); prefetched.submit(); outbox_empty(prefetched)
        controlled(delay=400, prefetch=10)
        first_claim(prefetched)
        urgent = work("prefetched-urgent", 3)
        for item in urgent.items:
            item["body"]["priority"] = 9
        urgent.submit()
        finish(prefetched, fixture=True); finish(urgent, fixture=True)
        urgent_start = min(row["started_at"] for row in attempts(urgent))
        ahead = sum(row["started_at"] < urgent_start for row in attempts(prefetched))
        assert ahead >= 2, "Prefetch scenario did not demonstrate any queued low-priority delivery ahead of urgent work"
        injections.append({"fault": "prefetch_priority_delay", "prefetch": 10, "lowAttemptsBeforeFirstUrgent": ahead})

        print("Checking the admission ceiling stays shared across both API instances.", flush=True)
        remaining = stack.sql("SELECT to_json(extract(epoch FROM(date_trunc('minute',clock_timestamp())+interval '1 minute'-clock_timestamp())))")
        if remaining < 5:
            time.sleep(float(remaining) + .1)
        owner, clients = owner_clients(stack, "admission ceiling", limit=5)
        admission_items = specs(12, sizes=(128,))
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda indexed: clients[indexed[0] % 2].request("/api/v1/jobs", "POST", indexed[1]["body"], indexed[1]["key"]), enumerate(admission_items)))
        accepted = [(item, result[1]) for item, result in zip(admission_items, results) if result[0] == 202]
        assert len(accepted) == 5 and sum(result[0] == 429 for result in results) == 7
        item, original = accepted[0]
        status, replay, _ = clients[1].request("/api/v1/jobs", "POST", item["body"], item["key"])
        assert status == 200 and replay["id"] == original["id"]
        wait(lambda: stack.sql("SELECT to_json(count(*)) FROM jobs WHERE owner_id='" + owner + "' AND status='SUCCEEDED'") == 5,
             "Admission scenario's accepted jobs did not finish")
        admission_rows = stack.sql("SELECT json_agg(row_to_json(r)) FROM (SELECT id,status,result,attempt_count FROM jobs WHERE owner_id='" + owner + "') r")
        admission_expected = {result["id"]: item for item, result in accepted}
        for row in admission_rows:
            assert row["status"] == "SUCCEEDED" and row["attempt_count"] == 1
            assert row["result"] == expected_result(admission_expected[row["id"]]["body"])
            del row["result"]
        compressed_rows(Path(args.output) / "admission-jobs.jsonl.gz", admission_rows)
        injections.append({"fault": "bounded_admission", "limitPerMinute": 5, "accepted": 5, "rejected429": 7, "replayCharged": False})

        count = sum(row["accountedFor"] for row in reports) + 5
        conservation = stack.sql("SELECT json_build_object('total',count(*),'succeeded',count(*) FILTER (WHERE status='SUCCEEDED')) FROM jobs")
        assert conservation == {"total": count, "succeeded": count}, "Suite-wide accepted-job conservation failed"
        wait(lambda: stack.broker_queue("jobs.dead").get("messages", -1) == 0,
             "Unresolved broker dead letters remain after resilience verification", timeout=30)
        summary = {"passed": True, "settings": stack.settings,
                   "scenarios": injections, "workloads": reports, "accountedJobs": count, "conservation": conservation,
                   "faultFixture": "Separate image, deterministic non-cooperative 100–2000 ms delay; excluded from capacity evidence",
                   "scope": "One execution of each listed scenario, not 100 independent failure trials or a production availability guarantee"}
    except BaseException as error:
        write_json(Path(args.output) / "failed.json", {"passed": False, "errorType": type(error).__name__, "message": str(error)})
        raise
    finally:
        stack.close()
    summary["cleanupVerified"] = True
    write_json(Path(args.output) / "report.json", summary)
    print("PASS: all bounded resilience/priority/admission scenarios; every workload job accounted for and verified.", flush=True)


PROJECT_REPLACEMENT = "job-platform-load-replacement"

if __name__ == "__main__":
    main()
