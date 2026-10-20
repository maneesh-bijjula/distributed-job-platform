"""Exercise the deployment profile, offline backup/restore, rollout recovery and SIGTERM.

Only job-platform-release-test containers/volumes are disposable. Private archives
remain under .deployment/; public reports contain counts/identities, never payloads or keys.
"""
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
from uuid import uuid4

from deploy import Deployment, ROOT, image_name
from load_support import Client, expected_result, wait, write_json

PROJECT = "job-platform-release-test"


def cleanup(deployment):
    if deployment.project != PROJECT:
        raise ValueError("Release verification cleanup is restricted to the test deployment")
    deployment.guard()
    raw = subprocess.check_output(["docker", "volume", "ls", "-q", "--filter",
                                   "label=com.docker.compose.project=" + PROJECT], text=True)
    for volume in raw.split():
        if volume not in {PROJECT + "_postgres-data", PROJECT + "_rabbitmq-data"}:
            raise ValueError("Refusing cleanup with an unexpected test volume")
    deployment.run(["down", "--volumes", "--remove-orphans"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="job-platform:local")
    parser.add_argument("--fixture-image", default="job-platform:load-controlled")
    parser.add_argument("--output", default="reports/phase6/release")
    parser.add_argument("--cleanup-only", action="store_true")
    args = parser.parse_args()
    image_name(args.fixture_image)
    deployment = Deployment(PROJECT)
    if args.cleanup_only:
        if deployment.config.exists():
            cleanup(deployment)
        return
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    if not deployment.config.exists():
        deployment.initialize(args.image)
    if deployment.settings()["APP_IMAGE"] != args.image:
        raise ValueError("Existing release-test configuration uses another image; edit its private deploy.env first")
    if deployment.containers():
        raise ValueError("A previous release-test stack exists; use --cleanup-only before a new run")
    token = (deployment.directory / "secrets/bootstrap-key").read_text()
    client = Client(int(deployment.settings()["DEPLOY_API_PORT"]), token)
    public = Client(int(deployment.settings()["DEPLOY_API_PORT"]), None)
    report = {"passed": False, "recordedAt": datetime.now(timezone.utc).isoformat(), "scenarios": []}
    bodies = {}

    def submitted(kind="SHA256", scheduled=False, timeout=120):
        body = {"type": kind, "text": "release verification " + uuid4().hex, "timeoutSeconds": timeout}
        if scheduled:
            body["scheduledAt"] = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        key = "release-" + uuid4().hex
        status, job, _ = client.request("/api/v1/jobs", "POST", body, key)
        assert status == 202, "Release submission failed"
        bodies[job["id"]] = body
        return job["id"], key

    def state(job):
        status, value, _ = client.request("/api/v1/jobs/" + job)
        assert status == 200, "Cannot inspect release job"
        return value

    def succeeded(job):
        wait(lambda: state(job)["status"] == "SUCCEEDED", "Release job did not complete", timeout=120)
        assert state(job)["result"] == expected_result(bodies[job]), "Release result mismatch"
        assert state(job)["attemptCount"] == 1, "Unexpected duplicate attempt"

    try:
        with deployment.lock():
            print("Checking fresh deployment, private configuration, and packaged API workflow.", flush=True)
            deployment.run(["up", "-d", "--wait", "--wait-timeout", "180", "api", "worker"])
            assert public.request("/actuator/health/readiness")[0] == 200
            assert public.request("/api/v1/jobs")[0] == 401
            for script in ("smoke.py", "auth-smoke.py"):
                result = subprocess.run(["python3", str(ROOT / "scripts" / script), "--base-url", "http://127.0.0.1:" + str(client.port)],
                                        env={**os.environ, "JOB_API_KEY": token}, capture_output=True, text=True, timeout=150)
                assert result.returncode == 0, "Packaged release workflow failed: " + script
            complete, replay_key = submitted("UPPERCASE")
            succeeded(complete)
            try:
                deployment.backup()
            except ValueError as error:
                assert "Stop all API" in str(error)
            else:
                raise AssertionError("Online backup maintenance guard failed")
            configured = json.loads(deployment.run(["config", "--format", "json"]))
            assert set(configured["services"]) == {"api", "worker", "postgres", "rabbitmq"}
            assert all("ports" not in configured["services"][name] for name in ("postgres", "rabbitmq", "worker"))
            assert configured["services"]["api"]["ports"][0]["host_ip"] == "127.0.0.1"
            for name in ("api", "worker"):
                assert configured["services"][name]["read_only"] is True
                assert not any("PASSWORD" in key or "BOOTSTRAP" in key for key in configured["services"][name]["environment"])
            report["scenarios"].append({"case": "fresh-deployment", "packagedLifecycle": True,
                                        "packagedAccessControl": True, "privateSecretMounts": True, "loopbackApiOnly": True})

            print("Checking a failed application rollout and restoration of the compatible image.", flush=True)
            deployment.run(["stop", "api", "worker"])
            before = deployment.fingerprint()
            bad = deployment.directory / "failed-rollout.yaml"
            bad.write_text("services:\n  api:\n    entrypoint: [/bin/false]\n    restart: 'no'\n")
            try:
                deployment.run(["-f", str(bad), "up", "-d", "--wait", "--wait-timeout", "30", "api"], timeout=60)
            except RuntimeError:
                pass
            else:
                raise AssertionError("Deliberately failed rollout unexpectedly started")
            assert deployment.fingerprint() == before
            deployment.run(["up", "-d", "--wait", "--wait-timeout", "180", "api", "worker"])
            client.reset()
            assert state(complete)["status"] == "SUCCEEDED"
            report["scenarios"].append({"case": "failed-rollout-recovery", "sameSchemaImageRecovery": True,
                                        "databaseUnchanged": True, "automaticMigrationRollbackClaimed": False})

            print("Checking SIGTERM during a real eight-second claimed execution (separate fixture image).", flush=True)
            deployment.run(["stop", "worker"])
            fixture = deployment.directory / "shutdown-fixture.yaml"
            fixture.write_text("services:\n  worker:\n    image: " + args.fixture_image +
                               "\n    environment:\n      LOAD_HANDLER_DELAY_MS: '8000'\n      WORKER_CONCURRENCY: '1'\n")
            deployment.run(["-f", str(fixture), "up", "-d", "worker"])
            active, _ = submitted()
            wait(lambda: state(active)["status"] == "RUNNING", "Shutdown fixture did not claim work", interval=.05)
            start = time.monotonic()
            deployment.run(["stop", "worker"])
            stop_seconds = time.monotonic() - start
            succeeded(active)
            worker = next(row for row in deployment.containers() if row["Service"] == "worker")
            # JVM shutdown hooks can finish normally while preserving SIGTERM's 128+15 status.
            assert worker["ExitCode"] in (0, 143), "Worker was killed or failed during shutdown"
            assert stop_seconds >= 5, "Shutdown scenario did not overlap the slow handler"
            report["scenarios"].append({"case": "graceful-worker-shutdown", "fixtureDelayMs": 8000,
                                        "stopSeconds": stop_seconds, "successfulAttempts": 1, "workerExitCode": worker["ExitCode"]})

            # Preserve a real terminal failure and resolved dead-letter audit in the archive too.
            deployment.run(["-f", str(fixture), "up", "-d", "worker"])
            timed_out, _ = submitted(timeout=1)
            wait(lambda: state(timed_out)["status"] == "FAILED", "Diagnostic deadline did not expire")
            status, inbox, _ = client.request("/api/v1/dead-letters?state=OPEN")
            assert status == 200
            entry = next(item for item in inbox["items"] if item["jobId"] == timed_out)
            assert client.request("/api/v1/dead-letters/" + entry["id"] + "/discard", "POST",
                                  key="release-discard-" + uuid4().hex)[0] == 200
            deployment.run(["stop", "worker"])

            print("Backing up completed, cancelled, queued and scheduled jobs with access/audit data.", flush=True)
            immediate = [submitted()[0] for _ in range(2)]
            scheduled, _ = submitted(scheduled=True)
            wait(lambda: deployment.sql("SELECT count(*) FROM job_outbox WHERE job_id IN (" +
                                         ",".join("'" + job + "'" for job in immediate) + ")") == "0",
                 "Immediate jobs were not durably dispatched")
            deployment.run(["stop", "api", "worker"])
            backup = deployment.backup()
            verified = deployment.verify_backup(backup)
            assert verified == deployment.fingerprint()
            try:
                deployment.restore(backup)
            except ValueError as error:
                assert "empty" in str(error)
            else:
                raise AssertionError("Restore overwrote a nonempty database")
            report["scenarios"].append({"case": "backup-restored-and-compared", "tables": len(verified["tables"]),
                                        "sequences": len(verified["sequences"]),
                                        "rows": {name: value["rows"] for name, value in verified["tables"].items()},
                                        "allFingerprintsMatch": True, "nonemptyRestoreRejected": True})

            print("Replacing only the disposable deployment's data volumes, then restoring the archive.", flush=True)
            started = time.monotonic()
            cleanup(deployment)
            deployment.run(["up", "-d", "--wait", "--wait-timeout", "180", "postgres", "rabbitmq"])
            restored = deployment.restore(backup)
            assert restored == verified
            # A new broker has no copies of previously dispatched jobs. Rebuild only queued work.
            recovery = deployment.reconcile_outbox()
            assert recovery["recreatedOutboxEntries"] == 2
            assert deployment.reconcile_outbox()["recreatedOutboxEntries"] == 0
            deployment.run(["up", "-d", "--wait", "--wait-timeout", "180", "api", "worker"])
            client.reset()
            for job in immediate:
                succeeded(job)
            assert state(timed_out)["status"] == "FAILED"
            status, retained, _ = client.request("/api/v1/dead-letters?state=DISCARDED")
            assert status == 200 and any(item["id"] == entry["id"] for item in retained["items"])
            assert state(scheduled)["status"] == "QUEUED", "Restore bypassed a future schedule"
            status, replay, _ = client.request("/api/v1/jobs", "POST", bodies[complete], replay_key)
            assert status == 200 and replay["id"] == complete, "Restore lost submission idempotency"
            assert client.request("/api/v1/jobs/" + scheduled + "/cancel", "POST")[0] == 200
            report["scenarios"].append({"case": "empty-volume-disaster-restore", "allFingerprintsMatchBeforeStartup": True,
                                        "recreatedOutboxEntries": 2, "repeatReconciliationNoOp": True,
                                        "recoveredJobsVerified": 2, "scheduledWorkNotRunEarly": True,
                                        "idempotencyPreserved": True, "resolvedDeadLetterPreserved": True,
                                        "localRecoverySeconds": time.monotonic() - started})

            print("Checking lost broker deliveries can be repaired from current PostgreSQL state.", flush=True)
            deployment.run(["stop", "worker"])
            missing, _ = submitted("WORD_COUNT")
            wait(lambda: deployment.sql("SELECT count(*) FROM job_outbox WHERE job_id='" + missing + "'") == "0",
                 "Broker-loss job did not dispatch")
            deployment.run(["stop", "api"])
            deployment.run(["exec", "-T", "rabbitmq", "rabbitmqctl", "purge_queue", "jobs.priority.ready"])
            repair = deployment.reconcile_outbox()
            assert repair["recreatedOutboxEntries"] == 1
            deployment.run(["up", "-d", "--wait", "--wait-timeout", "180", "api", "worker"])
            client.reset()
            succeeded(missing)
            report["scenarios"].append({"case": "broker-message-loss-repair", "recreatedOutboxEntries": 1,
                                        "resultVerified": True, "successfulAttempts": 1})
            logs = deployment.run(["logs", "--no-color", "api", "worker", "postgres", "rabbitmq"])
            private_values = [(deployment.directory / "secrets" / name).read_text()
                              for name in ("database-password", "broker-password", "bootstrap-key")]
            assert all(value not in logs for value in private_values), "Credential appeared in deployment logs"
            image = json.loads(subprocess.check_output(["docker", "image", "inspect", args.image], text=True))[0]
            report.update(passed=True, applicationImage=args.image, applicationImageId=image["Id"],
                          sourceCommit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                          workingTreeDirty=bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT)),
                          sourceSha256={str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                                        for path in sorted([*(ROOT / "src/main").rglob("*"), *(ROOT / "tools/loadtest/java").rglob("*"),
                                                            ROOT / "compose.deploy.yaml", ROOT / "Dockerfile", ROOT / "Dockerfile.loadtest",
                                                            ROOT / "scripts/deploy.py", ROOT / "scripts/release-smoke.py"]) if path.is_file()},
                          credentialLogScanPassed=True,
                          scope="Single-host local verification; not HA, zero RPO, public TLS or an availability SLA")
    finally:
        cleanup(deployment)
        report["cleanupVerified"] = True
        write_json(output / "report.json", report)
    print("PASS: fresh deployment, failed rollout, graceful shutdown, verified backup/restore and broker repair.", flush=True)


if __name__ == "__main__":
    main()
