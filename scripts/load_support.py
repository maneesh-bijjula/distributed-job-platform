"""Shared, standard-library-only helpers for the isolated load and fault runners."""
from __future__ import annotations

import base64
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import platform
import secrets
import subprocess
import sys
import threading
import time
from uuid import UUID, uuid4

ROOT = Path(__file__).resolve().parent.parent
PROJECT = "job-platform-load"
LABEL = "com.maneesh.job-platform.load-test"
SERVICES = {"api", "api2", "worker", "controlled-worker", "postgres", "rabbitmq"}


def now():
    return datetime.now(timezone.utc).isoformat()


def percentile(values, quantile):
    """Nearest-rank percentile; return None for no samples, never invent a zero latency."""
    if not 0 < quantile <= 1:
        raise ValueError("quantile must be in (0,1]")
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * quantile) - 1)] if ordered else None


def distribution(values):
    values = list(values)
    return {"samples": len(values), "mean": sum(values) / len(values) if values else None,
            "p50": percentile(values, .50), "p95": percentile(values, .95),
            "p99": percentile(values, .99), "max": max(values) if values else None}


def identifier(value):
    return str(UUID(str(value)))


def wait(predicate, message, timeout=120, interval=.25):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError(message)


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def compressed_rows(path, rows):
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, separators=(",", ":"), allow_nan=False) + "\n")


class Stack:
    """All destructive operations are confined to labeled, dedicated load-test resources."""
    def __init__(self, output, *, batch=20, delay=1000, prefetch=10, pool=10, concurrency=2, workers=1):
        self.output = Path(output).resolve()
        self.output.parent.mkdir(parents=True, exist_ok=True)
        self.output.mkdir(exist_ok=False)
        self.settings = {"batchSize": batch, "dispatchDelayMs": delay, "prefetch": prefetch,
                         "databasePoolSize": pool, "concurrencyPerWorker": concurrency, "workers": workers,
                         "apiInstances": 2, "leaseSeconds": 6, "heartbeatMs": 1000}
        path = ROOT / ".loadtest.env"
        if path.is_symlink():
            raise ValueError("Refusing a symlink for load credentials")
        if not path.exists():
            values = {key: secrets.token_urlsafe(32) for key in
                      ("LOAD_BOOTSTRAP_KEY", "LOAD_DATABASE_PASSWORD", "LOAD_BROKER_PASSWORD")}
            values["LOAD_BOOTSTRAP_KEY"] = "jpk_" + values["LOAD_BOOTSTRAP_KEY"]
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w") as stream:
                stream.write("".join(key + "=" + value + "\n" for key, value in values.items()))
        os.chmod(path, 0o600)
        self.private = dict(line.split("=", 1) for line in path.read_text().splitlines() if line)
        if set(self.private) != {"LOAD_BOOTSTRAP_KEY", "LOAD_DATABASE_PASSWORD", "LOAD_BROKER_PASSWORD"}:
            raise ValueError("Invalid isolated load credential file")
        if not self.private["LOAD_BOOTSTRAP_KEY"].startswith("jpk_") or len(self.private["LOAD_BOOTSTRAP_KEY"]) != 47:
            raise ValueError("Invalid load bootstrap token format")
        self.env = {**os.environ, **self.private, "LOAD_BATCH_SIZE": str(batch), "LOAD_DISPATCH_MS": str(delay),
                    "LOAD_PREFETCH": str(prefetch), "LOAD_POOL_SIZE": str(pool), "LOAD_CONCURRENCY": str(concurrency)}
        # Fixed loopback addresses prevent an accidental run against the ordinary application.
        self.env.update(LOAD_API_PORT="18080", LOAD_API2_PORT="18081", LOAD_MANAGEMENT_PORT="18082")
        self.command = ["docker", "compose", "-p", PROJECT, "--env-file", str(path), "-f", str(ROOT / "compose.load.yaml")]
        self.clients = [Client(18080, self.private["LOAD_BOOTSTRAP_KEY"]),
                        Client(18081, self.private["LOAD_BOOTSTRAP_KEY"])]
        self.stop_sampling = threading.Event()
        self.sampler = None
        self.started = False

    def docker(self, *args, timeout=180):
        result = subprocess.run(["docker", *args], cwd=ROOT, env=self.env, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
        if result.returncode:
            raise RuntimeError("Docker operation failed: " + args[0])
        return result.stdout

    def compose(self, *args, timeout=180):
        result = subprocess.run([*self.command, *args], cwd=ROOT, env=self.env, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
        if result.returncode:
            # Do not print a command environment or unfiltered container logs containing credentials.
            (self.output / "compose-error.txt").write_text(result.stdout)
            raise RuntimeError("Isolated Compose operation failed: " + args[0] + "; inspect compose-error.txt locally")
        return result.stdout

    def ids(self, service=None, all_containers=False):
        if service is not None and service not in SERVICES:
            raise ValueError("Unknown load service")
        args = ["ps", "-aq" if all_containers else "-q", "--filter", "label=com.docker.compose.project=" + PROJECT]
        if service:
            args += ["--filter", "label=com.docker.compose.service=" + service]
        return self.docker(*args).split()

    def inspect(self, container):
        data = json.loads(self.docker("inspect", container))[0]
        labels = data["Config"].get("Labels", {})
        if labels.get("com.docker.compose.project") != PROJECT or labels.get(LABEL) != "true":
            raise ValueError("Refusing to operate on a container outside the labeled load stack")
        return data

    def guard(self):
        for container in self.ids(all_containers=True):
            self.inspect(container)
        volumes = self.docker("volume", "ls", "-q", "--filter", "label=com.docker.compose.project=" + PROJECT).split()
        for volume in volumes:
            data = json.loads(self.docker("volume", "inspect", volume))[0]
            if data.get("Labels", {}).get(LABEL) != "true":
                raise ValueError("Refusing to remove an unrecognized volume")

    def container_action(self, action, *containers):
        if action not in {"pause", "unpause", "kill", "rm"}:
            raise ValueError("Unsupported fault action")
        for container in containers:
            self.inspect(container)
        return self.docker(action, *(["-f"] if action == "rm" else []), *containers)

    def up(self, *services, **settings):
        if not set(services) <= SERVICES:
            raise ValueError("Unknown service")
        self.env.update({key: str(value) for key, value in settings.items()})
        self.compose("--profile", "fault", "up", "-d", "--wait", "--wait-timeout", "150", *services, timeout=210)

    def start(self, build=True):
        self.guard()
        self.started = True
        # Fresh databases make independent benchmark runs comparable; these are load-only volumes.
        self.compose("--profile", "fault", "down", "--volumes", "--remove-orphans")
        if build:
            print("Building isolated application and fault fixture images.", flush=True)
            self.compose("--profile", "fault", "build", "api", "controlled-worker", timeout=600)
        self.up("api", "api2")
        self.compose("up", "-d", "--scale", "worker=" + str(self.settings["workers"]), "worker")
        assert self.sql("SELECT to_json(current_database())") == "jobs_load_test"
        for client in self.clients:
            assert client.request("/api/v1/auth/me")[0] == 200
        wait(lambda: self.broker_queue().get("consumers", 0) >= self.settings["workers"] * self.settings["concurrencyPerWorker"],
             "Not all configured worker consumers became ready", timeout=90)
        self.environment()
        self.sampler = threading.Thread(target=self.sample, daemon=True)
        self.sampler.start()

    def sql(self, statement):
        if not statement.lstrip().upper().startswith("SELECT "):
            raise ValueError("Harness database inspection must be read-only")
        result = self.compose("exec", "-T", "-e", "PGOPTIONS=-c default_transaction_read_only=on", "postgres", "psql", "-U", "jobs", "-d", "jobs_load_test",
                              "-v", "ON_ERROR_STOP=1", "-At", "-c", statement)
        return json.loads(result)

    def environment(self):
        info = json.loads(self.docker("info", "--format", "{{json .}}"))
        revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True))
        images = {}
        for image in ("job-platform:load", "job-platform:load-controlled", "postgres:17-alpine", "rabbitmq:4.1-management-alpine"):
            data = json.loads(self.docker("image", "inspect", image))[0]
            images[image] = {key: data.get(key) for key in ("Id", "RepoDigests", "Architecture")}
        limits = {}
        for container in self.ids():
            data = self.inspect(container)
            limits[data["Name"].lstrip("/")] = {key: data["HostConfig"][key] for key in ("Memory", "NanoCpus")}
        write_json(self.output / "environment.json", {"recordedAt": now(), "gitCommit": revision, "workingTreeDirty": dirty,
                   "command": ["python3", *sys.argv],
                   "sourceSha256": {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                                    for path in sorted([*(ROOT / "src/main").rglob("*"), *(ROOT / "tools/loadtest/java").rglob("*"), ROOT / "Dockerfile", ROOT / "Dockerfile.loadtest",
                                                        ROOT / "compose.load.yaml", ROOT / "pom.xml", ROOT / "scripts/load_support.py",
                                                        ROOT / "scripts/load-test.py", ROOT / "scripts/resilience-test.py"])
                                    if path.is_file()},
                   "hostOS": platform.system(), "hostArchitecture": platform.machine(), "python": platform.python_version(),
                   "docker": {key: info.get(key) for key in ("ServerVersion", "NCPU", "MemTotal", "Architecture", "OperatingSystem")},
                   "images": images, "containerLimits": limits, "settings": self.settings,
                   "timing": "Client monotonic elapsed time; DB timestamps for job/attempt intervals; nearest-rank percentiles",
                   "cpuWork": "Unmodified WORD_COUNT/SHA256/UPPERCASE unless a fault report explicitly identifies the fixture"})

    def sample(self):
        with gzip.open(self.output / "resource-samples.jsonl.gz", "wt") as stream:
            while not self.stop_sampling.is_set():
                try:
                    ids = self.ids()
                    if ids:
                        raw = self.docker("stats", "--no-stream", "--format", "{{json .}}", *ids, timeout=15)
                        for line in raw.splitlines():
                            stream.write(json.dumps({"at": now(), **json.loads(line)}) + "\n")
                        stream.flush()
                except (RuntimeError, subprocess.TimeoutExpired):
                    pass  # Resource sampling is diagnostic; lifecycle accounting is authoritative.
                self.stop_sampling.wait(2)

    def broker_publish(self, job_id, generation=0, priority=0):
        token = base64.b64encode(("jobs:" + self.private["LOAD_BROKER_PASSWORD"]).encode()).decode()
        client = Client(18082, None)
        status, body, _ = client.request("/api/exchanges/%2F/jobs.exchange/publish", "POST", {
            "properties": {"delivery_mode": 2, "priority": priority, "headers": {"job-generation": generation}},
            "routing_key": "jobs.execute.priority", "payload": identifier(job_id), "payload_encoding": "string"},
            authorization="Basic " + token)
        assert status == 200 and body.get("routed"), "Duplicate delivery was not routed"

    def broker_queue(self, queue="jobs.priority.ready"):
        if queue not in {"jobs.priority.ready", "jobs.ready", "jobs.dead"}:
            raise ValueError("Unknown diagnostic queue")
        token = base64.b64encode(("jobs:" + self.private["LOAD_BROKER_PASSWORD"]).encode()).decode()
        status, body, _ = Client(18082, None).request("/api/queues/%2F/" + queue, authorization="Basic " + token)
        return body if status == 200 else {}

    def logs(self, container):
        self.inspect(container)
        result = self.docker("logs", container)
        for token in self.private.values():
            assert token not in result, "A private credential appeared in load logs"
        return [json.loads(line) for line in result.splitlines() if line.startswith("{")]

    def close(self):
        self.stop_sampling.set()
        if self.sampler:
            self.sampler.join(timeout=20)
        if self.started:
            self.guard()
            states, oom = [], False
            try:
                for container in self.ids(all_containers=True):
                    data = self.inspect(container)
                    if data["State"].get("Paused"):
                        self.container_action("unpause", container)
                    oom = oom or data["State"].get("OOMKilled", False)
                    states.append({"name": data["Name"], "status": data["State"]["Status"], "oomKilled": data["State"].get("OOMKilled", False)})
                    if data["Config"]["Labels"].get("com.docker.compose.service") in {"api", "api2", "worker", "controlled-worker"}:
                        compressed_rows(self.output / (data["Name"].strip("/") + "-logs.jsonl.gz"), self.logs(container))
            finally:
                write_json(self.output / "container-final-state.json", states)
                self.compose("--profile", "fault", "down", "--volumes", "--remove-orphans")
            assert not oom, "A test container was OOM-killed; results are invalid"


class Client:
    def __init__(self, port, token):
        self.port, self.token = port, token
        self.local = threading.local()

    def reset(self):
        connection = getattr(self.local, "connection", None)
        if connection is not None:
            connection.close()
        self.local.connection = None

    def request(self, path, method="GET", body=None, key=None, authorization=None):
        connection = getattr(self.local, "connection", None)
        if connection is None:
            connection = self.local.connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=15)
        headers = {"Content-Type": "application/json"}
        if authorization or self.token:
            headers["Authorization"] = authorization or "Bearer " + self.token
        if key:
            headers["Idempotency-Key"] = key
        try:
            connection.request(method, path, body=None if body is None else json.dumps(body).encode(), headers=headers)
            response = connection.getresponse()
            raw = response.read()
            return response.status, json.loads(raw), dict(response.getheaders())
        except (OSError, http.client.HTTPException, ValueError):
            connection.close()
            self.local.connection = None
            return 0, {"error": "transport_or_decode_failure"}, {}


def owner_clients(stack, name, limit=10000):
    operator = stack.clients[0]
    status, owner, _ = operator.request("/api/v1/admin/owners", "POST", {"name": name, "submissionLimitPerMinute": limit})
    assert status == 201, "Cannot create isolated workload owner"
    status, key, _ = operator.request("/api/v1/admin/owners/" + owner["id"] + "/keys", "POST", {"name": name, "role": "USER"})
    assert status == 201, "Cannot issue isolated workload key"
    return identifier(owner["id"]), [Client(client.port, key["token"]) for client in stack.clients]


def expected_result(body):
    text, kind = body["text"], body["type"]
    if kind == "WORD_COUNT":
        return {"wordCount": len(text.split()), "characterCount": len(text)}
    if kind == "SHA256":
        return {"sha256": hashlib.sha256(text.encode()).hexdigest()}
    if kind == "UPPERCASE":
        return {"text": text.upper()}
    raise ValueError("Unknown workload handler")


def specs(count, seed=17, sizes=(128, 4096, 16000), schedule_seconds=0, priorities=False):
    run = "load-" + str(uuid4())
    scheduled = (datetime.now(timezone.utc) + timedelta(seconds=schedule_seconds)).isoformat() if schedule_seconds else None
    result = []
    for i in range(count):
        size = sizes[i % len(sizes)]
        fragment = "load %d item %d " % (seed, i)
        text = (fragment * (size // len(fragment) + 1))[:size]
        body = {"type": ("WORD_COUNT", "SHA256", "UPPERCASE")[i % 3], "text": text,
                "timeoutSeconds": 180, "priority": (9 if i % 2 else 0) if priorities else 0}
        if scheduled and i % 5 == 0:
            body["scheduledAt"] = scheduled
        result.append({"index": i, "key": run + "-" + str(i), "body": body})
    return result


class Workload:
    def __init__(self, stack, name, items, clients, duplicates=0, owner=None):
        self.stack, self.name, self.items = stack, name, items
        self.owner, self.clients = owner_clients(stack, name) if owner is None else (owner, clients)
        self.parallelism, self.duplicates = clients if isinstance(clients, int) else 8, duplicates
        self.responses, self.ids, self.latencies, self.logical_latencies = [], {}, [], []
        self.lock = threading.Lock()
        self.started, self.submitted, self.finished = None, None, None
        self.directory = stack.output / name
        self.directory.mkdir()
        self.journal = (self.directory / "requests.jsonl").open("w")

    def submit_one(self, item, ordinal):
        start, deadline, attempt = time.monotonic(), time.monotonic() + 120, 0
        while True:
            server = (item["index"] + ordinal // len(self.items) + attempt) % len(self.clients)
            began = time.monotonic()
            status, body, headers = self.clients[server].request("/api/v1/jobs", "POST", item["body"], item["key"])
            elapsed = (time.monotonic() - began) * 1000
            record = {"index": item["index"], "idempotencyKey": item["key"], "apiIndex": server, "try": attempt,
                      "at": now(), "httpStatus": status, "requestLatencyMs": elapsed, "type": item["body"]["type"],
                      "payloadBytes": len(item["body"]["text"].encode()), "priority": item["body"]["priority"]}
            if status in (200, 202):
                record["jobId"] = identifier(body["id"])
            with self.lock:
                self.responses.append(record)
                self.journal.write(json.dumps(record) + "\n")
                self.journal.flush()
            if status in (200, 202):
                with self.lock:
                    previous = self.ids.setdefault(item["index"], record["jobId"])
                    assert previous == record["jobId"], "An idempotency replay created another job"
                    self.latencies.append(elapsed)
                    self.logical_latencies.append((time.monotonic() - start) * 1000)
                return
            if status not in (0, 429, 502, 503, 504) or time.monotonic() > deadline:
                raise AssertionError("Workload submission failed with HTTP " + str(status))
            # Ambiguous responses always reuse the same key and body, including after failover.
            retry_after = next((value for key, value in headers.items() if key.lower() == "retry-after"), "0")
            time.sleep(min(60, max(.1, float(retry_after), min(1.0, .1 * (2 ** min(attempt, 4))))))
            attempt += 1

    def submit(self):
        self.started = time.monotonic()
        requests = list(self.items)
        requests += [self.items[i % len(self.items)] for i in range(self.duplicates)]
        try:
            with ThreadPoolExecutor(max_workers=self.parallelism) as pool:
                futures = [pool.submit(self.submit_one, item, i) for i, item in enumerate(requests)]
                last = time.monotonic()
                for future in as_completed(futures):
                    future.result()
                    if time.monotonic() - last >= 5:
                        print(self.name + ": admitted " + str(len(self.ids)) + "/" + str(len(self.items)), flush=True)
                        last = time.monotonic()
        finally:
            self.journal.close()
            with (self.directory / "requests.jsonl").open() as source:
                compressed_rows(self.directory / "requests.jsonl.gz", (json.loads(line) for line in source))
            (self.directory / "requests.jsonl").unlink()
        self.submitted = time.monotonic()

    def state(self):
        return self.stack.sql("SELECT json_build_object('total',count(*),'terminal',count(*) FILTER (WHERE status IN ('SUCCEEDED','FAILED','CANCELLED'))) FROM jobs WHERE owner_id='" + identifier(self.owner) + "'")

    def await_terminal(self, timeout=240):
        expected = len(self.items)
        def done():
            state = self.state()
            assert state["total"] == expected, "Accepted job conservation failed"
            return state["terminal"] == expected
        wait(done, "Accepted jobs did not all reach terminal state; inspect the isolated stack", timeout=timeout)
        self.finished = time.monotonic()

    def evidence(self, drain_started=None, allow_abandoned=False):
        owner = identifier(self.owner)
        jobs = self.stack.sql("""SELECT coalesce(json_agg(row_to_json(r)),'[]'::json) FROM (
            SELECT id,type,status,result,priority,scheduled_at,created_at,completed_at,eligible_at,attempt_count,
                extract(epoch FROM(completed_at-created_at))*1000 AS end_to_end_ms
            FROM jobs WHERE owner_id='""" + owner + "' ORDER BY id) r")
        attempts = self.stack.sql("""SELECT coalesce(json_agg(row_to_json(r)),'[]'::json) FROM (
            SELECT a.id,a.job_id,a.attempt_number,a.generation,a.worker_id,a.status,a.started_at,a.finished_at,
                a.queue_delay_seconds*1000 AS queue_delay_ms,
                extract(epoch FROM(a.finished_at-a.started_at))*1000 AS execution_ms
            FROM job_attempts a JOIN jobs j ON j.id=a.job_id WHERE j.owner_id='""" + owner + "' ORDER BY a.started_at,a.id) r")
        events = self.stack.sql("""SELECT coalesce(json_agg(row_to_json(r)),'[]'::json) FROM (
            SELECT e.job_id,e.event_type,count(*) AS count FROM job_events e JOIN jobs j ON j.id=e.job_id
            WHERE j.owner_id='""" + owner + "' GROUP BY e.job_id,e.event_type ORDER BY e.job_id,e.event_type) r")
        expected = {self.ids[item["index"]]: item for item in self.items}
        assert len(expected) == len(self.items) == len(jobs), "Accepted or duplicate job accounting mismatch"
        for job in jobs:
            item = expected[job["id"]]
            oracle = expected_result(item["body"])
            assert job["status"] == "SUCCEEDED" and job["result"] == oracle, "A job failed or produced an incorrect result"
            del job["result"]  # Synthetic content is unnecessary in the evidence ledger.
        per_job = Counter(attempt["job_id"] for attempt in attempts if attempt["status"] == "SUCCEEDED")
        assert set(per_job) == set(expected) and all(value == 1 for value in per_job.values()), "Duplicate or missing successful attempt"
        assert allow_abandoned or len(attempts) == len(jobs), "Unexpected repeated execution in the healthy workload"
        for event in events:
            if event["event_type"] in ("SUBMITTED", "SUCCEEDED"):
                assert event["count"] == 1, "Duplicate committed lifecycle transition"
        for kind in ("SUBMITTED", "SUCCEEDED"):
            assert {event["job_id"] for event in events if event["event_type"] == kind} == set(expected), "A lifecycle event is missing"
        compressed_rows(self.directory / "jobs.jsonl.gz", jobs)
        compressed_rows(self.directory / "attempts.jsonl.gz", attempts)
        compressed_rows(self.directory / "events.jsonl.gz", events)
        elapsed = self.finished - (drain_started if drain_started is not None else self.started)
        report = {"passed": True, "name": self.name, "ownerId": owner, "uniqueJobs": len(jobs),
                  "requestCount": len(self.responses), "duplicateRequests": self.duplicates,
                  "httpStatuses": dict(Counter(str(row["httpStatus"]) for row in self.responses)),
                  "apiRequests": dict(Counter(str(row["apiIndex"]) for row in self.responses)),
                  "attemptOutcomes": dict(Counter(row["status"] for row in attempts)),
                  "workerProcessesUsed": len({row["worker_id"] for row in attempts}),
                  "submissionSeconds": self.submitted - self.started, "admittedJobsPerSecond": len(jobs) / (self.submitted - self.started),
                  "completionMeasurement": "preloaded broker drain" if drain_started is not None else "submission through observed terminal state",
                  "completionSeconds": elapsed, "completedJobsPerSecond": len(jobs) / elapsed,
                  "successfulHttpRequestMs": distribution(self.latencies), "logicalSubmissionMs": distribution(self.logical_latencies),
                  "allHttpRequestMs": distribution(row["requestLatencyMs"] for row in self.responses),
                  "newJobRequestMs": distribution(row["requestLatencyMs"] for row in self.responses if row["httpStatus"] == 202),
                  "replayRequestMs": distribution(row["requestLatencyMs"] for row in self.responses if row["httpStatus"] == 200),
                  "transportErrors": sum(row["httpStatus"] == 0 for row in self.responses),
                  "httpErrorResponses": sum(row["httpStatus"] not in (0, 200, 202) for row in self.responses),
                  "jobEndToEndMs": distribution(row["end_to_end_ms"] for row in jobs),
                  "attemptQueueDelayMs": distribution(row["queue_delay_ms"] for row in attempts if row["queue_delay_ms"] is not None),
                  "attemptExecutionMs": distribution(row["execution_ms"] for row in attempts),
                  "payloadBytes": dict(Counter(str(len(item["body"]["text"].encode())) for item in self.items)),
                  "scheduledJobs": sum("scheduledAt" in item["body"] for item in self.items),
                  "accountedFor": len(jobs), "incorrectResults": 0, "duplicateSuccessfulAttempts": 0,
                  "pollIntervalSeconds": .25, "fixtureUsed": False}
        write_json(self.directory / "report.json", report)
        return report
