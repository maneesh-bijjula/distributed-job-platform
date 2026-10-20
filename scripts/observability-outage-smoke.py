"""Exercise local broker/database visibility and restoration without deleting data volumes.

Requires the job-platform Compose services and observability profile to be running.
Stops only this project's RabbitMQ and PostgreSQL, one at a time, and restores
them in finally blocks. This is a small operational check, not a load benchmark.
"""
import argparse
import json
from pathlib import Path
import subprocess
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4
from api_auth import api_key


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("--prometheus-url", default="http://localhost:9090")
    parser.add_argument("--report")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    token = api_key()
    stopped = set()
    report = {}

    def request(base, path, method="GET", body=None, authenticated=False, key=None):
        headers = {"Content-Type": "application/json"}
        if authenticated:
            headers["Authorization"] = "Bearer " + token
        if key:
            headers["Idempotency-Key"] = key
        req = Request(base.rstrip("/") + path, method=method, headers=headers,
                      data=None if body is None else json.dumps(body).encode())
        try:
            response = urlopen(req, timeout=15)
        except HTTPError as error:
            response = error
        with response:
            return response.status, json.load(response)

    def api(path, **kwargs):
        return request(args.base_url, path, **kwargs)

    def value(expression):
        status, result = request(args.prometheus_url, "/api/v1/query?" + urlencode({"query": expression}))
        assert status == 200 and result["status"] == "success"
        values = result["data"]["result"]
        return float(values[0]["value"][1]) if values else None

    def wait(predicate, message, timeout=75):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                if predicate():
                    return
            except (URLError, TimeoutError):
                pass
            time.sleep(1)
        raise AssertionError(message)

    def compose(*parts):
        subprocess.run(["docker", "compose", "-p", "job-platform", *parts], cwd=root,
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True, timeout=150)

    def restore(service):
        compose("up", "-d", "--wait", "--wait-timeout", "120", service)
        stopped.discard(service)

    try:
        wait(lambda: value('up{job="job-platform"}') == 1, "Monitoring must be healthy before testing outages")
        print("Checking broker outage: stopping this project's RabbitMQ.", flush=True)
        stopped.add("rabbitmq"); compose("stop", "rabbitmq")
        assert api("/actuator/health/liveness")[0] == 200
        assert api("/actuator/health/readiness")[0] == 200
        run = "outage-" + str(uuid4())
        status, job = api("/api/v1/jobs", method="POST", body={"type": "UPPERCASE", "text": "outage visibility check"}, authenticated=True, key=run)
        assert status == 202, "Broker outage prevented durable admission"
        path = "/api/v1/jobs/" + job["id"]
        wait(lambda: value('job_platform_dependency_up{dependency="rabbitmq"}') == 0, "Broker outage metric was not observed")
        wait(lambda: value('job_platform_outbox_entries{state="due"}') >= 1, "Pending outbox work was not visible")
        wait(lambda: value('ALERTS{alertname="JobPlatformBrokerUnavailable",alertstate="firing"}') == 1, "Broker alert did not fire")
        report["broker"] = {"liveness": 200, "readiness": 200, "admission": 202, "outboxVisible": True, "alertFired": True}
        print("Broker outage visible; API accepted durable work and the alert fired. Restoring RabbitMQ.", flush=True)
        restore("rabbitmq")
        wait(lambda: api(path, authenticated=True)[1]["status"] == "SUCCEEDED", "Accepted work did not finish after broker restoration")
        wait(lambda: value('job_platform_dependency_up{dependency="rabbitmq"}') == 1, "Broker metric did not recover")

        print("Checking database outage: stopping this project's PostgreSQL.", flush=True)
        stopped.add("postgres"); compose("stop", "postgres")
        assert api("/actuator/health/liveness")[0] == 200
        assert api("/actuator/health/readiness")[0] == 503
        status, error = api("/api/v1/jobs", authenticated=True)
        assert status == 503 and token not in json.dumps(error), "Authentication did not fail closed"
        wait(lambda: value('up{job="job-platform"}') == 0, "Database outage was not visible as a failed authenticated scrape")
        wait(lambda: value('ALERTS{alertname="JobPlatformScrapeUnavailable",alertstate="firing"}') == 1, "Unavailable-scrape alert did not fire")
        report["postgres"] = {"liveness": 200, "readiness": 503, "authenticatedApi": 503, "scrapeUp": 0, "alertFired": True}
        print("Database outage visible; readiness failed, authentication failed closed, and the alert fired. Restoring PostgreSQL.", flush=True)
        restore("postgres")
        wait(lambda: api("/actuator/health/readiness")[0] == 200, "API readiness did not recover")
        wait(lambda: value('up{job="job-platform"}') == 1, "Authenticated scrape did not recover")
        wait(lambda: value("job_platform_snapshot_up") == 1, "Database metric snapshot did not recover")
        assert api(path, authenticated=True)[1]["status"] == "SUCCEEDED"
        report["restored"] = True
    finally:
        for service in list(stopped):
            restore(service)
    if args.report:
        Path(args.report).write_text(json.dumps(report, indent=2) + "\n")
    print("PASS: broker/database outages, real firing alerts, correct probe behavior, durable job recovery, and service restoration.")


if __name__ == "__main__":
    main()
