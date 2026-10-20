"""Verify live metrics, Prometheus scrapes, provisioned Grafana queries and scrape-key isolation."""
import argparse
import base64
import json
from pathlib import Path
import time
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4
from api_auth import api_key


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("--prometheus-url", default="http://localhost:9090")
    parser.add_argument("--grafana-url", default="http://localhost:3001")
    parser.add_argument("--report")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    operator = api_key()
    scrape = (root / ".observability/metrics-token").read_text().strip()
    password = (root / ".observability/grafana-admin-password").read_text().strip()
    grafana_auth = "Basic " + base64.b64encode(("admin:" + password).encode()).decode()

    def request(base, path, method="GET", body=None, auth=None, key=None, raw=False):
        headers = {}
        if auth:
            headers["Authorization"] = auth
        if key:
            headers["Idempotency-Key"] = key
        if body is not None:
            headers["Content-Type"] = "application/json"
        req = Request(base.rstrip("/") + path, method=method, headers=headers,
                      data=None if body is None else json.dumps(body).encode())
        try:
            response = urlopen(req, timeout=15)
        except HTTPError as error:
            response = error
        with response:
            data = response.read().decode()
            return response.status, data if raw else json.loads(data)

    def api(path, method="GET", body=None, key=None, token=operator):
        return request(args.base_url, path, method, body, "Bearer " + token if token else None, key)

    def query(expression):
        status, result = request(args.prometheus_url, "/api/v1/query?" + urlencode({"query": expression}))
        assert status == 200 and result["status"] == "success", "Prometheus query failed"
        return result["data"]["result"]

    def scalar(expression):
        values = query(expression)
        return float(values[0]["value"][1]) if values else None

    def wait(predicate, message, timeout=40):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                if predicate():
                    return
            except (URLError, TimeoutError):
                pass
            time.sleep(0.5)
        raise AssertionError(message)

    wait(lambda: scalar('up{job="job-platform"}') == 1, "Prometheus did not authenticate and scrape the API")
    wait(lambda: scalar("job_platform_snapshot_up") == 1, "Database metric snapshot is unavailable")
    expected = {"jobs_accepted_total": 3, "jobs_started_total": 2, "jobs_succeeded_total": 2,
                "jobs_cancelled_total": 1, 'jobs_retried_total{mode="manual"}': 1,
                "execution_duration_seconds_count": 2, "queue_delay_seconds_count": 2}
    baseline = {metric: scalar("sum(job_platform_" + metric + ")") for metric in expected}
    before = baseline["jobs_accepted_total"]
    run = "metrics-smoke-" + str(uuid4())
    marker = "private-payload-" + str(uuid4())
    status, first = api("/api/v1/jobs", "POST", {"type": "UPPERCASE", "text": marker}, run)
    assert status == 202
    path = "/api/v1/jobs/" + first["id"]
    wait(lambda: api(path)[1]["status"] == "SUCCEEDED", "Diagnostic job did not finish")
    body = {"type": "UPPERCASE", "text": "delayed diagnostic",
            "scheduledAt": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()}
    status, future = api("/api/v1/jobs", "POST", body, run + "-future")
    assert status == 202
    future_path = "/api/v1/jobs/" + future["id"]
    assert api(future_path + "/cancel", "POST")[0] == 200
    status, child = api(future_path + "/retry", "POST", key=run + "-retry")
    assert status == 202
    wait(lambda: api("/api/v1/jobs/" + child["id"])[1]["status"] == "SUCCEEDED", "Linked diagnostic retry did not finish")
    for metric, delta in expected.items():
        wait(lambda: scalar("sum(job_platform_" + metric + ")") >= baseline[metric] + delta,
             "Metric did not reflect the committed workflow: " + metric)
    assert api("/api/v1/jobs", token=scrape)[0] == 403
    assert api("/api/v1/admin/owners", token=scrape)[0] == 403
    assert api("/actuator/health/liveness", token=None)[0] == 200
    assert api("/actuator/health/readiness", token=None)[0] == 200
    status, exported = request(args.base_url, "/actuator/prometheus", auth="Bearer " + scrape, raw=True)
    assert status == 200
    assert all(secret not in exported for secret in (operator, scrape, password, marker, first["id"]))
    status, config = request(args.prometheus_url, "/api/v1/status/config")
    assert status == 200 and scrape not in json.dumps(config)
    status, rules = request(args.prometheus_url, "/api/v1/rules")
    assert status == 200 and any(group["name"] == "job-platform" for group in rules["data"]["groups"])
    status, dashboard = request(args.grafana_url, "/api/dashboards/uid/job-platform", auth=grafana_auth)
    assert status == 200 and dashboard["meta"]["provisioned"], "Grafana dashboard was not provisioned"
    for panel in dashboard["dashboard"]["panels"]:
        for target in panel.get("targets", []):
            query(target["expr"])  # Execute each dashboard expression against the actual service data.
    now = int(time.time() * 1000)
    status, result = request(args.grafana_url, "/api/ds/query", "POST", {
        "from": str(now - 300000), "to": str(now),
        "queries": [{"refId": "A", "datasource": {"uid": "job-prometheus", "type": "prometheus"},
                     "expr": "sum(job_platform_jobs_accepted_total)", "instant": True, "range": False,
                     "intervalMs": 5000, "maxDataPoints": 60}],
    }, grafana_auth)
    assert status == 200, "Grafana datasource query failed"
    frames = result["results"]["A"].get("frames", [])
    assert any(frame.get("data", {}).get("values") and frame["data"]["values"][-1]
               and float(frame["data"]["values"][-1][-1]) >= before + 3 for frame in frames), "Grafana did not return actual job metrics"
    report = {"passed": True, "acceptedBefore": before, "acceptedAfter": scalar("sum(job_platform_jobs_accepted_total)"),
              "dashboardUid": "job-platform", "panelsVerified": len(dashboard["dashboard"]["panels"]),
              "rulesLoaded": True, "scrapeKeyCannotAccessJobsOrAdmin": True,
              "workflowMetricDeltasVerified": expected}
    if args.report:
        Path(args.report).write_text(json.dumps(report, indent=2) + "\n")
    print("PASS: committed lifecycle metrics and timings, authenticated Prometheus scrape, bounded labels,")
    print("      provisioned Grafana dashboard queries, live datasource values, and metrics-key isolation.")
    print("Dashboard panels verified: " + str(report["panelsVerified"]))


if __name__ == "__main__":
    main()
