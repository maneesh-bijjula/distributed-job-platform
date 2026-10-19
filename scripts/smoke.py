"""Check the packaged HTTP, PostgreSQL, RabbitMQ, and worker workflow.

Creates a scheduled job, cancels it, retries it, and verifies the result and
history. The two generated jobs remain available for inspection afterward.
Uses only the Python standard library.
"""

import argparse
import json
import time
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4
from api_auth import api_key


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8080")
    args = parser.parse_args()
    base = args.base_url.rstrip("/")
    token = api_key()

    def request(path, method="GET", body=None, key=None):
        headers = {"Authorization": "Bearer " + token}
        if body is not None:
            headers["Content-Type"] = "application/json"
        if key:
            headers["Idempotency-Key"] = key
        call = Request(base + path, method=method, headers=headers,
                       data=json.dumps(body).encode() if body is not None else None)
        try:
            response = urlopen(call, timeout=10)
        except HTTPError as error:
            response = error
        with response:
            return response.status, json.load(response)

    key = "smoke-" + str(uuid4())
    status, source = request("/api/v1/jobs", "POST", {
        "type": "UPPERCASE", "text": "next features smoke check", "priority": 8,
        "scheduledAt": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
    }, key)
    assert status == 202 and source["status"] == "QUEUED", source
    path = "/api/v1/jobs/" + source["id"]
    for _ in range(2):
        status, cancelled = request(path + "/cancel", "POST")
        assert status == 200 and cancelled["status"] == "CANCELLED", cancelled
    status, retried = request(path + "/retry", "POST", key=key + "-retry")
    assert status == 202 and retried["retryOf"] == source["id"], retried
    assert retried["priority"] == 8 and retried["scheduledAt"] is None, retried
    status, replay = request(path + "/retry", "POST", key=key + "-retry")
    assert status == 200 and replay["id"] == retried["id"], replay
    child = "/api/v1/jobs/" + retried["id"]
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        status, job = request(child)
        assert status == 200, job
        if job["status"] == "SUCCEEDED":
            break
        assert job["status"] in ("QUEUED", "RUNNING"), job
        time.sleep(0.2)
    else:
        raise AssertionError("Worker did not finish within 20 seconds")
    assert job["result"]["text"] == "NEXT FEATURES SMOKE CHECK", job
    status, events = request(path + "/events?size=2")
    assert status == 200 and events["hasNext"], events
    assert [event["type"] for event in events["items"]] == ["SUBMITTED", "CANCELLED"], events
    status, events = request(path + "/events?page=1&size=2")
    assert status == 200 and not events["hasNext"], events
    assert events["items"][0]["type"] == "RETRY_REQUESTED", events
    assert events["items"][0]["relatedJobId"] == retried["id"], events
    status, events = request(child + "/events")
    assert status == 200 and [event["type"] for event in events["items"]] == ["SUBMITTED", "STARTED", "SUCCEEDED"], events
    status, attempts = request(child + "/attempts")
    assert status == 200 and not attempts["hasNext"], attempts
    assert len(attempts["items"]) == 1 and attempts["items"][0]["status"] == "SUCCEEDED", attempts
    assert attempts["items"][0]["number"] == 1, attempts
    assert "claimToken" not in attempts["items"][0], attempts
    assert job["attemptCount"] == 1 and job["activeAttemptId"] is None, job
    assert job["deadlineAt"] is not None, job
    assert request(child + "/cancel", "POST")[0] == 409
    assert request(path + "/retry", "POST")[0] == 400
    assert request(path + "/retry", "POST", key=key)[0] == 409
    assert request(path)[1]["status"] == "CANCELLED"
    print("PASS: packaged cancellation, idempotent retry, worker result, persisted attempt, history pagination, and conflict checks.")
    print("Original job: " + source["id"])
    print("Successful retry: " + retried["id"])


if __name__ == "__main__":
    main()
