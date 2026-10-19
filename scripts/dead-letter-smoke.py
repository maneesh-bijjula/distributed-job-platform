"""Exercise packaged policy validation and explicit dead-letter operations.

Publishes two diagnostic messages to the local RabbitMQ dead exchange; creates
a scheduled job, redrives it without bypassing its schedule, cancels it, and
recovers it as a linked job. Leaves jobs and resolved inbox records inspectable.
"""
import argparse
import base64
import hashlib
import json
import os
import time
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("--management-url", default="http://localhost:15672")
    args = parser.parse_args()
    auth = base64.b64encode((os.getenv("RABBITMQ_USER", "jobs") + ":" +
                            os.getenv("RABBITMQ_PASSWORD", "jobs-local")).encode()).decode()

    def request(path, method="GET", body=None, key=None, management=False):
        headers = {}
        if body is not None:
            headers["Content-Type"] = "application/json"
        if key:
            headers["Idempotency-Key"] = key
        if management:
            headers["Authorization"] = "Basic " + auth
        base = args.management_url if management else args.base_url
        req = Request(base.rstrip("/") + path, method=method, headers=headers,
                      data=None if body is None else json.dumps(body).encode())
        try:
            response = urlopen(req, timeout=15)
        except HTTPError as error:
            response = error
        with response:
            return response.status, json.load(response)

    run = "dead-letter-smoke-" + str(uuid4())
    policy = {"maxAttempts": 3, "initialBackoffSeconds": 1, "maxBackoffSeconds": 5}
    body = {"type": "UPPERCASE", "text": "phase two packaged check", "retryPolicy": policy,
            "scheduledAt": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()}
    status, source = request("/api/v1/jobs", "POST", body, run)
    assert status == 202 and source["retryPolicy"] == policy, source
    assert request("/api/v1/jobs", "POST", body, run)[0] == 200
    assert request("/api/v1/jobs", "POST", {**body, "retryPolicy": {**policy, "maxAttempts": 2}}, run)[0] == 409

    malformed = "diagnostic malformed message " + run
    for payload in (source["id"], malformed):
        status, published = request("/api/exchanges/%2F/jobs.dead.exchange/publish", "POST", {
            "properties": {"delivery_mode": 2, "headers": {"job-generation": 0}},
            "routing_key": "dead", "payload": payload, "payload_encoding": "string",
        }, management=True)
        assert status == 200 and published["routed"], published

    found = {}
    target_hash = hashlib.sha256(malformed.encode()).hexdigest()
    # Bounded collection; retain unrelated records without resolving them.
    for _ in range(5):
        status, batch = request("/api/v1/dead-letters/collect?limit=20", "POST")
        assert status == 200, batch
        for entry_id in batch["entryIds"]:
            status, entry = request("/api/v1/dead-letters/" + entry_id)
            assert status == 200, entry
            if entry["jobId"] == source["id"]:
                found["source"] = entry
            if entry["payloadSha256"] == target_hash:
                found["malformed"] = entry
        if len(found) == 2:
            break
        time.sleep(0.1)
    assert len(found) == 2, found

    bad_path = "/api/v1/dead-letters/" + found["malformed"]["id"]
    assert request(bad_path + "/recover", "POST", key=run + "-bad-recovery")[0] == 409
    status, discarded = request(bad_path + "/discard", "POST", key=run + "-discard")
    assert status == 200 and discarded["state"] == "DISCARDED", discarded
    assert request(bad_path + "/discard", "POST", key=run + "-discard")[1] == discarded

    entry_path = "/api/v1/dead-letters/" + found["source"]["id"]
    status, recovered = request(entry_path + "/recover", "POST", key=run + "-redrive")
    assert status == 202 and recovered["recoveryJobId"] == source["id"], recovered
    assert request(entry_path + "/recover", "POST", key=run + "-redrive")[0] == 200
    path = "/api/v1/jobs/" + source["id"]
    status, queued = request(path)
    assert status == 200 and queued["status"] == "QUEUED" and queued["attemptCount"] == 0, queued
    assert queued["scheduledAt"] == source["scheduledAt"], queued
    assert request(path + "/cancel", "POST")[0] == 200
    # A new linked manual retry inherits the policy and starts a fresh execution budget.
    status, child = request(path + "/retry", "POST", key=run + "-manual")
    assert status == 202 and child["retryPolicy"] == policy, child
    child_path = "/api/v1/jobs/" + child["id"]
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        status, done = request(child_path)
        assert status == 200, done
        if done["status"] == "SUCCEEDED":
            break
        assert done["status"] in ("QUEUED", "RUNNING"), done
        time.sleep(0.1)
    else:
        raise AssertionError("Worker did not finish the linked retry")
    assert done["result"]["text"] == "PHASE TWO PACKAGED CHECK", done
    assert done["attemptCount"] == 1 and done["retryAt"] is None, done
    status, events = request(path + "/events")
    assert status == 200, events
    assert [event["type"] for event in events["items"]] == ["SUBMITTED", "DEAD_LETTER_RECOVERED", "CANCELLED", "RETRY_REQUESTED"], events
    print("PASS: policy validation/replay, bounded broker capture, malformed rejection, audited discard,")
    print("      idempotent queued redrive, preserved scheduling, and policy-inheriting linked retry.")
    print("Source job: " + source["id"])
    print("Successful retry: " + child["id"])


if __name__ == "__main__":
    main()
