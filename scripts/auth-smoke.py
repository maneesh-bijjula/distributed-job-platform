"""Verify packaged owner isolation, operator authorization, revocation and shared quotas.

Creates two diagnostic owners and three temporary keys, revokes those keys on
exit, and retains jobs and audit metadata for inspection. Prints no credentials.
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
    operator = api_key()
    run = "auth-smoke-" + str(uuid4())
    issued_ids = []

    def request(path, method="GET", body=None, token=operator, key=None):
        headers = {}
        if token:
            headers["Authorization"] = "Bearer " + token
        if key:
            headers["Idempotency-Key"] = key
        if body is not None:
            headers["Content-Type"] = "application/json"
        call = Request(base + path, method=method, headers=headers,
                       data=None if body is None else json.dumps(body).encode())
        try:
            response = urlopen(call, timeout=15)
        except HTTPError as error:
            response = error
        with response:
            return response.status, json.load(response), response.headers

    def owner(name, limit):
        status, result, _ = request("/api/v1/admin/owners", "POST", {
            "name": name + " " + run, "submissionLimitPerMinute": limit})
        assert status == 201, "Owner creation failed"
        return result["id"]

    def issue(owner_id):
        status, result, headers = request("/api/v1/admin/owners/" + owner_id + "/keys", "POST", {"name": "Diagnostic client"})
        assert status == 201 and "no-store" in headers["Cache-Control"], "Key issuance failed"
        issued_ids.append(result["key"]["id"])
        return result["token"]

    assert request("/actuator/health", token=None)[0] == 200
    assert request("/api/v1/jobs", token=None)[0] == 401
    assert request("/api/v1/jobs?api_key=unsupported-auth-method", token=None)[0] == 401
    try:
        alice_id, bob_id = owner("Alice", 2), owner("Bob", 3)
        alice, alice_second, bob = issue(alice_id), issue(alice_id), issue(bob_id)
        # Keep the fixed-window quota assertion away from the UTC minute boundary.
        second = datetime.now(timezone.utc).second
        if second >= 50:
            time.sleep(60 - second + 0.1)
        body = {"type": "UPPERCASE", "text": "isolated owner work",
                "scheduledAt": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()}
        status, source, _ = request("/api/v1/jobs", "POST", body, alice, run)
        assert status == 202 and source["ownerId"] == alice_id
        path = "/api/v1/jobs/" + source["id"]
        assert request("/api/v1/jobs", "POST", body, alice_second, run)[0] == 200
        status, bob_job, _ = request("/api/v1/jobs", "POST", body, bob, run)
        assert status == 202 and bob_job["id"] != source["id"] and bob_job["ownerId"] == bob_id
        for suffix in ("", "/events", "/attempts"):
            assert request(path + suffix, token=alice)[0] == 200
            assert request(path + suffix, token=bob)[0] == 404
            assert request(path + suffix, token=operator)[0] == 404
        for action in ("cancel", "retry"):
            assert request(path + "/" + action, "POST", token=bob, key=run + "-cross")[0] == 404
        for endpoint in ("/api/v1/admin/owners", "/api/v1/dead-letters", "/actuator/prometheus"):
            assert request(endpoint, token=alice)[0] == 403
        assert request("/api/v1/dead-letters/collect", "POST", token=alice)[0] == 403
        listed = request("/api/v1/jobs", token=bob)[1]["items"]
        assert listed and all(job["ownerId"] == bob_id for job in listed)
        assert request("/api/v1/jobs", "POST", body, alice_second, run + "-second")[0] == 202
        status, _, headers = request("/api/v1/jobs", "POST", body, alice, run + "-limited")
        assert status == 429 and 1 <= int(headers["Retry-After"]) <= 60
        assert request("/api/v1/jobs", "POST", body, alice, run)[0] == 200
        bob_path = "/api/v1/jobs/" + bob_job["id"]
        assert request(bob_path + "/cancel", "POST", token=bob)[0] == 200
        status, child, _ = request(bob_path + "/retry", "POST", token=bob, key=run + "-retry")
        assert status == 202 and child["ownerId"] == bob_id
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            status, done, _ = request("/api/v1/jobs/" + child["id"], token=bob)
            assert status == 200
            if done["status"] == "SUCCEEDED":
                break
            assert done["status"] in ("QUEUED", "RUNNING")
            time.sleep(0.1)
        else:
            raise AssertionError("Owner's linked retry did not finish")
        assert done["result"]["text"] == "ISOLATED OWNER WORK"
        assert request("/api/v1/admin/keys/" + issued_ids[0] + "/revoke", "POST")[0] == 200
        assert request(path, token=alice)[0] == 401
        assert request(path, token=alice_second)[0] == 200
        metadata = json.dumps(request("/api/v1/admin/owners/" + alice_id + "/keys")[1])
        assert alice not in metadata and alice_second not in metadata and "token_hash" not in metadata
        print("PASS: authenticated owner isolation on every job endpoint, owner-scoped idempotency,")
        print("      operator restrictions, shared owner quota, linked retry ownership, and immediate revocation.")
    finally:
        for key_id in issued_ids:
            assert request("/api/v1/admin/keys/" + key_id + "/revoke", "POST")[0] == 200


if __name__ == "__main__":
    main()
