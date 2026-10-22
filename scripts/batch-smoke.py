"""Verify packaged bulk admission, replay, per-item conflicts/quotas and real results.

Creates a diagnostic owner and sixteen jobs, retaining their durable history.
Revokes its temporary user key on exit and never prints credentials.
"""
import argparse
import hashlib
import json
import time
from datetime import datetime, timezone
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
    prefix = "batch-smoke-"+uuid4().hex

    def request(path, body=None, token=operator, method="POST"):
        call = Request(base+path, method=method, headers={"Authorization": "Bearer "+token,
                       "Content-Type": "application/json"}, data=None if body is None else json.dumps(body).encode())
        try:
            response = urlopen(call, timeout=20)
        except HTTPError as error:
            response = error
        with response:
            return response.status, json.load(response)

    status, owner = request("/api/v1/admin/owners", {"name": prefix, "submissionLimitPerMinute": 16})
    assert status == 201, "Cannot create diagnostic owner"
    status, issued = request("/api/v1/admin/owners/"+owner["id"]+"/keys", {"name": prefix})
    assert status == 201, "Cannot issue diagnostic user key"
    user = issued["token"]
    items = [{"idempotencyKey": prefix+"-"+str(i), "job": {"type": ("WORD_COUNT", "SHA256", "UPPERCASE")[i % 3],
              "text": "bulk submission check"}} for i in range(16)]
    try:
        second = datetime.now(timezone.utc).second
        if second >= 50:
            time.sleep(60-second+.1)
        status, result = request("/api/v1/jobs/batch", {"items": items}, user)
        assert status == 202 and result["created"] == 16 and result["unsuccessful"] == 0
        ids = [item["job"]["id"] for item in result["items"]]
        assert len(set(ids)) == 16 and all(item["job"]["ownerId"] == owner["id"] for item in result["items"])
        status, replay = request("/api/v1/jobs/batch", {"items": items}, user)
        assert status == 200 and replay["replayed"] == 16 and replay["created"] == 0
        assert [item["job"]["id"] for item in replay["items"]] == ids
        changed = {"idempotencyKey": items[0]["idempotencyKey"], "job": {"type": "WORD_COUNT", "text": "changed"}}
        new = {"idempotencyKey": prefix+"-limited", "job": {"type": "SHA256", "text": "limited"}}
        status, mixed = request("/api/v1/jobs/batch", {"items": [items[1], changed, new]}, user)
        assert status == 207 and [item["status"] for item in mixed["items"]] == [200, 409, 429]
        assert mixed["replayed"] == 1 and mixed["unsuccessful"] == 2
        assert 1 <= mixed["items"][2]["retryAfterSeconds"] <= 60
        assert request("/api/v1/jobs/batch", {"items": [new, new]}, user)[0] == 400
        deadline = time.monotonic()+30
        remaining = dict(zip(ids, items))
        while remaining and time.monotonic() < deadline:
            for job_id, item in list(remaining.items()):
                status, job = request("/api/v1/jobs/"+job_id, token=user, method="GET")
                assert status == 200 and job["status"] not in ("FAILED", "CANCELLED")
                if job["status"] == "SUCCEEDED":
                    expected = {"WORD_COUNT": {"wordCount": 3, "characterCount": 21},
                                "SHA256": {"sha256": hashlib.sha256(b"bulk submission check").hexdigest()},
                                "UPPERCASE": {"text": "BULK SUBMISSION CHECK"}}[item["job"]["type"]]
                    assert job["result"] == expected, "Incorrect batch job result"
                    del remaining[job_id]
            if remaining:
                time.sleep(.1)
        assert not remaining, "Not every acknowledged batch job completed"
        status, listing = request("/api/v1/jobs?size=100", token=user, method="GET")
        assert status == 200 and {job["id"] for job in listing["items"]} == set(ids)
    finally:
        assert request("/api/v1/admin/keys/"+issued["key"]["id"]+"/revoke")[0] == 200, "Temporary key revocation failed"
    print("PASS: 16/16 correct batch jobs, replay identity, independent quota/conflict outcomes, validation and key cleanup")


if __name__ == "__main__":
    main()
