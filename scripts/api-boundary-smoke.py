"""Check real HTTP request boundaries, including chunked transfer, against a packaged API.

Requires an operator key. Retains one cancelled diagnostic job and its audited owner;
the temporary user key is revoked on exit. Never prints credentials or response bodies.
"""
import argparse
import http.client
import json
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit
from uuid import uuid4
from api_auth import api_key


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8080")
    args = parser.parse_args()
    base = urlsplit(args.base_url)
    if base.scheme not in ("http", "https") or not base.hostname or base.username or base.password or base.query or base.fragment:
        raise ValueError("Use an HTTP(S) base URL without credentials, query or fragment")
    operator = api_key()
    prefix = "boundary-" + uuid4().hex

    def request(path, body=None, token=operator, chunked=False, key=None, method="POST"):
        connection_type = http.client.HTTPSConnection if base.scheme == "https" else http.client.HTTPConnection
        connection = connection_type(base.hostname, base.port, timeout=15)
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = "Bearer " + token
        if key:
            headers["Idempotency-Key"] = key
        # An iterator forces real HTTP chunked transfer with no Content-Length.
        data = (body[i:i+4096] for i in range(0, len(body), 4096)) if chunked else body
        try:
            connection.request(method, base.path.rstrip("/") + path, body=data,
                               headers=headers, encode_chunked=chunked)
            response = connection.getresponse()
            raw = response.read()
            return response.status, json.loads(raw), response.getheader("Content-Type", "")
        finally:
            connection.close()

    def encoded(value):
        return json.dumps(value, ensure_ascii=True).encode("ascii")

    status, owner, _ = request("/api/v1/admin/owners", encoded({"name": prefix}))
    assert status == 201, "Cannot create diagnostic owner"
    status, issued, _ = request("/api/v1/admin/owners/"+owner["id"]+"/keys", encoded({"name": prefix}))
    assert status == 201, "Cannot issue diagnostic user key"
    user = issued["token"]
    job = None
    try:
        oversized = b" " * 131073
        for chunked in (False, True):
            status, problem, content_type = request("/api/v1/jobs", oversized, user, chunked)
            assert status == 413 and problem["status"] == 413 and content_type.startswith("application/problem+json"), \
                "Oversized body was not rejected as a problem response"
        assert request("/api/v1/jobs", oversized, None)[0] == 401, "Authentication must precede body buffering"
        for body in (b'{"type":"SHA256","text":"one","text":"two"}',
                     b'{"type":"SHA256","text":"one"} {}',
                     b'{"type":"SHA256","text":"\\ud800"}',
                     b'{"type":"SHA256","text":"\\udc00"}'):
            assert request("/api/v1/jobs", body, user)[0] == 400, "Invalid JSON reached submission"
        assert request("/api/v1/admin/owners", b'{"name":"\\ud800"}')[0] == 400
        status, before, _ = request("/api/v1/jobs", token=user, method="GET")
        assert status == 200 and before["items"] == [], "Rejected input persisted a job"
        # Worst-case JSON escaping of the maximum legal UTF-16 text still fits the byte cap.
        body = encoded({"type": "SHA256", "text": "é"*15998+"😀",
                        "scheduledAt": (datetime.now(timezone.utc)+timedelta(hours=1)).isoformat()})
        body += b" " * (131072-len(body))
        assert len(body) == 131072
        status, job, _ = request("/api/v1/jobs", body, user, key=prefix)
        assert status == 202 and job["status"] == "QUEUED", "Exact-limit Unicode body failed"
        status, replay, _ = request("/api/v1/jobs", body, user, chunked=True, key=prefix)
        assert status == 200 and replay["id"] == job["id"], "Chunked exact-limit replay failed"
        status, jobs, _ = request("/api/v1/jobs", token=user, method="GET")
        assert status == 200 and len(jobs["items"]) == 1, "Boundary workflow created unexpected jobs"
    finally:
        try:
            if job is not None and "id" in job:
                assert request("/api/v1/jobs/"+job["id"]+"/cancel", token=user)[0] == 200
        finally:
            assert request("/api/v1/admin/keys/"+issued["key"]["id"]+"/revoke")[0] == 200
    print("PASS: packaged byte limits, chunked transfer, strict JSON, Unicode, authorization and replay boundaries")


if __name__ == "__main__":
    main()
