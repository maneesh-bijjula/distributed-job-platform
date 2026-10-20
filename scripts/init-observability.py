"""Provision a dedicated scrape key and local Grafana password without printing credentials."""
import argparse
import json
import os
from pathlib import Path
import secrets
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from api_auth import api_key


def private_write(path, value):
    if path.is_symlink():
        raise SystemExit("Refusing a symlink in the local observability secret directory")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        os.chmod(path, 0o600)
        handle.write(value)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8080")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    directory = root / ".observability"
    if directory.is_symlink():
        raise SystemExit("Refusing a symlink for the observability secret directory")
    directory.mkdir(mode=0o700, exist_ok=True)
    os.chmod(directory, 0o700)
    token_file = directory / "metrics-token"
    password_file = directory / "grafana-admin-password"

    def request(path, token, body=None):
        req = Request(args.base_url.rstrip("/") + path,
                      data=None if body is None else json.dumps(body).encode(),
                      headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
        try:
            response = urlopen(req, timeout=15)
        except HTTPError as error:
            response = error
        with response:
            return response.status, json.load(response)

    reuse = False
    if token_file.exists():
        if token_file.is_symlink():
            raise SystemExit("Refusing a symlink for the scrape credential")
        status, identity = request("/api/v1/auth/me", token_file.read_text().strip())
        if status == 200:
            if identity["identity"]["role"] != "METRICS":
                raise SystemExit("Scrape credential is not a METRICS key; refusing to mount an administrative key")
            os.chmod(token_file, 0o600)
            reuse = True
        elif status != 401:
            raise SystemExit("Cannot verify the existing scrape key; restore API/storage before retrying")
    if not reuse:
        operator = api_key()
        status, identity = request("/api/v1/auth/me", operator)
        if status != 200 or identity["identity"]["role"] != "OPERATOR":
            raise SystemExit("An operator key is required to provision monitoring")
        owner = identity["identity"]["ownerId"]
        status, issued = request("/api/v1/admin/owners/" + owner + "/keys", operator,
                                 {"name": "Local Prometheus", "role": "METRICS"})
        if status != 201:
            raise SystemExit("Cannot issue the dedicated metrics key")
        private_write(token_file, issued["token"])
        private_write(directory / "metrics-key.json", json.dumps({"keyId": issued["key"]["id"], "ownerId": owner}) + "\n")
    if not password_file.exists():
        private_write(password_file, secrets.token_urlsafe(32))
    elif password_file.is_symlink():
        raise SystemExit("Refusing a symlink for the Grafana password")
    else:
        os.chmod(password_file, 0o600)
    print("PASS: dedicated METRICS key and Grafana password are stored privately in ignored .observability/.")
    print("No credentials were printed. Re-running retains a valid key and the existing Grafana password.")


if __name__ == "__main__":
    main()
