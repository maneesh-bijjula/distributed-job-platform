"""Single-host deployment and offline recovery. Never operates on the development stack.

Credentials and database archives stay in ignored .deployment/, mode 0700.
Run --help for commands. Restore accepts an empty database only.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
from uuid import uuid4

ROOT = Path(__file__).resolve().parent.parent
PROJECTS = ("job-platform-release", "job-platform-release-test")
SERVICES = {"api", "worker", "postgres", "rabbitmq"}
LABEL = "com.docker.compose.project"
IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


def write_private(path, value, mode=0o600):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    with os.fdopen(fd, "w") as stream:
        # A restrictive caller umask must not make mounted secrets unreadable to the JVM.
        os.fchmod(stream.fileno(), mode)
        stream.write(value)


def image_name(value):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/@-]+", value) or (
            ":" not in value.rsplit("/", 1)[-1] and "@sha256:" not in value):
        raise ValueError("Use an explicit application image tag or digest")
    if value.endswith(":latest"):
        raise ValueError("Use a versioned image tag or digest instead of latest")
    return value


def sql_identifier(value):
    if not IDENTIFIER.fullmatch(value):
        raise ValueError("Unexpected database identifier")
    return '"' + value + '"'


class Deployment:
    def __init__(self, project="job-platform-release"):
        if project not in PROJECTS:
            raise ValueError("Unknown deployment project")
        self.project = project
        self.directory = ROOT / ".deployment" / project
        for path in (ROOT / ".deployment", self.directory):
            if path.is_symlink():
                raise ValueError("Refusing symlinked deployment directories")
        self.config = self.directory / "deploy.env"

    def initialize(self, image):
        image = image_name(image)
        self.directory.parent.mkdir(mode=0o700, exist_ok=True)
        os.chmod(self.directory.parent, 0o700)
        self.directory.mkdir(mode=0o700, exist_ok=False)
        secret_dir = self.directory / "secrets"
        secret_dir.mkdir(mode=0o700)
        database, broker = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
        bootstrap = "jpk_" + secrets.token_urlsafe(32)
        # Local Compose mounts retain host modes. Readable secret files inside 0700
        # directories allow the non-root JVM to read them without exposing host traversal.
        for name, value in (("database-password", database), ("broker-password", broker),
                            ("bootstrap-key", bootstrap)):
            write_private(secret_dir / name, value, 0o444)
        write_private(secret_dir / "rabbitmq.conf",
                      "default_user = jobs\ndefault_pass = " + broker +
                      "\ndefault_vhost = /\nmanagement.tcp.port = 15672\n", 0o444)
        port = 18088 if self.project.endswith("-test") else 8088
        write_private(self.config, f"APP_IMAGE={image}\nDEPLOY_API_PORT={port}\n")

    def settings(self):
        if self.config.is_symlink() or not self.config.is_file():
            raise ValueError("Initialize this deployment first; deploy.env must be a regular file")
        values = {}
        allowed = {"APP_IMAGE", "DEPLOY_API_PORT", "POSTGRES_IMAGE", "RABBITMQ_IMAGE", "JOB_DB_POOL_SIZE",
                   "JOB_WORKER_PREFETCH", "WORKER_CONCURRENCY", "JOB_DISPATCH_BATCH_SIZE", "JOB_DISPATCH_DELAY_MS",
                   "JOB_LEASE_SECONDS", "JOB_HEARTBEAT_MILLIS", "JOB_TIMEOUT_SECONDS"}
        for line in self.config.read_text().splitlines():
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            key, value = line.split("=", 1)
            if key not in allowed or key in values:
                raise ValueError("Unknown or duplicate deployment setting")
            values[key] = value
        if not {"APP_IMAGE", "DEPLOY_API_PORT"} <= values.keys():
            raise ValueError("Deployment settings require APP_IMAGE and DEPLOY_API_PORT")
        image_name(values["APP_IMAGE"])
        for key in ("POSTGRES_IMAGE", "RABBITMQ_IMAGE"):
            if key in values:
                image_name(values[key])
        port = int(values["DEPLOY_API_PORT"])
        if not 1024 <= port <= 65535:
            raise ValueError("Deployment port must be between 1024 and 65535")
        for key, value in values.items():
            if key not in {"APP_IMAGE", "POSTGRES_IMAGE", "RABBITMQ_IMAGE"} and not value.isdigit():
                raise ValueError("Deployment numeric setting is invalid")
        for name in ("database-password", "broker-password", "bootstrap-key", "rabbitmq.conf"):
            path = self.directory / "secrets" / name
            if path.is_symlink() or not path.is_file():
                raise ValueError("Missing or symlinked deployment secret")
        if (self.directory / "secrets").is_symlink():
            raise ValueError("Refusing symlinked secret directory")
        for path in (self.directory.parent, self.directory, self.directory / "secrets", self.config):
            if path.stat().st_mode & 0o077:
                raise ValueError("Deployment directories and deploy.env must be private to their owner")
        return values

    @contextmanager
    def lock(self):
        self.settings()
        path = self.directory / "operator.lock"
        if path.is_symlink():
            raise ValueError("Refusing symlinked operator lock")
        with path.open("a") as stream:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield

    def run(self, arguments, *, data=None, input_stream=None, output=None, timeout=240):
        values = self.settings()
        environment = dict(os.environ)
        # Do not let inherited shell variables silently change the selected profile.
        for key in ("POSTGRES_IMAGE", "RABBITMQ_IMAGE", "JOB_DB_POOL_SIZE", "JOB_WORKER_PREFETCH",
                    "WORKER_CONCURRENCY", "JOB_DISPATCH_BATCH_SIZE", "JOB_DISPATCH_DELAY_MS",
                    "JOB_LEASE_SECONDS", "JOB_HEARTBEAT_MILLIS", "JOB_TIMEOUT_SECONDS"):
            environment.pop(key, None)
        environment.update(values)
        environment["DEPLOY_SECRET_DIR"] = str(self.directory / "secrets")
        command = ["docker", "compose", "-p", self.project, "--env-file", str(self.config),
                   "-f", str(ROOT / "compose.deploy.yaml"), *arguments]
        result = subprocess.run(command, cwd=ROOT, env=environment, input=data, stdin=input_stream,
                                stdout=output if output is not None else subprocess.PIPE,
                                stderr=subprocess.PIPE, timeout=timeout)
        if result.returncode:
            # pg_restore diagnostics can contain row data. Keep them private, never in CI logs.
            path = self.directory / "last-error.txt"
            if path.is_symlink():
                raise ValueError("Refusing symlinked error file")
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "wb") as stream:
                os.fchmod(stream.fileno(), 0o600)
                stream.write(result.stderr)
                if output is None:
                    stream.write(result.stdout)
            raise RuntimeError("Deployment command failed; inspect private .deployment/<project>/last-error.txt")
        return result.stdout.decode().strip() if output is None else None

    def containers(self):
        raw = self.run(["ps", "-a", "--format", "json"])
        if not raw:
            return []
        # Compose v2 emits JSON Lines; older versions can return a JSON array.
        rows = json.loads(raw) if raw.startswith("[") else [json.loads(line) for line in raw.splitlines()]
        return rows

    def guard(self, offline=False):
        for row in self.containers():
            if row.get("Project") != self.project or row.get("Service") not in SERVICES:
                raise ValueError("Unexpected resource in deployment project")
            if offline and row["Service"] in {"api", "worker"} and row["State"] not in {"exited", "dead", "created"}:
                raise ValueError("Stop all API and worker containers before offline operations")

    def sql(self, query, database="jobs"):
        sql_identifier(database)
        return self.run(["exec", "-T", "postgres", "psql", "-X", "-U", "jobs", "-d", database,
                         "-v", "ON_ERROR_STOP=1", "-At", "-c", query])

    def fingerprint(self, database="jobs"):
        tables = json.loads(self.sql("SELECT coalesce(json_agg(tablename ORDER BY tablename),'[]') "
                                     "FROM pg_tables WHERE schemaname='public'", database))
        result = {"tables": {}, "sequences": {}}
        for table in tables:
            quoted = sql_identifier(table)
            result["tables"][table] = json.loads(self.sql(
                "SELECT json_build_object('rows',count(*),'md5',md5(coalesce(string_agg(h,'' ORDER BY h),''))) "
                f"FROM (SELECT md5(row_to_json(t)::text) h FROM public.{quoted} t) s", database))
        sequences = json.loads(self.sql("SELECT coalesce(json_agg(sequencename ORDER BY sequencename),'[]') "
                                        "FROM pg_sequences WHERE schemaname='public'", database))
        for sequence in sequences:
            result["sequences"][sequence] = json.loads(self.sql(
                f"SELECT json_build_object('lastValue',last_value,'called',is_called) FROM public.{sql_identifier(sequence)}",
                database))
        return result

    def backup(self):
        self.guard(offline=True)
        directory = self.directory / "backups"
        if directory.is_symlink():
            raise ValueError("Refusing symlinked backup directory")
        directory.mkdir(mode=0o700, exist_ok=True)
        if directory.stat().st_mode & 0o077:
            raise ValueError("Backup directory must be private to its owner")
        destination = directory / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + uuid4().hex[:8])
        destination.mkdir(mode=0o700)
        before = self.fingerprint()
        archive = destination / "database.dump"
        with os.fdopen(os.open(archive, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as stream:
            self.run(["exec", "-T", "postgres", "pg_dump", "-U", "jobs", "-d", "jobs", "-Fc",
                      "--no-owner", "--no-privileges"], output=stream)
        if self.fingerprint() != before:
            raise ValueError("Database changed during offline backup; this backup is incomplete")
        with archive.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        manifest = {"format": 1, "createdAt": datetime.now(timezone.utc).isoformat(), "project": self.project,
                    "applicationImage": self.settings()["APP_IMAGE"], "fingerprint": before,
                    "sha256": digest, "containerImageIds": {
                        row["Service"]: subprocess.check_output(
                            ["docker", "inspect", "--format", "{{.Image}}", row["ID"]], text=True).strip()
                        for row in self.containers()}}
        write_private(destination / "manifest.json", json.dumps(manifest, indent=2) + "\n")
        return destination

    def checked_archive(self, directory):
        directory = Path(directory).resolve()
        archive = directory / "database.dump"
        manifest_path = directory / "manifest.json"
        if any(path.is_symlink() or not path.is_file() for path in (archive, manifest_path)):
            raise ValueError("Backup archive and manifest must be regular files, not symlinks")
        manifest = json.loads(manifest_path.read_text())
        # Validate the verification target before pg_restore can mutate the empty database.
        # A matching archive checksum alone cannot make an incomplete manifest restorable.
        if not isinstance(manifest, dict) or type(manifest.get("format")) is not int or manifest["format"] != 1:
            raise ValueError("Unsupported backup manifest format")
        expected = manifest.get("fingerprint")
        if not isinstance(expected, dict) or set(expected) != {"tables", "sequences"}:
            raise ValueError("Backup manifest requires table and sequence fingerprints")
        for kind in ("tables", "sequences"):
            entries = expected[kind]
            if not isinstance(entries, dict):
                raise ValueError("Invalid backup fingerprints")
            for name, value in entries.items():
                sql_identifier(name)
                if not isinstance(value, dict):
                    raise ValueError("Invalid backup fingerprints")
                if kind == "tables":
                    valid = (set(value) == {"rows", "md5"} and type(value["rows"]) is int
                             and value["rows"] >= 0 and isinstance(value["md5"], str)
                             and re.fullmatch(r"[0-9a-f]{32}", value["md5"]))
                else:
                    valid = (set(value) == {"lastValue", "called"} and type(value["lastValue"]) is int
                             and type(value["called"]) is bool)
                if not valid:
                    raise ValueError("Invalid backup fingerprints")
        if not isinstance(manifest.get("sha256"), str) or not re.fullmatch(r"[0-9a-f]{64}", manifest["sha256"]):
            raise ValueError("Backup manifest requires a SHA-256 digest")
        with archive.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        if digest != manifest["sha256"]:
            raise ValueError("Backup integrity check failed")
        return archive, manifest

    def restore(self, directory, database="jobs"):
        self.guard(offline=True)
        archive, manifest = self.checked_archive(directory)
        # Never clean or overwrite an existing database. Restore into an empty replacement.
        empty = self.sql("SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                         "WHERE n.nspname NOT IN ('pg_catalog','information_schema') "
                         "AND n.nspname NOT LIKE 'pg_toast%' AND c.relkind IN ('r','p','S','v','m','f')", database)
        if empty != "0":
            raise ValueError("Restore target must be empty; existing application data will not be overwritten")
        # docker compose exec streams stdin without a shell or temporary archive in the container.
        with archive.open("rb") as stream:
            self.run(["exec", "-T", "postgres", "pg_restore", "-U", "jobs", "-d", database,
                      "--single-transaction", "--exit-on-error", "--no-owner", "--no-privileges"], input_stream=stream)
        actual = self.fingerprint(database)
        if actual != manifest["fingerprint"]:
            raise ValueError("Restored table/sequence fingerprints differ; keep applications stopped")
        return actual

    def verify_backup(self, directory):
        self.guard(offline=True)
        # Fixed scratch name, CREATE must succeed before finally may drop it.
        database = "jobs_restore_verification"
        self.sql(f"CREATE DATABASE {sql_identifier(database)}")
        try:
            return self.restore(directory, database)
        finally:
            self.sql(f"DROP DATABASE {sql_identifier(database)}")

    def reconcile_outbox(self):
        self.guard(offline=True)
        # No job/attempt state is changed. Existing messages may duplicate these deliveries;
        # worker ownership guards already support this at-least-once delivery contract.
        result = self.sql("""WITH recovered AS (
            INSERT INTO job_outbox(job_id,priority,available_at,generation)
            SELECT id,priority,GREATEST(clock_timestamp(),COALESCE(scheduled_at,created_at),retry_at),generation
            FROM jobs WHERE status='QUEUED' ON CONFLICT(job_id) DO NOTHING RETURNING job_id)
            SELECT json_build_object('recreatedOutboxEntries',count(*)) FROM recovered""")
        report = {"recordedAt": datetime.now(timezone.utc).isoformat(), "project": self.project,
                  "operation": "offline-queued-outbox-reconciliation", **json.loads(result)}
        write_private(self.directory / ("reconcile-" + uuid4().hex + ".json"), json.dumps(report, indent=2) + "\n")
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", choices=PROJECTS, default=PROJECTS[0])
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("init").add_argument("--image", required=True)
    for name in ("up", "infrastructure", "stop", "status", "backup", "reconcile-outbox"):
        sub.add_parser(name)
    for name in ("verify-backup", "restore"):
        sub.add_parser(name).add_argument("directory")
    args = parser.parse_args()
    deployment = Deployment(args.project)
    if args.action == "init":
        deployment.initialize(args.image)
        print("Initialized private deployment configuration; no credentials displayed.")
        return
    with deployment.lock():
        deployment.guard()
        if args.action in {"up", "infrastructure"}:
            services = ["postgres", "rabbitmq"] if args.action == "infrastructure" else ["api", "worker"]
            deployment.run(["up", "-d", "--wait", "--wait-timeout", "180", *services])
            print("Deployment services started; verify a submitted job before opening traffic.")
        elif args.action == "stop":
            deployment.run(["stop", "api", "worker"])
            deployment.guard(offline=True)
            print("API and workers stopped; database and broker retained.")
        elif args.action == "status":
            print(json.dumps([{key: row.get(key) for key in ("Service", "State", "Health", "Image")}
                              for row in deployment.containers()], indent=2))
        elif args.action == "backup":
            print("Backup created: " + str(deployment.backup()))
        elif args.action == "verify-backup":
            fingerprint = deployment.verify_backup(args.directory)
            print(f"PASS: restored {len(fingerprint['tables'])} tables and all sequences into a disposable database.")
        elif args.action == "restore":
            deployment.restore(args.directory)
            print("PASS: restored empty replacement database; keep traffic closed and reconcile outbox before starting.")
        elif args.action == "reconcile-outbox":
            print(json.dumps(deployment.reconcile_outbox(), indent=2))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, RuntimeError, OSError, subprocess.TimeoutExpired) as error:
        print("ERROR: " + str(error), file=sys.stderr)
        sys.exit(1)
