"""Profile the same production workload on the guarded disposable load stack.

Adds pg_stat_statements and sampled active-session waits. Diagnostic overhead is
part of the run; compare profiled runs to profiled runs, not historical timings.
"""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import threading
import time
from load_support import Stack, Workload, compressed_rows, now, specs, wait, write_json


def query_shape(query):
    """Keep SQL structure, remove comments and string literals before recording diagnostics."""
    # Application SQL contains no dollar-quoted bodies; utility statements are excluded below.
    query = re.sub(r"'(?:''|[^'])*'", "'?'", query)
    query = re.sub(r"/\*.*?\*/|--[^\n]*", " ", query, flags=re.S)
    return " ".join(query.split())[:4000]


class ProfiledStack(Stack):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        override = self.output / "profile.override.yaml"
        override.write_text("services:\n  postgres:\n    command: [postgres, -c, shared_buffers=64MB, -c, max_connections=100, "
                            "-c, shared_preload_libraries=pg_stat_statements, -c, track_io_timing=on]\n")
        self.command += ["-f", str(override)]
        self.profile_stop = threading.Event()
        self.profile_thread = None
        self.waits, self.profile_errors = [], []

    def start(self, build=True):
        super().start(build)
        # This is the fixed, labeled, disposable database, checked by Stack.start().
        self.compose("exec", "-T", "postgres", "psql", "-U", "jobs", "-d", "jobs_load_test",
                     "-v", "ON_ERROR_STOP=1", "-c", "CREATE EXTENSION IF NOT EXISTS pg_stat_statements")

    def environment(self):
        super().environment()
        path = self.output / "environment.json"
        data = json.loads(path.read_text())
        data["databaseProfiling"] = {"pgStatStatements": True, "trackIoTiming": True,
                                     "waitSamplingSeconds": .25, "observerOverheadIncluded": True}
        data["sourceSha256"]["scripts/profile-load.py"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        write_json(path, data)

    def begin_profile(self):
        self.sql("SELECT json_build_object('reset',pg_stat_statements_reset())")
        self.profile_thread = threading.Thread(target=self.sample_waits, daemon=True)
        self.profile_thread.start()

    def sample_waits(self):
        while not self.profile_stop.is_set():
            try:
                rows = self.sql("""SELECT coalesce(json_agg(row_to_json(s)),'[]'::json) FROM (
                    SELECT query_id::text,query,wait_event_type,wait_event,
                        cardinality(pg_blocking_pids(pid)) AS blockers,
                        extract(epoch FROM(clock_timestamp()-query_start))*1000 AS elapsed_ms
                    FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()
                        AND backend_type='client backend' AND state='active') s""")
                at = now()
                for row in rows:
                    row["query"] = query_shape(row["query"])
                    self.waits.append({"at": at, **row})
            except Exception as error:
                self.profile_errors.append(type(error).__name__)
            self.profile_stop.wait(.25)

    def finish_profile(self):
        self.profile_stop.set()
        if self.profile_thread:
            self.profile_thread.join(timeout=30)
            assert not self.profile_thread.is_alive(), "Profiler did not stop"
        rows = self.sql("""SELECT coalesce(json_agg(row_to_json(s)),'[]'::json) FROM (
            SELECT queryid::text,query,calls,total_exec_time,mean_exec_time,rows,shared_blks_hit,shared_blks_read,
                shared_blk_read_time,shared_blk_write_time,wal_bytes::text
            FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
                AND query NOT LIKE '%pg_stat_statements%' AND query NOT LIKE '%pg_stat_activity%'
                AND query ~* '^(SELECT|INSERT|UPDATE|DELETE|WITH)'
            ORDER BY total_exec_time DESC LIMIT 100) s""")
        for row in rows:
            row["query"] = query_shape(row["query"])
        output = json.dumps(rows) + json.dumps(self.waits)
        for secret in self.private.values():
            assert secret not in output, "Private credential appeared in profiling output"
        compressed_rows(self.output / "database-waits.jsonl.gz", self.waits)
        write_json(self.output / "database-statements.json", rows)
        groups = Counter((row["wait_event_type"] or "CPU_OR_RUNNABLE", row["wait_event"] or "NONE") for row in self.waits)
        write_json(self.output / "database-profile.json", {
            "passed": bool(self.waits) and not self.profile_errors, "activeSessionSamples": len(self.waits),
            "samplingErrors": self.profile_errors,
            "waitSamples": [{"type": key[0], "event": key[1], "samples": value} for key, value in groups.most_common()],
            "blockedSamples": sum(row["blockers"] > 0 for row in self.waits),
            "scope": "Measured admission+drain for pipeline; drain only for backlog. Statistics reset after warmup/preload. "
                     "Docker/psql sampling adds observer overhead; samples are not independent or exact wait-time percentages."})
        assert self.waits and not self.profile_errors, "No complete wait-sampling evidence"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--jobs", type=int, default=3000)
    parser.add_argument("--workers", type=int, choices=(1, 4), default=4)
    parser.add_argument("--mode", choices=("pipeline", "backlog"), default="backlog")
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()
    if not 100 <= args.jobs <= 10000:
        parser.error("jobs must be between 100 and 10000")
    stack = ProfiledStack(args.output, batch=50, delay=250, prefetch=1, pool=6, concurrency=2, workers=args.workers)
    report = None
    try:
        stack.start(build=not args.skip_build)
        warmup = Workload(stack, "warmup", specs(100, sizes=(128,), schedule_seconds=0), 8)
        warmup.submit(); warmup.await_terminal(); warmup.evidence()
        paused = stack.ids("worker") if args.mode == "backlog" else []
        if paused:
            stack.container_action("pause", *paused)
        work = Workload(stack, "measured", specs(args.jobs, schedule_seconds=0), 24, duplicates=args.jobs//5)
        if paused:
            work.submit()
            wait(lambda: stack.sql("SELECT to_json(count(*)) FROM job_outbox o JOIN jobs j ON j.id=o.job_id WHERE j.owner_id='"+work.owner+"'") == 0,
                 "Backlog was not published", timeout=240)
            assert work.state() == {"total": args.jobs, "terminal": 0}
        stack.begin_profile()
        started = time.monotonic()
        if paused:
            stack.container_action("unpause", *paused)
        else:
            work.submit()
        work.await_terminal(timeout=600)
        stack.finish_profile()
        report = work.evidence(drain_started=started if paused else None)
        report.update(mode=args.mode, databaseProfile=True, settings=stack.settings)
    finally:
        stack.profile_stop.set()
        if stack.profile_thread:
            stack.profile_thread.join(timeout=30)
        stack.close()
    report["cleanupVerified"] = True
    write_json(Path(args.output)/"report.json", report)
    print("PASS: profiled %d jobs at %.2f completed jobs/sec; raw SQL/wait diagnostics retained" %
          (report["accountedFor"], report["completedJobsPerSecond"]), flush=True)


if __name__ == "__main__":
    main()
