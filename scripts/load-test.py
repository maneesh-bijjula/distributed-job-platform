"""Reproducible bounded workload against a separate, disposable Docker stack.

Requires Docker Compose >=2.24.4 and Python >=3.9. This cannot target a remote or
ordinary job-platform API. Raw ledgers contain only generated diagnostic metadata.
"""
import argparse
import time
from pathlib import Path
from load_support import Stack, Workload, specs, wait, write_json


def bounded(low, high):
    def parse(value):
        result = int(value)
        if not low <= result <= high:
            raise argparse.ArgumentTypeError("value must be between %d and %d" % (low, high))
        return result
    return parse


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, help="new output directory; existing results are never overwritten")
    parser.add_argument("--jobs", type=bounded(1, 10000), default=300)
    parser.add_argument("--clients", type=bounded(1, 64), default=16)
    parser.add_argument("--workers", type=bounded(1, 4), default=1)
    parser.add_argument("--concurrency", type=bounded(1, 8), default=2)
    parser.add_argument("--pool", type=bounded(2, 50), default=10)
    parser.add_argument("--database-cpus", type=int, choices=(1, 2), default=1)
    parser.add_argument("--prefetch", type=bounded(1, 250), default=10)
    parser.add_argument("--batch", type=bounded(1, 100), default=20)
    parser.add_argument("--dispatch-ms", type=bounded(50, 60000), default=1000)
    parser.add_argument("--duplicate-percent", type=bounded(0, 100), default=20)
    parser.add_argument("--schedule-seconds", type=bounded(0, 30), default=2, help="one in five jobs is scheduled this far ahead")
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--mode", choices=("pipeline", "backlog"), default="pipeline")
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()
    stack = Stack(args.output, batch=args.batch, delay=args.dispatch_ms, prefetch=args.prefetch,
                  pool=args.pool, concurrency=args.concurrency, workers=args.workers, database_cpus=args.database_cpus)
    try:
        stack.start(build=not args.skip_build)
        warmup = Workload(stack, "warmup", specs(12, sizes=(128,)), 4)
        warmup.submit(); warmup.await_terminal(); warmup.evidence()
        paused = []
        if args.mode == "backlog":
            paused = stack.ids("worker")
            assert len(paused) == args.workers
            stack.container_action("pause", *paused)
        workload = Workload(stack, "measured", specs(args.jobs, args.seed, schedule_seconds=args.schedule_seconds),
                            args.clients, duplicates=args.jobs * args.duplicate_percent // 100)
        workload.submit()
        drain_started = None
        if paused:
            owner = workload.owner
            wait(lambda: stack.sql("SELECT to_json(count(*)) FROM job_outbox o JOIN jobs j ON j.id=o.job_id WHERE j.owner_id='" + owner + "'") == 0,
                 "Backlog was not completely published", timeout=240)
            assert workload.state() == {"total": args.jobs, "terminal": 0}
            drain_started = time.monotonic()
            stack.container_action("unpause", *paused)
        workload.await_terminal(timeout=600)
        report = workload.evidence(drain_started=drain_started)
        report.update(mode=args.mode, seed=args.seed, settings=stack.settings,
                      scheduledFraction="one in five" if args.schedule_seconds else "none",
                      workload="round-robin WORD_COUNT/SHA256/UPPERCASE, ASCII payloads of 128/4096/16000 bytes",
                      throughputScope="Includes two APIs, PostgreSQL, RabbitMQ and unmodified production workers; no artificial handler delay")
        assert report["workerProcessesUsed"] >= min(args.workers, args.jobs, 2), "Work did not reach multiple workers"
    except BaseException as error:
        write_json(Path(args.output) / "failed.json", {"passed": False, "errorType": type(error).__name__, "message": str(error)})
        raise
    finally:
        stack.close()
    report["cleanupVerified"] = True
    write_json(Path(args.output) / "report.json", report)
    print("PASS: %d/%d jobs accounted for; %.2f completed jobs/s; p95 queue %.2f ms; p95 attempt %.2f ms." % (
        report["accountedFor"], args.jobs, report["completedJobsPerSecond"], report["attemptQueueDelayMs"]["p95"],
        report["attemptExecutionMs"]["p95"]), flush=True)


if __name__ == "__main__":
    main()
