"""Protect the fault count and accepted-job denominator from false success."""
from collections import Counter
import gzip
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock
from uuid import uuid4
from fault_campaign import Campaign, KINDS, trial_plan, verified_fault, verify_population


class FaultCampaignTest(unittest.TestCase):
    def test_surviving_api_replay_evidence_is_not_overwritten_after_recovery(self):
        job_id = str(uuid4())
        clients = [Mock(), Mock()]
        for client in clients:
            client.request.return_value = (200, {"id": job_id}, {})
        with tempfile.TemporaryDirectory() as folder:
            work = Mock(directory=Path(folder), clients=clients, ids={0: job_id},
                        items=[{"index": 0, "key": "diagnostic", "body": {"type": "SHA256", "text": "test"}}])
            campaign = Campaign.__new__(Campaign)
            campaign.replay(work, 1, phase="surviving-api")
            campaign.replay(work, 0)
            with gzip.open(Path(folder) / "surviving-api-replays.jsonl.gz", "rt") as stream:
                self.assertEqual(json.loads(stream.readline())["apiIndex"], 1)
            with gzip.open(Path(folder) / "post-recovery-replays.jsonl.gz", "rt") as stream:
                self.assertEqual(json.loads(stream.readline())["apiIndex"], 0)

    def test_plan_is_balanced_reproducible_and_has_unique_trials(self):
        plan = trial_plan(100, 42)
        self.assertEqual(Counter(row["kind"] for row in plan), {kind: 20 for kind in KINDS})
        self.assertEqual([row["trial"] for row in plan], list(range(1, 101)))
        self.assertEqual(plan, trial_plan(100, 42))
        self.assertNotEqual(plan, trial_plan(100, 43))
        self.assertEqual(Counter(row["kind"] for row in trial_plan(5, 42)), {kind: 1 for kind in KINDS})

    def test_invalid_trial_denominators_are_rejected(self):
        for count in (-1, 0, 4, 6, 99, 101):
            with self.assertRaises(ValueError):
                trial_plan(count, 1)

    def test_only_observed_kill_or_pause_counts_as_a_fault(self):
        verified_fault("worker_sigkill", {"Running": False, "ExitCode": 137, "OOMKilled": False})
        verified_fault("worker_pause_resume", {"Running": True, "Paused": True})
        for state in ({"Running": True, "ExitCode": 137}, {"Running": False, "ExitCode": 0},
                      {"Running": False, "ExitCode": 137, "OOMKilled": True}):
            with self.assertRaises(AssertionError):
                verified_fault("worker_sigkill", state)
        with self.assertRaises(AssertionError):
            verified_fault("worker_pause_resume", {"Running": True, "Paused": False})

    def test_missing_or_duplicate_jobs_cannot_disappear_from_denominator(self):
        row = {"id": "accepted", "status": "SUCCEEDED", "successful_attempts": 1,
               "submitted_events": 1, "succeeded_events": 1}
        verify_population(["accepted"], [row])
        with self.assertRaises(AssertionError):
            verify_population(["accepted", "accepted"], [row])
        for rows in ([], [row, row], [{**row, "id": "unexpected"}]):
            with self.assertRaises(AssertionError):
                verify_population(["accepted"], rows)

    def test_terminal_failure_or_missing_success_audit_cannot_pass(self):
        row = {"id": "accepted", "status": "SUCCEEDED", "successful_attempts": 1,
               "submitted_events": 1, "succeeded_events": 1}
        for key, value in (("status", "FAILED"), ("successful_attempts", 2),
                           ("submitted_events", 0), ("succeeded_events", 2)):
            with self.assertRaises(AssertionError):
                verify_population(["accepted"], [{**row, key: value}])


if __name__ == "__main__":
    unittest.main()
