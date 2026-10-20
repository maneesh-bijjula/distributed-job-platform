"""Guardrails for benchmark arithmetic, replay semantics, and isolation."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
from uuid import uuid4
from load_support import Stack, Workload, distribution, expected_result, identifier, percentile, specs


class LoadSupportTest(unittest.TestCase):
    def test_nearest_rank_handles_small_and_empty_samples(self):
        self.assertIsNone(percentile([], .95))
        self.assertEqual(percentile([4], .99), 4)
        self.assertEqual(percentile(list(range(1, 101)), .95), 95)
        self.assertEqual(percentile([100, 1, 2], .5), 2)
        self.assertIsNone(distribution([])["mean"])
        with self.assertRaises(ValueError):
            percentile([1], 0)

    def test_workload_is_reproducible_but_run_keys_are_independent(self):
        a, b = specs(15, seed=42, schedule_seconds=2), specs(15, seed=42, schedule_seconds=2)
        self.assertEqual([x["body"]["text"] for x in a], [x["body"]["text"] for x in b])
        self.assertNotEqual(a[0]["key"], b[0]["key"])
        self.assertEqual(sum("scheduledAt" in x["body"] for x in a), 3)
        self.assertEqual([len(x["body"]["text"].encode()) for x in a[:3]], [128, 4096, 16000])

    def test_result_oracles_reject_unknown_handlers(self):
        self.assertEqual(expected_result({"type": "WORD_COUNT", "text": "a b"}), {"wordCount": 2, "characterCount": 3})
        self.assertEqual(expected_result({"type": "UPPERCASE", "text": "hello"}), {"text": "HELLO"})
        with self.assertRaises(ValueError):
            expected_result({"type": "UNKNOWN", "text": "private"})

    def test_uuid_guard_rejects_sql_in_fragments(self):
        with self.assertRaises(ValueError):
            identifier("'; DELETE FROM jobs; --")

    def test_foreign_container_is_rejected_before_mutation(self):
        stack = Stack.__new__(Stack)
        stack.docker = Mock(return_value=json.dumps([{"Config": {"Labels": {"com.docker.compose.project": "job-platform"}}}]))
        with self.assertRaises(ValueError):
            stack.container_action("kill", "ordinary-worker")
        stack.docker.assert_called_once_with("inspect", "ordinary-worker")

    def test_inspection_rejects_writes_before_using_docker(self):
        stack = Stack.__new__(Stack)
        stack.compose = Mock()
        with self.assertRaises(ValueError):
            stack.sql("TRUNCATE jobs")
        stack.compose.assert_not_called()

    def test_ambiguous_retry_and_cross_api_replay_retain_body_key_and_id(self):
        calls, job_id = [], str(uuid4())
        def request(path, method, body, key):
            calls.append((json.dumps(body, sort_keys=True), key))
            return ((0, {}, {}) if len(calls) == 1 else (202 if len(calls) == 2 else 200, {"id": job_id}, {}))
        clients = [Mock(request=Mock(side_effect=request)), Mock(request=Mock(side_effect=request))]
        with tempfile.TemporaryDirectory() as folder:
            stack = Mock(output=Path(folder))
            with patch("load_support.owner_clients", return_value=(str(uuid4()), clients)), patch("load_support.time.sleep"):
                work = Workload(stack, "retry", specs(1), 1, duplicates=1)
                work.submit()
            self.assertEqual(len(set(calls)), 1)
            self.assertEqual(work.ids, {0: job_id})
            self.assertEqual([row["httpStatus"] for row in work.responses], [0, 202, 200])
            self.assertTrue((work.directory / "requests.jsonl.gz").is_file())
            self.assertGreater(clients[0].request.call_count, 0)
            self.assertGreater(clients[1].request.call_count, 0)

    def test_authentication_failure_is_not_hidden_by_retry(self):
        clients = [Mock(request=Mock(return_value=(401, {}, {})))]
        with tempfile.TemporaryDirectory() as folder:
            stack = Mock(output=Path(folder))
            with patch("load_support.owner_clients", return_value=(str(uuid4()), clients)):
                work = Workload(stack, "denied", specs(1), 1)
                with self.assertRaises(AssertionError):
                    work.submit()
            self.assertEqual(clients[0].request.call_count, 1)


if __name__ == "__main__":
    unittest.main()
