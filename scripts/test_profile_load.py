"""Privacy and lifecycle checks for isolated database profiling."""
import importlib.util
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import Mock

spec = importlib.util.spec_from_file_location("profile_load", Path(__file__).with_name("profile-load.py"))
profile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profile)


class ProfileLoadTest(unittest.TestCase):
    def test_sql_shapes_remove_literals_and_comments(self):
        sql = "SELECT * FROM jobs WHERE input_text='private ''quote'' value' AND id=$1 /* private token */ -- private comment\n FOR UPDATE"
        self.assertEqual(profile.query_shape(sql), "SELECT * FROM jobs WHERE input_text='?' AND id=$1 FOR UPDATE")

    def test_sql_shapes_are_bounded(self):
        self.assertEqual(len(profile.query_shape("SELECT "+"x"*5000)), 4000)

    def stack(self, folder):
        stack = profile.ProfiledStack.__new__(profile.ProfiledStack)
        stack.output = Path(folder)
        stack.profile_stop = threading.Event()
        stack.profile_thread = None
        stack.private = {"credential": "private-value"}
        stack.waits = []
        stack.profile_errors = []
        stack.sql = Mock(return_value=[])
        return stack

    def test_missing_samples_or_sampler_errors_invalidate_profile(self):
        with tempfile.TemporaryDirectory() as folder:
            stack = self.stack(folder)
            with self.assertRaisesRegex(AssertionError, "complete wait"):
                stack.finish_profile()
            stack.waits = [{"query": "SELECT $1", "wait_event_type": "Lock", "wait_event": "transactionid", "blockers": 1}]
            stack.profile_errors = ["TimeoutExpired"]
            with self.assertRaisesRegex(AssertionError, "complete wait"):
                stack.finish_profile()

    def test_credentials_abort_publication(self):
        with tempfile.TemporaryDirectory() as folder:
            stack = self.stack(folder)
            stack.waits = [{"query": "private-value", "wait_event_type": None, "wait_event": None, "blockers": 0}]
            with self.assertRaisesRegex(AssertionError, "credential"):
                stack.finish_profile()
            self.assertFalse((stack.output/"database-waits.jsonl.gz").exists())


if __name__ == "__main__":
    unittest.main()
