"""Safety tests for recovery tools; actual restore checks run in release-smoke.py."""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from deploy import Deployment, image_name, sql_identifier


class DeployTest(unittest.TestCase):
    def test_rejects_other_projects(self):
        for name in ("job-platform", "clouddrive", "job-platform-release-test-other"):
            with self.assertRaises(ValueError):
                Deployment(name)

    def test_offline_guard_refuses_running_or_paused_apps(self):
        for state in ("running", "paused", "restarting"):
            deployment = Deployment()
            with patch.object(deployment, "containers", return_value=[{
                    "Project": deployment.project, "Service": "worker", "State": state}]):
                with self.assertRaises(ValueError):
                    deployment.guard(offline=True)

    def test_offline_guard_allows_infrastructure_and_stopped_app(self):
        deployment = Deployment()
        with patch.object(deployment, "containers", return_value=[
                {"Project": deployment.project, "Service": "postgres", "State": "running"},
                {"Project": deployment.project, "Service": "api", "State": "exited"}]):
            deployment.guard(offline=True)

    def test_resource_guard_refuses_foreign_metadata(self):
        deployment = Deployment("job-platform-release-test")
        with patch.object(deployment, "containers", return_value=[{
                "Project": "job-platform-release", "Service": "postgres", "State": "running"}]):
            with self.assertRaisesRegex(ValueError, "Unexpected resource"):
                deployment.guard()

    def test_scratch_restore_failure_drops_only_created_scratch_database(self):
        deployment = Deployment()
        with patch.object(deployment, "guard"), patch.object(deployment, "sql") as sql, \
                patch.object(deployment, "restore", side_effect=ValueError("corrupt")):
            with self.assertRaises(ValueError):
                deployment.verify_backup("unused")
            self.assertEqual(sql.call_count, 2)
            self.assertEqual(sql.call_args_list[0].args[0], 'CREATE DATABASE "jobs_restore_verification"')
            self.assertEqual(sql.call_args_list[1].args[0], 'DROP DATABASE "jobs_restore_verification"')

    def test_restore_refuses_nonempty_database_before_pg_restore(self):
        deployment = Deployment()
        with patch.object(deployment, "guard"), patch.object(deployment, "checked_archive", return_value=(None, {})), \
                patch.object(deployment, "sql", return_value="1"), patch.object(deployment, "run") as run:
            with self.assertRaisesRegex(ValueError, "empty"):
                deployment.restore("unused")
            run.assert_not_called()

    def test_failed_scratch_create_never_drops_existing_database(self):
        deployment = Deployment()
        with patch.object(deployment, "guard"), patch.object(deployment, "sql", side_effect=RuntimeError("exists")) as sql:
            with self.assertRaises(RuntimeError):
                deployment.verify_backup("unused")
            self.assertEqual(sql.call_count, 1)
            self.assertTrue(sql.call_args.args[0].startswith("CREATE DATABASE"))

    def test_corrupted_archive_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "database.dump").write_bytes(b"original")
            (directory / "manifest.json").write_text(json.dumps({"format": 1,
                "sha256": hashlib.sha256(b"original").hexdigest()}))
            Deployment().checked_archive(directory)
            (directory / "database.dump").write_bytes(b"corrupted")
            with self.assertRaisesRegex(ValueError, "integrity"):
                Deployment().checked_archive(directory)

    def test_identifier_and_image_validation(self):
        for name in ("jobs;DROP TABLE jobs", 'x"', "public.jobs", ""):
            with self.assertRaises(ValueError):
                sql_identifier(name)
        for image in ("job-platform", "job-platform:latest", "a:v\nAPP_IMAGE=b:v", "$(whoami):v1"):
            with self.assertRaises(ValueError):
                image_name(image)
        self.assertEqual(image_name("job-platform:release-abc123"), "job-platform:release-abc123")

    def test_initialize_never_replaces_credentials(self):
        with tempfile.TemporaryDirectory() as temporary, patch("deploy.ROOT", Path(temporary)):
            deployment = Deployment()
            previous = os.umask(0o077)
            try:
                deployment.initialize("job-platform:test")
            finally:
                os.umask(previous)
            before = (deployment.directory / "secrets/bootstrap-key").read_bytes()
            with self.assertRaises(FileExistsError):
                deployment.initialize("job-platform:new")
            self.assertEqual(before, (deployment.directory / "secrets/bootstrap-key").read_bytes())
            self.assertEqual(deployment.directory.stat().st_mode & 0o777, 0o700)
            self.assertEqual((deployment.directory / "secrets").stat().st_mode & 0o777, 0o700)
            self.assertEqual((deployment.directory / "secrets/bootstrap-key").stat().st_mode & 0o777, 0o444)


if __name__ == "__main__":
    unittest.main()
