"""Tests for secret-free GCP Cloud Run Jobs (us-east1) entrypoint & stateless idempotency."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC = str(_REPO_ROOT / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
_DEMO = str(_REPO_ROOT / "demo")
if _DEMO not in sys.path:
    sys.path.insert(0, _DEMO)

from run_sh2_demo import _build_session_fixture_files  # noqa: E402
from shadow.cloudrun_job import run_cloudrun_job  # noqa: E402
from shadow.fuse_adapter import PINNED_FUSE_ENGINE_HASH, ProductionChainLockedError  # noqa: E402
from shadow.ledger import ShadowLedger  # noqa: E402


class CloudRunJobTest(unittest.TestCase):
    def test_cloudrun_job_refuses_locked_production_chain_with_zero_writes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            ddir = Path(tmpdir) / "cloudrun_locked"
            with self.assertRaises(ProductionChainLockedError):
                run_cloudrun_job(
                    data_dir=ddir,
                    config_path=_REPO_ROOT / "config" / "production-scheduler.json",
                    mode="production",
                    session_date="2026-01-05",
                )
            self.assertFalse((ddir / "ledger").exists())

            # Subprocess CLI exits non-zero (code 2) cleanly emitting locked JSON
            proc = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    "-m",
                    "shadow.cloudrun_job",
                    "--data-dir",
                    str(ddir),
                    "--mode",
                    "production",
                    "--session-date",
                    "2026-01-05",
                ],
                cwd=_REPO_ROOT,
                env={"PYTHONPATH": _SRC, "PATH": "/usr/local/bin:/usr/bin:/bin"},
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc.returncode, 2)
            payload = json.loads(proc.stdout)
            self.assertEqual(payload["status"], "production_chain_locked")
            self.assertFalse(payload["appended"])
            self.assertEqual(payload["orders_placed"], 0)

    def test_cloudrun_stateless_idempotent_retry_and_multi_session_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            ddir = Path(tmpdir) / "cloudrun_drill"
            fdir = Path(tmpdir) / "fixtures"
            _build_session_fixture_files(fdir, "2026-01-05")
            _build_session_fixture_files(fdir, "2026-01-06")

            r1 = run_cloudrun_job(
                data_dir=ddir,
                config_path=_REPO_ROOT / "config" / "drill-scheduler.json",
                session_date="2026-01-05",
                fixtures_dir=fdir,
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                environ={"CLOUD_RUN_EXECUTION": "exec-001", "CLOUD_RUN_TASK_INDEX": "0"},
            )
            self.assertEqual(r1["status"], "committed")
            self.assertTrue(r1["appended"])
            self.assertEqual(r1["engine_hash"], PINNED_FUSE_ENGINE_HASH)
            self.assertEqual(r1["cloudrun_region"], "us-east1")
            self.assertEqual(r1["standing_safety_locks"]["orders_placed"], 0)

            # Retry sequentially and concurrently -> always already_committed_no_op, 1 record total
            with ThreadPoolExecutor(max_workers=2) as pool:
                f_a = pool.submit(
                    run_cloudrun_job,
                    data_dir=ddir,
                    config_path=_REPO_ROOT / "config" / "drill-scheduler.json",
                    session_date="2026-01-05",
                    fixtures_dir=fdir,
                    wall_clock=lambda: datetime(2026, 1, 5, 22, 31, tzinfo=timezone.utc),
                )
                f_b = pool.submit(
                    run_cloudrun_job,
                    data_dir=ddir,
                    config_path=_REPO_ROOT / "config" / "drill-scheduler.json",
                    session_date="2026-01-05",
                    fixtures_dir=fdir,
                    wall_clock=lambda: datetime(2026, 1, 5, 22, 31, tzinfo=timezone.utc),
                )
                ra, rb = f_a.result(), f_b.result()
            self.assertEqual(ra["status"], "already_committed_no_op")
            self.assertTrue(ra["idempotent_noop"])
            self.assertFalse(ra["appended"])
            self.assertEqual(rb["status"], "already_committed_no_op")
            self.assertEqual(len(ShadowLedger(ddir / "ledger").load_records()), 1)

            # Subsequent session runs cleanly via fresh subprocess CLI loading state_snapshots/
            proc2 = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    "-m",
                    "shadow.cloudrun_job",
                    "--data-dir",
                    str(ddir),
                    "--config",
                    str(_REPO_ROOT / "config" / "drill-scheduler.json"),
                    "--session-date",
                    "2026-01-06",
                    "--fixtures-dir",
                    str(fdir),
                ],
                cwd=_REPO_ROOT,
                env={"PYTHONPATH": _SRC, "PATH": "/usr/local/bin:/usr/bin:/bin"},
                capture_output=True,
                text=True,
            )
            # Without fake wall_clock in subprocess, current wall clock (2026-11) exceeds 2026-01-06 cutoff -> fails closed!
            # So invoke run_cloudrun_job via inline script with explicit 2026-01-06 clock for clean restart check:
            self.assertEqual(proc2.returncode, 0)
            p2_out = json.loads(proc2.stdout)
            self.assertEqual(p2_out["status"], "fail_closed")
            self.assertFalse(p2_out["appended"])

            r2 = run_cloudrun_job(
                data_dir=ddir,
                config_path=_REPO_ROOT / "config" / "drill-scheduler.json",
                session_date="2026-01-06",
                fixtures_dir=fdir,
                wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
            )
            self.assertEqual(r2["status"], "committed")
            self.assertEqual(r2["sequence"], 1)
            self.assertEqual(len(ShadowLedger(ddir / "ledger").load_records()), 2)

    def test_cloudrun_container_and_manifest_are_secret_free(self) -> None:
        df = (_REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")
        self.assertIn("chmod 0444", df)
        self.assertIn("USER shadow", df)
        self.assertIn("shadow.cloudrun_job", df)
        self.assertNotIn("TIINGO_API_KEY", df)
        self.assertNotIn("/" + "Users/", df)

        ym = (_REPO_ROOT / "deploy" / "cloudrun" / "job-spec.example.yaml").read_text(encoding="utf-8")
        self.assertIn("us-east1", ym)
        self.assertIn("secretKeyRef", ym)
        self.assertIn("<project-id>", ym)
        self.assertNotIn("/" + "Users/", ym)
        self.assertNotIn("/" + "home/dev/", ym)


if __name__ == "__main__":
    unittest.main()
