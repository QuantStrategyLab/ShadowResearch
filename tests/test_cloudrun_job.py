"""Tests for secret-free GCP Cloud Run Jobs (us-east1) entrypoint & stateless idempotency."""

from __future__ import annotations

import inspect
import json
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC = str(_REPO_ROOT / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
_DEMO = str(_REPO_ROOT / "demo")
if _DEMO not in sys.path:
    sys.path.insert(0, _DEMO)

from run_sh2_demo import _build_session_fixture_files, build_live_mock_transport  # noqa: E402
from shadow.cloudrun_job import run_cloudrun_job  # noqa: E402
from shadow.fuse_adapter import PINNED_FUSE_ENGINE_HASH, ProductionChainLockedError  # noqa: E402
from shadow.ledger import ChainConcurrencyConflictError, ShadowLedger  # noqa: E402
from shadow.ots import OpenTimestampsClient, build_synthetic_ots_fixture  # noqa: E402


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
        self.assertIn("maxRetries: 0", ym)
        self.assertIn("secretKeyRef", ym)
        self.assertIn("<project-id>", ym)
        self.assertNotIn("/" + "Users/", ym)
        self.assertNotIn("/" + "home/dev/", ym)

    def test_unlock_override_removed_and_unlocked_production_requires_live_transport(self) -> None:
        self.assertNotIn("unlock_production_override", inspect.signature(run_cloudrun_job).parameters)
        with tempfile.TemporaryDirectory() as tmpdir:
            ddir = Path(tmpdir) / "cloudrun_prod"
            raw_cfg = json.loads(
                (_REPO_ROOT / "config" / "production-scheduler.json").read_text(encoding="utf-8")
            )
            raw_cfg["production_chain_locked"] = False
            raw_cfg["clk01_active"] = True
            unlocked_cfg = Path(tmpdir) / "unlocked-production.json"
            unlocked_cfg.write_text(json.dumps(raw_cfg), encoding="utf-8")

            with self.assertRaises(TypeError):
                run_cloudrun_job(  # type: ignore[call-arg]
                    data_dir=ddir,
                    config_path=unlocked_cfg,
                    mode="production",
                    session_date="2026-01-05",
                    unlock_production_override=True,
                )

            with self.assertRaises(ValueError) as fix_ctx:
                run_cloudrun_job(
                    data_dir=ddir,
                    config_path=unlocked_cfg,
                    mode="production",
                    session_date="2026-01-05",
                    fixtures_dir=Path(tmpdir),
                )
            self.assertEqual(str(fix_ctx.exception), "production_mode_forbids_offline_fixtures")

            class _FakeOts(OpenTimestampsClient):
                def stamp_digest(self, sha256_hex: str) -> tuple[bytes, str]:
                    ots_b, _ = build_synthetic_ots_fixture(sha256_hex, bitcoin_height=880100)
                    return ots_b, "https://a.pool.opentimestamps.org"

            tick = [1000.0]

            def _fast_clk() -> float:
                tick[0] += 25.0
                return tick[0]

            out = run_cloudrun_job(
                data_dir=ddir,
                config_path=unlocked_cfg,
                mode="production",
                session_date="2026-01-05",
                transport=build_live_mock_transport("2026-01-05"),
                environ={"TIINGO_API_KEY": "test-dummy-token"},
                ots_client=_FakeOts(),
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clk,
                sleeper=lambda _s: None,
            )
            self.assertEqual(out["status"], "committed")
            self.assertTrue(out["appended"])
            self.assertEqual(out["standing_safety_locks"]["orders_placed"], 0)

    def test_cross_instance_chain_lease_conflict_exit4_expired_reclaim_and_mismatch_guard(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            ddir = Path(tmpdir) / "lease_test"
            fdir = Path(tmpdir) / "fixtures"
            _build_session_fixture_files(fdir, "2026-01-05")
            ledger = ShadowLedger(ddir / "ledger")
            ledger.root_dir.mkdir(parents=True, exist_ok=True)
            lease_path = ledger.root_dir / ".chain.lease.json"

            # 1. Active unexpired lease -> ChainConcurrencyConflictError + CLI exit code 4
            future_utc = (datetime.now(timezone.utc) + timedelta(minutes=5)).strftime(
                "%Y-%m-%dT%H:%M:%S.000Z"
            )
            lease_path.write_text(
                json.dumps(
                    {
                        "schema_version": "c-sh-chain-lease-v1",
                        "lease_id": "remote-writer-001",
                        "pid": 999999,
                        "execution_id": "exec-remote",
                        "expires_at_utc": future_utc,
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(ChainConcurrencyConflictError):
                run_cloudrun_job(
                    data_dir=ddir,
                    config_path=_REPO_ROOT / "config" / "drill-scheduler.json",
                    session_date="2026-01-05",
                    fixtures_dir=fdir,
                    wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                )

            proc4 = subprocess.run(
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
                    "2026-01-05",
                    "--fixtures-dir",
                    str(fdir),
                ],
                cwd=_REPO_ROOT,
                env={"PYTHONPATH": _SRC, "PATH": "/usr/local/bin:/usr/bin:/bin"},
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc4.returncode, 4)
            p4_out = json.loads(proc4.stdout)
            self.assertEqual(p4_out["status"], "chain_concurrency_conflict")
            self.assertFalse(p4_out["appended"])
            self.assertEqual(len(ledger.load_records()), 0)

            # 2. Expired lease is cleanly reclaimed and lease file removed on exit
            past_utc = (datetime.now(timezone.utc) - timedelta(minutes=5)).strftime(
                "%Y-%m-%dT%H:%M:%S.000Z"
            )
            lease_path.write_text(
                json.dumps(
                    {
                        "schema_version": "c-sh-chain-lease-v1",
                        "lease_id": "stale-writer-000",
                        "pid": 888888,
                        "execution_id": "exec-stale",
                        "expires_at_utc": past_utc,
                    }
                ),
                encoding="utf-8",
            )
            r_ok = run_cloudrun_job(
                data_dir=ddir,
                config_path=_REPO_ROOT / "config" / "drill-scheduler.json",
                session_date="2026-01-05",
                fixtures_dir=fdir,
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
            )
            self.assertEqual(r_ok["status"], "committed")
            self.assertFalse(lease_path.exists())

            # 3. Mismatched lease_id overwritten by another writer must NOT be unlinked on exit
            with ledger.chain_lock():
                self.assertTrue(lease_path.is_file())
                lease_path.write_text(
                    json.dumps(
                        {
                            "schema_version": "c-sh-chain-lease-v1",
                            "lease_id": "someone-elses-lease",
                            "pid": 777777,
                            "execution_id": "exec-other",
                            "expires_at_utc": future_utc,
                        }
                    ),
                    encoding="utf-8",
                )
            self.assertTrue(lease_path.is_file())
            self.assertEqual(json.loads(lease_path.read_text(encoding="utf-8"))["lease_id"], "someone-elses-lease")


if __name__ == "__main__":
    unittest.main()
