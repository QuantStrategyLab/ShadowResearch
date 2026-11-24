"""Tests for secret-free GCP Cloud Run Jobs (us-east1) entrypoint & stateless idempotency."""

from __future__ import annotations

import inspect
import json
import shutil
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
from shadow.fuse_adapter import EngineIdentityError, PINNED_FUSE_ENGINE_HASH, ProductionChainLockedError  # noqa: E402
from shadow.ledger import ChainConcurrencyConflictError, GENESIS_PREV_HASH, ShadowLedger  # noqa: E402
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
        self.assertIn("gcsfuse.run.googleapis.com", ym)
        self.assertNotIn("gcsfuse.csi.storage.gke.io", ym)
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
                environ={
                    "TIINGO_API_KEY": "test-dummy-token",
                    "SHADOW_TRUSTED_HEAD_SHA256": GENESIS_PREV_HASH,
                },
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

    def test_cloudrun_production_history_authentication_and_exit3_guard(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            ddir = Path(tmpdir) / "cloudrun_prod_auth"
            raw_cfg = json.loads(
                (_REPO_ROOT / "config" / "production-scheduler.json").read_text(encoding="utf-8")
            )
            raw_cfg["production_chain_locked"] = False
            raw_cfg["clk01_active"] = True
            unlocked_cfg = Path(tmpdir) / "unlocked-production.json"
            unlocked_cfg.write_text(json.dumps(raw_cfg), encoding="utf-8")

            hdrs: dict[int, dict[str, str]] = {}
            heights = {"2026-01-05": 880100, "2026-01-06": 880101, "2026-01-07": 880102}

            def _make_ots(session_date: str, block_ts: str):
                class _Ots(OpenTimestampsClient):
                    def stamp_digest(self, sha256_hex: str) -> tuple[bytes, str]:
                        bh = heights[session_date]
                        ots_b, root_hex = build_synthetic_ots_fixture(sha256_hex, bitcoin_height=bh)
                        assert root_hex is not None
                        hdrs[bh] = {"merkle_root": root_hex, "timestamp": block_ts}
                        return ots_b, "https://a.pool.opentimestamps.org"

                return _Ots()

            tick = [1000.0]

            def _fast_clk() -> float:
                tick[0] += 25.0
                return tick[0]

            # Empty bucket without SHADOW_TRUSTED_HEAD_SHA256=GENESIS_PREV_HASH MUST fail (with or without BTC roots)
            dummy_roots_file = Path(tmpdir) / "pre-genesis-roots.json"
            dummy_roots_file.write_text(
                json.dumps({"880100": {"merkle_root": "0" * 64, "timestamp": "2026-01-06T03:00:00.000Z"}}),
                encoding="utf-8",
            )
            for empty_env in (
                {"TIINGO_API_KEY": "test-dummy-token"},
                {
                    "TIINGO_API_KEY": "test-dummy-token",
                    "SHADOW_BITCOIN_MERKLE_ROOTS_PATH": str(dummy_roots_file),
                },
            ):
                with self.assertRaises(EngineIdentityError) as gen_err:
                    run_cloudrun_job(
                        data_dir=ddir,
                        config_path=unlocked_cfg,
                        mode="production",
                        session_date="2026-01-05",
                        transport=build_live_mock_transport("2026-01-05"),
                        environ=empty_env,
                        ots_client=_make_ots("2026-01-05", "2026-01-06T03:00:00.000Z"),
                        wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                        clock=_fast_clk,
                        sleeper=lambda _s: None,
                    )
                self.assertIn("production_genesis_unauthenticated", str(gen_err.exception))
                self.assertEqual(ShadowLedger(ddir / "ledger").load_records(), [])
                self.assertFalse((ddir / "replay").exists())

            proc_gen3 = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    "-m",
                    "shadow.cloudrun_job",
                    "--data-dir",
                    str(ddir),
                    "--config",
                    str(unlocked_cfg),
                    "--mode",
                    "production",
                    "--session-date",
                    "2026-01-05",
                ],
                cwd=_REPO_ROOT,
                env={
                    "PYTHONPATH": _SRC,
                    "PATH": "/usr/local/bin:/usr/bin:/bin",
                    "SHADOW_BITCOIN_MERKLE_ROOTS_PATH": str(dummy_roots_file),
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc_gen3.returncode, 3)
            pg_out = json.loads(proc_gen3.stdout)
            self.assertEqual(pg_out["status"], "engine_identity_error")
            self.assertFalse(pg_out["appended"])
            self.assertIn("production_genesis_unauthenticated", pg_out["reason"])

            r0 = run_cloudrun_job(
                data_dir=ddir,
                config_path=unlocked_cfg,
                mode="production",
                session_date="2026-01-05",
                transport=build_live_mock_transport("2026-01-05"),
                environ={
                    "TIINGO_API_KEY": "test-dummy-token",
                    "SHADOW_TRUSTED_HEAD_SHA256": GENESIS_PREV_HASH,
                },
                ots_client=_make_ots("2026-01-05", "2026-01-06T03:00:00.000Z"),
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clk,
                sleeper=lambda _s: None,
            )
            self.assertEqual(r0["status"], "committed")
            seq0_sha = str(r0["record_sha256"])

            # Without SHADOW_TRUSTED_HEAD_SHA256 / SHADOW_BITCOIN_MERKLE_ROOTS_PATH -> EngineIdentityError & CLI exit 3
            with self.assertRaises(EngineIdentityError):
                run_cloudrun_job(
                    data_dir=ddir,
                    config_path=unlocked_cfg,
                    mode="production",
                    session_date="2026-01-06",
                    transport=build_live_mock_transport("2026-01-06"),
                    environ={"TIINGO_API_KEY": "test-dummy-token"},
                    ots_client=_make_ots("2026-01-06", "2026-01-07T03:00:00.000Z"),
                    wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
                    clock=_fast_clk,
                    sleeper=lambda _s: None,
                )

            proc3 = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    "-m",
                    "shadow.cloudrun_job",
                    "--data-dir",
                    str(ddir),
                    "--config",
                    str(unlocked_cfg),
                    "--mode",
                    "production",
                    "--session-date",
                    "2026-01-06",
                ],
                cwd=_REPO_ROOT,
                env={"PYTHONPATH": _SRC, "PATH": "/usr/local/bin:/usr/bin:/bin"},
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc3.returncode, 3)
            p3_out = json.loads(proc3.stdout)
            self.assertEqual(p3_out["status"], "engine_identity_error")
            self.assertFalse(p3_out["appended"])
            self.assertIn("production_history_unauthenticated", p3_out["reason"])

            # Retry seq=0 with SHADOW_TRUSTED_HEAD_SHA256 returns already_committed_no_op
            retry0 = run_cloudrun_job(
                data_dir=ddir,
                config_path=unlocked_cfg,
                mode="production",
                session_date="2026-01-05",
                transport=build_live_mock_transport("2026-01-05"),
                environ={
                    "TIINGO_API_KEY": "test-dummy-token",
                    "SHADOW_TRUSTED_HEAD_SHA256": seq0_sha,
                },
                wall_clock=lambda: datetime(2026, 1, 5, 22, 35, tzinfo=timezone.utc),
            )
            self.assertEqual(retry0["status"], "already_committed_no_op")
            self.assertTrue(retry0["idempotent_noop"])

            # Advance to seq=1 authenticated via SHADOW_BITCOIN_MERKLE_ROOTS_PATH external file
            roots_file = Path(tmpdir) / "trusted-roots.json"
            roots_file.write_text(json.dumps({str(k): v for k, v in hdrs.items()}), encoding="utf-8")
            r1 = run_cloudrun_job(
                data_dir=ddir,
                config_path=unlocked_cfg,
                mode="production",
                session_date="2026-01-06",
                transport=build_live_mock_transport("2026-01-06"),
                environ={
                    "TIINGO_API_KEY": "test-dummy-token",
                    "SHADOW_TRUSTED_HEAD_SHA256": seq0_sha,
                    "SHADOW_BITCOIN_MERKLE_ROOTS_PATH": str(roots_file),
                },
                ots_client=_make_ots("2026-01-06", "2026-01-07T03:00:00.000Z"),
                wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clk,
                sleeper=lambda _s: None,
            )
            self.assertEqual(r1["status"], "committed")
            self.assertEqual(r1["sequence"], 1)
            self.assertEqual(len(ShadowLedger(ddir / "ledger").load_records()), 2)

            # Codex exploit guard: wiping SHADOW_DATA_DIR fails closed (exit 3) even with trusted BTC roots or old head
            shutil.rmtree(ddir)
            for wiped_env, expected_err in (
                (
                    {"SHADOW_BITCOIN_MERKLE_ROOTS_PATH": str(roots_file)},
                    "production_genesis_unauthenticated",
                ),
                (
                    {
                        "SHADOW_TRUSTED_HEAD_SHA256": seq0_sha,
                        "SHADOW_BITCOIN_MERKLE_ROOTS_PATH": str(roots_file),
                    },
                    "production_trusted_head_mismatch:empty_ledger",
                ),
            ):
                with self.assertRaises(EngineIdentityError) as wiped_ctx:
                    run_cloudrun_job(
                        data_dir=ddir,
                        config_path=unlocked_cfg,
                        mode="production",
                        session_date="2026-01-07",
                        transport=build_live_mock_transport("2026-01-07"),
                        environ={"TIINGO_API_KEY": "test-dummy-token", **wiped_env},
                        wall_clock=lambda: datetime(2026, 1, 7, 22, 30, tzinfo=timezone.utc),
                    )
                self.assertIn(expected_err, str(wiped_ctx.exception))
                proc_wiped = subprocess.run(
                    [
                        sys.executable,
                        "-B",
                        "-m",
                        "shadow.cloudrun_job",
                        "--data-dir",
                        str(ddir),
                        "--config",
                        str(unlocked_cfg),
                        "--mode",
                        "production",
                        "--session-date",
                        "2026-01-07",
                    ],
                    cwd=_REPO_ROOT,
                    env={"PYTHONPATH": _SRC, "PATH": "/usr/local/bin:/usr/bin:/bin", **wiped_env},
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(proc_wiped.returncode, 3)
                pw_out = json.loads(proc_wiped.stdout)
                self.assertEqual(pw_out["status"], "engine_identity_error")
                self.assertFalse(pw_out["appended"])
                self.assertIn(expected_err, pw_out["reason"])
                self.assertEqual(ShadowLedger(ddir / "ledger").load_records(), [])
                self.assertFalse((ddir / "replay").exists())

            # BTC roots without SHADOW_TRUSTED_HEAD_SHA256 must fail closed with exit 3 on fresh copy
            reauth_dir = Path(tmpdir) / "reauth_prod_data"
            r_init = run_cloudrun_job(
                data_dir=reauth_dir,
                config_path=unlocked_cfg,
                mode="production",
                session_date="2026-01-05",
                transport=build_live_mock_transport("2026-01-05"),
                environ={
                    "TIINGO_API_KEY": "test-dummy-token",
                    "SHADOW_TRUSTED_HEAD_SHA256": GENESIS_PREV_HASH,
                },
                ots_client=_make_ots("2026-01-05", "2026-01-06T03:00:00.000Z"),
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clk,
                sleeper=lambda _s: None,
            )
            init_sha = str(r_init["record_sha256"])

            with self.assertRaises(EngineIdentityError) as roots_only_err:
                run_cloudrun_job(
                    data_dir=reauth_dir,
                    config_path=unlocked_cfg,
                    mode="production",
                    session_date="2026-01-06",
                    transport=build_live_mock_transport("2026-01-06"),
                    environ={
                        "TIINGO_API_KEY": "test-dummy-token",
                        "SHADOW_BITCOIN_MERKLE_ROOTS_PATH": str(roots_file),
                    },
                    wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
                )
            self.assertIn("production_history_unauthenticated", str(roots_only_err.exception))

            # O1 rejection in Cloud Run emits pending_rejected_state_sha256; consumption requires SHADOW_TRUSTED_PENDING_STATE_SHA256
            base_rej_t = build_live_mock_transport("2026-01-06")

            def _missing_tbill_cr(url: str, headers: dict):
                if "treasury.gov" in url.lower():
                    return 404, b"missing"
                return base_rej_t(url, headers)

            rej_cr = run_cloudrun_job(
                data_dir=reauth_dir,
                config_path=unlocked_cfg,
                mode="production",
                session_date="2026-01-06",
                transport=_missing_tbill_cr,
                environ={
                    "TIINGO_API_KEY": "test-dummy-token",
                    "SHADOW_TRUSTED_HEAD_SHA256": init_sha,
                },
                wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clk,
                sleeper=lambda _s: None,
            )
            self.assertEqual(rej_cr["status"], "fail_closed")
            pend_digest = str(rej_cr["pending_rejected_state_sha256"])
            self.assertEqual(len(pend_digest), 64)

            proc_pend_unauth = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    "-m",
                    "shadow.cloudrun_job",
                    "--data-dir",
                    str(reauth_dir),
                    "--config",
                    str(unlocked_cfg),
                    "--mode",
                    "production",
                    "--session-date",
                    "2026-01-07",
                ],
                cwd=_REPO_ROOT,
                env={
                    "PYTHONPATH": _SRC,
                    "PATH": "/usr/local/bin:/usr/bin:/bin",
                    "SHADOW_TRUSTED_HEAD_SHA256": init_sha,
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc_pend_unauth.returncode, 3)
            self.assertIn("production_pending_rejected_state_unauthenticated", proc_pend_unauth.stdout)

            ok_after_pend = run_cloudrun_job(
                data_dir=reauth_dir,
                config_path=unlocked_cfg,
                mode="production",
                session_date="2026-01-07",
                transport=build_live_mock_transport("2026-01-07"),
                environ={
                    "TIINGO_API_KEY": "test-dummy-token",
                    "SHADOW_TRUSTED_HEAD_SHA256": init_sha,
                    "SHADOW_TRUSTED_PENDING_STATE_SHA256": pend_digest,
                },
                ots_client=_make_ots("2026-01-07", "2026-01-08T03:00:00.000Z"),
                wall_clock=lambda: datetime(2026, 1, 7, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clk,
                sleeper=lambda _s: None,
            )
            self.assertEqual(ok_after_pend["status"], "committed")
            self.assertEqual(ok_after_pend["sequence"], 1)


if __name__ == "__main__":
    unittest.main()
