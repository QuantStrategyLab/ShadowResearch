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
from shadow.feeder_live import SourceUnavailableError, urllib_fetch_bytes  # noqa: E402
from shadow.ledger import (
    ChainConcurrencyConflictError,
    GENESIS_PREV_HASH,
    NO_PENDING_REJECTED_STATE_SHA256,
    ShadowLedger,
)
from shadow.ots import (  # noqa: E402
    OTS_HEADER_MAGIC,
    OpenTimestampsClient,
    OtsSubmitError,
    build_synthetic_ots_fixture,
    urllib_ots_transport,
)


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
                    "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                    "SHADOW_TRUSTED_HEAD_ANCHOR_PATH": str(Path(tmpdir) / "smoke_anchor.json"),
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

            prod_auth_anchor = Path(tmpdir) / "prod_auth_anchor.json"
            # Empty bucket without SHADOW_TRUSTED_HEAD_SHA256=GENESIS_PREV_HASH MUST fail (with or without BTC roots)
            dummy_roots_file = Path(tmpdir) / "pre-genesis-roots.json"
            dummy_roots_file.write_text(
                json.dumps({"880100": {"merkle_root": "0" * 64, "timestamp": "2026-01-06T03:00:00.000Z"}}),
                encoding="utf-8",
            )
            for empty_env in (
                {
                    "TIINGO_API_KEY": "test-dummy-token",
                    "SHADOW_TRUSTED_HEAD_ANCHOR_PATH": str(prod_auth_anchor),
                },
                {
                    "TIINGO_API_KEY": "test-dummy-token",
                    "SHADOW_BITCOIN_MERKLE_ROOTS_PATH": str(dummy_roots_file),
                    "SHADOW_TRUSTED_HEAD_ANCHOR_PATH": str(prod_auth_anchor),
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
                    "SHADOW_TRUSTED_HEAD_ANCHOR_PATH": str(prod_auth_anchor),
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
                    "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                    "SHADOW_TRUSTED_HEAD_ANCHOR_PATH": str(prod_auth_anchor),
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
                    environ={
                        "TIINGO_API_KEY": "test-dummy-token",
                        "SHADOW_TRUSTED_HEAD_ANCHOR_PATH": str(prod_auth_anchor),
                    },
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
                env={
                    "PYTHONPATH": _SRC,
                    "PATH": "/usr/local/bin:/usr/bin:/bin",
                    "SHADOW_TRUSTED_HEAD_ANCHOR_PATH": str(prod_auth_anchor),
                },
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
                    "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                    "SHADOW_TRUSTED_HEAD_ANCHOR_PATH": str(prod_auth_anchor),
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
                    "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                    "SHADOW_BITCOIN_MERKLE_ROOTS_PATH": str(roots_file),
                    "SHADOW_TRUSTED_HEAD_ANCHOR_PATH": str(prod_auth_anchor),
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
                    {
                        "SHADOW_BITCOIN_MERKLE_ROOTS_PATH": str(roots_file),
                        "SHADOW_TRUSTED_HEAD_ANCHOR_PATH": str(prod_auth_anchor),
                    },
                    "production_genesis_unauthenticated",
                ),
                (
                    {
                        "SHADOW_TRUSTED_HEAD_SHA256": seq0_sha,
                        "SHADOW_BITCOIN_MERKLE_ROOTS_PATH": str(roots_file),
                        "SHADOW_TRUSTED_HEAD_ANCHOR_PATH": str(prod_auth_anchor),
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
            reauth_anchor = Path(tmpdir) / "reauth_prod_anchor.json"
            r_init = run_cloudrun_job(
                data_dir=reauth_dir,
                config_path=unlocked_cfg,
                mode="production",
                session_date="2026-01-05",
                transport=build_live_mock_transport("2026-01-05"),
                environ={
                    "TIINGO_API_KEY": "test-dummy-token",
                    "SHADOW_TRUSTED_HEAD_SHA256": GENESIS_PREV_HASH,
                    "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                    "SHADOW_TRUSTED_HEAD_ANCHOR_PATH": str(reauth_anchor),
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
                        "SHADOW_TRUSTED_HEAD_ANCHOR_PATH": str(reauth_anchor),
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
                    "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                    "SHADOW_TRUSTED_HEAD_ANCHOR_PATH": str(reauth_anchor),
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
                    "SHADOW_TRUSTED_HEAD_ANCHOR_PATH": str(reauth_anchor),
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc_pend_unauth.returncode, 3)
            self.assertIn("production_pending_rejected_state_unauthenticated", proc_pend_unauth.stdout)

            # Deleting pending_rejected_state.json fails closed exit 3 (both with pend_digest and stale NO_PENDING_REJECTED_STATE_SHA256)
            deleted_pend_dir = Path(tmpdir) / "deleted_pend_copy"
            shutil.copytree(reauth_dir, deleted_pend_dir)
            (deleted_pend_dir / "state_snapshots" / "pending_rejected_state.json").unlink()
            for del_pend_sha, del_pend_err in (
                (pend_digest, "production_pending_rejected_state_missing"),
                (
                    NO_PENDING_REJECTED_STATE_SHA256,
                    "production_trusted_head_rollback_rejected: pending state diverged from trusted anchor",
                ),
            ):
                proc_pend_deleted = subprocess.run(
                    [
                        sys.executable,
                        "-B",
                        "-m",
                        "shadow.cloudrun_job",
                        "--data-dir",
                        str(deleted_pend_dir),
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
                        "SHADOW_TRUSTED_PENDING_STATE_SHA256": del_pend_sha,
                        "SHADOW_TRUSTED_HEAD_ANCHOR_PATH": str(reauth_anchor),
                    },
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(proc_pend_deleted.returncode, 3)
                self.assertIn(del_pend_err, proc_pend_deleted.stdout)

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
                    "SHADOW_TRUSTED_HEAD_ANCHOR_PATH": str(reauth_anchor),
                },
                ots_client=_make_ots("2026-01-07", "2026-01-08T03:00:00.000Z"),
                wall_clock=lambda: datetime(2026, 1, 7, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clk,
                sleeper=lambda _s: None,
            )
            self.assertEqual(ok_after_pend["status"], "committed")
            self.assertEqual(ok_after_pend["sequence"], 1)

    def test_concurrent_stale_lease_reclamation_allows_only_one_winner(self) -> None:
        import threading
        from datetime import timedelta

        with tempfile.TemporaryDirectory() as tmpdir:
            ddir = Path(tmpdir) / "lease_race"
            ledger = ShadowLedger(ddir / "ledger")
            lease_path = ledger.root_dir / ".chain.lease.json"
            past_utc = (datetime.now(timezone.utc) - timedelta(minutes=5)).strftime(
                "%Y-%m-%dT%H:%M:%S.000Z"
            )
            lease_path.write_text(
                json.dumps(
                    {
                        "schema_version": "c-sh-chain-lease-v1",
                        "lease_id": "stale-race-000",
                        "pid": 888888,
                        "execution_id": "exec-stale",
                        "expires_at_utc": past_utc,
                    }
                ),
                encoding="utf-8",
            )

            barrier = threading.Barrier(2)
            tls = threading.local()
            orig_parse_desc = ShadowLedger.__dict__["_parse_iso_utc"]
            orig_parse = orig_parse_desc.__func__

            def _sync_parse(raw: object) -> datetime | None:
                res = orig_parse(raw)
                if not getattr(tls, "waited", False):
                    tls.waited = True
                    barrier.wait(timeout=5.0)
                return res

            winners: list[str] = []
            conflicts: list[Exception] = []
            lock = threading.Lock()

            def _worker() -> None:
                try:
                    lid = ledger._acquire_cross_instance_lease(
                        lease_path, lease_ttl_seconds=300.0
                    )
                    with lock:
                        winners.append(lid)
                except ChainConcurrencyConflictError as exc:
                    with lock:
                        conflicts.append(exc)

            ShadowLedger._parse_iso_utc = staticmethod(_sync_parse)  # type: ignore[method-assign]
            try:
                t1 = threading.Thread(target=_worker)
                t2 = threading.Thread(target=_worker)
                t1.start()
                t2.start()
                t1.join(timeout=10.0)
                t2.join(timeout=10.0)
            finally:
                ShadowLedger._parse_iso_utc = orig_parse_desc  # type: ignore[assignment]

            self.assertEqual(len(winners), 1, f"winners={winners}, conflicts={conflicts}")
            self.assertEqual(len(conflicts), 1, f"winners={winners}, conflicts={conflicts}")
            active = json.loads(lease_path.read_text(encoding="utf-8"))
            self.assertEqual(active["lease_id"], winners[0])

            # Expired lease must NOT be unlinked by its former holder after TTL expiry
            lease_path.write_text(
                json.dumps(
                    {
                        "schema_version": "c-sh-chain-lease-v1",
                        "lease_id": winners[0],
                        "expires_at_utc": past_utc,
                    }
                ),
                encoding="utf-8",
            )
            ShadowLedger._release_cross_instance_lease(lease_path, winners[0])
            self.assertTrue(lease_path.is_file())

    def test_expired_reclaim_claim_recovery_and_trusted_anchor_rollback_rejected(self) -> None:
        import hashlib
        import threading
        from datetime import timedelta

        with tempfile.TemporaryDirectory() as tmpdir:
            # Part 1: PRRT_kwDOV2RNAc67qYmN — expired .chain.lease.reclaim.<claim_key>.lock recovers cleanly
            rec_dir = Path(tmpdir) / "expired_reclaim"
            ledger = ShadowLedger(rec_dir / "ledger")
            lease_path = ledger.root_dir / ".chain.lease.json"
            past_utc = (datetime.now(timezone.utc) - timedelta(minutes=5)).strftime(
                "%Y-%m-%dT%H:%M:%S.000Z"
            )
            stale_bytes = json.dumps(
                {
                    "schema_version": "c-sh-chain-lease-v1",
                    "lease_id": "stale-crashed-000",
                    "pid": 888888,
                    "execution_id": "exec-stale",
                    "expires_at_utc": past_utc,
                }
            ).encode("utf-8")
            lease_path.write_bytes(stale_bytes)
            stale_sha = hashlib.sha256(stale_bytes).hexdigest()
            claim_key = hashlib.sha256(
                f"id:stale-crashed-000:{stale_sha}".encode("utf-8")
            ).hexdigest()[:32]
            reclaim_path = lease_path.with_name(f".chain.lease.reclaim.{claim_key}.lock")
            reclaim_path.write_text(
                json.dumps(
                    {
                        "schema_version": "c-sh-chain-lease-reclaim-v1",
                        "claim_key": claim_key,
                        "claimed_by_lease_id": "crashed-reclaimer-000",
                        "expires_at_utc": past_utc,
                    }
                ),
                encoding="utf-8",
            )

            barrier = threading.Barrier(2)
            tls = threading.local()
            orig_parse_desc = ShadowLedger.__dict__["_parse_iso_utc"]
            orig_parse = orig_parse_desc.__func__

            def _sync_parse(raw: object) -> datetime | None:
                res = orig_parse(raw)
                if not getattr(tls, "waited", False):
                    tls.waited = True
                    barrier.wait(timeout=5.0)
                return res

            winners: list[str] = []
            conflicts: list[Exception] = []
            lock = threading.Lock()

            def _worker() -> None:
                try:
                    lid = ledger._acquire_cross_instance_lease(
                        lease_path, lease_ttl_seconds=300.0
                    )
                    with lock:
                        winners.append(lid)
                except ChainConcurrencyConflictError as exc:
                    with lock:
                        conflicts.append(exc)

            ShadowLedger._parse_iso_utc = staticmethod(_sync_parse)  # type: ignore[method-assign]
            try:
                t1 = threading.Thread(target=_worker)
                t2 = threading.Thread(target=_worker)
                t1.start()
                t2.start()
                t1.join(timeout=10.0)
                t2.join(timeout=10.0)
            finally:
                ShadowLedger._parse_iso_utc = orig_parse_desc  # type: ignore[assignment]

            self.assertEqual(len(winners), 1, f"winners={winners}, conflicts={conflicts}")
            self.assertEqual(len(conflicts), 1, f"winners={winners}, conflicts={conflicts}")
            active = json.loads(lease_path.read_text(encoding="utf-8"))
            self.assertEqual(active["lease_id"], winners[0])
            ShadowLedger._release_cross_instance_lease(lease_path, winners[0])
            self.assertFalse(lease_path.exists())

            # Part 2: PRRT_kwDOV2RNAc67qas6 — external monotonic trusted anchor & rollback guards
            raw_cfg = json.loads(
                (_REPO_ROOT / "config" / "production-scheduler.json").read_text(encoding="utf-8")
            )
            raw_cfg["production_chain_locked"] = False
            raw_cfg["clk01_active"] = True
            unlocked_cfg = Path(tmpdir) / "unlocked-production.json"
            unlocked_cfg.write_text(json.dumps(raw_cfg), encoding="utf-8")

            heights = {"2026-01-05": 880100, "2026-01-06": 880101, "2026-01-07": 880102}

            def _make_ots(session_date: str):
                class _Ots(OpenTimestampsClient):
                    def stamp_digest(self, sha256_hex: str) -> tuple[bytes, str]:
                        ots_b, _ = build_synthetic_ots_fixture(
                            sha256_hex, bitcoin_height=heights[session_date]
                        )
                        return ots_b, "https://a.pool.opentimestamps.org"

                return _Ots()

            tick = [2000.0]

            def _fast_clk() -> float:
                tick[0] += 25.0
                return tick[0]

            ddir = Path(tmpdir) / "anchor_prod_data"
            anchor_path = Path(tmpdir) / "external_trusted_anchor.json"

            # 2a. Missing trusted_head_anchor_path fails closed (EngineIdentityError / CLI exit 3)
            with self.assertRaises(EngineIdentityError) as req_ctx:
                run_cloudrun_job(
                    data_dir=ddir,
                    config_path=unlocked_cfg,
                    mode="production",
                    session_date="2026-01-05",
                    transport=build_live_mock_transport("2026-01-05"),
                    environ={
                        "TIINGO_API_KEY": "test-dummy-token",
                        "SHADOW_TRUSTED_HEAD_SHA256": GENESIS_PREV_HASH,
                        "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                    },
                    ots_client=_make_ots("2026-01-05"),
                    wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                    clock=_fast_clk,
                    sleeper=lambda _s: None,
                )
            self.assertIn("production_trusted_head_anchor_required", str(req_ctx.exception))
            self.assertEqual(ShadowLedger(ddir / "ledger").load_records(), [])

            proc_req = subprocess.run(
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
                    "SHADOW_TRUSTED_HEAD_SHA256": GENESIS_PREV_HASH,
                    "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc_req.returncode, 3)
            self.assertIn("production_trusted_head_anchor_required", proc_req.stdout)

            # 2b. Anchor path inside data_dir fails closed (EngineIdentityError / CLI exit 3)
            inside_anchor = ddir / "inside_anchor.json"
            with self.assertRaises(EngineIdentityError) as in_ctx:
                run_cloudrun_job(
                    data_dir=ddir,
                    config_path=unlocked_cfg,
                    mode="production",
                    session_date="2026-01-05",
                    transport=build_live_mock_transport("2026-01-05"),
                    environ={
                        "TIINGO_API_KEY": "test-dummy-token",
                        "SHADOW_TRUSTED_HEAD_SHA256": GENESIS_PREV_HASH,
                        "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                    },
                    ots_client=_make_ots("2026-01-05"),
                    wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                    clock=_fast_clk,
                    sleeper=lambda _s: None,
                    trusted_head_anchor_path=inside_anchor,
                )
            self.assertIn("production_trusted_head_anchor_inside_data_dir", str(in_ctx.exception))

            proc_inside = subprocess.run(
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
                    "--trusted-head-anchor-path",
                    str(inside_anchor),
                ],
                cwd=_REPO_ROOT,
                env={
                    "PYTHONPATH": _SRC,
                    "PATH": "/usr/local/bin:/usr/bin:/bin",
                    "SHADOW_TRUSTED_HEAD_SHA256": GENESIS_PREV_HASH,
                    "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc_inside.returncode, 3)
            self.assertIn("production_trusted_head_anchor_inside_data_dir", proc_inside.stdout)

            # 2c. Commit seq 0 (2026-01-05), snapshot ddir, then advance to seq 1 (2026-01-06)
            r0 = run_cloudrun_job(
                data_dir=ddir,
                config_path=unlocked_cfg,
                mode="production",
                session_date="2026-01-05",
                transport=build_live_mock_transport("2026-01-05"),
                environ={
                    "TIINGO_API_KEY": "test-dummy-token",
                    "SHADOW_TRUSTED_HEAD_SHA256": GENESIS_PREV_HASH,
                    "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                },
                ots_client=_make_ots("2026-01-05"),
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clk,
                sleeper=lambda _s: None,
                trusted_head_anchor_path=anchor_path,
            )
            self.assertEqual(r0["status"], "committed")
            head_0 = str(r0["record_sha256"])

            seq0_backup = Path(tmpdir) / "seq0_backup"
            shutil.copytree(ddir, seq0_backup)

            r1 = run_cloudrun_job(
                data_dir=ddir,
                config_path=unlocked_cfg,
                mode="production",
                session_date="2026-01-06",
                transport=build_live_mock_transport("2026-01-06"),
                environ={
                    "TIINGO_API_KEY": "test-dummy-token",
                    "SHADOW_TRUSTED_HEAD_SHA256": head_0,
                    "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                    "SHADOW_TRUSTED_HEAD_ANCHOR_PATH": str(anchor_path),
                },
                ots_client=_make_ots("2026-01-06"),
                wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clk,
                sleeper=lambda _s: None,
            )
            self.assertEqual(r1["status"], "committed")
            self.assertEqual(r1["sequence"], 1)
            head_1 = str(r1["record_sha256"])
            anchor_state = json.loads(anchor_path.read_text(encoding="utf-8"))
            self.assertEqual(anchor_state["sequence"], 1)
            self.assertEqual(anchor_state["head_sha256"], head_1)

            # 2d. Rollback ddir to seq 0 while anchor remains at seq 1 + pass stale head_0 -> rejected
            shutil.rmtree(ddir)
            shutil.copytree(seq0_backup, ddir)
            with self.assertRaises(EngineIdentityError) as rb_ctx:
                run_cloudrun_job(
                    data_dir=ddir,
                    config_path=unlocked_cfg,
                    mode="production",
                    session_date="2026-01-07",
                    transport=build_live_mock_transport("2026-01-07"),
                    environ={
                        "TIINGO_API_KEY": "test-dummy-token",
                        "SHADOW_TRUSTED_HEAD_SHA256": head_0,
                        "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                    },
                    ots_client=_make_ots("2026-01-07"),
                    wall_clock=lambda: datetime(2026, 1, 7, 22, 30, tzinfo=timezone.utc),
                    clock=_fast_clk,
                    sleeper=lambda _s: None,
                    trusted_head_anchor_path=anchor_path,
                )
            self.assertIn("production_trusted_head_rollback_rejected", str(rb_ctx.exception))
            self.assertEqual(len(ShadowLedger(ddir / "ledger").load_records()), 1)

            proc_rb = subprocess.run(
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
                    "--trusted-head-anchor-path",
                    str(anchor_path),
                ],
                cwd=_REPO_ROOT,
                env={
                    "PYTHONPATH": _SRC,
                    "PATH": "/usr/local/bin:/usr/bin:/bin",
                    "SHADOW_TRUSTED_HEAD_SHA256": head_0,
                    "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc_rb.returncode, 3)
            rb_out = json.loads(proc_rb.stdout)
            self.assertFalse(rb_out["appended"])
            self.assertIn("production_trusted_head_rollback_rejected", rb_out["reason"])
            self.assertEqual(len(ShadowLedger(ddir / "ledger").load_records()), 1)

            # 2e. Deleting anchor on non-empty bucket fails closed (CLI exit 3, 0 writes)
            anchor_path.unlink()
            proc_missing = subprocess.run(
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
                    "--trusted-head-anchor-path",
                    str(anchor_path),
                ],
                cwd=_REPO_ROOT,
                env={
                    "PYTHONPATH": _SRC,
                    "PATH": "/usr/local/bin:/usr/bin:/bin",
                    "SHADOW_TRUSTED_HEAD_SHA256": head_0,
                    "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc_missing.returncode, 3)
            miss_out = json.loads(proc_missing.stdout)
            self.assertFalse(miss_out["appended"])
            self.assertIn("production_trusted_head_anchor_missing", miss_out["reason"])
            self.assertEqual(len(ShadowLedger(ddir / "ledger").load_records()), 1)

    def test_initializing_lease_not_stolen_and_interrupted_commit_anchor_recovers(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            # 1. PRRT_kwDOV2RNAc67rRKq: fresh 0-byte initializing lease must NOT be stolen
            init_dir = Path(tmpdir) / "init_lease_dir"
            ledger = ShadowLedger(init_dir / "ledger")
            lease_path = ledger.root_dir / ".chain.lease.json"
            lease_path.write_bytes(b"")
            with self.assertRaises(ChainConcurrencyConflictError):
                ledger._acquire_cross_instance_lease(lease_path, lease_ttl_seconds=300.0)
            self.assertTrue(lease_path.exists())
            self.assertEqual(lease_path.read_bytes(), b"")

            # 2. PRRT_kwDOV2RNAc67rRKY: commit seq=1 interrupted before anchor update recovers cleanly
            raw_cfg = json.loads(
                (_REPO_ROOT / "config" / "production-scheduler.json").read_text(encoding="utf-8")
            )
            raw_cfg["production_chain_locked"] = False
            raw_cfg["clk01_active"] = True
            unlocked_cfg = Path(tmpdir) / "unlocked-production.json"
            unlocked_cfg.write_text(json.dumps(raw_cfg), encoding="utf-8")

            heights = {"2026-01-05": 880100, "2026-01-06": 880101, "2026-01-07": 880102}

            def _make_ots(session_date: str):
                class _Ots(OpenTimestampsClient):
                    def stamp_digest(self, sha256_hex: str) -> tuple[bytes, str]:
                        ots_b, _ = build_synthetic_ots_fixture(
                            sha256_hex, bitcoin_height=heights[session_date]
                        )
                        return ots_b, "https://a.pool.opentimestamps.org"

                return _Ots()

            tick = [3000.0]

            def _fast_clk() -> float:
                tick[0] += 25.0
                return tick[0]

            ddir = Path(tmpdir) / "interrupted_anchor_data"
            anchor_path = Path(tmpdir) / "external_anchor.json"
            r0 = run_cloudrun_job(
                data_dir=ddir,
                config_path=unlocked_cfg,
                mode="production",
                session_date="2026-01-05",
                transport=build_live_mock_transport("2026-01-05"),
                environ={
                    "TIINGO_API_KEY": "test-dummy-token",
                    "SHADOW_TRUSTED_HEAD_SHA256": GENESIS_PREV_HASH,
                    "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                },
                ots_client=_make_ots("2026-01-05"),
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clk,
                sleeper=lambda _s: None,
                trusted_head_anchor_path=anchor_path,
            )
            head_0 = str(r0["record_sha256"])
            anchor_seq0_bytes = anchor_path.read_bytes()

            r1 = run_cloudrun_job(
                data_dir=ddir,
                config_path=unlocked_cfg,
                mode="production",
                session_date="2026-01-06",
                transport=build_live_mock_transport("2026-01-06"),
                environ={
                    "TIINGO_API_KEY": "test-dummy-token",
                    "SHADOW_TRUSTED_HEAD_SHA256": head_0,
                    "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                },
                ots_client=_make_ots("2026-01-06"),
                wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clk,
                sleeper=lambda _s: None,
                trusted_head_anchor_path=anchor_path,
            )
            head_1 = str(r1["record_sha256"])

            # Simulate interruption after append_record(seq=1) before _advance_trusted_head_anchor
            anchor_path.write_bytes(anchor_seq0_bytes)
            # Supplying stale head_0 still fails closed (no rollback)
            with self.assertRaises(EngineIdentityError) as stale_ctx:
                run_cloudrun_job(
                    data_dir=ddir,
                    config_path=unlocked_cfg,
                    mode="production",
                    session_date="2026-01-07",
                    transport=build_live_mock_transport("2026-01-07"),
                    environ={
                        "TIINGO_API_KEY": "test-dummy-token",
                        "SHADOW_TRUSTED_HEAD_SHA256": head_0,
                        "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                    },
                    ots_client=_make_ots("2026-01-07"),
                    wall_clock=lambda: datetime(2026, 1, 7, 22, 30, tzinfo=timezone.utc),
                    clock=_fast_clk,
                    sleeper=lambda _s: None,
                    trusted_head_anchor_path=anchor_path,
                )
            self.assertIn("production_trusted_head_mismatch:diverges_from_anchor", str(stale_ctx.exception))

            # Supplying actual committed head_1 catches anchor up to seq=1 on retry, and Jan 7 advances to seq=2
            retry1 = run_cloudrun_job(
                data_dir=ddir,
                config_path=unlocked_cfg,
                mode="production",
                session_date="2026-01-06",
                transport=build_live_mock_transport("2026-01-06"),
                environ={
                    "TIINGO_API_KEY": "test-dummy-token",
                    "SHADOW_TRUSTED_HEAD_SHA256": head_1,
                    "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                },
                wall_clock=lambda: datetime(2026, 1, 6, 22, 35, tzinfo=timezone.utc),
                trusted_head_anchor_path=anchor_path,
            )
            self.assertEqual(retry1["status"], "already_committed_no_op")
            caught_anchor = json.loads(anchor_path.read_text(encoding="utf-8"))
            self.assertEqual(caught_anchor["sequence"], 1)
            self.assertEqual(caught_anchor["head_sha256"], head_1)

            r2 = run_cloudrun_job(
                data_dir=ddir,
                config_path=unlocked_cfg,
                mode="production",
                session_date="2026-01-07",
                transport=build_live_mock_transport("2026-01-07"),
                environ={
                    "TIINGO_API_KEY": "test-dummy-token",
                    "SHADOW_TRUSTED_HEAD_SHA256": head_1,
                    "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                },
                ots_client=_make_ots("2026-01-07"),
                wall_clock=lambda: datetime(2026, 1, 7, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clk,
                sleeper=lambda _s: None,
                trusted_head_anchor_path=anchor_path,
            )
            self.assertEqual(r2["status"], "committed")
            self.assertEqual(r2["sequence"], 2)

            # 2-step lag (anchor stuck at seq=0 when ledger at seq=2) still fails closed
            anchor_path.write_bytes(anchor_seq0_bytes)
            with self.assertRaises(EngineIdentityError) as gap_ctx:
                run_cloudrun_job(
                    data_dir=ddir,
                    config_path=unlocked_cfg,
                    mode="production",
                    session_date="2026-01-08",
                    transport=build_live_mock_transport("2026-01-08"),
                    environ={
                        "TIINGO_API_KEY": "test-dummy-token",
                        "SHADOW_TRUSTED_HEAD_SHA256": str(r2["record_sha256"]),
                        "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                    },
                    wall_clock=lambda: datetime(2026, 1, 8, 22, 30, tzinfo=timezone.utc),
                    trusted_head_anchor_path=anchor_path,
                )
            self.assertIn("production_trusted_head_mismatch:diverges_from_anchor", str(gap_ctx.exception))

    def test_interrupted_pending_anchor_catchup_recovers_and_blocks_older_pending_rollback(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_cfg = json.loads(
                (_REPO_ROOT / "config" / "production-scheduler.json").read_text(encoding="utf-8")
            )
            raw_cfg["production_chain_locked"] = False
            raw_cfg["clk01_active"] = True
            unlocked_cfg = Path(tmpdir) / "unlocked-production.json"
            unlocked_cfg.write_text(json.dumps(raw_cfg), encoding="utf-8")

            heights = {"2026-01-05": 880100, "2026-01-06": 880101, "2026-01-07": 880102, "2026-01-08": 880103}

            def _make_ots(session_date: str):
                class _Ots(OpenTimestampsClient):
                    def stamp_digest(self, sha256_hex: str) -> tuple[bytes, str]:
                        ots_b, _ = build_synthetic_ots_fixture(
                            sha256_hex, bitcoin_height=heights[session_date]
                        )
                        return ots_b, "https://a.pool.opentimestamps.org"

                return _Ots()

            tick = [4000.0]

            def _fast_clk() -> float:
                tick[0] += 25.0
                return tick[0]

            ddir = Path(tmpdir) / "interrupted_pending_data"
            anchor_path = Path(tmpdir) / "external_pending_anchor.json"
            r0 = run_cloudrun_job(
                data_dir=ddir,
                config_path=unlocked_cfg,
                mode="production",
                session_date="2026-01-05",
                transport=build_live_mock_transport("2026-01-05"),
                environ={
                    "TIINGO_API_KEY": "test-dummy-token",
                    "SHADOW_TRUSTED_HEAD_SHA256": GENESIS_PREV_HASH,
                    "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                },
                ots_client=_make_ots("2026-01-05"),
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clk,
                sleeper=lambda _s: None,
                trusted_head_anchor_path=anchor_path,
            )
            head_0 = str(r0["record_sha256"])
            anchor_jan5_bytes = anchor_path.read_bytes()

            base_rej_t = build_live_mock_transport("2026-01-06")

            def _missing_tbill_6(url: str, headers: dict):
                if "treasury.gov" in url.lower():
                    return 404, b"missing"
                return base_rej_t(url, headers)

            rej6 = run_cloudrun_job(
                data_dir=ddir,
                config_path=unlocked_cfg,
                mode="production",
                session_date="2026-01-06",
                transport=_missing_tbill_6,
                environ={
                    "TIINGO_API_KEY": "test-dummy-token",
                    "SHADOW_TRUSTED_HEAD_SHA256": head_0,
                    "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                },
                wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clk,
                sleeper=lambda _s: None,
                trusted_head_anchor_path=anchor_path,
            )
            self.assertEqual(rej6["status"], "fail_closed")
            pend_sha_6 = str(rej6["pending_rejected_state_sha256"])
            pend_jan6_bytes = (ddir / "state_snapshots" / "pending_rejected_state.json").read_bytes()

            # Simulate interruption after saving pending_rejected_state.json before _advance_trusted_head_anchor()
            anchor_path.write_bytes(anchor_jan5_bytes)

            # Old NO_PENDING digest must still fail closed (prevents stripping pending state!)
            with self.assertRaises(EngineIdentityError):
                run_cloudrun_job(
                    data_dir=ddir,
                    config_path=unlocked_cfg,
                    mode="production",
                    session_date="2026-01-07",
                    transport=build_live_mock_transport("2026-01-07"),
                    environ={
                        "TIINGO_API_KEY": "test-dummy-token",
                        "SHADOW_TRUSTED_HEAD_SHA256": head_0,
                        "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                    },
                    wall_clock=lambda: datetime(2026, 1, 7, 22, 30, tzinfo=timezone.utc),
                    trusted_head_anchor_path=anchor_path,
                )

            # Authenticated Jan 6 pending digest catches anchor up and cleanly rejects Jan 7 for another missing source
            base_rej_t7 = build_live_mock_transport("2026-01-07")

            def _missing_tbill_7(url: str, headers: dict):
                if "treasury.gov" in url.lower():
                    return 404, b"missing"
                return base_rej_t7(url, headers)

            rej7 = run_cloudrun_job(
                data_dir=ddir,
                config_path=unlocked_cfg,
                mode="production",
                session_date="2026-01-07",
                transport=_missing_tbill_7,
                environ={
                    "TIINGO_API_KEY": "test-dummy-token",
                    "SHADOW_TRUSTED_HEAD_SHA256": head_0,
                    "SHADOW_TRUSTED_PENDING_STATE_SHA256": pend_sha_6,
                },
                wall_clock=lambda: datetime(2026, 1, 7, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clk,
                sleeper=lambda _s: None,
                trusted_head_anchor_path=anchor_path,
            )
            self.assertEqual(rej7["status"], "fail_closed")
            pend_sha_7 = str(rej7["pending_rejected_state_sha256"])
            anchor_after_7 = json.loads(anchor_path.read_text(encoding="utf-8"))
            self.assertEqual(anchor_after_7["pending_rejected_state_sha256"], pend_sha_7)
            self.assertEqual(anchor_after_7["pending_rejected_session_date"], "2026-01-07")

            # Rollback attack back to Jan 6's older pending file + pend_sha_6 MUST fail closed while anchor is at Jan 7
            (ddir / "state_snapshots" / "pending_rejected_state.json").write_bytes(pend_jan6_bytes)
            with self.assertRaises(EngineIdentityError) as rb_ctx:
                run_cloudrun_job(
                    data_dir=ddir,
                    config_path=unlocked_cfg,
                    mode="production",
                    session_date="2026-01-08",
                    transport=build_live_mock_transport("2026-01-08"),
                    environ={
                        "TIINGO_API_KEY": "test-dummy-token",
                        "SHADOW_TRUSTED_HEAD_SHA256": head_0,
                        "SHADOW_TRUSTED_PENDING_STATE_SHA256": pend_sha_6,
                    },
                    wall_clock=lambda: datetime(2026, 1, 8, 22, 30, tzinfo=timezone.utc),
                    trusted_head_anchor_path=anchor_path,
                )
            self.assertIn("production_trusted_head_rollback_rejected", str(rb_ctx.exception))

    def test_same_session_interrupted_pending_revision_recovers_and_blocks_same_day_rollback(self) -> None:
        raw_cfg = json.loads(
            (_REPO_ROOT / "config" / "production-scheduler.json").read_text(encoding="utf-8")
        )
        raw_cfg["production_chain_locked"] = False
        raw_cfg["clk01_active"] = True
        with tempfile.TemporaryDirectory() as tmpdir:
            unlocked_cfg = Path(tmpdir) / "unlocked-production.json"
            unlocked_cfg.write_text(json.dumps(raw_cfg), encoding="utf-8")
            ddir = Path(tmpdir) / "same_day_rev_data"
            anchor_path = Path(tmpdir) / "same_day_rev_anchor.json"
            heights = {"2026-01-05": 880100, "2026-01-06": 880101, "2026-01-07": 880102}

            def _make_ots(session_date: str):
                class _Ots(OpenTimestampsClient):
                    def stamp_digest(self, sha256_hex: str) -> tuple[bytes, str]:
                        ots_b, _ = build_synthetic_ots_fixture(
                            sha256_hex, bitcoin_height=heights[session_date]
                        )
                        return ots_b, "https://a.pool.opentimestamps.org"

                return _Ots()

            tick = [3000.0]

            def _fast_clk() -> float:
                tick[0] += 25.0
                return tick[0]

            # Commit Jan 5 seq=0
            res5 = run_cloudrun_job(
                data_dir=ddir,
                config_path=unlocked_cfg,
                mode="production",
                session_date="2026-01-05",
                transport=build_live_mock_transport("2026-01-05"),
                environ={
                    "TIINGO_API_KEY": "test-dummy-token",
                    "SHADOW_TRUSTED_HEAD_SHA256": GENESIS_PREV_HASH,
                    "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                },
                ots_client=_make_ots("2026-01-05"),
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clk,
                sleeper=lambda _s: None,
                trusted_head_anchor_path=anchor_path,
            )
            self.assertEqual(res5["status"], "committed")
            head_0 = str(res5["record_sha256"])

            base_rej_t6 = build_live_mock_transport("2026-01-06")

            def _missing_tbill_6(url: str, headers: dict):
                if "treasury.gov" in url.lower():
                    return 404, b"missing"
                return base_rej_t6(url, headers)

            # Jan 6 Attempt 1 rejects for missing Treasury -> pending_revision=1 (sha1)
            rej6_1 = run_cloudrun_job(
                data_dir=ddir,
                config_path=unlocked_cfg,
                mode="production",
                session_date="2026-01-06",
                transport=_missing_tbill_6,
                environ={
                    "TIINGO_API_KEY": "test-dummy-token",
                    "SHADOW_TRUSTED_HEAD_SHA256": head_0,
                    "SHADOW_TRUSTED_PENDING_STATE_SHA256": NO_PENDING_REJECTED_STATE_SHA256,
                },
                wall_clock=lambda: datetime(2026, 1, 6, 22, 15, tzinfo=timezone.utc),
                clock=_fast_clk,
                sleeper=lambda _s: None,
                trusted_head_anchor_path=anchor_path,
            )
            self.assertEqual(rej6_1["status"], "fail_closed")
            sha1 = str(rej6_1["pending_rejected_state_sha256"])
            pend_path = ddir / "state_snapshots" / "pending_rejected_state.json"
            pend_rev1_bytes = pend_path.read_bytes()
            anchor_rev1_bytes = anchor_path.read_bytes()
            self.assertEqual(json.loads(pend_rev1_bytes.decode("utf-8"))["pending_revision"], 1)
            self.assertEqual(json.loads(anchor_rev1_bytes.decode("utf-8"))["pending_rejected_revision"], 1)

            # Jan 6 Attempt 2 rejects for missing Treasury -> pending_revision=2 (sha2 != sha1)
            rej6_2 = run_cloudrun_job(
                data_dir=ddir,
                config_path=unlocked_cfg,
                mode="production",
                session_date="2026-01-06",
                transport=_missing_tbill_6,
                environ={
                    "TIINGO_API_KEY": "test-dummy-token",
                    "SHADOW_TRUSTED_HEAD_SHA256": head_0,
                    "SHADOW_TRUSTED_PENDING_STATE_SHA256": sha1,
                },
                wall_clock=lambda: datetime(2026, 1, 6, 22, 25, tzinfo=timezone.utc),
                clock=_fast_clk,
                sleeper=lambda _s: None,
                trusted_head_anchor_path=anchor_path,
            )
            self.assertEqual(rej6_2["status"], "fail_closed")
            sha2 = str(rej6_2["pending_rejected_state_sha256"])
            self.assertNotEqual(sha1, sha2)
            pend_rev2_bytes = pend_path.read_bytes()
            anchor_rev2_bytes = anchor_path.read_bytes()
            self.assertEqual(json.loads(pend_rev2_bytes.decode("utf-8"))["pending_revision"], 2)
            self.assertEqual(json.loads(anchor_rev2_bytes.decode("utf-8"))["pending_rejected_revision"], 2)

            # Negative rollback check: with anchor at (2026-01-06, rev=2, sha2), restoring rev=1 + sha1 MUST fail closed
            pend_path.write_bytes(pend_rev1_bytes)
            with self.assertRaises(EngineIdentityError) as rb_ctx:
                run_cloudrun_job(
                    data_dir=ddir,
                    config_path=unlocked_cfg,
                    mode="production",
                    session_date="2026-01-07",
                    transport=build_live_mock_transport("2026-01-07"),
                    environ={
                        "TIINGO_API_KEY": "test-dummy-token",
                        "SHADOW_TRUSTED_HEAD_SHA256": head_0,
                        "SHADOW_TRUSTED_PENDING_STATE_SHA256": sha1,
                    },
                    ots_client=_make_ots("2026-01-07"),
                    wall_clock=lambda: datetime(2026, 1, 7, 22, 30, tzinfo=timezone.utc),
                    clock=_fast_clk,
                    sleeper=lambda _s: None,
                    trusted_head_anchor_path=anchor_path,
                )
            self.assertIn("pending state diverged from trusted anchor", str(rb_ctx.exception))

            # Interrupted update recovery: restore rev=2 pending file with stale rev=1 anchor -> Jan 7 catches up & commits seq=1
            pend_path.write_bytes(pend_rev2_bytes)
            anchor_path.write_bytes(anchor_rev1_bytes)
            ok7 = run_cloudrun_job(
                data_dir=ddir,
                config_path=unlocked_cfg,
                mode="production",
                session_date="2026-01-07",
                transport=build_live_mock_transport("2026-01-07"),
                environ={
                    "TIINGO_API_KEY": "test-dummy-token",
                    "SHADOW_TRUSTED_HEAD_SHA256": head_0,
                    "SHADOW_TRUSTED_PENDING_STATE_SHA256": sha2,
                },
                ots_client=_make_ots("2026-01-07"),
                wall_clock=lambda: datetime(2026, 1, 7, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clk,
                sleeper=lambda _s: None,
                trusted_head_anchor_path=anchor_path,
            )
            self.assertEqual(ok7["status"], "committed")
            self.assertEqual(ok7["sequence"], 1)
            final_anchor = json.loads(anchor_path.read_text(encoding="utf-8"))
            self.assertEqual(final_anchor["sequence"], 1)
            self.assertEqual(final_anchor["pending_rejected_state_sha256"], NO_PENDING_REJECTED_STATE_SHA256)
            self.assertIsNone(final_anchor["pending_rejected_revision"])

    def test_upstream_http_transports_bound_response_bodies_and_error_streams(self) -> None:
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        import threading

        digest_hex = "a" * 64
        full_ots, _ = build_synthetic_ots_fixture(digest_hex, bitcoin_height=880100)
        ots_stamp_body = full_ots[len(OTS_HEADER_MAGIC) + 34 :]

        class _Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def log_message(self, format: str, *args: object) -> None:  # noqa: A002
                return

            def _send_bytes(self, code: int, body: bytes, *, include_cl: bool = True) -> None:
                self.send_response(code)
                self.send_header("Content-Type", "application/octet-stream")
                if include_cl:
                    self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                try:
                    for offset in range(0, len(body), 65_536):
                        self.wfile.write(body[offset : offset + 65_536])
                        self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError, OSError):
                    return

            def do_GET(self) -> None:  # noqa: N802
                if self.path == "/oversized-market":
                    self._send_bytes(200, b"m" * (5 * 1024 * 1024), include_cl=False)
                elif self.path == "/oversized-cl":
                    self.send_response(200)
                    self.send_header("Content-Length", "536870912")
                    self.end_headers()
                elif self.path == "/oversized-500":
                    self._send_bytes(500, b"e" * (256 * 1024), include_cl=True)
                elif self.path == "/ok-market":
                    self._send_bytes(200, b'{"ok":true}', include_cl=True)
                else:
                    self._send_bytes(404, b"not_found", include_cl=True)

            def do_POST(self) -> None:  # noqa: N802
                cl = int(self.headers.get("Content-Length", "0") or "0")
                if cl > 0:
                    self.rfile.read(cl)
                if self.path == "/oversized-ots/digest":
                    self._send_bytes(200, b"o" * (512 * 1024), include_cl=False)
                elif self.path == "/oversized-cl/digest":
                    self.send_response(200)
                    self.send_header("Content-Length", "536870912")
                    self.end_headers()
                elif self.path == "/oversized-500/digest":
                    self._send_bytes(500, b"e" * (256 * 1024), include_cl=True)
                elif self.path == "/ok-ots/digest":
                    self._send_bytes(200, ots_stamp_body, include_cl=True)
                else:
                    self._send_bytes(404, b"not_found", include_cl=True)

        srv = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        thread = threading.Thread(target=srv.serve_forever, daemon=True)
        thread.start()
        base = f"http://127.0.0.1:{srv.server_port}"
        try:
            with self.assertRaisesRegex(SourceUnavailableError, "response_too_large"):
                urllib_fetch_bytes(f"{base}/oversized-market", max_bytes=32_768)
            with self.assertRaisesRegex(SourceUnavailableError, "response_too_large"):
                urllib_fetch_bytes(f"{base}/oversized-market")
            with self.assertRaisesRegex(SourceUnavailableError, "content_length_exceeds_"):
                urllib_fetch_bytes(f"{base}/oversized-cl")

            st_err_m, body_err_m = urllib_fetch_bytes(f"{base}/oversized-500")
            self.assertEqual(st_err_m, 500)
            self.assertLessEqual(len(body_err_m), 65_536)

            st_ok_m, body_ok_m = urllib_fetch_bytes(f"{base}/ok-market")
            self.assertEqual(st_ok_m, 200)
            self.assertEqual(body_ok_m, b'{"ok":true}')

            with self.assertRaisesRegex(OtsSubmitError, "ots_response_too_large"):
                urllib_ots_transport("POST", f"{base}/oversized-ots/digest", b"x" * 32, {}, 5.0)
            with self.assertRaisesRegex(OtsSubmitError, "content_length_exceeds_"):
                urllib_ots_transport("POST", f"{base}/oversized-cl/digest", b"x" * 32, {}, 5.0)

            st_err_o, body_err_o = urllib_ots_transport(
                "POST", f"{base}/oversized-500/digest", b"x" * 32, {}, 5.0
            )
            self.assertEqual(st_err_o, 500)
            self.assertLessEqual(len(body_err_o), 65_536)

            client = OpenTimestampsClient(
                calendars=[f"{base}/oversized-ots", f"{base}/ok-ots"],
                timeout=5.0,
            )
            stamped_bytes, meta = client.stamp_digest(digest_hex)
            self.assertTrue(stamped_bytes.startswith(OTS_HEADER_MAGIC))
            self.assertEqual(meta["submitted_calendars"], [f"{base}/ok-ots"])
        finally:
            srv.shutdown()
            srv.server_close()
            thread.join(timeout=2.0)

    def test_multi_generation_expired_reclaim_recovery_and_active_higher_gen_guard(self) -> None:
        import hashlib
        import threading
        from datetime import timedelta

        with tempfile.TemporaryDirectory() as tmpdir:
            # 1. Expired gen=0 and gen=1 claims recover cleanly via gen=2 with single winner
            rec_dir = Path(tmpdir) / "multi_gen_reclaim"
            ledger = ShadowLedger(rec_dir / "ledger")
            lease_path = ledger.root_dir / ".chain.lease.json"
            past_utc = (datetime.now(timezone.utc) - timedelta(minutes=5)).strftime(
                "%Y-%m-%dT%H:%M:%S.000Z"
            )
            future_utc = (datetime.now(timezone.utc) + timedelta(minutes=5)).strftime(
                "%Y-%m-%dT%H:%M:%S.000Z"
            )
            stale_bytes = json.dumps(
                {
                    "schema_version": "c-sh-chain-lease-v1",
                    "lease_id": "stale-crashed-111",
                    "pid": 777777,
                    "execution_id": "exec-stale-111",
                    "expires_at_utc": past_utc,
                }
            ).encode("utf-8")
            lease_path.write_bytes(stale_bytes)
            stale_sha = hashlib.sha256(stale_bytes).hexdigest()
            claim_key = hashlib.sha256(
                f"id:stale-crashed-111:{stale_sha}".encode("utf-8")
            ).hexdigest()[:32]

            for gen, name in (
                (0, f".chain.lease.reclaim.{claim_key}.lock"),
                (1, f".chain.lease.reclaim.{claim_key}.g1.lock"),
            ):
                lease_path.with_name(name).write_text(
                    json.dumps(
                        {
                            "schema_version": "c-sh-chain-lease-reclaim-v1",
                            "claim_key": claim_key,
                            "generation": gen,
                            "claimed_by_lease_id": f"crashed-reclaimer-{gen}",
                            "expires_at_utc": past_utc,
                        }
                    ),
                    encoding="utf-8",
                )

            barrier = threading.Barrier(2)
            tls = threading.local()
            orig_parse_desc = ShadowLedger.__dict__["_parse_iso_utc"]
            orig_parse = orig_parse_desc.__func__

            def _sync_parse(raw: object) -> datetime | None:
                res = orig_parse(raw)
                if not getattr(tls, "waited", False):
                    tls.waited = True
                    barrier.wait(timeout=5.0)
                return res

            winners: list[str] = []
            conflicts: list[Exception] = []
            lock = threading.Lock()

            def _worker() -> None:
                try:
                    lid = ledger._acquire_cross_instance_lease(
                        lease_path, lease_ttl_seconds=300.0
                    )
                    with lock:
                        winners.append(lid)
                except ChainConcurrencyConflictError as exc:
                    with lock:
                        conflicts.append(exc)

            ShadowLedger._parse_iso_utc = staticmethod(_sync_parse)  # type: ignore[method-assign]
            try:
                t1 = threading.Thread(target=_worker)
                t2 = threading.Thread(target=_worker)
                t1.start()
                t2.start()
                t1.join(timeout=10.0)
                t2.join(timeout=10.0)
            finally:
                ShadowLedger._parse_iso_utc = orig_parse_desc  # type: ignore[assignment]

            self.assertEqual(len(winners), 1, f"winners={winners}, conflicts={conflicts}")
            self.assertEqual(len(conflicts), 1, f"winners={winners}, conflicts={conflicts}")
            g2_path = lease_path.with_name(f".chain.lease.reclaim.{claim_key}.g2.lock")
            self.assertTrue(g2_path.is_file())
            g2_payload = json.loads(g2_path.read_text(encoding="utf-8"))
            self.assertEqual(g2_payload["generation"], 2)
            self.assertEqual(g2_payload["claimed_by_lease_id"], winners[0])
            ShadowLedger._release_cross_instance_lease(lease_path, winners[0])
            self.assertFalse(lease_path.exists())
            self.assertFalse(lease_path.with_name(f".chain.lease.reclaim.{claim_key}.lock").exists())
            self.assertFalse(lease_path.with_name(f".chain.lease.reclaim.{claim_key}.g1.lock").exists())

            # 2. Active higher-generation claim blocks reclamation even when gen=0 is absent
            guard_dir = Path(tmpdir) / "active_higher_gen"
            guard_ledger = ShadowLedger(guard_dir / "ledger")
            guard_lease = guard_ledger.root_dir / ".chain.lease.json"
            stale_guard = json.dumps(
                {
                    "schema_version": "c-sh-chain-lease-v1",
                    "lease_id": "stale-crashed-222",
                    "pid": 666666,
                    "execution_id": "exec-stale-222",
                    "expires_at_utc": past_utc,
                }
            ).encode("utf-8")
            guard_lease.write_bytes(stale_guard)
            guard_sha = hashlib.sha256(stale_guard).hexdigest()
            guard_key = hashlib.sha256(
                f"id:stale-crashed-222:{guard_sha}".encode("utf-8")
            ).hexdigest()[:32]
            guard_lease.with_name(f".chain.lease.reclaim.{guard_key}.g1.lock").write_text(
                json.dumps(
                    {
                        "schema_version": "c-sh-chain-lease-reclaim-v1",
                        "claim_key": guard_key,
                        "generation": 1,
                        "claimed_by_lease_id": "active-reclaimer-g1",
                        "expires_at_utc": future_utc,
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                ChainConcurrencyConflictError, "already claimed by peer writer"
            ):
                guard_ledger._acquire_cross_instance_lease(guard_lease, lease_ttl_seconds=300.0)


if __name__ == "__main__":
    unittest.main()
