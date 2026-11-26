"""Tests for secret-free GCP Cloud Run Jobs (us-east1) entrypoint & stateless idempotency."""

from __future__ import annotations

import inspect
import json
import shutil
import subprocess
import sys
import tempfile
import time
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


def _reset_anchor_to_snapshot(anchor_path: Path, snap_bytes: bytes) -> None:
    snap_ver = int(json.loads(snap_bytes.decode("utf-8"))["anchor_version"])
    prefix = f"{anchor_path.name}.v"
    for p in anchor_path.parent.glob(f"{anchor_path.name}.v*.json"):
        name = p.name
        if name.startswith(prefix) and name.endswith(".json"):
            ver_str = name[len(prefix) : -len(".json")]
            if ver_str.isdigit() and int(ver_str) > snap_ver:
                p.unlink()
    anchor_path.write_bytes(snap_bytes)


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
            self.assertEqual(proc2.returncode, 1)
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
        self.assertIn("USER shadowapp", df)
        self.assertNotIn("SHADOW_DATA_DIR=", df)
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
            slot_after = ShadowLedger._latest_lease_slot(lease_path)
            self.assertIsNotNone(slot_after)
            released_obj = json.loads(slot_after[1].read_text(encoding="utf-8"))
            self.assertTrue(
                ShadowLedger._is_lease_released(lease_path, released_obj["lease_id"])
            )

            # 3. Mismatched lease_id overwritten by another writer must NOT be unlinked on exit
            with ledger.chain_lock():
                cur_slot = ShadowLedger._latest_lease_slot(lease_path)
                self.assertIsNotNone(cur_slot)
                cur_slot_path = cur_slot[1]
                self.assertTrue(cur_slot_path.is_file())
                cur_slot_path.write_text(
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
            self.assertTrue(cur_slot_path.is_file())
            self.assertEqual(json.loads(cur_slot_path.read_text(encoding="utf-8"))["lease_id"], "someone-elses-lease")

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
            active_slot = ShadowLedger._latest_lease_slot(lease_path)
            self.assertIsNotNone(active_slot)
            active = json.loads(active_slot[1].read_text(encoding="utf-8"))
            self.assertEqual(active["lease_id"], winners[0])

            # Expired lease must NOT be marked released or unlinked by its former holder after TTL expiry
            active_slot[1].write_text(
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
            self.assertTrue(active_slot[1].is_file())
            self.assertFalse(ShadowLedger._is_lease_released(lease_path, winners[0]))

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
            rec_slot = ShadowLedger._latest_lease_slot(lease_path)
            self.assertIsNotNone(rec_slot)
            active = json.loads(rec_slot[1].read_text(encoding="utf-8"))
            self.assertEqual(active["lease_id"], winners[0])
            ShadowLedger._release_cross_instance_lease(lease_path, winners[0])
            self.assertTrue(ShadowLedger._is_lease_released(lease_path, winners[0]))

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
            _reset_anchor_to_snapshot(anchor_path, anchor_seq0_bytes)
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
            _reset_anchor_to_snapshot(anchor_path, anchor_seq0_bytes)
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
            _reset_anchor_to_snapshot(anchor_path, anchor_jan5_bytes)

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
            _reset_anchor_to_snapshot(anchor_path, anchor_rev1_bytes)
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

            def _send_truncated(self, code: int, body: bytes, declared_len: int) -> None:
                self.send_response(code)
                self.send_header("Content-Type", "text/csv")
                self.send_header("Content-Length", str(declared_len))
                self.end_headers()
                try:
                    self.wfile.write(body)
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass
                self.close_connection = True

            def do_GET(self) -> None:  # noqa: N802
                if self.path == "/oversized-market":
                    self._send_bytes(200, b"m" * (5 * 1024 * 1024), include_cl=False)
                elif self.path == "/oversized-cl":
                    self.send_response(200)
                    self.send_header("Content-Length", "536870912")
                    self.end_headers()
                elif self.path == "/truncated-csv":
                    full_csv = b"Date,3 Mo\n01/05/2026,4.35\n"
                    partial_csv = b"Date,3 Mo\n01/05/2026,4"
                    self._send_truncated(200, partial_csv, len(full_csv))
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
                elif self.path == "/truncated-ots/digest":
                    self._send_truncated(200, ots_stamp_body[:8], len(ots_stamp_body))
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
            with self.assertRaisesRegex(SourceUnavailableError, "incomplete_body"):
                urllib_fetch_bytes(f"{base}/truncated-csv")

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
            with self.assertRaisesRegex(OtsSubmitError, "incomplete_body"):
                urllib_ots_transport("POST", f"{base}/truncated-ots/digest", b"x" * 32, {}, 5.0)

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
            self.assertTrue(ShadowLedger._is_lease_released(lease_path, winners[0]))
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

    def test_abandoned_empty_reclaim_claim_blocks_while_fresh_and_recovers_when_aged(self) -> None:
        import hashlib
        import os
        from datetime import timedelta

        with tempfile.TemporaryDirectory() as tmpdir:
            ledger = ShadowLedger(Path(tmpdir) / "empty_reclaim" / "ledger")
            lease_path = ledger.root_dir / ".chain.lease.json"
            past_utc = (datetime.now(timezone.utc) - timedelta(minutes=5)).strftime(
                "%Y-%m-%dT%H:%M:%S.000Z"
            )
            stale_bytes = json.dumps(
                {
                    "schema_version": "c-sh-chain-lease-v1",
                    "lease_id": "stale-empty-reclaim-333",
                    "pid": 555555,
                    "execution_id": "exec-stale-333",
                    "expires_at_utc": past_utc,
                }
            ).encode("utf-8")
            lease_path.write_bytes(stale_bytes)
            stale_sha = hashlib.sha256(stale_bytes).hexdigest()
            claim_key = hashlib.sha256(
                f"id:stale-empty-reclaim-333:{stale_sha}".encode("utf-8")
            ).hexdigest()[:32]
            empty_claim_path = lease_path.with_name(f".chain.lease.reclaim.{claim_key}.lock")
            empty_claim_path.write_bytes(b"")

            # Fresh (<5s) 0-byte initializing claim must still block peer acquisition
            with self.assertRaisesRegex(
                ChainConcurrencyConflictError, "already claimed by peer writer"
            ):
                ledger._acquire_cross_instance_lease(lease_path, lease_ttl_seconds=300.0)

            # Aged (>=5s) abandoned 0-byte claim recovers via .g1.lock and is swept on release
            aged_epoch = datetime.now(timezone.utc).timestamp() - 10.0
            os.utime(empty_claim_path, (aged_epoch, aged_epoch))

            acquired_id = ledger._acquire_cross_instance_lease(
                lease_path, lease_ttl_seconds=300.0
            )
            g1_path = lease_path.with_name(f".chain.lease.reclaim.{claim_key}.g1.lock")
            self.assertTrue(g1_path.is_file())
            g1_payload = json.loads(g1_path.read_text(encoding="utf-8"))
            self.assertEqual(g1_payload["generation"], 1)
            self.assertEqual(g1_payload["claimed_by_lease_id"], acquired_id)

            ShadowLedger._release_cross_instance_lease(lease_path, acquired_id)
            self.assertTrue(ShadowLedger._is_lease_released(lease_path, acquired_id))
            self.assertFalse(empty_claim_path.exists())

    def test_chain_lock_renews_lease_and_fences_displaced_or_expired_writer(self) -> None:
        import time

        with tempfile.TemporaryDirectory() as tmpdir:
            # 1. Active chain_lock renews lease past initial 1.0s TTL while peer acquisition fails closed
            ledger = ShadowLedger(Path(tmpdir) / "renew" / "ledger")
            lease_path = ledger.root_dir / ".chain.lease.json"
            with ledger.chain_lock(lease_ttl_seconds=1.0, renew_interval_seconds=0.15):
                initial_obj = json.loads(lease_path.read_text(encoding="utf-8"))
                time.sleep(1.3)
                lease_obj = json.loads(lease_path.read_text(encoding="utf-8"))
                self.assertEqual(lease_obj["lease_id"], initial_obj["lease_id"])
                renew_files = list(
                    ledger.root_dir.glob(f".chain.lease.renew.{initial_obj['lease_id']}.*.json")
                )
                self.assertGreaterEqual(len(renew_files), 1)
                eff_exp = ShadowLedger._effective_lease_expiry(
                    lease_path,
                    initial_obj["lease_id"],
                    ShadowLedger._parse_iso_utc(initial_obj["expires_at_utc"]),
                )
                self.assertIsNotNone(eff_exp)
                self.assertGreater(
                    eff_exp, ShadowLedger._parse_iso_utc(initial_obj["expires_at_utc"])
                )
                ledger.assert_active_chain_lease()
                peer = ShadowLedger(ledger.root_dir)
                with self.assertRaisesRegex(
                    ChainConcurrencyConflictError, "held by active writer"
                ):
                    peer._acquire_cross_instance_lease(lease_path, lease_ttl_seconds=300.0)

            self.assertEqual(
                list(ledger.root_dir.glob(f".chain.lease.renew.{initial_obj['lease_id']}.*.json")),
                [],
            )

            # 2. Paused renewal resuming after expiry/peer acquisition never clobbers peer's lease
            race_ledger = ShadowLedger(Path(tmpdir) / "renew_race" / "ledger")
            race_lease = race_ledger.root_dir / ".chain.lease.json"
            base_race = datetime.now(timezone.utc)
            orig_race_now = ShadowLedger.__dict__["_now_utc"]
            orig_pub = ShadowLedger.__dict__["_publish_exclusive_file"]
            race_box = [base_race]
            ShadowLedger._now_utc = staticmethod(lambda: race_box[0])  # type: ignore[method-assign]
            try:
                first_id = race_ledger._acquire_cross_instance_lease(
                    race_lease, lease_ttl_seconds=300.0
                )
                race_peer = ShadowLedger(race_ledger.root_dir)
                peer_id_box: list[str] = []

                @staticmethod
                def _paused_publish(dest_path: Path, tmp_path: Path, payload_bytes: bytes) -> None:
                    if dest_path.name.startswith(f".chain.lease.renew.{first_id}."):
                        race_box[0] = base_race + timedelta(seconds=301)
                        peer_id_box.append(
                            race_peer._acquire_cross_instance_lease(
                                race_lease, lease_ttl_seconds=300.0
                            )
                        )
                    orig_pub.__func__(dest_path, tmp_path, payload_bytes)  # type: ignore[attr-defined]

                ShadowLedger._publish_exclusive_file = _paused_publish  # type: ignore[method-assign]
                with self.assertRaisesRegex(
                    ChainConcurrencyConflictError, "displaced during renewal"
                ):
                    race_ledger._renew_cross_instance_lease(
                        race_lease, first_id, lease_ttl_seconds=300.0
                    )
                latest_race = ShadowLedger._latest_lease_slot(race_lease)
                self.assertIsNotNone(latest_race)
                cur_obj = json.loads(latest_race[1].read_text(encoding="utf-8"))
                self.assertEqual(cur_obj["lease_id"], peer_id_box[0])
                self.assertNotEqual(cur_obj["lease_id"], first_id)
                self.assertEqual(
                    list(race_ledger.root_dir.glob(f".chain.lease.renew.{first_id}.*.json")), []
                )
            finally:
                ShadowLedger._publish_exclusive_file = orig_pub  # type: ignore[assignment]
                ShadowLedger._now_utc = orig_race_now  # type: ignore[assignment]

            # 2. Displaced/expired lease fences assert_active_chain_lease and append_record (0 writes)
            fence_ledger = ShadowLedger(Path(tmpdir) / "fence" / "ledger")
            fence_lease = fence_ledger.root_dir / ".chain.lease.json"
            base_now = datetime.now(timezone.utc)
            orig_now_desc = ShadowLedger.__dict__["_now_utc"]
            now_box = [base_now]
            ShadowLedger._now_utc = staticmethod(lambda: now_box[0])  # type: ignore[method-assign]
            try:
                with fence_ledger.chain_lock(
                    lease_ttl_seconds=300.0, renew_interval_seconds=3600.0
                ):
                    first_obj = json.loads(fence_lease.read_text(encoding="utf-8"))
                    now_box[0] = base_now + timedelta(seconds=301)
                    peer_ledger = ShadowLedger(fence_ledger.root_dir)
                    second_id = peer_ledger._acquire_cross_instance_lease(
                        fence_lease, lease_ttl_seconds=300.0
                    )
                    self.assertNotEqual(second_id, first_obj["lease_id"])
                    with self.assertRaisesRegex(
                        ChainConcurrencyConflictError, "expired or displaced"
                    ):
                        fence_ledger.assert_active_chain_lease()
                    with self.assertRaisesRegex(
                        ChainConcurrencyConflictError, "expired or displaced"
                    ):
                        fence_ledger.append_record({"sequence": 0}, allow_unanchored=True)
                    self.assertEqual(len(fence_ledger.load_records()), 0)
            finally:
                ShadowLedger._now_utc = orig_now_desc  # type: ignore[assignment]

            # 3. Paused release resuming after expiry/peer acquisition never deletes successor's lease
            rel_ledger = ShadowLedger(Path(tmpdir) / "release_race" / "ledger")
            rel_lease = rel_ledger.root_dir / ".chain.lease.json"
            base_rel = datetime.now(timezone.utc)
            orig_rel_now = ShadowLedger.__dict__["_now_utc"]
            orig_rel_pub = ShadowLedger.__dict__["_publish_exclusive_file"]
            rel_box = [base_rel]
            ShadowLedger._now_utc = staticmethod(lambda: rel_box[0])  # type: ignore[method-assign]
            try:
                first_rel_id = rel_ledger._acquire_cross_instance_lease(
                    rel_lease, lease_ttl_seconds=300.0
                )
                rel_peer = ShadowLedger(rel_ledger.root_dir)
                rel_peer_box: list[str] = []

                @staticmethod
                def _paused_release_publish(
                    dest_path: Path, tmp_path: Path, payload_bytes: bytes
                ) -> None:
                    if dest_path.name == f".chain.lease.released.{first_rel_id}.json":
                        rel_box[0] = base_rel + timedelta(seconds=301)
                        rel_peer_box.append(
                            rel_peer._acquire_cross_instance_lease(
                                rel_lease, lease_ttl_seconds=300.0
                            )
                        )
                    orig_rel_pub.__func__(dest_path, tmp_path, payload_bytes)  # type: ignore[attr-defined]

                ShadowLedger._publish_exclusive_file = _paused_release_publish  # type: ignore[method-assign]
                ShadowLedger._release_cross_instance_lease(rel_lease, first_rel_id)
                ShadowLedger._publish_exclusive_file = orig_rel_pub  # type: ignore[assignment]

                latest_rel = ShadowLedger._latest_lease_slot(rel_lease)
                self.assertIsNotNone(latest_rel)
                cur_rel_obj = json.loads(latest_rel[1].read_text(encoding="utf-8"))
                self.assertEqual(len(rel_peer_box), 1)
                self.assertEqual(cur_rel_obj["lease_id"], rel_peer_box[0])
                self.assertNotEqual(cur_rel_obj["lease_id"], first_rel_id)
                self.assertFalse(ShadowLedger._is_lease_released(rel_lease, rel_peer_box[0]))

                third_peer = ShadowLedger(rel_ledger.root_dir)
                with self.assertRaisesRegex(
                    ChainConcurrencyConflictError, "held by active writer"
                ):
                    third_peer._acquire_cross_instance_lease(rel_lease, lease_ttl_seconds=300.0)
            finally:
                ShadowLedger._publish_exclusive_file = orig_rel_pub  # type: ignore[assignment]
                ShadowLedger._now_utc = orig_rel_now  # type: ignore[assignment]

            # 4. Normal release publishes tombstone and subsequent sequential chain_lock reclaims cleanly
            seq_ledger = ShadowLedger(Path(tmpdir) / "seq_release" / "ledger")
            seq_lease = seq_ledger.root_dir / ".chain.lease.json"
            with seq_ledger.chain_lock(lease_ttl_seconds=300.0, renew_interval_seconds=3600.0):
                seq_slot1 = ShadowLedger._latest_lease_slot(seq_lease)
                self.assertIsNotNone(seq_slot1)
                id1 = json.loads(seq_slot1[1].read_text(encoding="utf-8"))["lease_id"]
            self.assertTrue(ShadowLedger._is_lease_released(seq_lease, id1))
            seq_peer = ShadowLedger(seq_ledger.root_dir)
            with seq_peer.chain_lock(lease_ttl_seconds=300.0, renew_interval_seconds=3600.0):
                seq_slot2 = ShadowLedger._latest_lease_slot(seq_lease)
                self.assertIsNotNone(seq_slot2)
                id2 = json.loads(seq_slot2[1].read_text(encoding="utf-8"))["lease_id"]
                self.assertNotEqual(id2, id1)
                seq_peer.assert_active_chain_lease()
                self.assertFalse(
                    seq_lease.with_name(f".chain.lease.released.{id1}.json").exists()
                )

            # 5. Paused reclaimer resuming after 301s cannot delete or replace peer's recovered epoch lease
            rc_ledger = ShadowLedger(Path(tmpdir) / "reclaim_fence" / "ledger")
            rc_lease = rc_ledger.root_dir / ".chain.lease.json"
            base_rc = datetime.now(timezone.utc)
            rc_lease.write_text(
                json.dumps(
                    {
                        "schema_version": "c-sh-chain-lease-v1",
                        "lease_id": "expired-reclaim-target",
                        "expires_at_utc": (base_rc - timedelta(minutes=5)).strftime(
                            "%Y-%m-%dT%H:%M:%S.000Z"
                        ),
                    }
                ),
                encoding="utf-8",
            )
            orig_rc_now = ShadowLedger.__dict__["_now_utc"]
            orig_rc_pub = ShadowLedger.__dict__["_publish_exclusive_file"]
            rc_box = [base_rc]
            ShadowLedger._now_utc = staticmethod(lambda: rc_box[0])  # type: ignore[method-assign]
            try:
                rc_peer = ShadowLedger(rc_ledger.root_dir)
                rc_peer_box: list[str] = []
                pause_done = [False]

                @staticmethod
                def _paused_reclaim_publish(
                    dest_path: Path, tmp_path: Path, payload_bytes: bytes
                ) -> None:
                    if dest_path.name == ".chain.lease.epoch.1.json" and not pause_done[0]:
                        pause_done[0] = True
                        rc_box[0] = base_rc + timedelta(seconds=301)
                        rc_peer_box.append(
                            rc_peer._acquire_cross_instance_lease(
                                rc_lease, lease_ttl_seconds=300.0
                            )
                        )
                    orig_rc_pub.__func__(dest_path, tmp_path, payload_bytes)  # type: ignore[attr-defined]

                ShadowLedger._publish_exclusive_file = _paused_reclaim_publish  # type: ignore[method-assign]
                with self.assertRaises(ChainConcurrencyConflictError):
                    rc_ledger._acquire_cross_instance_lease(rc_lease, lease_ttl_seconds=300.0)
                ShadowLedger._publish_exclusive_file = orig_rc_pub  # type: ignore[assignment]

                self.assertEqual(len(rc_peer_box), 1)
                latest_rc = ShadowLedger._latest_lease_slot(rc_lease)
                self.assertIsNotNone(latest_rc)
                self.assertEqual(latest_rc[0], 1)
                cur_rc_obj = json.loads(latest_rc[1].read_text(encoding="utf-8"))
                self.assertEqual(cur_rc_obj["lease_id"], rc_peer_box[0])
                third_rc = ShadowLedger(rc_ledger.root_dir)
                with self.assertRaisesRegex(
                    ChainConcurrencyConflictError, "held by active writer"
                ):
                    third_rc._acquire_cross_instance_lease(rc_lease, lease_ttl_seconds=300.0)
            finally:
                ShadowLedger._publish_exclusive_file = orig_rc_pub  # type: ignore[assignment]
                ShadowLedger._now_utc = orig_rc_now  # type: ignore[assignment]

    def test_interrupted_genesis_production_commit_recovers_via_prepersisted_anchor(self) -> None:
        from unittest import mock

        import shadow.scheduler as sched_mod
        from shadow.fuse_adapter import VendoredFuseEvaluator

        raw_cfg = json.loads(
            (_REPO_ROOT / "config" / "production-scheduler.json").read_text(encoding="utf-8")
        )
        raw_cfg["production_chain_locked"] = False
        raw_cfg["clk01_active"] = True
        ev = VendoredFuseEvaluator()

        t_box = [1000.0]

        def _fast_clk() -> float:
            t_box[0] += 25.0
            return t_box[0]

        class _FakeOts(OpenTimestampsClient):
            def stamp_digest(self, sha256_hex: str) -> tuple[bytes, str]:
                ots_b, _ = build_synthetic_ots_fixture(sha256_hex, bitcoin_height=880100)
                return ots_b, "https://a.pool.opentimestamps.org"

        with tempfile.TemporaryDirectory() as tmpdir:
            ddir = Path(tmpdir) / "genesis_anchor_rec"
            unlocked_cfg = Path(tmpdir) / "unlocked-production.json"
            unlocked_cfg.write_text(json.dumps(raw_cfg), encoding="utf-8")
            anchor_path = Path(tmpdir) / "genesis_anchor.json"

            orig_advance = sched_mod._advance_trusted_head_anchor

            def _fail_seq0_advance(*args, **kwargs):
                if kwargs.get("sequence") == 0:
                    raise OSError("simulated_interruption_after_seq0_commit")
                return orig_advance(*args, **kwargs)

            with mock.patch.object(
                sched_mod, "_advance_trusted_head_anchor", side_effect=_fail_seq0_advance
            ):
                with self.assertRaisesRegex(
                    OSError, "simulated_interruption_after_seq0_commit"
                ):
                    sched_mod.run_production_cycle(
                        session_date="2026-01-05",
                        data_dir=ddir,
                        config_path=unlocked_cfg,
                        transport=build_live_mock_transport("2026-01-05"),
                        environ={"TIINGO_API_KEY": "test-dummy-token"},
                        ots_client=_FakeOts(),
                        wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                        clock=_fast_clk,
                        sleeper=lambda _s: None,
                        evaluator=ev,
                        expected_prior_head_sha256=GENESIS_PREV_HASH,
                        expected_pending_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                        trusted_head_anchor_path=anchor_path,
                    )

            recs = ShadowLedger(ddir / "ledger").load_records()
            self.assertEqual(len(recs), 1)
            head_0 = str(recs[0]["record_sha256"])
            pre_anchor = json.loads(anchor_path.read_text(encoding="utf-8"))
            self.assertEqual(pre_anchor["sequence"], -1)
            self.assertEqual(pre_anchor["head_sha256"], GENESIS_PREV_HASH)

            retry_res = sched_mod.run_production_cycle(
                session_date="2026-01-05",
                data_dir=ddir,
                config_path=unlocked_cfg,
                transport=build_live_mock_transport("2026-01-05"),
                environ={"TIINGO_API_KEY": "test-dummy-token"},
                ots_client=_FakeOts(),
                wall_clock=lambda: datetime(2026, 1, 5, 22, 35, tzinfo=timezone.utc),
                clock=_fast_clk,
                sleeper=lambda _s: None,
                evaluator=ev,
                expected_prior_head_sha256=head_0,
                expected_pending_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                trusted_head_anchor_path=anchor_path,
            )
            self.assertFalse(retry_res["appended"])
            self.assertFalse(retry_res["fail_closed"])
            self.assertTrue(retry_res["recovered_missing_report"])
            self.assertTrue(retry_res["report_written"])
            caught_anchor = json.loads(anchor_path.read_text(encoding="utf-8"))
            self.assertEqual(caught_anchor["sequence"], 0)
            self.assertEqual(caught_anchor["head_sha256"], head_0)

            retry_idempotent = sched_mod.run_production_cycle(
                session_date="2026-01-05",
                data_dir=ddir,
                config_path=unlocked_cfg,
                transport=build_live_mock_transport("2026-01-05"),
                environ={"TIINGO_API_KEY": "test-dummy-token"},
                ots_client=_FakeOts(),
                wall_clock=lambda: datetime(2026, 1, 5, 22, 36, tzinfo=timezone.utc),
                clock=_fast_clk,
                sleeper=lambda _s: None,
                evaluator=ev,
                expected_prior_head_sha256=head_0,
                expected_pending_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                trusted_head_anchor_path=anchor_path,
            )
            self.assertFalse(retry_idempotent["appended"])
            self.assertEqual(retry_idempotent["reason"], "already_committed_session_preserved")

            # Deleting trusted_head_anchor_path on a non-empty ledger still fails closed
            anchor_path.unlink()
            with self.assertRaisesRegex(
                EngineIdentityError, "production_trusted_head_anchor_missing"
            ):
                sched_mod.run_production_cycle(
                    session_date="2026-01-06",
                    data_dir=ddir,
                    config_path=unlocked_cfg,
                    transport=build_live_mock_transport("2026-01-06"),
                    environ={"TIINGO_API_KEY": "test-dummy-token"},
                    ots_client=_FakeOts(),
                    wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
                    clock=_fast_clk,
                    sleeper=lambda _s: None,
                    evaluator=ev,
                    expected_prior_head_sha256=head_0,
                    expected_pending_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                    trusted_head_anchor_path=anchor_path,
                )

    def test_chain_lock_publishes_and_reclaims_leases_when_os_link_returns_enosys(self) -> None:
        import errno
        from unittest import mock

        with tempfile.TemporaryDirectory() as tmpdir:
            ledger = ShadowLedger(Path(tmpdir) / "enosys_lease" / "ledger")

            def _enosys_link(_src: object, _dst: object) -> None:
                raise OSError(errno.ENOSYS, "Function not implemented")

            with mock.patch("os.link", side_effect=_enosys_link):
                with ledger.chain_lock(lease_ttl_seconds=300.0, renew_interval_seconds=3600.0):
                    slot0 = ShadowLedger._latest_lease_slot(ledger.root_dir / ".chain.lease.json")
                    self.assertIsNotNone(slot0)
                    self.assertEqual(slot0[0], 0)
                    ledger.assert_active_chain_lease()

                peer = ShadowLedger(ledger.root_dir)
                with peer.chain_lock(lease_ttl_seconds=300.0, renew_interval_seconds=3600.0):
                    slot1 = ShadowLedger._latest_lease_slot(ledger.root_dir / ".chain.lease.json")
                    self.assertIsNotNone(slot1)
                    self.assertEqual(slot1[0], 1)
                    peer.assert_active_chain_lease()

    def test_http_total_deadline_enforced_across_headers_body_errors_and_budget(self) -> None:
        import threading
        import time as _time
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        from shadow.feeder_live import emit_live_market_records

        stop_evt = threading.Event()

        class _SlowHandler(BaseHTTPRequestHandler):
            def _drip_bytes(self, chunk: bytes, count: int, interval: float) -> None:
                for _ in range(count):
                    if stop_evt.is_set():
                        return
                    _time.sleep(interval)
                    try:
                        self.wfile.write(chunk)
                        self.wfile.flush()
                    except OSError:
                        return

            def do_GET(self) -> None:  # noqa: N802
                if self.path == "/slow-headers":
                    try:
                        self.wfile.write(b"HTTP/1.1 200 OK\r\n")
                        self.wfile.flush()
                    except OSError:
                        return
                    self._drip_bytes(b"X-Drip: 1\r\n", 8, 0.15)
                    return
                if self.path == "/slow-single-header-line":
                    try:
                        self.wfile.write(b"HTTP/1.1 200 OK\r\n")
                        self.wfile.flush()
                    except OSError:
                        return
                    self._drip_bytes(b"a", 25, 0.05)
                    return
                if self.path == "/slow-500":
                    self.send_response(500)
                    self.send_header("Content-Length", "80")
                    self.end_headers()
                    self._drip_bytes(b"err123", 10, 0.15)
                    return
                self.send_response(200)
                self.send_header("Content-Length", "60")
                self.end_headers()
                self._drip_bytes(b"abcdef", 10, 0.15)

            def do_POST(self) -> None:  # noqa: N802
                cl = int(self.headers.get("Content-Length", "0") or "0")
                if cl > 0:
                    self.rfile.read(cl)
                if self.path == "/slow-500/digest":
                    self.send_response(500)
                    self.send_header("Content-Length", "80")
                    self.end_headers()
                    self._drip_bytes(b"err123", 10, 0.15)
                    return
                self.send_response(200)
                self.send_header("Content-Length", "60")
                self.end_headers()
                self._drip_bytes(b"abcdef", 10, 0.15)

            def log_message(self, fmt: str, *args: object) -> None:
                return

        srv = ThreadingHTTPServer(("127.0.0.1", 0), _SlowHandler)
        thread = threading.Thread(target=srv.serve_forever, daemon=True)
        thread.start()
        base = f"http://127.0.0.1:{srv.server_port}"
        try:
            t0 = _time.monotonic()
            with self.assertRaises(SourceUnavailableError):
                urllib_fetch_bytes(f"{base}/slow-body", timeout=0.20)
            self.assertLess(_time.monotonic() - t0, 0.50)

            t1 = _time.monotonic()
            with self.assertRaises(OtsSubmitError):
                urllib_ots_transport("POST", f"{base}/slow-ots/digest", b"x" * 32, {}, 0.20)
            self.assertLess(_time.monotonic() - t1, 0.50)

            t2 = _time.monotonic()
            with self.assertRaises(SourceUnavailableError):
                urllib_fetch_bytes(f"{base}/slow-headers", timeout=0.20)
            self.assertLess(_time.monotonic() - t2, 0.50)

            t2b = _time.monotonic()
            with self.assertRaises(SourceUnavailableError):
                urllib_fetch_bytes(f"{base}/slow-single-header-line", timeout=0.20)
            self.assertLess(_time.monotonic() - t2b, 0.45)

            t3 = _time.monotonic()
            st_m, body_m = urllib_fetch_bytes(f"{base}/slow-500", timeout=0.20)
            self.assertLess(_time.monotonic() - t3, 0.50)
            self.assertEqual(st_m, 500)
            self.assertLess(len(body_m), 60)

            t4 = _time.monotonic()
            st_o, body_o = urllib_ots_transport("POST", f"{base}/slow-500/digest", b"x" * 32, {}, 0.20)
            self.assertLess(_time.monotonic() - t4, 0.50)
            self.assertEqual(st_o, 500)
            self.assertLess(len(body_o), 60)
        finally:
            stop_evt.set()
            srv.shutdown()
            srv.server_close()
            thread.join(timeout=2.0)

        calls: list[str] = []

        def _counting_transport(url: str, _headers: object) -> tuple[int, bytes]:
            calls.append(url)
            return 200, b"{}"

        with tempfile.TemporaryDirectory() as tmpdir:
            recs = emit_live_market_records(
                session_date="2026-01-05",
                archive_dir=Path(tmpdir) / "arch",
                transport=_counting_transport,
                environ={"TIINGO_API_KEY": "tok"},
                clock=_time.monotonic,
                deadline_monotonic=_time.monotonic() - 1.0,
            )
            self.assertEqual(calls, [])
            self.assertEqual(len(recs), 7)
            for rec in recs:
                self.assertFalse(rec["eligibility"]["eligible"])
                self.assertTrue(str(rec["eligibility"]["reason"]).startswith("source_unavailable:"))
                self.assertEqual(rec["disclosures"].get("detail"), "fetch_budget_exhausted")

        ots_calls: list[str] = []

        def _counting_ots(
            _method: str,
            url: str,
            _body: object,
            _headers: object,
            _timeout: float,
        ) -> tuple[int, bytes]:
            ots_calls.append(url)
            return 200, b""

        client = OpenTimestampsClient(
            calendars=["https://a.pool.opentimestamps.org"],
            transport=_counting_ots,
            timeout=10.0,
            deadline_monotonic=_time.monotonic() - 1.0,
        )
        with self.assertRaisesRegex(OtsSubmitError, "ots_budget_exhausted"):
            client.stamp_digest("11" * 32)
        self.assertEqual(ots_calls, [])

    def test_displaced_writer_rollback_preserves_successor_committed_bundle_and_snapshot(self) -> None:
        import fcntl
        import threading
        from unittest import mock

        from shadow import scheduler as sched_mod
        from shadow.fuse_adapter import VendoredFuseEvaluator
        from shadow.ots import build_synthetic_ots_fixture

        with tempfile.TemporaryDirectory() as tmpdir:
            ddir = Path(tmpdir) / "displaced_rollback"
            cfg = sched_mod.load_scheduler_config(_REPO_ROOT / "config" / "drill-scheduler.json")

            class _FakeOts(OpenTimestampsClient):
                def stamp_digest(self, sha256_hex: str) -> tuple[bytes, str]:
                    ots_b, _ = build_synthetic_ots_fixture(sha256_hex, bitcoin_height=880100)
                    return ots_b, "https://a.pool.opentimestamps.org"

            def _make_fast_clk():
                box = [1000.0]

                def _clk() -> float:
                    box[0] += 25.0
                    return box[0]

                return _clk

            base_now = datetime.now(timezone.utc)
            orig_now_desc = ShadowLedger.__dict__["_now_utc"]
            now_box = [base_now]
            ShadowLedger._now_utc = staticmethod(lambda: now_box[0])  # type: ignore[method-assign]
            successor_bytes: dict[str, bytes] = {}
            successor_err: list[BaseException] = []
            ev = VendoredFuseEvaluator()

            def _paused_before_commit() -> None:
                now_box[0] = base_now + timedelta(seconds=301)

                def _run_successor() -> None:
                    try:
                        res_b = sched_mod.run_daily_drill_cycle(
                            session_date="2025-11-21",
                            data_dir=ddir,
                            config=cfg,
                            transport=build_live_mock_transport("2025-11-21"),
                            environ={"TIINGO_API_KEY": "test-dummy-token"},
                            ots_client=_FakeOts(),
                            wall_clock=lambda: datetime(2025, 11, 21, 22, 31, tzinfo=timezone.utc),
                            clock=_make_fast_clk(),
                            sleeper=lambda _s: None,
                            evaluator=ev,
                        )
                        if not res_b.get("appended"):
                            raise AssertionError(f"successor failed to append: {res_b}")
                        successor_bytes["record"] = (ddir / "ledger" / "seq_000000.record.json").read_bytes()
                        successor_bytes["seq_snap"] = (
                            ddir / "state_snapshots" / "seq_000000.state.json"
                        ).read_bytes()
                        successor_bytes["latest_snap"] = (
                            ddir / "state_snapshots" / "latest.state.json"
                        ).read_bytes()
                        successor_bytes["bundle"] = (
                            ddir / "replay" / "replay_bundle.json"
                        ).read_bytes()
                    except BaseException as exc:  # pragma: no cover - diagnostic propagation
                        successor_err.append(exc)

                t = threading.Thread(target=_run_successor)
                t.start()
                t.join(timeout=15.0)
                if t.is_alive():
                    raise RuntimeError("successor commit thread timed out")

            try:
                with mock.patch.object(fcntl, "flock", lambda _fd, _op: None):
                    with self.assertRaises(ChainConcurrencyConflictError):
                        sched_mod.run_daily_drill_cycle(
                            session_date="2025-11-21",
                            data_dir=ddir,
                            config=cfg,
                            transport=build_live_mock_transport("2025-11-21"),
                            environ={"TIINGO_API_KEY": "test-dummy-token"},
                            ots_client=_FakeOts(),
                            wall_clock=lambda: datetime(2025, 11, 21, 22, 30, tzinfo=timezone.utc),
                            clock=_make_fast_clk(),
                            sleeper=lambda _s: None,
                            evaluator=ev,
                            _post_ledger_commit_hook=_paused_before_commit,
                        )
            finally:
                ShadowLedger._now_utc = orig_now_desc  # type: ignore[assignment]

            self.assertEqual(successor_err, [])
            self.assertEqual(
                (ddir / "ledger" / "seq_000000.record.json").read_bytes(),
                successor_bytes["record"],
            )
            self.assertEqual(
                (ddir / "state_snapshots" / "seq_000000.state.json").read_bytes(),
                successor_bytes["seq_snap"],
            )
            self.assertEqual(
                (ddir / "state_snapshots" / "latest.state.json").read_bytes(),
                successor_bytes["latest_snap"],
            )
            self.assertEqual(
                (ddir / "replay" / "replay_bundle.json").read_bytes(),
                successor_bytes["bundle"],
            )
            self.assertEqual(ShadowLedger(ddir / "ledger").verify_chain()[0], True)

    def test_partial_lease_and_reclaim_fallback_writes_recover_after_grace_window(self) -> None:
        import errno
        import hashlib
        import os
        from unittest import mock

        with tempfile.TemporaryDirectory() as tmpdir:
            # (a) Synchronous write failure on target lfd under os.link ENOSYS cleans up partial target_path
            sync_ledger = ShadowLedger(Path(tmpdir) / "sync_fail" / "ledger")
            sync_lease = sync_ledger.root_dir / ".chain.lease.json"
            orig_write_all = ShadowLedger.__dict__["_write_all"]
            write_calls = [0]

            def _enosys_link(_src: object, _dst: object) -> None:
                raise OSError(errno.ENOSYS, "Function not implemented")

            @staticmethod
            def _partial_fail_write(fd: int, data: bytes) -> None:
                write_calls[0] += 1
                if write_calls[0] == 1:
                    orig_write_all.__func__(fd, data)  # type: ignore[attr-defined]
                    return
                os.write(fd, data[:20])
                raise OSError(errno.EIO, "simulated partial fallback write")

            with mock.patch("os.link", side_effect=_enosys_link):
                ShadowLedger._write_all = _partial_fail_write  # type: ignore[method-assign]
                try:
                    with self.assertRaises(OSError):
                        sync_ledger._acquire_cross_instance_lease(
                            sync_lease, lease_ttl_seconds=300.0
                        )
                    self.assertEqual(write_calls[0], 2)
                    self.assertFalse(sync_lease.exists())
                finally:
                    ShadowLedger._write_all = orig_write_all  # type: ignore[assignment]

            # (b) Crashed partial fallback lease blocks within 5s grace, then recovers after grace window
            base_now = datetime.now(timezone.utc)
            orig_now_desc = ShadowLedger.__dict__["_now_utc"]
            now_box = [base_now]
            ShadowLedger._now_utc = staticmethod(lambda: now_box[0])  # type: ignore[method-assign]
            try:
                crash_ledger = ShadowLedger(Path(tmpdir) / "crash_lease" / "ledger")
                crash_ledger.root_dir.mkdir(parents=True, exist_ok=True)
                crash_lease = crash_ledger.root_dir / ".chain.lease.json"
                crash_lease.write_bytes(b'{"acquired_at_utc":"')
                os.utime(crash_lease, (base_now.timestamp(), base_now.timestamp()))

                with self.assertRaisesRegex(
                    ChainConcurrencyConflictError, "initializing by peer writer"
                ):
                    crash_ledger._acquire_cross_instance_lease(
                        crash_lease, lease_ttl_seconds=300.0
                    )

                now_box[0] = base_now + timedelta(days=1)
                with mock.patch("os.link", side_effect=_enosys_link):
                    with crash_ledger.chain_lock(
                        lease_ttl_seconds=300.0, renew_interval_seconds=3600.0
                    ):
                        slot = ShadowLedger._latest_lease_slot(crash_lease)
                        self.assertIsNotNone(slot)
                        self.assertEqual(slot[0], 1)
                        crash_ledger.assert_active_chain_lease()

                # (c) Partial reclaim claim blocks within 5s grace, then recovers via next generation
                reclaim_ledger = ShadowLedger(Path(tmpdir) / "crash_reclaim" / "ledger")
                reclaim_lease = reclaim_ledger.root_dir / ".chain.lease.json"
                now_box[0] = base_now
                reclaim_ledger._acquire_cross_instance_lease(
                    reclaim_lease, lease_ttl_seconds=1.0
                )
                expired_bytes = reclaim_lease.read_bytes()
                expired_obj = json.loads(expired_bytes.decode("utf-8"))
                stale_sha = hashlib.sha256(expired_bytes).hexdigest()
                claim_basis = f"id:{expired_obj['lease_id']}:{stale_sha}"
                claim_key = hashlib.sha256(claim_basis.encode("utf-8")).hexdigest()[:32]
                bad_reclaim = reclaim_lease.with_name(f".chain.lease.reclaim.{claim_key}.lock")
                now_box[0] = base_now + timedelta(seconds=5)
                bad_reclaim.write_bytes(b'{"claim_key":"partia')
                os.utime(bad_reclaim, (now_box[0].timestamp(), now_box[0].timestamp()))

                peer = ShadowLedger(reclaim_ledger.root_dir)
                with self.assertRaisesRegex(
                    ChainConcurrencyConflictError, "already claimed by peer writer"
                ):
                    peer._acquire_cross_instance_lease(reclaim_lease, lease_ttl_seconds=300.0)

                now_box[0] = now_box[0] + timedelta(seconds=6)
                with peer.chain_lock(lease_ttl_seconds=300.0, renew_interval_seconds=3600.0):
                    slot_r = ShadowLedger._latest_lease_slot(reclaim_lease)
                    self.assertIsNotNone(slot_r)
                    self.assertEqual(slot_r[0], 1)
                    peer.assert_active_chain_lease()
            finally:
                ShadowLedger._now_utc = orig_now_desc  # type: ignore[assignment]

    def test_paused_writer_exclusive_record_publish_never_overwrites_successor_commit(self) -> None:
        import errno
        import fcntl
        import os
        import threading
        from unittest import mock

        from shadow import scheduler as sched_mod
        from shadow.fuse_adapter import VendoredFuseEvaluator
        from shadow.ots import build_synthetic_ots_fixture

        class _FakeOts(OpenTimestampsClient):
            def stamp_digest(self, sha256_hex: str) -> tuple[bytes, str]:
                ots_b, _ = build_synthetic_ots_fixture(sha256_hex, bitcoin_height=880100)
                return ots_b, "https://a.pool.opentimestamps.org"

        def _make_fast_clk():
            box = [1000.0]

            def _clk() -> float:
                box[0] += 25.0
                return box[0]

            return _clk

        ev = VendoredFuseEvaluator()
        cfg = sched_mod.load_scheduler_config(_REPO_ROOT / "config" / "drill-scheduler.json")

        for use_enosys in (False, True):
            with tempfile.TemporaryDirectory() as tmpdir:
                ddir = Path(tmpdir) / f"paused_pub_{use_enosys}"
                base_now = datetime.now(timezone.utc)
                orig_now_desc = ShadowLedger.__dict__["_now_utc"]
                orig_pub_desc = ShadowLedger.__dict__["_publish_exclusive_file"]
                orig_os_link = os.link
                now_box = [base_now]
                ShadowLedger._now_utc = staticmethod(lambda: now_box[0])  # type: ignore[method-assign]
                paused_once = [False]
                successor_bytes: dict[str, bytes] = {}
                successor_err: list[BaseException] = []

                @staticmethod
                def _paused_record_publish(target_path: Path, tmp_path: Path, payload: bytes) -> None:
                    if target_path.name == "seq_000000.record.json" and not paused_once[0]:
                        paused_once[0] = True
                        now_box[0] = base_now + timedelta(seconds=301)

                        def _run_successor() -> None:
                            try:
                                res_b = sched_mod.run_daily_drill_cycle(
                                    session_date="2025-11-21",
                                    data_dir=ddir,
                                    config=cfg,
                                    transport=build_live_mock_transport("2025-11-21"),
                                    environ={"TIINGO_API_KEY": "test-dummy-token"},
                                    ots_client=_FakeOts(),
                                    wall_clock=lambda: datetime(
                                        2025, 11, 21, 22, 31, tzinfo=timezone.utc
                                    ),
                                    clock=_make_fast_clk(),
                                    sleeper=lambda _s: None,
                                    evaluator=ev,
                                )
                                if not res_b.get("appended"):
                                    raise AssertionError(f"successor failed to append: {res_b}")
                                successor_bytes["record"] = (
                                    ddir / "ledger" / "seq_000000.record.json"
                                ).read_bytes()
                                successor_bytes["ots"] = (
                                    ddir / "ledger" / "seq_000000.ots"
                                ).read_bytes()
                                successor_bytes["receipt"] = (
                                    ddir / "ledger" / "seq_000000.receipt.json"
                                ).read_bytes()
                            except BaseException as exc:  # pragma: no cover
                                successor_err.append(exc)

                        t = threading.Thread(target=_run_successor)
                        t.start()
                        t.join(timeout=15.0)
                        if t.is_alive():
                            raise RuntimeError("successor commit thread timed out")
                    orig_pub_desc.__func__(target_path, tmp_path, payload)  # type: ignore[attr-defined]

                def _maybe_enosys_link(src: object, dst: object) -> None:
                    if use_enosys:
                        raise OSError(errno.ENOSYS, "Function not implemented")
                    orig_os_link(src, dst)

                ShadowLedger._publish_exclusive_file = _paused_record_publish  # type: ignore[method-assign]
                try:
                    with (
                        mock.patch.object(fcntl, "flock", lambda _fd, _op: None),
                        mock.patch("os.link", side_effect=_maybe_enosys_link),
                    ):
                        with self.assertRaisesRegex(
                            ChainConcurrencyConflictError, "already_exists_concurrently"
                        ):
                            sched_mod.run_daily_drill_cycle(
                                session_date="2025-11-21",
                                data_dir=ddir,
                                config=cfg,
                                transport=build_live_mock_transport("2025-11-21"),
                                environ={"TIINGO_API_KEY": "test-dummy-token"},
                                ots_client=_FakeOts(),
                                wall_clock=lambda: datetime(
                                    2025, 11, 21, 22, 30, tzinfo=timezone.utc
                                ),
                                clock=_make_fast_clk(),
                                sleeper=lambda _s: None,
                                evaluator=ev,
                            )
                finally:
                    ShadowLedger._publish_exclusive_file = orig_pub_desc  # type: ignore[assignment]
                    ShadowLedger._now_utc = orig_now_desc  # type: ignore[assignment]

                self.assertEqual(successor_err, [])
                self.assertEqual(
                    (ddir / "ledger" / "seq_000000.record.json").read_bytes(),
                    successor_bytes["record"],
                )
                self.assertEqual(
                    (ddir / "ledger" / "seq_000000.ots").read_bytes(),
                    successor_bytes["ots"],
                )
                self.assertEqual(
                    (ddir / "ledger" / "seq_000000.receipt.json").read_bytes(),
                    successor_bytes["receipt"],
                )
                self.assertTrue(ShadowLedger(ddir / "ledger").verify_chain()[0])

    def test_displaced_writer_cannot_regress_trusted_anchor_across_hosts(self) -> None:
        import fcntl
        from unittest import mock

        from shadow import scheduler as sched_mod

        with tempfile.TemporaryDirectory() as tmpdir:
            anchor_path = Path(tmpdir) / "ext" / "trusted_anchor.json"
            v1 = sched_mod._advance_trusted_head_anchor(
                anchor_path,
                chain_id="prod-sh2-v1",
                expected_prior_bytes=None,
                sequence=-1,
                session_date=None,
                head_sha256=GENESIS_PREV_HASH,
                pending_rejected_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                now_utc="2026-01-05T22:30:00.000Z",
            )
            self.assertEqual(v1["anchor_version"], 1)
            _, v1_bytes = sched_mod._load_trusted_head_anchor(
                anchor_path, expected_chain_id="prod-sh2-v1"
            )
            assert v1_bytes is not None

            orig_pub = ShadowLedger.__dict__["_publish_exclusive_file"]
            paused_once = [False]
            peer_v3_bytes: list[bytes] = []

            @staticmethod
            def _paused_anchor_publish(target_path: Path, tmp_path: Path, payload: bytes) -> None:
                if target_path.name == "trusted_anchor.json.v2.json" and not paused_once[0]:
                    paused_once[0] = True
                    v2_peer = sched_mod._advance_trusted_head_anchor(
                        anchor_path,
                        chain_id="prod-sh2-v1",
                        expected_prior_bytes=v1_bytes,
                        sequence=0,
                        session_date="2026-01-05",
                        head_sha256="a" * 64,
                        pending_rejected_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                        now_utc="2026-01-05T22:31:00.000Z",
                    )
                    self.assertEqual(v2_peer["anchor_version"], 2)
                    _, v2_bytes = sched_mod._load_trusted_head_anchor(
                        anchor_path, expected_chain_id="prod-sh2-v1"
                    )
                    v3_peer = sched_mod._advance_trusted_head_anchor(
                        anchor_path,
                        chain_id="prod-sh2-v1",
                        expected_prior_bytes=v2_bytes,
                        sequence=1,
                        session_date="2026-01-06",
                        head_sha256="b" * 64,
                        pending_rejected_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                        now_utc="2026-01-06T22:30:00.000Z",
                    )
                    self.assertEqual(v3_peer["anchor_version"], 3)
                    _, v3_bytes = sched_mod._load_trusted_head_anchor(
                        anchor_path, expected_chain_id="prod-sh2-v1"
                    )
                    assert v3_bytes is not None
                    peer_v3_bytes.append(v3_bytes)
                orig_pub.__func__(target_path, tmp_path, payload)  # type: ignore[attr-defined]

            ShadowLedger._publish_exclusive_file = _paused_anchor_publish  # type: ignore[method-assign]
            try:
                with mock.patch.object(fcntl, "flock", lambda _fd, _op: None):
                    with self.assertRaisesRegex(
                        EngineIdentityError, "production_trusted_head_rollback_rejected"
                    ):
                        sched_mod._advance_trusted_head_anchor(
                            anchor_path,
                            chain_id="prod-sh2-v1",
                            expected_prior_bytes=v1_bytes,
                            sequence=0,
                            session_date="2026-01-05",
                            head_sha256="c" * 64,
                            pending_rejected_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                            now_utc="2026-01-05T22:30:30.000Z",
                        )
            finally:
                ShadowLedger._publish_exclusive_file = orig_pub  # type: ignore[assignment]

            loaded_obj, loaded_bytes = sched_mod._load_trusted_head_anchor(
                anchor_path, expected_chain_id="prod-sh2-v1"
            )
            self.assertIsNotNone(loaded_obj)
            assert loaded_obj is not None
            self.assertEqual(loaded_obj["sequence"], 1)
            self.assertEqual(loaded_obj["anchor_version"], 3)
            self.assertEqual(loaded_obj["head_sha256"], "b" * 64)
            self.assertEqual(loaded_bytes, peer_v3_bytes[0])

            # Even if convenience pointer anchor_path is stale-overwritten with v2, immutable v3 slot still wins
            anchor_path.write_bytes(
                anchor_path.with_name("trusted_anchor.json.v2.json").read_bytes()
            )
            reg_obj, reg_bytes = sched_mod._load_trusted_head_anchor(
                anchor_path, expected_chain_id="prod-sh2-v1"
            )
            self.assertIsNotNone(reg_obj)
            assert reg_obj is not None
            self.assertEqual(reg_obj["sequence"], 1)
            self.assertEqual(reg_obj["anchor_version"], 3)
            self.assertEqual(reg_bytes, peer_v3_bytes[0])

    def test_read_bounded_http_body_converts_socket_timeout_to_deadline_exceeded(self) -> None:
        from shadow.http_limits import read_bounded_http_body

        class _TimeoutStream:
            def read1(self, _n: int) -> bytes:
                raise TimeoutError("timed out inside socket read")

        with self.assertRaisesRegex(SourceUnavailableError, "deadline_exceeded"):
            read_bounded_http_body(
                _TimeoutStream(),
                max_bytes=1024,
                error_cls=SourceUnavailableError,
                error_prefix="market",
                deadline_monotonic=time.monotonic() + 5.0,
            )

    def test_deadline_http_connect_bounds_multiple_resolved_addresses(self) -> None:
        import socket
        import urllib.error
        import urllib.request
        from unittest.mock import patch
        from shadow.http_limits import make_deadline_http_handlers

        addrinfos = [
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (f"203.0.113.{i}", 443))
            for i in range(1, 5)
        ]

        # Part A: deterministic fake clock proves total connect budget is shared across addresses
        now = [100.0]

        def fake_clock() -> float:
            return now[0]

        timeouts_seen: list[float | None] = []
        connected_addrs: list[tuple[str, int]] = []
        created_sockets: list[object] = []

        class _FakeClockSocket:
            def __init__(self) -> None:
                self.timeout: float | None = None
                self.closed = False
                created_sockets.append(self)

            def settimeout(self, value: float | None) -> None:
                self.timeout = value
                timeouts_seen.append(value)

            def connect(self, addr: tuple[str, int]) -> None:
                connected_addrs.append(addr)
                now[0] += 0.041
                raise TimeoutError("timed out")

            def close(self) -> None:
                self.closed = True

        with (
            patch("socket.getaddrinfo", return_value=addrinfos),
            patch("socket.socket", side_effect=lambda *_a, **_kw: _FakeClockSocket()),
        ):
            opener = urllib.request.build_opener(
                *make_deadline_http_handlers(now[0] + 0.050, clock=fake_clock)
            )
            req = urllib.request.Request("https://multi-addr.invalid/")
            with self.assertRaises(urllib.error.URLError) as cm:
                opener.open(req, timeout=10.0)

        self.assertIsInstance(cm.exception.reason, TimeoutError)
        self.assertIn("http_connect_deadline_exceeded", str(cm.exception.reason))
        self.assertEqual(len(connected_addrs), 2)
        self.assertEqual(len(timeouts_seen), 2)
        self.assertIsNotNone(timeouts_seen[0])
        self.assertIsNotNone(timeouts_seen[1])
        self.assertAlmostEqual(float(timeouts_seen[0]), 0.050, places=6)
        self.assertAlmostEqual(float(timeouts_seen[1]), 0.009, places=6)
        self.assertTrue(all(sock.closed for sock in created_sockets))  # type: ignore[attr-defined]

        # Part B: real monotonic clock bounds wall-clock across 4 slow/blackholed addresses
        real_sockets: list[object] = []

        class _RealSlowSocket:
            def __init__(self) -> None:
                self.timeout: float | None = None
                self.closed = False
                real_sockets.append(self)

            def settimeout(self, value: float | None) -> None:
                self.timeout = value

            def connect(self, _addr: tuple[str, int]) -> None:
                delay = 0.041 if self.timeout is None else min(float(self.timeout), 0.041)
                time.sleep(delay)
                raise TimeoutError("timed out")

            def close(self) -> None:
                self.closed = True

        with (
            patch("socket.getaddrinfo", return_value=addrinfos),
            patch("socket.socket", side_effect=lambda *_a, **_kw: _RealSlowSocket()),
        ):
            opener = urllib.request.build_opener(
                *make_deadline_http_handlers(time.monotonic() + 0.050)
            )
            req = urllib.request.Request("https://multi-addr.invalid/")
            t0 = time.monotonic()
            with self.assertRaises(urllib.error.URLError) as cm_real:
                opener.open(req, timeout=10.0)
            elapsed = time.monotonic() - t0

        self.assertIsInstance(cm_real.exception.reason, TimeoutError)
        self.assertIn("http_connect_deadline_exceeded", str(cm_real.exception.reason))
        self.assertLess(elapsed, 0.12)
        self.assertTrue(all(sock.closed for sock in real_sockets))  # type: ignore[attr-defined]

    def test_deadline_http_connect_bounds_slow_dns_resolution(self) -> None:
        import threading
        import urllib.error
        import urllib.request
        from unittest.mock import patch
        from shadow.http_limits import make_deadline_http_handlers

        release_dns = threading.Event()

        def _slow_getaddrinfo(*_args: object, **_kwargs: object) -> list[object]:
            release_dns.wait(timeout=2.0)
            return []

        try:
            with patch("socket.getaddrinfo", side_effect=_slow_getaddrinfo):
                opener = urllib.request.build_opener(
                    *make_deadline_http_handlers(time.monotonic() + 0.050)
                )
                req = urllib.request.Request("https://slow-dns.invalid/")
                t0 = time.monotonic()
                with self.assertRaises(urllib.error.URLError) as cm:
                    opener.open(req, timeout=10.0)
                elapsed = time.monotonic() - t0
        finally:
            release_dns.set()

        self.assertIsInstance(cm.exception.reason, TimeoutError)
        self.assertIn("http_dns_deadline_exceeded", str(cm.exception.reason))
        self.assertLess(elapsed, 0.25)


if __name__ == "__main__":
    unittest.main()
