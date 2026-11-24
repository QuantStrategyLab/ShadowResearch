"""Unit and integration tests for SH1-W6 drill-chain scheduler, missed-run detector, and templates."""

from __future__ import annotations

import hashlib
import json
import os
import plistlib
import re
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from collections.abc import Mapping
from dataclasses import replace
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

_SRC = str(Path(__file__).resolve().parents[1] / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from shadow.calendar_consumer import load_spec, resolve_session
from shadow.ledger import (
    GENESIS_PREV_HASH,
    LedgerAppendError,
    ShadowLedger,
    build_record_body,
    compute_code_manifest_hash,
    sha256_canonical,
)
from shadow.ots import OTS_HEADER_MAGIC, build_synthetic_ots_fixture
from shadow.replay import replay_window
from shadow.scheduler import (
    DEFAULT_DEPLOY_DIR,
    DEFAULT_SCHEDULER_CONFIG_PATH,
    CalendarCoverageError,
    DrillOnlyViolationError,
    DrillSchedulerConfig,
    _commit_drill_session_atomically,
    _effective_runtime_config,
    _et_hhmm_to_utc_ms,
    _resolve_auto_run_target,
    check_footprint_budget,
    compute_session_deadlines,
    detect_missed_runs,
    load_scheduler_config,
    main as scheduler_cli_main,
    ny_local_time_matches,
    rotate_logs,
    run_daily_drill_cycle,
    run_dry_run_harness,
    validate_scheduler_templates,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
FEEDER_FIXTURES = REPO_ROOT / "tests" / "fixtures" / "feeder"
DRILL_FIXTURES = REPO_ROOT / "tests" / "fixtures" / "drill"


def _tick_clock(session_date: str, hhmmss: str = "22:30:00"):
    base = datetime.fromisoformat(f"{session_date}T{hhmmss}+00:00")
    step = {"i": 0}

    def _next() -> datetime:
        step["i"] += 1
        return base + timedelta(seconds=step["i"])

    return _next


def _mock_ots_transport(
    method: str,
    url: str,
    body: bytes | None,
    headers: Mapping[str, str],
    timeout: float,
) -> tuple[int, bytes]:
    if url.endswith("/digest") and body is not None:
        ots_full, _ = build_synthetic_ots_fixture(body.hex(), calendar_uri=url.rsplit("/", 1)[0])
        return 200, ots_full[len(OTS_HEADER_MAGIC) + 2 + 32 :]
    return 404, b""


def _mock_market_transport(
    url: str,
    headers: Mapping[str, str],
    *,
    withhold_inst: str | None = None,
) -> tuple[int, bytes]:
    if "/daily/TQQQ/prices" in url:
        if withhold_inst == "TQQQ":
            return 503, b"outage"
        return 200, json.dumps(
            [
                {"date": "2025-11-21T00:00:00.000Z", "close": 47.48, "divCash": 0.0, "splitFactor": 1.0},
                {"date": "2025-11-24T00:00:00.000Z", "close": 48.05, "divCash": 0.0, "splitFactor": 1.0},
                {"date": "2025-11-25T00:00:00.000Z", "close": 48.10, "divCash": 0.0, "splitFactor": 1.0},
            ]
        ).encode("utf-8")
    if "/daily/SOXL/prices" in url:
        if withhold_inst == "SOXL":
            return 503, b"outage"
        return 200, json.dumps(
            [
                {"date": "2025-11-21T00:00:00.000Z", "close": 31.73, "divCash": 0.0, "splitFactor": 1.0},
                {"date": "2025-11-24T00:00:00.000Z", "close": 32.00, "divCash": 0.0, "splitFactor": 1.0},
                {"date": "2025-11-25T00:00:00.000Z", "close": 32.25, "divCash": 0.0, "splitFactor": 1.0},
            ]
        ).encode("utf-8")
    if "/daily/QQQ/prices" in url:
        if withhold_inst == "QQQ":
            return 503, b"outage"
        return 200, json.dumps(
            [
                {"date": "2025-11-21T00:00:00.000Z", "close": 510.12, "divCash": 0.0, "splitFactor": 1.0},
                {"date": "2025-11-24T00:00:00.000Z", "close": 511.50, "divCash": 0.0, "splitFactor": 1.0},
                {"date": "2025-11-25T00:00:00.000Z", "close": 512.80, "divCash": 0.0, "splitFactor": 1.0},
            ]
        ).encode("utf-8")
    if "%5EXNDX" in url:
        if withhold_inst == "XNDX":
            return 429, b"Edge: Too Many Requests"
        return 200, json.dumps(
            {
                "chart": {
                    "result": [
                        {
                            "meta": {"symbol": "^XNDX", "instrumentType": "INDEX"},
                            "timestamp": [1763758800, 1764018000, 1764104400],
                            "indicators": {"quote": [{"close": [29462.35, 29520.0, 29600.10]}]},
                        }
                    ]
                }
            }
        ).encode("utf-8")
    if "XSOX" in url:
        if withhold_inst == "SEMI":
            return 429, b"Edge: Too Many Requests"
        return 200, json.dumps(
            {
                "chart": {
                    "result": [
                        {
                            "meta": {"symbol": "XSOX", "instrumentType": "INDEX"},
                            "timestamp": [1763758800, 1764018000, 1764104400],
                            "indicators": {"quote": [{"close": [8179.87, 8195.0, 8210.50]}]},
                        }
                    ]
                }
            }
        ).encode("utf-8")
    if "markets.newyorkfed.org" in url:
        if withhold_inst == "SOFR":
            return 503, b"outage"
        return 200, json.dumps(
            {
                "refRates": [
                    {"effectiveDate": "2025-11-20", "type": "SOFR", "percentRate": 3.91, "revisionIndicator": ""},
                    {"effectiveDate": "2025-11-21", "type": "SOFR", "percentRate": 3.92, "revisionIndicator": ""},
                    {"effectiveDate": "2025-11-24", "type": "SOFR", "percentRate": 3.90, "revisionIndicator": ""},
                ]
            }
        ).encode("utf-8")
    if "treasury.example.test" in url:
        if withhold_inst == "TBILL_3M":
            return 503, b"outage"
        return 200, b"Date,3 Mo\n2025-11-20,3.90\n2025-11-24,3.89\n"
    return 404, b""


class SchedulerTest(unittest.TestCase):
    """Verify drill cycle, OTS receipts, completeness report, missed-run detection, and templates."""

    def test_config_and_templates_forbid_production_and_enforce_caps(self) -> None:
        cfg = load_scheduler_config(DEFAULT_SCHEDULER_CONFIG_PATH)
        self.assertEqual(cfg.mode, "drill")
        self.assertFalse(cfg.clk01_active)

        deadlines_est = compute_session_deadlines("2025-11-21", cfg)
        self.assertEqual(deadlines_est["scheduled_decision_instant_utc"], "2025-11-21T22:30:00.000Z")
        self.assertEqual(deadlines_est["commitment_deadline_utc"], "2025-11-22T04:30:00.000Z")

        deadlines_edt = compute_session_deadlines("2025-07-03", cfg)
        self.assertEqual(deadlines_edt["scheduled_decision_instant_utc"], "2025-07-03T21:30:00.000Z")
        self.assertEqual(deadlines_edt["commitment_deadline_utc"], "2025-07-04T03:30:00.000Z")

        spec = load_spec()
        hd_cal = resolve_session("XNYS", "2025-07-03", spec=spec)
        deadlines_hd_edt = compute_session_deadlines("2025-07-03", cfg, calendar_session=hd_cal)
        self.assertEqual(deadlines_hd_edt["scheduled_decision_instant_utc"], "2025-07-03T18:30:00.000Z")
        self.assertEqual(deadlines_hd_edt["decision_cutoff_utc"], "2025-07-03T19:00:00.000Z")

        tmpl = validate_scheduler_templates(DEFAULT_DEPLOY_DIR)
        self.assertTrue(tmpl["templates_ok"], tmpl["errors"])

        with tempfile.TemporaryDirectory() as tmpdir:
            bad_cfg_path = Path(tmpdir) / "bad.json"
            bad_cfg_path.write_text(
                json.dumps({"chain_id": "prod", "mode": "production", "clk01_active": True}),
                encoding="utf-8",
            )
            with self.assertRaises(DrillOnlyViolationError):
                load_scheduler_config(bad_cfg_path)

    def test_daily_drill_cycle_and_o1_o2_and_missed_run_detection(self) -> None:
        cfg = load_scheduler_config(DEFAULT_SCHEDULER_CONFIG_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "drill_data"

            # 1. Closed holiday (Thanksgiving 2025-11-27) -> O2 fail-closed, 0 appends
            holiday_res = run_daily_drill_cycle(
                session_date="2025-11-27",
                data_dir=data_dir,
                config=cfg,
            )
            self.assertFalse(holiday_res["appended"])
            self.assertTrue(holiday_res["fail_closed"])
            self.assertEqual(holiday_res["reason"], "O2")
            self.assertEqual(holiday_res["report"]["notifications_sent"], 0)

            # 2. Pre-close call (20:55Z < 21:00Z close) rejected -> session_not_yet_closed, 0 appends
            pre_res = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=Path(tmpdir) / "pre_dir",
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=lambda: datetime(2025, 11, 21, 20, 55, 0, tzinfo=timezone.utc),
                treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                sleeper=lambda _: None,
            )
            self.assertFalse(pre_res["appended"])
            self.assertEqual(pre_res["reason"], "session_not_yet_closed")

            # 3. Withheld required series (TBILL_3M) inside [close, decision_cutoff] -> O1 fail-closed, 0 appends
            o1_dir = Path(tmpdir) / "o1_dir"
            o1_res = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=o1_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h, withhold_inst="TBILL_3M"),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21"),
                treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                sleeper=lambda _: None,
            )
            self.assertFalse(o1_res["appended"])
            self.assertTrue(o1_res["fail_closed"])
            self.assertIn("TBILL_3M", o1_res["reason"])
            self.assertFalse((o1_dir / "ledger" / "seq_000000.record.json").exists())

            # 4. Normal drill cycle on 2025-11-21 inside [close=21:00Z, decision_cutoff=23:00Z]
            ok_1 = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=data_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21"),
                treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                sleeper=lambda _: None,
            )
            self.assertTrue(ok_1["appended"])
            self.assertEqual(ok_1["sequence"], 0)
            self.assertEqual(ok_1["report"]["consecutive_session_count"], 1)
            self.assertTrue(ok_1["report"]["decision_timestamp_utc"].startswith("2025-11-21T22:30:"))
            self.assertTrue(ok_1["report"]["receipt"]["obtained_at_utc"].startswith("2025-11-21T22:30:"))
            self.assertTrue((data_dir / "ledger" / "seq_000000.ots").is_file())
            self.assertEqual(ok_1["report"]["receipt"]["status"], "timely")
            self.assertFalse(ok_1["report"]["receipt"]["creditable"])
            self.assertTrue((data_dir / "reports" / "completeness_2025-11-21.md").is_file())

            # 5. Suppress 2025-11-24 firing deliberately, then run 2025-11-25 (consecutive resets to 1!)
            ok_3 = run_daily_drill_cycle(
                session_date="2025-11-25",
                data_dir=data_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-25"),
                treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                sleeper=lambda _: None,
            )
            self.assertTrue(ok_3["appended"])
            self.assertEqual(ok_3["sequence"], 1)
            self.assertEqual(ok_3["report"]["consecutive_session_count"], 1)

            # Missed-run detector flags 2025-11-24 and adds zero backfilled ledger records
            missed = detect_missed_runs(
                window_start_date="2025-11-21",
                as_of_utc="2025-11-26T05:00:00.000Z",
                data_dir=data_dir,
                config=cfg,
            )
            self.assertTrue(missed["missed_run_detected"])
            self.assertEqual(missed["silent_missed_days"], ["2025-11-24"])
            self.assertEqual(missed["ok_days"], ["2025-11-21", "2025-11-25"])
            self.assertTrue(missed["zero_backfill_enforced"])
            self.assertEqual(len(list((data_dir / "ledger").glob("seq_*.record.json"))), 2)

            # Bit-for-bit replay passes across appended drill chain (pending OTS stays non-creditable)
            replay_res = replay_window(data_dir / "replay", data_dir / "ledger")
            self.assertTrue(replay_res["bit_for_bit_match"])
            self.assertTrue(replay_res["chain_ok"])
            self.assertTrue(replay_res["receipts_ok"])
            self.assertFalse(replay_res["receipts_creditable"])
            self.assertTrue(replay_res["archived_inputs_ok"])

            # Footprint well under 2 GiB
            fp = check_footprint_budget(data_dir, cfg.max_footprint_bytes)
            self.assertTrue(fp["within_budget"])

    def test_late_and_missing_ots_receipts_flagged_failed(self) -> None:
        cfg = replace(
            load_scheduler_config(DEFAULT_SCHEDULER_CONFIG_PATH),
            treasury_endpoint="https://treasury.example.test/yield{year}.csv",
        )
        eff_cfg = _effective_runtime_config(cfg)
        spec = load_spec()
        with tempfile.TemporaryDirectory() as tmpdir:
            late_dir = Path(tmpdir) / "late_data"
            # Direct ledger append with late receipt (after 2025-11-22T04:30:00.000Z)
            ledger = ShadowLedger(late_dir / "ledger")
            cal_0 = resolve_session("XNYS", "2025-11-21", spec=spec)
            built_0 = build_record_body(
                chain_id=cfg.chain_id,
                sequence=0,
                prev_record_sha256=GENESIS_PREV_HASH,
                mode="drill",
                session_date="2025-11-21",
                decision_timestamp_utc="2025-11-21T20:55:00.000Z",
                session_inputs={
                    "E": "50000",
                    "E_star": "50000",
                    "H": "0",
                    "L": "0",
                    "funds": {
                        "TQQQ": {"s": "1", "close_price": "47.48", "q_pre": 0, "sigma_forecast": "0.24"},
                        "SOXL": {"s": "1", "close_price": "31.73", "q_pre": 0, "sigma_forecast": "0.24"},
                    },
                },
                calendar_session=cal_0,
                code_version_sha=compute_code_manifest_hash(
                    REPO_ROOT, calendar_version=cfg.calendar_version
                ),
                runtime_config=eff_cfg,
                calendar_spec=spec,
            )
            ots_0, _ = build_synthetic_ots_fixture(sha256_canonical(built_0["record_body"]))
            ledger.append_record(
                built_0["record_body"],
                obtained_at_utc="2025-11-22T05:00:00.000Z",
                ots_bytes=ots_0,
                commitment_deadline_utc="2025-11-22T04:30:00.000Z",
            )
            missed_late = detect_missed_runs(
                window_start_date="2025-11-21",
                as_of_utc="2025-11-22T05:00:00.000Z",
                data_dir=late_dir,
                config=cfg,
            )
            self.assertEqual(missed_late["ok_days"], [])
            self.assertEqual(missed_late["failed_days"], ["2025-11-21"])

            # Missing OTS receipt (all OTS calendars fail during timely window)
            missing_dir = Path(tmpdir) / "missing_data"
            missing_res = run_daily_drill_cycle(
                session_date="2025-11-24",
                data_dir=missing_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=lambda m, u, b, h, t: (503, b"ots down"),
                wall_clock=_tick_clock("2025-11-24"),
                sleeper=lambda _: None,
            )
            self.assertTrue(missing_res["appended"])
            self.assertEqual(missing_res["report"]["receipt"]["status"], "missing")

            missed_missing = detect_missed_runs(
                window_start_date="2025-11-24",
                as_of_utc="2025-11-25T05:00:00.000Z",
                data_dir=missing_dir,
                config=cfg,
            )
            self.assertEqual(missed_missing["ok_days"], [])
            self.assertEqual(missed_missing["failed_days"], ["2025-11-24"])

    def test_log_rotation_and_detached_dry_run_cli(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            logs_dir = Path(tmpdir) / "logs"
            logs_dir.mkdir()
            old_log = logs_dir / "old.log"
            old_log.write_bytes(b"a" * 100)
            os.utime(old_log, (1000, 1000))
            new_log = logs_dir / "new.log"
            new_log.write_bytes(b"b" * 100)
            rot = rotate_logs(logs_dir, max_age_days=30, max_total_bytes=150, now_ts=1000 + 35 * 86400)
            self.assertEqual(rot["removed_old"], 1)
            self.assertFalse(old_log.exists())
            self.assertTrue(new_log.exists())

        res = run_dry_run_harness(REPO_ROOT)
        self.assertTrue(res["templates"]["templates_ok"])
        self.assertTrue(res["missed_run_detected"])
        self.assertEqual(res["silent_missed_days"], ["2025-11-24"])
        self.assertTrue(res["replay"]["bit_for_bit_match"])

        # Detached subprocess with stripped environment (no AI session env) succeeds independently
        clean_env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "PYTHONPATH": str(REPO_ROOT / "src")}
        proc = subprocess.run(
            [sys.executable, "-m", "shadow.scheduler", "dry-run"],
            cwd=str(REPO_ROOT),
            env=clean_env,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertTrue(payload["replay"]["bit_for_bit_match"])
        self.assertEqual(scheduler_cli_main(["dry-run"]), 0)

    def test_normal_wall_clock_and_post_deadline_backfill_rejection(self) -> None:
        # Item #1 regression: real wall clock; CLI run-drill forbids --now-utc; post-deadline fails closed
        cfg = load_scheduler_config(DEFAULT_SCHEDULER_CONFIG_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            # 0. Normal CLI run-drill refuses --now-utc backdating completely
            with self.assertRaises(SystemExit) as sys_exit:
                scheduler_cli_main(
                    [
                        "run-drill",
                        "--session",
                        "2025-11-21",
                        "--now-utc",
                        "2025-11-21T22:30:00.000Z",
                        "--data-dir",
                        str(root / "cli_bf"),
                    ]
                )
            self.assertNotEqual(sys_exit.exception.code, 0)

            # simulate-drill refuses non-isolated drill chain or drill path
            rc_bad_chain = scheduler_cli_main(
                [
                    "simulate-drill",
                    "--session",
                    "2025-11-21",
                    "--now-utc",
                    "2025-11-21T22:30:00.000Z",
                    "--chain-id",
                    "drill-sh1-v1",
                    "--data-dir",
                    str(root / "sim_bad"),
                ]
            )
            self.assertEqual(rc_bad_chain, 1)

            rc_bad_dir = scheduler_cli_main(
                [
                    "simulate-drill",
                    "--session",
                    "2025-11-21",
                    "--now-utc",
                    "2025-11-21T22:30:00.000Z",
                    "--chain-id",
                    "sim-test",
                    "--data-dir",
                    str(root / "drill" / "out"),
                ]
            )
            self.assertEqual(rc_bad_dir, 1)

            # 1. Normal call on historical 2025-11-21 using real UTC clock rejects backfill (0 appends)
            backfill_res = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=root / "bf1",
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                sleeper=lambda _: None,
            )
            self.assertFalse(backfill_res["appended"])
            self.assertTrue(backfill_res["fail_closed"])
            self.assertEqual(backfill_res["reason"], "late_after_commitment_deadline_backfill_rejected")
            self.assertFalse((root / "bf1" / "ledger" / "seq_000000.record.json").exists())

            # 2. Clock crosses 23:30 ET commitment deadline AFTER stamp_digest -> append gate fails closed
            roll_calls = {"n": 0}

            def _rollover_clock() -> datetime:
                roll_calls["n"] += 1
                # Calls 1..9: entry + 7 provider reads + decision_utc; Call 10+: post-stamp past deadline
                if roll_calls["n"] <= 9:
                    return datetime(2025, 11, 21, 22, 30, roll_calls["n"], tzinfo=timezone.utc)
                return datetime(2025, 11, 22, 4, 31, 0, tzinfo=timezone.utc)

            rollover_res = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=root / "bf2",
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_rollover_clock,
                treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                sleeper=lambda _: None,
            )
            self.assertFalse(rollover_res["appended"])
            self.assertTrue(rollover_res["fail_closed"])
            self.assertEqual(rollover_res["reason"], "late_after_commitment_deadline_backfill_rejected")
            self.assertFalse((root / "bf2" / "ledger" / "seq_000000.record.json").exists())

            # 3. Within deadline, decision_utc and post-stamp receipt times reflect distinct wall-clock reads
            ok_calls = {"n": 0}

            def _ok_clock() -> datetime:
                ok_calls["n"] += 1
                if ok_calls["n"] <= 9:
                    return datetime(2025, 11, 21, 22, 30, ok_calls["n"], tzinfo=timezone.utc)
                return datetime(2025, 11, 21, 22, 32, ok_calls["n"], tzinfo=timezone.utc)

            ok_res = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=root / "ok",
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_ok_clock,
                treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                sleeper=lambda _: None,
            )
            self.assertTrue(ok_res["appended"])
            self.assertEqual(ok_res["report"]["decision_timestamp_utc"], "2025-11-21T22:30:09.000Z")
            self.assertEqual(ok_res["report"]["receipt"]["obtained_at_utc"], "2025-11-21T22:32:10.000Z")

            # 4. R3 #1 regression: real run-drill CLI MUST pass wall_clock=None so clock advances (> 0 ms) across reads
            captured_kwargs: dict[str, object] = {}

            def _spy_run_daily_drill_cycle(**kwargs):  # type: ignore[no-untyped-def]
                captured_kwargs.update(kwargs)
                eff_clk = kwargs.get("wall_clock") or (lambda: datetime.now(timezone.utc))
                t1 = eff_clk()
                time.sleep(0.025)
                t2 = eff_clk()
                delta_ms = (t2 - t1).total_seconds() * 1000.0
                return {"appended": False, "fail_closed": True, "reason": "spy_probe", "delta_ms": delta_ms}

            with patch("shadow.scheduler.run_daily_drill_cycle", side_effect=_spy_run_daily_drill_cycle):
                rc_cli = scheduler_cli_main(
                    [
                        "run-drill",
                        "--session",
                        "2025-11-21",
                        "--data-dir",
                        str(root / "cli_live_clock"),
                    ]
                )
            self.assertEqual(rc_cli, 0)
            self.assertIsNone(captured_kwargs["wall_clock"])

            # Verify effective clock delta > 15 ms (non-frozen clock) AND real cycle with wall_clock=None
            # records distinct > 0 ms post-response timestamps across provider calls
            probe_fn = captured_kwargs["wall_clock"] or (lambda: datetime.now(timezone.utc))
            pt1 = probe_fn()
            time.sleep(0.025)
            pt2 = probe_fn()
            self.assertGreater((pt2 - pt1).total_seconds() * 1000.0, 10.0)

            now_ny_dt = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
            # Pick a synthetic calendar spec where today_ny closed 2 hours before now_ny (or anchor at today's NY date)
            ny_date_str = now_ny_dt.date().isoformat()
            now_utc = datetime.now(timezone.utc)
            open_utc_win = now_utc - timedelta(hours=3)
            close_utc_win = now_utc - timedelta(minutes=10)
            ny_close_local = close_utc_win.astimezone(ZoneInfo("America/New_York"))
            # Run test only when today NY local has at least 4h left before 23:30 ET so decision_cutoff > now_utc
            # Use isolated synthetic 1-day spec where close/cutoff/deadline wrap today's real UTC clock cleanly
            synth_spec_dir = root / "live_clock_spec"
            synth_spec_dir.mkdir()
            exc_file = synth_spec_dir / f"exceptions-{ny_date_str[:4]}.json"
            exc_file.write_text(
                json.dumps(
                    {
                        "calendar_version": cfg.calendar_version,
                        "status": "synthetic-test",
                        "exchange": "XNYS",
                        "timezone": "America/New_York",
                        "coverage_start_date": "2025-01-01",
                        "coverage_end_date": ny_date_str,
                        "regular_open_local": "09:30",
                        "regular_close_local": "16:00",
                        "holidays": {},
                        "unscheduled_closures": {},
                        "early_closes": {},
                    }
                ),
                encoding="utf-8",
            )
            spec_md = "# synthetic\n"
            exc_raw = exc_file.read_bytes()
            files_map = {
                "SPEC.md": hashlib.sha256(spec_md.encode("utf-8")).hexdigest(),
                exc_file.name: hashlib.sha256(exc_raw).hexdigest(),
            }
            spec_digest_val = hashlib.sha256(spec_md.encode("utf-8") + exc_raw).hexdigest()
            (synth_spec_dir / "SPEC.md").write_text(spec_md, encoding="utf-8")
            (synth_spec_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "calendar_version": cfg.calendar_version,
                        "spec_digest": spec_digest_val,
                        "files": files_map,
                    }
                ),
                encoding="utf-8",
            )
            # Real live-clock test (wall_clock=None) works 24/7 cleanly via close=now-10m < now < cutoff=now+20m < deadline=now+40m
            real_cal = {
                "venue": "XNYS",
                "session_date": ny_date_str,
                "session_state": "regular",
                "is_trading_day": True,
                "blocks_eligibility": False,
                "reason": None,
                "close_utc": close_utc_win.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "order_cutoff_utc": close_utc_win.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "calendar_version": cfg.calendar_version,
                "spec_digest": sha256_canonical({"files": files_map}),
            }
            live_deadlines = {
                "session_close_utc": close_utc_win.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "scheduled_decision_instant_utc": (now_utc + timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "decision_cutoff_utc": (now_utc + timedelta(minutes=20)).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "commitment_deadline_utc": (now_utc + timedelta(minutes=40)).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            }
            from shadow.feeder import expected_prior_sofr_business_day

            sofr_eff = expected_prior_sofr_business_day(ny_date_str)

            def _sleep_transport(url: str, headers: Mapping[str, str]) -> tuple[int, bytes]:
                if "/daily/" in url:
                    time.sleep(0.02)
                    return 200, json.dumps(
                        [{"date": f"{ny_date_str}T00:00:00.000Z", "close": 50.0, "divCash": 0.0, "splitFactor": 1.0}]
                    ).encode("utf-8")
                if "finance.yahoo.com" in url:
                    time.sleep(0.02)
                    bar_ts = int(now_utc.timestamp())
                    return 200, json.dumps(
                        {"chart": {"result": [{"meta": {"symbol": "^XNDX" if "%5EXNDX" in url else "XSOX", "instrumentType": "INDEX"}, "timestamp": [bar_ts], "indicators": {"quote": [{"close": [29000.0]}]}}]}}
                    ).encode("utf-8")
                if "markets.newyorkfed.org" in url:
                    time.sleep(0.02)
                    return 200, json.dumps(
                        {"refRates": [{"effectiveDate": sofr_eff, "type": "SOFR", "percentRate": 3.91, "revisionIndicator": ""}]}
                    ).encode("utf-8")
                if "treasury.example.test" in url:
                    time.sleep(0.02)
                    return 200, f"Date,3 Mo\n{ny_date_str},3.90\n".encode("utf-8")
                return 404, b""

            with patch("shadow.scheduler.resolve_session", return_value=real_cal), patch(
                "shadow.feeder_live.resolve_session", return_value=real_cal
            ), patch("shadow.feeder.resolve_session", return_value=real_cal), patch(
                "shadow.scheduler.compute_session_deadlines", return_value=live_deadlines
            ):
                live_res = run_daily_drill_cycle(
                    session_date=ny_date_str,
                    data_dir=root / "live_clock_run",
                    config=cfg,
                    spec_dir=synth_spec_dir,
                    transport=_sleep_transport,
                    environ={"TIINGO_API_KEY": "test-key"},
                    ots_transport=_mock_ots_transport,
                    wall_clock=None,
                    treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                    sleeper=lambda _: None,
                )
            self.assertTrue(live_res["appended"], live_res)
            rets = [s["retrieval_time_utc"] for s in live_res["report"]["required_series"]]
            self.assertEqual(len(set(rets)), 7)
            self.assertGreater(live_res["report"]["receipt"]["obtained_at_utc"], rets[0])

    def test_market_cutoff_separate_from_commitment_deadline(self) -> None:
        # Item #2 regression: three-timestamp EOD contract [session_close, decision_cutoff, commitment_deadline]
        cfg = load_scheduler_config(DEFAULT_SCHEDULER_CONFIG_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "cutoff"
            # 23:05Z = 18:05 EST: after 23:00Z (18:00 EST) decision_cutoff, yet hours before 04:30Z deadline
            res = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=data_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=lambda: datetime(2025, 11, 21, 23, 5, 0, tzinfo=timezone.utc),
                treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                sleeper=lambda _: None,
            )
            self.assertFalse(res["appended"])
            self.assertTrue(res["fail_closed"])
            self.assertIn("late_after_decision_cutoff", res["reason"])
            self.assertEqual(res["report"]["required_series"][0]["status"], "late")
            self.assertFalse((data_dir / "ledger" / "seq_000000.record.json").exists())

            # Half-day 2025-11-28: close=18:00Z (13:00 EST), half-day cutoff=20:00Z (15:00 EST)
            pre_hd = run_daily_drill_cycle(
                session_date="2025-11-28",
                data_dir=Path(tmpdir) / "hd_pre",
                config=cfg,
                wall_clock=lambda: datetime(2025, 11, 28, 17, 55, 0, tzinfo=timezone.utc),
            )
            self.assertFalse(pre_hd["appended"])
            self.assertEqual(pre_hd["reason"], "session_not_yet_closed")

            late_hd = run_daily_drill_cycle(
                session_date="2025-11-28",
                data_dir=Path(tmpdir) / "hd_late",
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=lambda: datetime(2025, 11, 28, 20, 5, 0, tzinfo=timezone.utc),
                treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                sleeper=lambda _: None,
            )
            self.assertFalse(late_hd["appended"])
            self.assertIn("late_after_decision_cutoff", late_hd["reason"])

    def test_dst_est_and_edt_timer_schedules_and_ny_guard(self) -> None:
        # Finding #6 regression: EST (UTC-5) and EDT (UTC-4) offsets work across DST without drifting
        # Summer EDT (2025-07-01 17:30 EDT = 2025-07-01 21:30 UTC = 2025-07-02 05:30 SGT)
        self.assertTrue(ny_local_time_matches(datetime(2025, 7, 1, 21, 30, 0, tzinfo=timezone.utc), "17:30"))
        self.assertFalse(ny_local_time_matches(datetime(2025, 7, 1, 22, 30, 0, tzinfo=timezone.utc), "17:30"))
        # Winter EST (2025-11-21 17:30 EST = 2025-11-21 22:30 UTC = 2025-11-22 06:30 SGT)
        self.assertFalse(ny_local_time_matches(datetime(2025, 11, 21, 21, 30, 0, tzinfo=timezone.utc), "17:30"))
        self.assertTrue(ny_local_time_matches(datetime(2025, 11, 21, 22, 30, 0, tzinfo=timezone.utc), "17:30"))

        # Missed-run 23:45 America/New_York: 11:45 SGT (03:45 UTC) in EDT vs 12:45 SGT (04:45 UTC) in EST
        self.assertTrue(ny_local_time_matches(datetime(2025, 7, 2, 3, 45, 0, tzinfo=timezone.utc), "23:45"))
        self.assertFalse(ny_local_time_matches(datetime(2025, 7, 2, 4, 45, 0, tzinfo=timezone.utc), "23:45"))
        self.assertFalse(ny_local_time_matches(datetime(2025, 11, 22, 3, 45, 0, tzinfo=timezone.utc), "23:45"))
        self.assertTrue(ny_local_time_matches(datetime(2025, 11, 22, 4, 45, 0, tzinfo=timezone.utc), "23:45"))

        # Off-season DST launchd trigger exits 0 cleanly as no-op under --require-ny-hhmm
        with tempfile.TemporaryDirectory() as tmpdir:
            rc = scheduler_cli_main(
                [
                    "detect-missed",
                    "--start-date",
                    "2025-11-21",
                    "--as-of-utc",
                    "2025-11-22T03:45:00.000Z",
                    "--require-ny-hhmm",
                    "23:45",
                    "--data-dir",
                    tmpdir,
                ]
            )
            self.assertEqual(rc, 0)

    def test_calendar_coverage_insufficient_never_truncates_and_honors_version(self) -> None:
        # Finding #7 regression: out-of-coverage dates fail closed cleanly and configured calendar_version is honored
        cfg = load_scheduler_config(DEFAULT_SCHEDULER_CONFIG_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(CalendarCoverageError) as ctx:
                detect_missed_runs(
                    window_start_date="2025-11-21",
                    as_of_utc="2026-11-24T23:50:00.000Z",
                    data_dir=tmpdir,
                    config=cfg,
                )
            self.assertIn("calendar_coverage_insufficient", str(ctx.exception))

            # CLI exits 1 (not silent empty success!) when coverage is insufficient
            rc = scheduler_cli_main(
                [
                    "detect-missed",
                    "--start-date",
                    "2026-11-24",
                    "--as-of-utc",
                    "2026-11-24T23:50:00.000Z",
                    "--data-dir",
                    tmpdir,
                ]
            )
            self.assertEqual(rc, 1)

            # Honoring configured calendar_version fails closed on missing calendar package instead of falling back to v1
            custom_cfg = DrillSchedulerConfig(calendar_version="csa22-missing")
            with self.assertRaises(CalendarCoverageError):
                run_daily_drill_cycle(
                    session_date="2025-11-21",
                    data_dir=tmpdir,
                    config=custom_cfg,
                )
            with self.assertRaises(CalendarCoverageError):
                detect_missed_runs(
                    window_start_date="2025-11-21",
                    as_of_utc="2025-11-22T05:00:00.000Z",
                    data_dir=tmpdir,
                    config=custom_cfg,
                )

    def test_r2_fixtures_seven_series_holiday_noop_report_immutability_venue_and_atomic_recovery(self) -> None:
        cfg = load_scheduler_config(DEFAULT_SCHEDULER_CONFIG_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            # 1. Item #7 regression: 6-series fixture missing QQQ MUST fail O1; complete 7-series succeeds
            six_dir = root / "six_fix"
            six_res = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=six_dir,
                config=cfg,
                fixtures_dir=FEEDER_FIXTURES,
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21"),
            )
            self.assertFalse(six_res["appended"])
            self.assertTrue(six_res["fail_closed"])
            self.assertIn("QQQ", six_res["reason"])

            seven_dir = root / "seven_fix"
            seven_res = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=seven_dir,
                config=cfg,
                fixtures_dir=DRILL_FIXTURES,
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21"),
            )
            self.assertTrue(seven_res["appended"])
            self.assertEqual(len(seven_res["report"]["required_series"]), 7)

            # 2. Item #8: re-running same session does NOT overwrite completeness_2025-11-21.json
            rep_json = seven_dir / "reports" / "completeness_2025-11-21.json"
            orig_rep_bytes = rep_json.read_bytes()
            rerun_late = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=seven_dir,
                config=cfg,
                fixtures_dir=DRILL_FIXTURES,
                ots_transport=_mock_ots_transport,
                wall_clock=lambda: datetime(2025, 11, 22, 5, 0, 0, tzinfo=timezone.utc),
            )
            self.assertFalse(rerun_late["appended"])
            self.assertTrue(rerun_late["existing_report_preserved"])
            self.assertEqual(rep_json.read_bytes(), orig_rep_bytes)

            # 3. Item #8: Holiday --session auto is a holiday_no_op and never touches Wednesday report
            resolved_hol, no_op = _resolve_auto_run_target(
                cfg,
                datetime(2025, 11, 27, 22, 30, 0, tzinfo=timezone.utc),
            )
            self.assertIsNone(resolved_hol)
            assert no_op is not None
            self.assertEqual(no_op["status"], "holiday_no_op")
            self.assertEqual(no_op["ny_date"], "2025-11-27")
            self.assertEqual(no_op["reports_written"], 0)

            # 4. Item #8: A failure report created AFTER commitment deadline stays silent_missed_days
            missed_dir = root / "late_rep_audit"
            (missed_dir / "reports").mkdir(parents=True)
            (missed_dir / "reports" / "completeness_2025-11-21.json").write_text(
                json.dumps(
                    {
                        "session_date": "2025-11-21",
                        "created_at_utc": "2025-11-22T05:00:00.000Z",
                        "ledger": {"appended": False, "fail_closed_reason": "O1:late_run"},
                    }
                ),
                encoding="utf-8",
            )
            missed_audit = detect_missed_runs(
                window_start_date="2025-11-21",
                as_of_utc="2025-11-22T06:00:00.000Z",
                data_dir=missed_dir,
                config=cfg,
            )
            self.assertEqual(missed_audit["silent_missed_days"], ["2025-11-21"])
            self.assertTrue(missed_audit["missed_run_detected"])

            # 5. Item #6: Unsupported venue in detect_missed_runs raises CalendarCoverageError (not silent skip)
            bad_venue_cfg = DrillSchedulerConfig(venue="XHKG")
            with self.assertRaises(CalendarCoverageError):
                detect_missed_runs(
                    window_start_date="2025-11-21",
                    as_of_utc="2025-11-22T06:00:00.000Z",
                    data_dir=root / "bad_venue",
                    config=bad_venue_cfg,
                )

            # 6. Item #11: Mid-commit crash rolls back ledger+receipt+.ots+bundle cleanly so retry succeeds
            crash_dir = root / "crash_recovery"

            def _boom() -> None:
                raise OSError("simulated disk write failure before bundle update")

            with self.assertRaises(OSError):
                run_daily_drill_cycle(
                    session_date="2025-11-21",
                    data_dir=crash_dir,
                    config=cfg,
                    transport=lambda u, h: _mock_market_transport(u, h),
                    environ={"TIINGO_API_KEY": "test-key"},
                    ots_transport=_mock_ots_transport,
                    wall_clock=_tick_clock("2025-11-21"),
                    treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                    sleeper=lambda _: None,
                    _post_ledger_commit_hook=_boom,
                )
            self.assertFalse((crash_dir / "ledger" / "seq_000000.record.json").exists())
            self.assertFalse((crash_dir / "ledger" / "seq_000000.ots").exists())
            self.assertFalse((crash_dir / "replay" / "replay_bundle.json").exists())

            recovered = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=crash_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21"),
                treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                sleeper=lambda _: None,
            )
            self.assertTrue(recovered["appended"])
            self.assertEqual(recovered["report"]["consecutive_session_count"], 1)

            # 7. Item #11: Consecutive trading-day count increments across Fri->Mon and skips holiday Thu->Fri
            def _multi_day_transport(url: str, headers: Mapping[str, str]) -> tuple[int, bytes]:
                if "/daily/TQQQ/prices" in url:
                    return 200, json.dumps(
                        [
                            {"date": "2025-11-21T00:00:00.000Z", "close": 47.48, "divCash": 0.0, "splitFactor": 1.0},
                            {"date": "2025-11-24T00:00:00.000Z", "close": 48.05, "divCash": 0.0, "splitFactor": 1.0},
                            {"date": "2025-11-26T00:00:00.000Z", "close": 48.20, "divCash": 0.0, "splitFactor": 1.0},
                            {"date": "2025-11-28T00:00:00.000Z", "close": 48.35, "divCash": 0.0, "splitFactor": 1.0},
                        ]
                    ).encode("utf-8")
                if "/daily/SOXL/prices" in url:
                    return 200, json.dumps(
                        [
                            {"date": "2025-11-21T00:00:00.000Z", "close": 31.73, "divCash": 0.0, "splitFactor": 1.0},
                            {"date": "2025-11-24T00:00:00.000Z", "close": 32.00, "divCash": 0.0, "splitFactor": 1.0},
                            {"date": "2025-11-26T00:00:00.000Z", "close": 32.40, "divCash": 0.0, "splitFactor": 1.0},
                            {"date": "2025-11-28T00:00:00.000Z", "close": 32.55, "divCash": 0.0, "splitFactor": 1.0},
                        ]
                    ).encode("utf-8")
                if "/daily/QQQ/prices" in url:
                    return 200, json.dumps(
                        [
                            {"date": "2025-11-21T00:00:00.000Z", "close": 510.12, "divCash": 0.0, "splitFactor": 1.0},
                            {"date": "2025-11-24T00:00:00.000Z", "close": 511.50, "divCash": 0.0, "splitFactor": 1.0},
                            {"date": "2025-11-26T00:00:00.000Z", "close": 513.00, "divCash": 0.0, "splitFactor": 1.0},
                            {"date": "2025-11-28T00:00:00.000Z", "close": 514.00, "divCash": 0.0, "splitFactor": 1.0},
                        ]
                    ).encode("utf-8")
                if "%5EXNDX" in url:
                    return 200, json.dumps(
                        {
                            "chart": {
                                "result": [
                                    {
                                        "meta": {"symbol": "^XNDX", "instrumentType": "INDEX"},
                                        "timestamp": [1763758800, 1764018000, 1764190800, 1764352800],
                                        "indicators": {"quote": [{"close": [29462.35, 29520.0, 29650.0, 29700.0]}]},
                                    }
                                ]
                            }
                        }
                    ).encode("utf-8")
                if "XSOX" in url:
                    return 200, json.dumps(
                        {
                            "chart": {
                                "result": [
                                    {
                                        "meta": {"symbol": "XSOX", "instrumentType": "INDEX"},
                                        "timestamp": [1763758800, 1764018000, 1764190800, 1764352800],
                                        "indicators": {"quote": [{"close": [8179.87, 8195.0, 8220.0, 8230.0]}]},
                                    }
                                ]
                            }
                        }
                    ).encode("utf-8")
                if "markets.newyorkfed.org" in url:
                    return 200, json.dumps(
                        {
                            "refRates": [
                                {"effectiveDate": "2025-11-20", "type": "SOFR", "percentRate": 3.91, "revisionIndicator": ""},
                                {"effectiveDate": "2025-11-21", "type": "SOFR", "percentRate": 3.92, "revisionIndicator": ""},
                                {"effectiveDate": "2025-11-25", "type": "SOFR", "percentRate": 3.90, "revisionIndicator": ""},
                                {"effectiveDate": "2025-11-26", "type": "SOFR", "percentRate": 3.89, "revisionIndicator": ""},
                            ]
                        }
                    ).encode("utf-8")
                if "treasury.example.test" in url:
                    return 200, (
                        b"Date,3 Mo\n"
                        b"2025-11-20,3.90\n"
                        b"2025-11-21,3.90\n"
                        b"2025-11-24,3.89\n"
                        b"2025-11-25,3.88\n"
                        b"2025-11-26,3.88\n"
                        b"2025-11-28,3.87\n"
                    )
                return _mock_market_transport(url, headers)

            streak_dir = root / "streak"
            r_fri = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=streak_dir,
                config=cfg,
                transport=_multi_day_transport,
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21"),
                treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                sleeper=lambda _: None,
            )
            self.assertEqual(r_fri["report"]["consecutive_session_count"], 1)

            r_mon = run_daily_drill_cycle(
                session_date="2025-11-24",
                data_dir=streak_dir,
                config=cfg,
                transport=_multi_day_transport,
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-24"),
                treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                sleeper=lambda _: None,
            )
            self.assertEqual(r_mon["report"]["consecutive_session_count"], 2)

            # Skip Tue 2025-11-25 -> Wed 2025-11-26 resets streak to 1!
            r_wed = run_daily_drill_cycle(
                session_date="2025-11-26",
                data_dir=streak_dir,
                config=cfg,
                transport=_multi_day_transport,
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-26"),
                treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                sleeper=lambda _: None,
            )
            self.assertEqual(r_wed["report"]["consecutive_session_count"], 1)

            # Skip closed holiday Thu 2025-11-27 -> Half-day Fri 2025-11-28 (14:30 EST = 19:30Z) reaches 2!
            r_post_hol = run_daily_drill_cycle(
                session_date="2025-11-28",
                data_dir=streak_dir,
                config=cfg,
                transport=_multi_day_transport,
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-28", "19:30:00"),
                treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                sleeper=lambda _: None,
            )
            self.assertEqual(r_post_hol["report"]["consecutive_session_count"], 2)

    def test_half_day_schedule_coverage_all_250_days_and_auto_noop(self) -> None:
        # R3 #2 regression: verify BOTH systemd timer and launchd plist hit inside (close, cutoff] on ALL 250/250 2025 sessions
        cfg = load_scheduler_config(DEFAULT_SCHEDULER_CONFIG_PATH)
        spec = load_spec()
        ny_tz = ZoneInfo("America/New_York")
        sgt_tz = ZoneInfo("Asia/Singapore")

        timer_text = (DEFAULT_DEPLOY_DIR / "shadow-drill.timer").read_text(encoding="utf-8")
        timer_slots = re.findall(r"OnCalendar=Mon\.\.Fri \*-\*-\* (\d{2}:\d{2}):00 America/New_York", timer_text)
        self.assertEqual(sorted(timer_slots), ["14:30", "17:30"])

        plist_obj = plistlib.loads((DEFAULT_DEPLOY_DIR / "com.quantstrategylab.shadow-drill.plist").read_bytes())
        plist_args = plist_obj["ProgramArguments"]
        req_hhmm = plist_args[plist_args.index("--require-ny-hhmm") + 1]
        sgt_intervals = plist_obj["StartCalendarInterval"]

        trading_days: list[str] = []
        half_days: list[str] = []
        cur = date(2025, 1, 1)
        while cur <= date(2025, 12, 31):
            ds = cur.isoformat()
            cal = resolve_session("XNYS", ds, spec=spec)
            if cal["is_trading_day"] and not cal["blocks_eligibility"]:
                trading_days.append(ds)
                if cal["session_state"] == "half_day":
                    half_days.append(ds)
                deadlines = compute_session_deadlines(ds, cfg, calendar_session=cal)
                close_utc = deadlines["session_close_utc"]
                cutoff_utc = deadlines["decision_cutoff_utc"]

                # Check systemd timer slots in America/New_York
                systemd_hits = []
                for hhmm in timer_slots:
                    hh, mm = map(int, hhmm.split(":"))
                    trig_utc = datetime.combine(cur, dt_time(hh, mm), tzinfo=ny_tz).astimezone(timezone.utc)
                    trig_ms = trig_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    if close_utc < trig_ms <= cutoff_utc:
                        systemd_hits.append(trig_ms)
                self.assertEqual(len(systemd_hits), 1, f"systemd missed or duplicated {ds}: {systemd_hits}")

                # Check launchd SGT StartCalendarInterval triggers filtered by --require-ny-hhmm
                # Apple StartCalendarInterval: Sun=0/7, Mon=1, Tue=2..Sat=6 -> Python (weekday + 1) % 7
                launchd_hits = []
                sgt_next_day = cur + timedelta(days=1)
                expected_apple_wd = (sgt_next_day.weekday() + 1) % 7
                for entry in sgt_intervals:
                    if entry["Weekday"] != expected_apple_wd:
                        continue
                    sgt_dt = datetime.combine(
                        sgt_next_day, dt_time(entry["Hour"], entry["Minute"]), tzinfo=sgt_tz
                    )
                    utc_dt = sgt_dt.astimezone(timezone.utc)
                    if not ny_local_time_matches(utc_dt, req_hhmm):
                        continue
                    trig_ms = utc_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    if close_utc < trig_ms <= cutoff_utc:
                        launchd_hits.append(trig_ms)
                self.assertEqual(len(launchd_hits), 1, f"launchd missed or duplicated {ds}: {launchd_hits}")
            cur += timedelta(days=1)

        self.assertEqual(len(trading_days), 250)
        self.assertEqual(half_days, ["2025-07-03", "2025-11-28", "2025-12-24"])

        # Auto-session behavior:
        # (a) 14:30 ET slot on regular day 2025-11-21 -> pre_close_slot_no_op, 0 reports written
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            reg_dir = root / "reg_auto"
            dt_1430_reg = datetime(2025, 11, 21, 14, 30, 0, tzinfo=ny_tz).astimezone(timezone.utc)
            tgt_reg, noop_reg = _resolve_auto_run_target(cfg, dt_1430_reg, data_dir=reg_dir)
            self.assertIsNone(tgt_reg)
            assert noop_reg is not None
            self.assertEqual(noop_reg["status"], "pre_close_slot_no_op")
            self.assertEqual(noop_reg["reports_written"], 0)
            self.assertFalse((reg_dir / "reports").exists())

            # (b) Half-day 2025-11-28: 14:30 ET slot runs and commits seq=0; 17:30 ET slot returns already_committed_no_op
            def _hd_transport(url: str, headers: Mapping[str, str]) -> tuple[int, bytes]:
                if "/daily/TQQQ/prices" in url:
                    return 200, json.dumps(
                        [{"date": "2025-11-28T00:00:00.000Z", "close": 48.35, "divCash": 0.0, "splitFactor": 1.0}]
                    ).encode("utf-8")
                if "/daily/SOXL/prices" in url:
                    return 200, json.dumps(
                        [{"date": "2025-11-28T00:00:00.000Z", "close": 32.55, "divCash": 0.0, "splitFactor": 1.0}]
                    ).encode("utf-8")
                if "/daily/QQQ/prices" in url:
                    return 200, json.dumps(
                        [{"date": "2025-11-28T00:00:00.000Z", "close": 514.00, "divCash": 0.0, "splitFactor": 1.0}]
                    ).encode("utf-8")
                if "%5EXNDX" in url:
                    return 200, json.dumps(
                        {
                            "chart": {
                                "result": [
                                    {
                                        "meta": {"symbol": "^XNDX", "instrumentType": "INDEX"},
                                        "timestamp": [1764352800],
                                        "indicators": {"quote": [{"close": [29700.0]}]},
                                    }
                                ]
                            }
                        }
                    ).encode("utf-8")
                if "XSOX" in url:
                    return 200, json.dumps(
                        {
                            "chart": {
                                "result": [
                                    {
                                        "meta": {"symbol": "XSOX", "instrumentType": "INDEX"},
                                        "timestamp": [1764352800],
                                        "indicators": {"quote": [{"close": [8230.0]}]},
                                    }
                                ]
                            }
                        }
                    ).encode("utf-8")
                if "markets.newyorkfed.org" in url:
                    return 200, json.dumps(
                        {"refRates": [{"effectiveDate": "2025-11-26", "type": "SOFR", "percentRate": 3.89, "revisionIndicator": ""}]}
                    ).encode("utf-8")
                if "treasury.example.test" in url:
                    return 200, b"Date,3 Mo\n2025-11-26,3.88\n"
                return 404, b""

            hd_dir = root / "sim_hd_auto"
            dt_1430_hd = datetime(2025, 11, 28, 14, 30, 0, tzinfo=ny_tz).astimezone(timezone.utc)
            self.assertTrue(ny_local_time_matches(dt_1430_hd, req_hhmm))
            tgt_hd1, noop_hd1 = _resolve_auto_run_target(cfg, dt_1430_hd, data_dir=hd_dir)
            self.assertEqual(tgt_hd1, "2025-11-28")
            self.assertIsNone(noop_hd1)
            hd_res = run_daily_drill_cycle(
                session_date="2025-11-28",
                data_dir=hd_dir,
                config=cfg,
                transport=_hd_transport,
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-28", "19:30:00"),
                treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                sleeper=lambda _: None,
            )
            self.assertTrue(hd_res["appended"], hd_res)
            rep_bytes_before = (hd_dir / "reports" / "completeness_2025-11-28.json").read_bytes()

            dt_1730_hd = datetime(2025, 11, 28, 17, 30, 0, tzinfo=ny_tz).astimezone(timezone.utc)
            tgt_hd2, noop_hd2 = _resolve_auto_run_target(cfg, dt_1730_hd, data_dir=hd_dir)
            self.assertIsNone(tgt_hd2)
            assert noop_hd2 is not None
            self.assertEqual(noop_hd2["status"], "already_committed_no_op")
            self.assertEqual(noop_hd2["reports_written"], 0)
            self.assertEqual(len(ShadowLedger(hd_dir / "ledger").load_records()), 1)
            self.assertEqual((hd_dir / "reports" / "completeness_2025-11-28.json").read_bytes(), rep_bytes_before)

    def test_r3_serialized_lock_conflict_orphan_recovery_and_report_lower_bound(self) -> None:
        cfg = load_scheduler_config(DEFAULT_SCHEDULER_CONFIG_PATH)
        spec = load_spec()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            # (a) Concurrent/duplicate seq conflict MUST NOT delete existing committed record, .ots, receipt, or replay_bundle.json
            conflict_dir = root / "conflict_data"
            ok0 = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=conflict_dir,
                config=cfg,
                fixtures_dir=DRILL_FIXTURES,
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21"),
            )
            self.assertTrue(ok0["appended"])
            ledger = ShadowLedger(conflict_dir / "ledger")
            rec0_before = ledger._record_path(0).read_bytes()
            rcp0_before = ledger._receipt_path(0).read_bytes()
            ots0_before = ledger._ots_path(0).read_bytes()
            bundle_before = (conflict_dir / "replay" / "replay_bundle.json").read_bytes()

            # Simulate stale-seq concurrent worker attempting _commit_drill_session_atomically with seq=0
            env0 = ledger.load_records()[0]
            ots0, _ = build_synthetic_ots_fixture(env0["record_sha256"])
            with self.assertRaises(LedgerAppendError):
                _commit_drill_session_atomically(
                    ledger=ledger,
                    seq=0,
                    body=env0["record_body"],
                    ots_bytes=ots0,
                    rcp_obtained_utc="2025-11-21T22:35:00.000Z",
                    deadline_utc="2025-11-22T04:30:00.000Z",
                    allow_unanchored=False,
                    bundle_dir=conflict_dir / "replay",
                    cfg=cfg,
                    spec=spec,
                    code_sha=env0["record_body"]["code_version_sha"],
                    runtime_config=_effective_runtime_config(cfg),
                    archive_dir=conflict_dir / "archive",
                    session_entry={"session_date": "2025-11-21", "decision_timestamp_utc": "2025-11-21T22:30:00.000Z", "inputs": {}},
                )
            # Existing seq 0 record, receipt, .ots, AND replay_bundle MUST remain byte-for-byte untouched!
            self.assertEqual(ledger._record_path(0).read_bytes(), rec0_before)
            self.assertEqual(ledger._receipt_path(0).read_bytes(), rcp0_before)
            self.assertEqual(ledger._ots_path(0).read_bytes(), ots0_before)
            self.assertEqual((conflict_dir / "replay" / "replay_bundle.json").read_bytes(), bundle_before)

            # Real concurrent threads on the same data_dir serialize via .chain.lock without deleting each other
            conc_dir = root / "concurrent_threads"
            thread_results: list[object] = []

            def _worker(ds: str) -> None:
                try:
                    r = run_daily_drill_cycle(
                        session_date=ds,
                        data_dir=conc_dir,
                        config=cfg,
                        transport=lambda u, h: _mock_market_transport(u, h),
                        environ={"TIINGO_API_KEY": "test-key"},
                        ots_transport=_mock_ots_transport,
                        wall_clock=_tick_clock(ds),
                        treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                        sleeper=lambda _: None,
                    )
                    thread_results.append(("ok", ds, r["sequence"]))
                except Exception as exc:
                    thread_results.append(("err", ds, type(exc).__name__))

            t_a = threading.Thread(target=_worker, args=("2025-11-21",))
            t_b = threading.Thread(target=_worker, args=("2025-11-24",))
            t_a.start()
            t_b.start()
            t_a.join()
            t_b.join()
            self.assertEqual(len([x for x in thread_results if x[0] == "ok"]), 2, thread_results)
            self.assertTrue(ShadowLedger(conc_dir / "ledger").verify_chain()[0])

            # (b) Simulated abrupt crash leaving orphan .ots, .receipt.json, and bundle entry without seq_000000.record.json
            orphan_dir = root / "orphan_crash"
            (orphan_dir / "ledger").mkdir(parents=True)
            (orphan_dir / "replay").mkdir(parents=True)
            (orphan_dir / "ledger" / "seq_000000.ots").write_bytes(b"stale-orphan-ots")
            (orphan_dir / "ledger" / "seq_000000.receipt.json").write_text('{"orphan":true}', encoding="utf-8")
            (orphan_dir / "replay" / "replay_bundle.json").write_text(
                json.dumps(
                    {
                        "chain_id": cfg.chain_id,
                        "mode": cfg.mode,
                        "calendar_version": cfg.calendar_version,
                        "calendar_spec_dir": spec["spec_dir"],
                        "calendar_spec_digest": spec["spec_digest"],
                        "code_version_sha": compute_code_manifest_hash(REPO_ROOT, calendar_version=cfg.calendar_version),
                        "runtime_config": _effective_runtime_config(cfg),
                        "archive_dir": str(orphan_dir / "archive"),
                        "sessions": [{"session_date": "2025-11-21", "orphan": True}],
                    }
                ),
                encoding="utf-8",
            )
            recovered_orphan = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=orphan_dir,
                config=cfg,
                fixtures_dir=DRILL_FIXTURES,
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21"),
            )
            self.assertTrue(recovered_orphan["appended"])
            self.assertEqual(len(ShadowLedger(orphan_dir / "ledger").load_records()), 1)
            self.assertTrue(ShadowLedger(orphan_dir / "ledger").verify_chain()[0])
            rep_orphan = replay_window(orphan_dir / "replay", orphan_dir / "ledger")
            self.assertTrue(rep_orphan["bit_for_bit_match"])
            self.assertTrue(rep_orphan["archived_inputs_ok"])

            # (c) R3 #6 regression: future session requested before session_close_utc writes ZERO completeness report,
            # AND a pre-placed pre-close report STILL classifies the session as silent_missed!
            pre_future_dir = root / "future_preclose"
            fut_res = run_daily_drill_cycle(
                session_date="2025-11-24",
                data_dir=pre_future_dir,
                config=cfg,
                wall_clock=lambda: datetime(2025, 11, 21, 22, 0, 0, tzinfo=timezone.utc),
            )
            self.assertFalse(fut_res["appended"])
            self.assertEqual(fut_res["reason"], "session_not_yet_closed")
            self.assertIsNone(fut_res["report"])
            self.assertFalse(fut_res["report_written"])
            self.assertFalse((pre_future_dir / "reports" / "completeness_2025-11-24.json").exists())

            # Even if a pre-close completeness_2025-11-24.json is planted on Nov 21, detect_missed_runs marks Nov 24 silent_missed
            (pre_future_dir / "reports").mkdir(parents=True, exist_ok=True)
            (pre_future_dir / "reports" / "completeness_2025-11-24.json").write_text(
                json.dumps(
                    {
                        "session_date": "2025-11-24",
                        "created_at_utc": "2025-11-21T22:00:00.000Z",
                        "ledger": {"appended": False, "fail_closed_reason": "O2:future"},
                    }
                ),
                encoding="utf-8",
            )
            missed_fut = detect_missed_runs(
                window_start_date="2025-11-24",
                as_of_utc="2025-11-25T05:00:00.000Z",
                data_dir=pre_future_dir,
                config=cfg,
            )
            self.assertTrue(missed_fut["missed_run_detected"])
            self.assertEqual(missed_fut["silent_missed_days"], ["2025-11-24"])

    def test_r4_missed_run_subcases_post_record_recovery_and_intraday_revision_retry(self) -> None:
        cfg = replace(
            load_scheduler_config(DEFAULT_SCHEDULER_CONFIG_PATH),
            treasury_endpoint="https://treasury.example.test/yield{year}.csv",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            # (1) R4 #2/#6 case 1: suppressed 14:30 ET half-day followed by 17:30 ET run writes late_after_decision_cutoff -> silent_missed_days
            hd_late_dir = root / "hd_late"
            hd_late_res = run_daily_drill_cycle(
                session_date="2025-11-28",
                data_dir=hd_late_dir,
                config=cfg,
                fixtures_dir=DRILL_FIXTURES,
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-28", "22:30:00"),
            )
            self.assertFalse(hd_late_res["appended"])
            self.assertIn("late_after_decision_cutoff", hd_late_res["reason"])
            hd_missed = detect_missed_runs(
                window_start_date="2025-11-28",
                as_of_utc="2025-11-29T05:00:00.000Z",
                data_dir=hd_late_dir,
                config=cfg,
            )
            self.assertTrue(hd_missed["missed_run_detected"])
            self.assertEqual(hd_missed["silent_missed_days"], ["2025-11-28"])

            # (2) R4 #2/#6 case 2: crash AFTER seq_000000.record.json commit BEFORE completeness report write
            post_rec_dir = root / "post_rec_crash"

            def _post_rec_boom() -> None:
                raise OSError("simulated kill right after seq_000000.record.json write")

            with self.assertRaises(OSError):
                run_daily_drill_cycle(
                    session_date="2025-11-21",
                    data_dir=post_rec_dir,
                    config=cfg,
                    fixtures_dir=DRILL_FIXTURES,
                    ots_transport=_mock_ots_transport,
                    wall_clock=_tick_clock("2025-11-21"),
                    _post_record_commit_hook=_post_rec_boom,
                )
            self.assertTrue((post_rec_dir / "ledger" / "seq_000000.record.json").is_file())
            self.assertFalse((post_rec_dir / "reports" / "completeness_2025-11-21.json").exists())

            # Before retry reconciles missing report, detector MUST NOT classify session as 'ok'
            before_rec_missed = detect_missed_runs(
                window_start_date="2025-11-21",
                as_of_utc="2025-11-22T05:00:00.000Z",
                data_dir=post_rec_dir,
                config=cfg,
            )
            self.assertEqual(before_rec_missed["ok_days"], [])
            self.assertEqual(before_rec_missed["failed_days"], ["2025-11-21"])
            self.assertTrue(before_rec_missed["missed_run_detected"])

            # Retry recovers missing completeness report cleanly without duplicate_session_append crash
            rec_retry = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=post_rec_dir,
                config=cfg,
                fixtures_dir=DRILL_FIXTURES,
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21"),
            )
            self.assertFalse(rec_retry["appended"])
            self.assertTrue(rec_retry.get("recovered_missing_report"))
            self.assertTrue(rec_retry["report_written"])
            self.assertTrue((post_rec_dir / "reports" / "completeness_2025-11-21.json").is_file())
            self.assertTrue((post_rec_dir / "reports" / "completeness_2025-11-21.md").is_file())
            after_rec_missed = detect_missed_runs(
                window_start_date="2025-11-21",
                as_of_utc="2025-11-22T05:00:00.000Z",
                data_dir=post_rec_dir,
                config=cfg,
            )
            self.assertEqual(after_rec_missed["ok_days"], ["2025-11-21"])
            self.assertFalse(after_rec_missed["missed_run_detected"])

            # (3) R4 #2/#6 case 3: receipt obtained_at_utc earlier than decision_timestamp_utc MUST NOT pass detector
            backdated_rcp_dir = root / "backdated_rcp"
            ok_bd = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=backdated_rcp_dir,
                config=cfg,
                fixtures_dir=DRILL_FIXTURES,
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21"),
            )
            self.assertTrue(ok_bd["appended"])
            rcp0_path = backdated_rcp_dir / "ledger" / "seq_000000.receipt.json"
            rcp0_obj = json.loads(rcp0_path.read_text(encoding="utf-8"))
            rcp0_obj["obtained_at_utc"] = "2025-11-20T22:00:00.000Z"
            rcp0_path.write_text(json.dumps(rcp0_obj, sort_keys=True) + "\n", encoding="utf-8")
            bd_missed = detect_missed_runs(
                window_start_date="2025-11-21",
                as_of_utc="2025-11-22T05:00:00.000Z",
                data_dir=backdated_rcp_dir,
                config=cfg,
            )
            self.assertEqual(bd_missed["ok_days"], [])
            self.assertEqual(bd_missed["failed_days"], ["2025-11-21"])

            # (4) R4 #5: unanchored retry deletes stale orphan .ots/.receipt AND intraday SOFR revision advances to rev1 cleanly
            rev_dir = root / "orphan_unanchored_rev"
            phase = {"attempt": 1}

            def _revising_transport(url: str, headers: Mapping[str, str]) -> tuple[int, bytes]:
                if "markets.newyorkfed.org" in url and phase["attempt"] == 2:
                    return 200, json.dumps(
                        {
                            "refRates": [
                                {
                                    "effectiveDate": "2025-11-20",
                                    "type": "SOFR",
                                    "percentRate": 3.94,
                                    "revisionIndicator": "R",
                                }
                            ]
                        }
                    ).encode("utf-8")
                return _mock_market_transport(url, headers)

            # First attempt stages rev0 archive + bundle + orphan .ots/.receipt? Wait: simulate abrupt crash right before .record.json!
            # We let _post_ledger_commit_hook run AFTER .ots/.receipt/bundle staged and leave them behind by raising BaseException?
            # Cleanly test both: attempt 1 commits nothing via _post_ledger_commit_hook (leaving rev0 on disk), then also plant stale orphan .ots/.receipt!
            try:
                run_daily_drill_cycle(
                    session_date="2025-11-21",
                    data_dir=rev_dir,
                    config=cfg,
                    transport=_revising_transport,
                    environ={"TIINGO_API_KEY": "test-key"},
                    ots_transport=_mock_ots_transport,
                    wall_clock=_tick_clock("2025-11-21"),
                    sleeper=lambda _: None,
                    _post_ledger_commit_hook=lambda: (_ for _ in ()).throw(OSError("abort before marker")),
                )
            except OSError:
                pass
            # Plant stale orphan .ots and .receipt matching attempt 1 (as if crash skipped unlink)
            (rev_dir / "ledger" / "seq_000000.ots").write_bytes(b"stale-ots-attempt-1")
            (rev_dir / "ledger" / "seq_000000.receipt.json").write_text('{"anchor":"opentimestamps"}', encoding="utf-8")
            rev0_sofr = rev_dir / "archive" / "rev0_nyfed_sofr_2025-11-21.json"
            self.assertTrue(rev0_sofr.is_file())
            rev0_sofr_bytes = rev0_sofr.read_bytes()

            # Attempt 2: NY Fed publishes revised SOFR AND OTS is unavailable -> falls back to unanchored
            phase["attempt"] = 2
            res_rev2 = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=rev_dir,
                config=cfg,
                transport=_revising_transport,
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=lambda m, u, b, h, t: (503, b"ots down"),
                wall_clock=_tick_clock("2025-11-21"),
                sleeper=lambda _: None,
                allow_unanchored_on_ots_failure=True,
            )
            self.assertTrue(res_rev2["appended"])
            self.assertEqual(res_rev2["report"]["receipt"]["status"], "missing")
            # Orphan .ots and .receipt MUST be gone so detector agrees it is failed_documented (not 'ok')!
            self.assertFalse((rev_dir / "ledger" / "seq_000000.ots").exists())
            self.assertFalse((rev_dir / "ledger" / "seq_000000.receipt.json").exists())
            # Intraday SOFR revision archived as rev1 cleanly while rev0 remained immutable
            self.assertEqual(rev0_sofr.read_bytes(), rev0_sofr_bytes)
            self.assertTrue((rev_dir / "archive" / "rev1_nyfed_sofr_2025-11-21.json").is_file())
            sofr_summary = next(s for s in res_rev2["report"]["required_series"] if s["instrument"] == "SOFR")
            self.assertEqual(sofr_summary["status"], "revised")
            rev_missed = detect_missed_runs(
                window_start_date="2025-11-21",
                as_of_utc="2025-11-22T05:00:00.000Z",
                data_dir=rev_dir,
                config=cfg,
            )
            self.assertEqual(rev_missed["ok_days"], [])
            self.assertEqual(rev_missed["failed_days"], ["2025-11-21"])

    def test_r5_strict_schedule_config_retry_report_upgrade_and_missing_bundle_recovery(self) -> None:
        cfg = replace(
            load_scheduler_config(DEFAULT_SCHEDULER_CONFIG_PATH),
            treasury_endpoint="https://treasury.example.test/yield{year}.csv",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            # (a) R5 #4: omitted commitment_deadline_et and non-HH:MM "23:0" MUST fail closed
            raw_cfg = json.loads(DEFAULT_SCHEDULER_CONFIG_PATH.read_text(encoding="utf-8"))
            del raw_cfg["commitment_deadline_et"]
            missing_dl_cfg = root / "missing_deadline.json"
            missing_dl_cfg.write_text(json.dumps(raw_cfg), encoding="utf-8")
            with self.assertRaises(ValueError):
                load_scheduler_config(missing_dl_cfg)

            raw_bad_hhmm = json.loads(DEFAULT_SCHEDULER_CONFIG_PATH.read_text(encoding="utf-8"))
            raw_bad_hhmm["commitment_deadline_et"] = "23:0"
            bad_hhmm_cfg = root / "bad_hhmm.json"
            bad_hhmm_cfg.write_text(json.dumps(raw_bad_hhmm), encoding="utf-8")
            with self.assertRaises(ValueError):
                load_scheduler_config(bad_hhmm_cfg)

            with self.assertRaises(ValueError):
                DrillSchedulerConfig(commitment_deadline_et="23:0")
            with self.assertRaises(ValueError):
                _et_hhmm_to_utc_ms("2025-11-21", "23:0")
            with self.assertRaises(ValueError):
                ny_local_time_matches(datetime(2025, 11, 22, 4, 0, tzinfo=timezone.utc), "23:0")

            # (b) R5 #2/#6 case 1: 17:30 ET missing QQQ -> 17:36 ET retry succeeds & upgrades report -> 17:42 ET no-op -> 'ok'
            upgrade_dir = root / "retry_upgrade"
            r_miss_qqq = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=upgrade_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h, withhold_inst="QQQ"),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21", "22:30:00"),
                sleeper=lambda _: None,
            )
            self.assertFalse(r_miss_qqq["appended"])
            self.assertTrue(r_miss_qqq["report_written"])
            self.assertFalse(r_miss_qqq["report"]["ledger"]["appended"])

            r_ok_retry = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=upgrade_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21", "22:36:00"),
                sleeper=lambda _: None,
            )
            self.assertTrue(r_ok_retry["appended"])
            self.assertTrue(r_ok_retry["report_written"])
            upgraded_rep = json.loads((upgrade_dir / "reports" / "completeness_2025-11-21.json").read_text(encoding="utf-8"))
            self.assertTrue(upgraded_rep["ledger"]["appended"])
            self.assertEqual(len(upgraded_rep["required_series"]), 7)
            upgraded_bytes = (upgrade_dir / "reports" / "completeness_2025-11-21.json").read_bytes()

            r_rerun = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=upgrade_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21", "22:42:00"),
                sleeper=lambda _: None,
            )
            self.assertFalse(r_rerun["appended"])
            self.assertTrue(r_rerun["existing_report_preserved"])
            self.assertEqual((upgrade_dir / "reports" / "completeness_2025-11-21.json").read_bytes(), upgraded_bytes)

            missed_upgraded = detect_missed_runs(
                window_start_date="2025-11-21",
                as_of_utc="2025-11-22T05:00:00.000Z",
                data_dir=upgrade_dir,
                config=cfg,
            )
            self.assertEqual(missed_upgraded["ok_days"], ["2025-11-21"])
            self.assertEqual(missed_upgraded["silent_missed_days"], [])
            self.assertFalse(missed_upgraded["missed_run_detected"])

            # Verify a late_after_decision_cutoff report is NEVER upgraded on subsequent run
            late_no_up_dir = root / "late_no_upgrade"
            r_late_hd = run_daily_drill_cycle(
                session_date="2025-11-28",
                data_dir=late_no_up_dir,
                config=cfg,
                fixtures_dir=DRILL_FIXTURES,
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-28", "22:30:00"),
            )
            self.assertIn("late_after_decision_cutoff", r_late_hd["reason"])
            late_hd_bytes = (late_no_up_dir / "reports" / "completeness_2025-11-28.json").read_bytes()
            r_back_in = run_daily_drill_cycle(
                session_date="2025-11-28",
                data_dir=late_no_up_dir,
                config=cfg,
                fixtures_dir=DRILL_FIXTURES,
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-28", "19:30:00"),
            )
            self.assertTrue(r_back_in["existing_report_preserved"])
            self.assertFalse(r_back_in["report_written"])
            self.assertEqual((late_no_up_dir / "reports" / "completeness_2025-11-28.json").read_bytes(), late_hd_bytes)

            # (c) R5 #2/#6 case 2: missing replay_bundle.json during post-record recovery MUST fail closed and NEVER become 'ok'
            no_bundle_dir = root / "post_rec_no_bundle"
            with self.assertRaises(OSError):
                run_daily_drill_cycle(
                    session_date="2025-11-21",
                    data_dir=no_bundle_dir,
                    config=cfg,
                    fixtures_dir=DRILL_FIXTURES,
                    ots_transport=_mock_ots_transport,
                    wall_clock=_tick_clock("2025-11-21"),
                    _post_record_commit_hook=lambda: (_ for _ in ()).throw(OSError("crash after record commit")),
                )
            (no_bundle_dir / "replay" / "replay_bundle.json").unlink()
            rec_no_bundle = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=no_bundle_dir,
                config=cfg,
                fixtures_dir=DRILL_FIXTURES,
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21"),
            )
            self.assertFalse(rec_no_bundle["appended"])
            self.assertTrue(rec_no_bundle["fail_closed"])
            self.assertEqual(rec_no_bundle["reason"], "incomplete_missing_replay_bundle")
            self.assertFalse(rec_no_bundle["report_written"])
            self.assertFalse((no_bundle_dir / "reports" / "completeness_2025-11-21.json").exists())

            missed_no_bundle = detect_missed_runs(
                window_start_date="2025-11-21",
                as_of_utc="2025-11-22T05:00:00.000Z",
                data_dir=no_bundle_dir,
                config=cfg,
            )
            self.assertEqual(missed_no_bundle["ok_days"], [])
            self.assertEqual(missed_no_bundle["evaluated_trading_days"][0]["status"], "incomplete_missing_replay_bundle")
            self.assertTrue(missed_no_bundle["missed_run_detected"])

    def test_r6_unicode_dst_list_missing_bundle_null_quote_and_halfday_json_md_heal(self) -> None:
        cfg = replace(
            load_scheduler_config(DEFAULT_SCHEDULER_CONFIG_PATH),
            treasury_endpoint="https://treasury.example.test/yield{year}.csv",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            # (a) R6 #4: full-width Unicode digits & invalid trailing tokens in DST list MUST raise ValueError
            fullwidth_raw = json.loads(DEFAULT_SCHEDULER_CONFIG_PATH.read_text(encoding="utf-8"))
            fullwidth_raw["commitment_deadline_et"] = "\uff12\uff13:\uff13\uff10"
            fw_cfg_path = root / "fullwidth_deadline.json"
            fw_cfg_path.write_text(json.dumps(fullwidth_raw), encoding="utf-8")
            with self.assertRaises(ValueError):
                load_scheduler_config(fw_cfg_path)
            with self.assertRaises(ValueError):
                DrillSchedulerConfig(commitment_deadline_et="\uff12\uff13:\uff13\uff10")
            with self.assertRaises(ValueError):
                _et_hhmm_to_utc_ms("2025-11-21", "\uff12\uff13:\uff13\uff10")
            with self.assertRaises(ValueError):
                ny_local_time_matches(
                    datetime(2025, 11, 21, 22, 30, tzinfo=timezone.utc),
                    "17:30,99:99",
                )
            with self.assertRaises(ValueError):
                ny_local_time_matches(
                    datetime(2025, 11, 21, 22, 30, tzinfo=timezone.utc),
                    "17:30,",
                )

            # (b) R6 #2/#6: Day 1 commits -> delete replay_bundle.json -> same-day retry & Day 2 append MUST fail closed
            missing_b_dir = root / "missing_bundle_cross_day"
            d1_ok = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=missing_b_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21"),
                sleeper=lambda _: None,
            )
            self.assertTrue(d1_ok["appended"])
            (missing_b_dir / "replay" / "replay_bundle.json").unlink()

            same_day_retry = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=missing_b_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21", "22:40:00"),
                sleeper=lambda _: None,
            )
            self.assertFalse(same_day_retry["appended"])
            self.assertTrue(same_day_retry["fail_closed"])
            self.assertEqual(same_day_retry["reason"], "incomplete_missing_replay_bundle")

            next_day_try = run_daily_drill_cycle(
                session_date="2025-11-24",
                data_dir=missing_b_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-24"),
                sleeper=lambda _: None,
            )
            self.assertFalse(next_day_try["appended"])
            self.assertTrue(next_day_try["fail_closed"])
            self.assertEqual(next_day_try["reason"], "incomplete_missing_replay_bundle")
            self.assertFalse((missing_b_dir / "replay" / "replay_bundle.json").exists())
            self.assertEqual(len(ShadowLedger(missing_b_dir / "ledger").load_records()), 1)

            # (c) R6 #2/#6: report/bundle with all 7 ticker names where 1 ticker has null/missing value MUST NOT enter ok_days
            null_q_dir = root / "null_quote_guard"
            ok_nq = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=null_q_dir,
                config=cfg,
                fixtures_dir=DRILL_FIXTURES,
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21"),
            )
            self.assertTrue(ok_nq["appended"])
            nq_rep_path = null_q_dir / "reports" / "completeness_2025-11-21.json"
            nq_rep = json.loads(nq_rep_path.read_text(encoding="utf-8"))
            nq_rep["required_series"][0]["normalized_value"] = None
            nq_rep["required_series"][0]["status"] = "missing"
            nq_rep_path.write_text(json.dumps(nq_rep, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            missed_nq = detect_missed_runs(
                window_start_date="2025-11-21",
                as_of_utc="2025-11-22T05:00:00.000Z",
                data_dir=null_q_dir,
                config=cfg,
            )
            self.assertEqual(missed_nq["ok_days"], [])
            self.assertTrue(missed_nq["missed_run_detected"])

            # (d) R6 #2/#6 case 1: Half-day --session auto heals stale pre-upgrade fail-closed reports after committed crash
            def _hd_transport(url: str, headers: Mapping[str, str], *, withhold_qqq: bool = False) -> tuple[int, bytes]:
                if "/daily/TQQQ/prices" in url:
                    return 200, json.dumps([{"date": "2025-11-28T00:00:00.000Z", "close": 48.35, "divCash": 0.0, "splitFactor": 1.0}]).encode("utf-8")
                if "/daily/SOXL/prices" in url:
                    return 200, json.dumps([{"date": "2025-11-28T00:00:00.000Z", "close": 32.55, "divCash": 0.0, "splitFactor": 1.0}]).encode("utf-8")
                if "/daily/QQQ/prices" in url:
                    if withhold_qqq:
                        return 503, b"outage"
                    return 200, json.dumps([{"date": "2025-11-28T00:00:00.000Z", "close": 514.00, "divCash": 0.0, "splitFactor": 1.0}]).encode("utf-8")
                if "%5EXNDX" in url:
                    return 200, json.dumps({"chart": {"result": [{"meta": {"symbol": "^XNDX", "instrumentType": "INDEX"}, "timestamp": [1764352800], "indicators": {"quote": [{"close": [29700.0]}]}}]}}).encode("utf-8")
                if "XSOX" in url:
                    return 200, json.dumps({"chart": {"result": [{"meta": {"symbol": "XSOX", "instrumentType": "INDEX"}, "timestamp": [1764352800], "indicators": {"quote": [{"close": [8230.0]}]}}]}}).encode("utf-8")
                if "markets.newyorkfed.org" in url:
                    return 200, json.dumps({"refRates": [{"effectiveDate": "2025-11-26", "type": "SOFR", "percentRate": 3.89, "revisionIndicator": ""}]}).encode("utf-8")
                if "treasury.example.test" in url:
                    return 200, b"Date,3 Mo\n2025-11-28,3.88\n"
                return 404, b""

            hd_auto_dir = root / "halfday_auto_heal"
            hd_fail = run_daily_drill_cycle(
                session_date="2025-11-28",
                data_dir=hd_auto_dir,
                config=cfg,
                transport=lambda u, h: _hd_transport(u, h, withhold_qqq=True),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-28", "19:30:00"),
                sleeper=lambda _: None,
            )
            self.assertFalse(hd_fail["appended"])
            with self.assertRaises(OSError):
                run_daily_drill_cycle(
                    session_date="2025-11-28",
                    data_dir=hd_auto_dir,
                    config=cfg,
                    transport=lambda u, h: _hd_transport(u, h, withhold_qqq=False),
                    environ={"TIINGO_API_KEY": "test-key"},
                    ots_transport=_mock_ots_transport,
                    wall_clock=_tick_clock("2025-11-28", "19:35:00"),
                    sleeper=lambda _: None,
                    _post_record_commit_hook=lambda: (_ for _ in ()).throw(OSError("crash before report upgrade")),
                )
            # Both JSON & MD exist on disk from earlier failure -> _resolve_auto_run_target MUST NOT return already_committed_no_op!
            resolved_hd, noop_hd = _resolve_auto_run_target(
                cfg,
                datetime(2025, 11, 28, 22, 30, 0, tzinfo=timezone.utc),  # 17:30 EST auto slot
                data_dir=hd_auto_dir,
            )
            self.assertEqual(resolved_hd, "2025-11-28")
            self.assertIsNone(noop_hd)
            healed_hd = run_daily_drill_cycle(
                session_date="2025-11-28",
                data_dir=hd_auto_dir,
                config=cfg,
                wall_clock=lambda: datetime(2025, 11, 28, 22, 30, 5, tzinfo=timezone.utc),
            )
            self.assertFalse(healed_hd["appended"])
            self.assertTrue(healed_hd.get("recovered_missing_report"))
            self.assertTrue(healed_hd["report_written"])
            resolved_after, noop_after = _resolve_auto_run_target(
                cfg,
                datetime(2025, 11, 28, 22, 31, 0, tzinfo=timezone.utc),
                data_dir=hd_auto_dir,
            )
            self.assertIsNone(resolved_after)
            assert noop_after is not None
            self.assertEqual(noop_after["status"], "already_committed_no_op")
            missed_hd_ok = detect_missed_runs(
                window_start_date="2025-11-28",
                as_of_utc="2025-11-29T05:00:00.000Z",
                data_dir=hd_auto_dir,
                config=cfg,
            )
            self.assertEqual(missed_hd_ok["ok_days"], ["2025-11-28"])
            self.assertFalse(missed_hd_ok["missed_run_detected"])

            # (e) R6 #2/#6 case 2: crash right AFTER JSON upgrade and BEFORE Markdown upgrade MUST NOT pass ok_days and heals on retry
            split_up_dir = root / "split_json_md_upgrade"
            r_init_fail = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=split_up_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h, withhold_inst="QQQ"),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21", "22:30:00"),
                sleeper=lambda _: None,
            )
            self.assertFalse(r_init_fail["appended"])
            with self.assertRaises(OSError):
                run_daily_drill_cycle(
                    session_date="2025-11-21",
                    data_dir=split_up_dir,
                    config=cfg,
                    transport=lambda u, h: _mock_market_transport(u, h),
                    environ={"TIINGO_API_KEY": "test-key"},
                    ots_transport=_mock_ots_transport,
                    wall_clock=_tick_clock("2025-11-21", "22:36:00"),
                    sleeper=lambda _: None,
                    _post_report_json_hook=lambda: (_ for _ in ()).throw(OSError("crash between JSON and MD write")),
                )
            stale_md_text = (split_up_dir / "reports" / "completeness_2025-11-21.md").read_text(encoding="utf-8")
            self.assertIn("**Ledger Appended:** `False`", stale_md_text)

            before_md_heal = detect_missed_runs(
                window_start_date="2025-11-21",
                as_of_utc="2025-11-22T05:00:00.000Z",
                data_dir=split_up_dir,
                config=cfg,
            )
            self.assertEqual(before_md_heal["ok_days"], [])
            self.assertTrue(before_md_heal["missed_run_detected"])

            healed_md_res = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=split_up_dir,
                config=cfg,
                wall_clock=_tick_clock("2025-11-21", "22:42:00"),
            )
            self.assertTrue(healed_md_res.get("recovered_missing_report"))
            self.assertTrue(healed_md_res["report_written"])
            healed_md_text = (split_up_dir / "reports" / "completeness_2025-11-21.md").read_text(encoding="utf-8")
            self.assertIn("**Ledger Appended:** `True`", healed_md_text)
            after_md_heal = detect_missed_runs(
                window_start_date="2025-11-21",
                as_of_utc="2025-11-22T05:00:00.000Z",
                data_dir=split_up_dir,
                config=cfg,
            )
            self.assertEqual(after_md_heal["ok_days"], ["2025-11-21"])
            self.assertFalse(after_md_heal["missed_run_detected"])

    def test_r7_bundle_inputs_digest_report_integrity_crash_tail_and_empty_cli_flags(self) -> None:
        cfg = replace(
            load_scheduler_config(DEFAULT_SCHEDULER_CONFIG_PATH),
            treasury_endpoint="https://treasury.example.test/yield{year}.csv",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            # 1. R7 #1: Tampering TQQQ q_pre (0 -> 1) in replay_bundle.json MUST fail retry, next-day append, and detect_missed
            qpre_dir = root / "bundle_qpre_tamper"
            d1_ok = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=qpre_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21"),
                sleeper=lambda _: None,
            )
            self.assertTrue(d1_ok["appended"])
            qpre_bundle_path = qpre_dir / "replay" / "replay_bundle.json"
            qpre_bundle = json.loads(qpre_bundle_path.read_text(encoding="utf-8"))
            qpre_bundle["sessions"][0]["inputs"]["funds"]["TQQQ"]["q_pre"] = 1
            qpre_bundle_path.write_text(json.dumps(qpre_bundle, indent=2, sort_keys=True) + "\n", encoding="utf-8")

            qpre_retry = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=qpre_dir,
                config=cfg,
                wall_clock=_tick_clock("2025-11-21", "22:40:00"),
            )
            self.assertFalse(qpre_retry["appended"])
            self.assertTrue(qpre_retry["fail_closed"])
            self.assertEqual(qpre_retry["reason"], "incomplete_or_corrupt_replay_bundle")
            self.assertIn("inputs_digest", qpre_retry["detail"])

            qpre_day2 = run_daily_drill_cycle(
                session_date="2025-11-24",
                data_dir=qpre_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-24"),
                sleeper=lambda _: None,
            )
            self.assertFalse(qpre_day2["appended"])
            self.assertTrue(qpre_day2["fail_closed"])
            self.assertEqual(len(ShadowLedger(qpre_dir / "ledger").load_records()), 1)

            qpre_missed = detect_missed_runs(
                window_start_date="2025-11-21",
                as_of_utc="2025-11-22T05:00:00.000Z",
                data_dir=qpre_dir,
                config=cfg,
            )
            self.assertEqual(qpre_missed["ok_days"], [])
            self.assertTrue(qpre_missed["missed_run_detected"])
            self.assertEqual(qpre_missed["evaluated_trading_days"][0]["status"], "incomplete_or_corrupt_replay_bundle")

            # 2. R7 #2: Tampered report JSON quote ('47.48' -> '999') MUST NOT be copied to MD or pass ok_days; retry restores '47.48'
            rep_tamper_dir = root / "report_price_999"
            self.assertTrue(
                run_daily_drill_cycle(
                    session_date="2025-11-21",
                    data_dir=rep_tamper_dir,
                    config=cfg,
                    transport=lambda u, h: _mock_market_transport(u, h),
                    environ={"TIINGO_API_KEY": "test-key"},
                    ots_transport=_mock_ots_transport,
                    wall_clock=_tick_clock("2025-11-21"),
                    sleeper=lambda _: None,
                )["appended"]
            )
            t_json_path = rep_tamper_dir / "reports" / "completeness_2025-11-21.json"
            t_md_path = rep_tamper_dir / "reports" / "completeness_2025-11-21.md"
            t_rep = json.loads(t_json_path.read_text(encoding="utf-8"))
            t_rep["required_series"][0]["normalized_value"] = "999"
            t_json_path.write_text(json.dumps(t_rep, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            # Even if Markdown were also rewritten to match '999', detector MUST still fail vs authoritative bundle+ledger!
            t_md_path.write_text(t_md_path.read_text(encoding="utf-8").replace("`47.48`", "`999`"), encoding="utf-8")

            missed_999 = detect_missed_runs(
                window_start_date="2025-11-21",
                as_of_utc="2025-11-22T05:00:00.000Z",
                data_dir=rep_tamper_dir,
                config=cfg,
            )
            self.assertEqual(missed_999["ok_days"], [])
            self.assertTrue(missed_999["missed_run_detected"])
            self.assertEqual(
                missed_999["evaluated_trading_days"][0]["status"],
                "incomplete_missing_completeness_report",
            )

            healed_999 = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=rep_tamper_dir,
                config=cfg,
                wall_clock=_tick_clock("2025-11-21", "22:40:00"),
            )
            self.assertFalse(healed_999["appended"])
            self.assertTrue(healed_999["recovered_missing_report"])
            self.assertFalse(healed_999["fail_closed"])
            self.assertTrue(healed_999["report_written"])
            self.assertEqual(
                json.loads(t_json_path.read_text(encoding="utf-8"))["required_series"][0]["normalized_value"],
                "47.48",
            )
            self.assertNotIn("`999`", t_md_path.read_text(encoding="utf-8"))
            self.assertIn("`47.48`", t_md_path.read_text(encoding="utf-8"))
            missed_999_after = detect_missed_runs(
                window_start_date="2025-11-21",
                as_of_utc="2025-11-22T05:00:00.000Z",
                data_dir=rep_tamper_dir,
                config=cfg,
            )
            self.assertEqual(missed_999_after["ok_days"], ["2025-11-21"])
            self.assertFalse(missed_999_after["missed_run_detected"])

            # 3. R7 #3: Deleted JSON (keeping MD), null quote in JSON, AND unwritable MD failure handling
            partial_dir = root / "partial_report_heal"
            self.assertTrue(
                run_daily_drill_cycle(
                    session_date="2025-11-21",
                    data_dir=partial_dir,
                    config=cfg,
                    fixtures_dir=DRILL_FIXTURES,
                    ots_transport=_mock_ots_transport,
                    wall_clock=_tick_clock("2025-11-21"),
                )["appended"]
            )
            p_json = partial_dir / "reports" / "completeness_2025-11-21.json"
            p_md = partial_dir / "reports" / "completeness_2025-11-21.md"

            # (3a) Delete JSON keeping MD -> retry regenerates BOTH cleanly and detect_missed marks ok
            p_json.unlink()
            self.assertTrue(p_md.is_file())
            rec_del_json = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=partial_dir,
                config=cfg,
                wall_clock=_tick_clock("2025-11-21", "22:40:00"),
            )
            self.assertTrue(rec_del_json["recovered_missing_report"])
            self.assertFalse(rec_del_json["fail_closed"])
            self.assertTrue(rec_del_json["report_written"])
            self.assertTrue(p_json.is_file() and p_md.is_file())
            self.assertEqual(
                detect_missed_runs(
                    window_start_date="2025-11-21",
                    as_of_utc="2025-11-22T05:00:00.000Z",
                    data_dir=partial_dir,
                    config=cfg,
                )["ok_days"],
                ["2025-11-21"],
            )

            # (3b) Set one quote to null in JSON keeping MD -> retry regenerates BOTH cleanly
            null_rep = json.loads(p_json.read_text(encoding="utf-8"))
            null_rep["required_series"][0]["normalized_value"] = None
            null_rep["required_series"][0]["status"] = "missing"
            p_json.write_text(json.dumps(null_rep, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            rec_null_json = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=partial_dir,
                config=cfg,
                wall_clock=_tick_clock("2025-11-21", "22:45:00"),
            )
            self.assertTrue(rec_null_json["recovered_missing_report"])
            self.assertFalse(rec_null_json["fail_closed"])
            self.assertTrue(rec_null_json["report_written"])
            self.assertIsNotNone(json.loads(p_json.read_text(encoding="utf-8"))["required_series"][0]["normalized_value"])
            self.assertEqual(
                detect_missed_runs(
                    window_start_date="2025-11-21",
                    as_of_utc="2025-11-22T05:00:00.000Z",
                    data_dir=partial_dir,
                    config=cfg,
                )["ok_days"],
                ["2025-11-21"],
            )

            # (3c) Block MD write with a directory at p_md -> retry MUST fail_closed=True, NEVER fake success
            p_json.unlink()
            p_md.unlink()
            p_md.mkdir()
            rec_unwritable = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=partial_dir,
                config=cfg,
                wall_clock=_tick_clock("2025-11-21", "22:50:00"),
            )
            self.assertFalse(rec_unwritable["recovered_missing_report"])
            self.assertTrue(rec_unwritable["fail_closed"])
            self.assertFalse(rec_unwritable["report_written"])
            p_md.rmdir()

            # 4. R7 #4: Simulated abrupt crash between bundle write and seq_000001.record.json commit
            class _SimulatedSigkill(BaseException):
                pass

            crash_tail_dir = root / "crash_between_bundle_and_record"
            self.assertTrue(
                run_daily_drill_cycle(
                    session_date="2025-11-21",
                    data_dir=crash_tail_dir,
                    config=cfg,
                    transport=lambda u, h: _mock_market_transport(u, h),
                    environ={"TIINGO_API_KEY": "test-key"},
                    ots_transport=_mock_ots_transport,
                    wall_clock=_tick_clock("2025-11-21"),
                    sleeper=lambda _: None,
                )["appended"]
            )
            with self.assertRaises(_SimulatedSigkill):
                run_daily_drill_cycle(
                    session_date="2025-11-24",
                    data_dir=crash_tail_dir,
                    config=cfg,
                    transport=lambda u, h: _mock_market_transport(u, h),
                    environ={"TIINGO_API_KEY": "test-key"},
                    ots_transport=_mock_ots_transport,
                    wall_clock=_tick_clock("2025-11-24"),
                    sleeper=lambda _: None,
                    _post_ledger_commit_hook=lambda: (_ for _ in ()).throw(
                        _SimulatedSigkill("kill after bundle write before record rename")
                    ),
                )
            # Confirm crashed on-disk state: 1 ledger record, 2 bundle sessions, zero pre-commit seq 1 ots/receipt
            self.assertEqual(len(ShadowLedger(crash_tail_dir / "ledger").load_records()), 1)
            crashed_bundle = json.loads((crash_tail_dir / "replay" / "replay_bundle.json").read_text(encoding="utf-8"))
            self.assertEqual(len(crashed_bundle["sessions"]), 2)
            self.assertFalse((crash_tail_dir / "ledger" / "seq_000001.receipt.json").exists())
            self.assertFalse((crash_tail_dir / "ledger" / "seq_000001.ots").exists())
            self.assertFalse((crash_tail_dir / "ledger" / "seq_000001.record.json").exists())

            # Before retry, detect_missed keeps Day 1 'ok' and flags crashed Day 2 as missed
            missed_pre = detect_missed_runs(
                window_start_date="2025-11-21",
                as_of_utc="2025-11-25T05:00:00.000Z",
                data_dir=crash_tail_dir,
                config=cfg,
            )
            self.assertEqual(missed_pre["ok_days"], ["2025-11-21"])
            self.assertEqual(missed_pre["silent_missed_days"], ["2025-11-24"])

            # Copy crashed directory to also test skipping Day 2 straight to Day 3!
            import shutil

            skip_day2_dir = root / "crash_tail_skip_day2_to_day3"
            shutil.copytree(crash_tail_dir, skip_day2_dir)

            # Retry Day 2 and continue with Day 3
            d2_recovered = run_daily_drill_cycle(
                session_date="2025-11-24",
                data_dir=crash_tail_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-24", "22:35:00"),
                sleeper=lambda _: None,
            )
            self.assertTrue(d2_recovered["appended"])
            self.assertEqual(d2_recovered["sequence"], 1)

            d3_after = run_daily_drill_cycle(
                session_date="2025-11-25",
                data_dir=crash_tail_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-25"),
                sleeper=lambda _: None,
            )
            self.assertTrue(d3_after["appended"])
            self.assertEqual(d3_after["sequence"], 2)
            rep_crash_3 = replay_window(crash_tail_dir / "replay", crash_tail_dir / "ledger")
            self.assertTrue(rep_crash_3["bit_for_bit_match"])
            self.assertTrue(rep_crash_3["chain_ok"])
            self.assertTrue(rep_crash_3["receipts_ok"])
            self.assertTrue(rep_crash_3["archived_inputs_ok"])
            self.assertEqual(rep_crash_3["replayed_count"], 3)

            # Skipping Day 2 directly to Day 3 after Day 2 crash also recovers & replays bit-for-bit
            d3_skip = run_daily_drill_cycle(
                session_date="2025-11-25",
                data_dir=skip_day2_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-25"),
                sleeper=lambda _: None,
            )
            self.assertTrue(d3_skip["appended"])
            self.assertEqual(d3_skip["sequence"], 1)
            rep_skip = replay_window(skip_day2_dir / "replay", skip_day2_dir / "ledger")
            self.assertTrue(rep_skip["bit_for_bit_match"])
            self.assertTrue(rep_skip["archived_inputs_ok"])
            self.assertEqual(rep_skip["replayed_count"], 2)
            missed_skip = detect_missed_runs(
                window_start_date="2025-11-21",
                as_of_utc="2025-11-26T05:00:00.000Z",
                data_dir=skip_day2_dir,
                config=cfg,
            )
            self.assertEqual(missed_skip["ok_days"], ["2025-11-21", "2025-11-25"])
            self.assertEqual(missed_skip["silent_missed_days"], ["2025-11-24"])

            # Negative crash-tail check: if prefix[0] is tampered OR len == len(ledger)+2, MUST fail closed (never prune!)
            bad_prefix_dir = root / "crash_tail_tampered_prefix"
            shutil.copytree(skip_day2_dir, bad_prefix_dir)
            bp_path = bad_prefix_dir / "replay" / "replay_bundle.json"
            bp_obj = json.loads(bp_path.read_text(encoding="utf-8"))
            bp_obj["sessions"][0]["inputs"]["funds"]["TQQQ"]["q_pre"] = 1
            bp_obj["sessions"].append({"session_date": "2025-11-26"})
            bp_path.write_text(json.dumps(bp_obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            bad_prefix_res = run_daily_drill_cycle(
                session_date="2025-11-26",
                data_dir=bad_prefix_dir,
                config=cfg,
                fixtures_dir=DRILL_FIXTURES,
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-26"),
            )
            self.assertTrue(bad_prefix_res["fail_closed"])
            self.assertFalse(bad_prefix_res["appended"])
            self.assertEqual(len(ShadowLedger(bad_prefix_dir / "ledger").load_records()), 2)

            # Duplicate committed session_date in trailing bundle entry (len == ledger + 1) MUST also fail closed without pruning
            dup_tail_dir = root / "crash_tail_dup_date"
            shutil.copytree(skip_day2_dir, dup_tail_dir)
            dt_path = dup_tail_dir / "replay" / "replay_bundle.json"
            dt_obj = json.loads(dt_path.read_text(encoding="utf-8"))
            dt_obj["sessions"].append({"session_date": "2025-11-21"})
            dt_path.write_text(json.dumps(dt_obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            dup_tail_res = run_daily_drill_cycle(
                session_date="2025-11-26",
                data_dir=dup_tail_dir,
                config=cfg,
                fixtures_dir=DRILL_FIXTURES,
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-26"),
            )
            self.assertTrue(dup_tail_res["fail_closed"])
            self.assertFalse(dup_tail_res["appended"])
            self.assertIn("invalid_or_duplicate_trailing_bundle_session", dup_tail_res["detail"])

            # 5. R7 #5: Explicit empty '--require-ny-hhmm ""' and sibling empty '--as-of-utc ""' fail closed non-zero
            run_empty_cli = root / "cli_empty_hhmm"
            proc_run_empty = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "shadow.scheduler",
                    "run-drill",
                    "--session",
                    "auto",
                    "--data-dir",
                    str(run_empty_cli),
                    "--require-ny-hhmm",
                    "",
                ],
                cwd=str(REPO_ROOT),
                env={"PATH": "/usr/bin:/bin:/usr/local/bin", "PYTHONPATH": str(REPO_ROOT / "src")},
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertNotEqual(proc_run_empty.returncode, 0, proc_run_empty.stdout + proc_run_empty.stderr)
            self.assertIn("invalid_require_ny_hhmm", proc_run_empty.stdout)
            self.assertFalse((run_empty_cli / "ledger").exists())

            sim_cli_dir = root / "sim_empty_hhmm"
            # At 16:30 EST (21:30Z), --require-ny-hhmm '17:30' skips cleanly (rc=0, 0 records); --require-ny-hhmm '' fails (rc=1, 0 records)
            self.assertEqual(
                scheduler_cli_main(
                    [
                        "simulate-drill",
                        "--session",
                        "auto",
                        "--now-utc",
                        "2025-11-21T21:30:00.000Z",
                        "--data-dir",
                        str(sim_cli_dir),
                        "--fixtures-dir",
                        str(DRILL_FIXTURES),
                        "--require-ny-hhmm",
                        "17:30",
                    ]
                ),
                0,
            )
            self.assertFalse((sim_cli_dir / "ledger").exists())

            self.assertNotEqual(
                scheduler_cli_main(
                    [
                        "simulate-drill",
                        "--session",
                        "auto",
                        "--now-utc",
                        "2025-11-21T21:30:00.000Z",
                        "--data-dir",
                        str(sim_cli_dir),
                        "--fixtures-dir",
                        str(DRILL_FIXTURES),
                        "--require-ny-hhmm",
                        "",
                    ]
                ),
                0,
            )
            self.assertFalse((sim_cli_dir / "ledger").exists())

            self.assertNotEqual(
                scheduler_cli_main(
                    [
                        "detect-missed",
                        "--start-date",
                        "2025-11-21",
                        "--as-of-utc",
                        "2025-11-22T05:00:00.000Z",
                        "--data-dir",
                        str(rep_tamper_dir),
                        "--require-ny-hhmm",
                        "",
                    ]
                ),
                0,
            )
            self.assertNotEqual(
                scheduler_cli_main(
                    [
                        "detect-missed",
                        "--start-date",
                        "2025-11-21",
                        "--as-of-utc",
                        "",
                        "--data-dir",
                        str(rep_tamper_dir),
                    ]
                ),
                0,
            )

    def test_r8_committed_tail_not_pruned_and_config_change_no_overwrite(self) -> None:
        cfg = replace(
            load_scheduler_config(DEFAULT_SCHEDULER_CONFIG_PATH),
            treasury_endpoint="https://treasury.example.test/yield{year}.csv",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            # 1. R8 #1: Deleting committed Day 2 record.json while completeness_2025-11-24.json still shows
            #    appended=True MUST fail closed and MUST NOT prune Day 2 bundle/OTS/receipt on Day 3!
            missing_rec_dir = root / "committed_missing_record"
            for ds in ("2025-11-21", "2025-11-24"):
                res = run_daily_drill_cycle(
                    session_date=ds,
                    data_dir=missing_rec_dir,
                    config=cfg,
                    transport=lambda u, h: _mock_market_transport(u, h),
                    environ={"TIINGO_API_KEY": "test-key"},
                    ots_transport=_mock_ots_transport,
                    wall_clock=_tick_clock(ds),
                    sleeper=lambda _: None,
                )
                self.assertTrue(res["appended"])

            rec1 = missing_rec_dir / "ledger" / "seq_000001.record.json"
            ots1 = missing_rec_dir / "ledger" / "seq_000001.ots"
            rcp1 = missing_rec_dir / "ledger" / "seq_000001.receipt.json"
            self.assertTrue(rec1.is_file() and ots1.is_file() and rcp1.is_file())
            rec1.unlink()

            day3_res = run_daily_drill_cycle(
                session_date="2025-11-25",
                data_dir=missing_rec_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-25"),
                sleeper=lambda _: None,
            )
            self.assertFalse(day3_res["appended"])
            self.assertTrue(day3_res["fail_closed"])
            self.assertEqual(day3_res["reason"], "incomplete_or_corrupt_replay_bundle")
            self.assertIn("committed_session_missing_ledger_record:2025-11-24:seq=1", day3_res["detail"])
            # Crucially, Day 2 evidence was NOT destroyed
            self.assertTrue(ots1.is_file())
            self.assertTrue(rcp1.is_file())
            bundle_after = json.loads((missing_rec_dir / "replay" / "replay_bundle.json").read_text(encoding="utf-8"))
            self.assertEqual([s.get("session_date") for s in bundle_after["sessions"]], ["2025-11-21", "2025-11-24"])

            missed = detect_missed_runs(
                window_start_date="2025-11-21",
                as_of_utc="2025-11-25T05:00:00.000Z",
                data_dir=missing_rec_dir,
                config=cfg,
            )
            self.assertTrue(missed["missed_run_detected"])
            self.assertEqual(missed["ok_days"], [])
            self.assertIn("2025-11-24", missed["silent_missed_days"])
            self.assertIn("2025-11-24", missed["failed_days"])

            # 2. R8 #2: Tightening scheduler cutoff/deadline after commit MUST NOT rewrite report or count ok_days
            cfg_dir = root / "config_conflict"
            commit_res = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=cfg_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21"),
                sleeper=lambda _: None,
            )
            self.assertTrue(commit_res["appended"])
            rep_path = cfg_dir / "reports" / "completeness_2025-11-21.json"
            orig_rep_bytes = rep_path.read_bytes()

            tighter_cfg = replace(
                cfg,
                decision_instant_et="16:30",
                decision_cutoff_et="17:00",
                commitment_deadline_et="17:01",
            )
            # Retry inside tighter window (16:40 ET = 21:40 UTC) MUST fail closed & keep original report intact
            retry_tight = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=cfg_dir,
                config=tighter_cfg,
                wall_clock=_tick_clock("2025-11-21", "21:40:00"),
            )
            self.assertFalse(retry_tight["appended"])
            self.assertTrue(retry_tight["fail_closed"])
            self.assertFalse(retry_tight["recovered_missing_report"])
            self.assertFalse(retry_tight["report_written"])
            self.assertTrue(retry_tight["existing_report_preserved"])
            self.assertEqual(rep_path.read_bytes(), orig_rep_bytes)

            # Tightening cutoff alone (keeping 23:30 deadline) MUST also refuse ok_days in detect_missed_runs
            tighter_cutoff_only = replace(
                cfg,
                decision_instant_et="16:30",
                decision_cutoff_et="17:00",
            )
            missed_tight = detect_missed_runs(
                window_start_date="2025-11-21",
                as_of_utc="2025-11-22T05:00:00.000Z",
                data_dir=cfg_dir,
                config=tighter_cutoff_only,
            )
            self.assertEqual(missed_tight["ok_days"], [])
            self.assertIn("2025-11-21", missed_tight["failed_days"])

    def test_r9_seq0_missing_markdown_evidence_and_bundle_config_conflict(self) -> None:
        cfg = replace(
            load_scheduler_config(DEFAULT_SCHEDULER_CONFIG_PATH),
            treasury_endpoint="https://treasury.example.test/yield{year}.csv",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            # 1. R9 #1: Removing seq_000000.record.json while keeping committed artifacts MUST fail closed without overwriting seq 0
            seq0_dir = root / "seq0_missing_record"
            d1_ok = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=seq0_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21"),
                sleeper=lambda _: None,
            )
            self.assertTrue(d1_ok["appended"])
            rec0 = seq0_dir / "ledger" / "seq_000000.record.json"
            ots0 = seq0_dir / "ledger" / "seq_000000.ots"
            rcp0 = seq0_dir / "ledger" / "seq_000000.receipt.json"
            bundle0_bytes = (seq0_dir / "replay" / "replay_bundle.json").read_bytes()
            ots0_bytes = ots0.read_bytes()
            rcp0_bytes = rcp0.read_bytes()
            rec0.unlink()

            day2_after_seq0 = run_daily_drill_cycle(
                session_date="2025-11-24",
                data_dir=seq0_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-24"),
                sleeper=lambda _: None,
            )
            self.assertFalse(day2_after_seq0["appended"])
            self.assertTrue(day2_after_seq0["fail_closed"])
            self.assertEqual(day2_after_seq0["reason"], "committed_evidence_without_ledger_records")
            self.assertEqual((seq0_dir / "replay" / "replay_bundle.json").read_bytes(), bundle0_bytes)
            self.assertEqual(ots0.read_bytes(), ots0_bytes)
            self.assertEqual(rcp0.read_bytes(), rcp0_bytes)
            missed_seq0 = detect_missed_runs(
                window_start_date="2025-11-21",
                as_of_utc="2025-11-25T05:00:00.000Z",
                data_dir=seq0_dir,
                config=cfg,
            )
            self.assertEqual(missed_seq0["ok_days"], [])

            # 2. R9 #2: Missing JSON or literal 'null' JSON with Markdown '- **Ledger Appended:** `True`' MUST fail closed without pruning seq 1
            for mode in ("deleted_json", "null_json"):
                md_dir = root / f"md_only_{mode}"
                for ds in ("2025-11-21", "2025-11-24"):
                    self.assertTrue(
                        run_daily_drill_cycle(
                            session_date=ds,
                            data_dir=md_dir,
                            config=cfg,
                            transport=lambda u, h: _mock_market_transport(u, h),
                            environ={"TIINGO_API_KEY": "test-key"},
                            ots_transport=_mock_ots_transport,
                            wall_clock=_tick_clock(ds),
                            sleeper=lambda _: None,
                        )["appended"]
                    )
                (md_dir / "ledger" / "seq_000001.record.json").unlink()
                rep1_json = md_dir / "reports" / "completeness_2025-11-24.json"
                if mode == "deleted_json":
                    rep1_json.unlink()
                else:
                    rep1_json.write_text("null\n", encoding="utf-8")
                self.assertTrue((md_dir / "reports" / "completeness_2025-11-24.md").is_file())
                ots1_b = (md_dir / "ledger" / "seq_000001.ots").read_bytes()
                rcp1_b = (md_dir / "ledger" / "seq_000001.receipt.json").read_bytes()

                d3_md = run_daily_drill_cycle(
                    session_date="2025-11-25",
                    data_dir=md_dir,
                    config=cfg,
                    transport=lambda u, h: _mock_market_transport(u, h),
                    environ={"TIINGO_API_KEY": "test-key"},
                    ots_transport=_mock_ots_transport,
                    wall_clock=_tick_clock("2025-11-25"),
                    sleeper=lambda _: None,
                )
                self.assertFalse(d3_md["appended"])
                self.assertTrue(d3_md["fail_closed"])
                self.assertEqual((md_dir / "ledger" / "seq_000001.ots").read_bytes(), ots1_b)
                self.assertEqual((md_dir / "ledger" / "seq_000001.receipt.json").read_bytes(), rcp1_b)
                missed_md = detect_missed_runs(
                    window_start_date="2025-11-21",
                    as_of_utc="2025-11-25T05:00:00.000Z",
                    data_dir=md_dir,
                    config=cfg,
                )
                self.assertEqual(missed_md["ok_days"], [])

            # 3. R9 #3: Crash after commit BEFORE writing completeness JSON/MD, then tighten cutoff (18:00 -> 17:45) MUST fail closed
            cfg_bundle_dir = root / "bundle_cutoff_tightened"
            self.assertTrue(
                run_daily_drill_cycle(
                    session_date="2025-11-21",
                    data_dir=cfg_bundle_dir,
                    config=cfg,
                    transport=lambda u, h: _mock_market_transport(u, h),
                    environ={"TIINGO_API_KEY": "test-key"},
                    ots_transport=_mock_ots_transport,
                    wall_clock=_tick_clock("2025-11-21"),
                    sleeper=lambda _: None,
                )["appended"]
            )
            (cfg_bundle_dir / "reports" / "completeness_2025-11-21.json").unlink()
            (cfg_bundle_dir / "reports" / "completeness_2025-11-21.md").unlink()
            tighter_1745 = replace(cfg, decision_cutoff_et="17:45")
            retry_1745 = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=cfg_bundle_dir,
                config=tighter_1745,
                wall_clock=_tick_clock("2025-11-21", "22:40:00"),
            )
            self.assertFalse(retry_1745["appended"])
            self.assertTrue(retry_1745["fail_closed"])
            self.assertEqual(retry_1745["reason"], "committed_bundle_deadline_config_mismatch")
            self.assertFalse(retry_1745["report_written"])
            self.assertFalse((cfg_bundle_dir / "reports" / "completeness_2025-11-21.json").exists())
            missed_1745 = detect_missed_runs(
                window_start_date="2025-11-21",
                as_of_utc="2025-11-22T05:00:00.000Z",
                data_dir=cfg_bundle_dir,
                config=tighter_1745,
            )
            self.assertEqual(missed_1745["ok_days"], [])

    def test_r10_post_deadline_committed_config_conflict_and_prior_fail_report_masking_missing_seq0(self) -> None:
        import contextlib
        import io
        from shadow import scheduler as sched_mod

        cfg = replace(
            load_scheduler_config(DEFAULT_SCHEDULER_CONFIG_PATH),
            treasury_endpoint="https://treasury.example.test/yield{year}.csv",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            # 1. R10 #1: Commit under 18:00 ET, crash before writing report, tighten cutoff to 17:45 ET,
            #    then retry PAST commitment deadline (23:31 ET) -> MUST fail closed with committed_bundle_deadline_config_mismatch
            #    and write ZERO appended=False report!
            post_dl_dir = root / "post_deadline_committed_tightened"
            with self.assertRaisesRegex(RuntimeError, "crash_before_report"):
                run_daily_drill_cycle(
                    session_date="2025-11-21",
                    data_dir=post_dl_dir,
                    config=cfg,
                    transport=lambda u, h: _mock_market_transport(u, h),
                    environ={"TIINGO_API_KEY": "test-key"},
                    ots_transport=_mock_ots_transport,
                    wall_clock=_tick_clock("2025-11-21"),
                    sleeper=lambda _: None,
                    _post_record_commit_hook=lambda: (_ for _ in ()).throw(RuntimeError("crash_before_report")),
                )
            self.assertTrue((post_dl_dir / "ledger" / "seq_000000.record.json").is_file())
            self.assertFalse((post_dl_dir / "reports" / "completeness_2025-11-21.json").exists())

            tighter_1745 = replace(cfg, decision_cutoff_et="17:45")
            late_retry = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=post_dl_dir,
                config=tighter_1745,
                wall_clock=_tick_clock("2025-11-22", "04:31:00"),  # 23:31 EST on 2025-11-21
            )
            self.assertFalse(late_retry["appended"])
            self.assertTrue(late_retry["fail_closed"])
            self.assertEqual(late_retry["reason"], "committed_bundle_deadline_config_mismatch")
            self.assertFalse(late_retry["report_written"])
            self.assertFalse((post_dl_dir / "reports" / "completeness_2025-11-21.json").exists())
            self.assertFalse((post_dl_dir / "reports" / "completeness_2025-11-21.md").exists())

            # Same-config retry past commitment deadline after post-commit missing report ALSO never writes appended=False
            late_same_cfg = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=post_dl_dir,
                config=cfg,
                wall_clock=_tick_clock("2025-11-22", "04:31:00"),
            )
            self.assertFalse(late_same_cfg["appended"])
            self.assertTrue(late_same_cfg["fail_closed"])
            self.assertEqual(late_same_cfg["reason"], "late_after_commitment_deadline_backfill_rejected")
            self.assertFalse(late_same_cfg["report_written"])
            self.assertFalse((post_dl_dir / "reports" / "completeness_2025-11-21.json").exists())

            # 2. R10 #2: Earlier fail-closed report (withhold TQQQ) followed by same-day commit crash before report upgrade,
            #    then seq_000000.record.json deleted -> detect-missed MUST flag missed_run_detected=True & exit non-zero!
            masked_dir = root / "prior_fail_then_missing_seq0"
            first_fail = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=masked_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h, withhold_inst="TQQQ"),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21"),
                sleeper=lambda _: None,
            )
            self.assertTrue(first_fail["report_written"])
            self.assertTrue(first_fail["fail_closed"])
            with self.assertRaisesRegex(RuntimeError, "crash_after_commit"):
                run_daily_drill_cycle(
                    session_date="2025-11-21",
                    data_dir=masked_dir,
                    config=cfg,
                    transport=lambda u, h: _mock_market_transport(u, h),
                    environ={"TIINGO_API_KEY": "test-key"},
                    ots_transport=_mock_ots_transport,
                    wall_clock=_tick_clock("2025-11-21"),
                    sleeper=lambda _: None,
                    _post_record_commit_hook=lambda: (_ for _ in ()).throw(RuntimeError("crash_after_commit")),
                )
            (masked_dir / "ledger" / "seq_000000.record.json").unlink()
            orig_evidence = [
                (masked_dir / rel).read_bytes()
                for rel in ("replay/replay_bundle.json", "ledger/seq_000000.ots", "ledger/seq_000000.receipt.json")
            ]

            cli_out = io.StringIO()
            with contextlib.redirect_stdout(cli_out):
                rc = sched_mod.main(
                    [
                        "detect-missed",
                        "--start-date",
                        "2025-11-21",
                        "--as-of-utc",
                        "2025-11-22T05:00:00.000Z",
                        "--data-dir",
                        str(masked_dir),
                    ]
                )
            audit = json.loads(cli_out.getvalue())
            self.assertEqual(rc, 2)
            self.assertTrue(audit["missed_run_detected"])
            self.assertEqual(audit["silent_missed_days"], ["2025-11-21"])
            self.assertEqual(audit["failed_days"], ["2025-11-21"])
            self.assertEqual(audit["evaluated_trading_days"][0]["status"], "incomplete_or_corrupt_replay_bundle")
            self.assertEqual(
                [
                    (masked_dir / rel).read_bytes()
                    for rel in ("replay/replay_bundle.json", "ledger/seq_000000.ots", "ledger/seq_000000.receipt.json")
                ],
                orig_evidence,
            )

    def test_r11_missing_committed_record_never_overwritten_on_retry_or_next_day(self) -> None:
        import contextlib
        import io
        from shadow import scheduler as sched_mod

        cfg = replace(
            load_scheduler_config(DEFAULT_SCHEDULER_CONFIG_PATH),
            treasury_endpoint="https://treasury.example.test/yield{year}.csv",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            # 1. Empty-ledger seq_0 committed after earlier fail-closed report, then seq_000000.record.json unlinked:
            #    same-day 17:40 ET retry MUST refuse overwriting seq_000000 and preserve bundle/OTS/receipt bytes!
            d1_dir = root / "same_day_seq0_missing_record"
            self.assertTrue(
                run_daily_drill_cycle(
                    session_date="2025-11-21",
                    data_dir=d1_dir,
                    config=cfg,
                    transport=lambda u, h: _mock_market_transport(u, h, withhold_inst="TQQQ"),
                    environ={"TIINGO_API_KEY": "test-key"},
                    ots_transport=_mock_ots_transport,
                    wall_clock=_tick_clock("2025-11-21"),
                    sleeper=lambda _: None,
                )["fail_closed"]
            )
            with self.assertRaisesRegex(RuntimeError, "crash_after_commit"):
                run_daily_drill_cycle(
                    session_date="2025-11-21",
                    data_dir=d1_dir,
                    config=cfg,
                    transport=lambda u, h: _mock_market_transport(u, h),
                    environ={"TIINGO_API_KEY": "test-key"},
                    ots_transport=_mock_ots_transport,
                    wall_clock=_tick_clock("2025-11-21"),
                    sleeper=lambda _: None,
                    _post_record_commit_hook=lambda: (_ for _ in ()).throw(RuntimeError("crash_after_commit")),
                )
            (d1_dir / "ledger" / "seq_000000.record.json").unlink()
            orig_seq0 = [
                (d1_dir / rel).read_bytes()
                for rel in ("replay/replay_bundle.json", "ledger/seq_000000.ots", "ledger/seq_000000.receipt.json")
            ]

            same_day_retry = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=d1_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21", "22:40:00"),
                sleeper=lambda _: None,
            )
            self.assertFalse(same_day_retry["appended"])
            self.assertTrue(same_day_retry["fail_closed"])
            self.assertEqual(same_day_retry["reason"], "committed_evidence_without_ledger_records")
            self.assertEqual(
                [
                    (d1_dir / rel).read_bytes()
                    for rel in ("replay/replay_bundle.json", "ledger/seq_000000.ots", "ledger/seq_000000.receipt.json")
                ],
                orig_seq0,
            )

            # 2. Non-empty ledger seq_1 committed after earlier fail-closed report on Day 2, then seq_000001.record.json unlinked:
            #    Day 3 normal run MUST refuse pruning Day 2 or reusing seq 1, and detect-missed continues exiting 2!
            d2_dir = root / "nonempty_seq1_missing_record"
            self.assertTrue(
                run_daily_drill_cycle(
                    session_date="2025-11-21",
                    data_dir=d2_dir,
                    config=cfg,
                    transport=lambda u, h: _mock_market_transport(u, h),
                    environ={"TIINGO_API_KEY": "test-key"},
                    ots_transport=_mock_ots_transport,
                    wall_clock=_tick_clock("2025-11-21"),
                    sleeper=lambda _: None,
                )["appended"]
            )
            self.assertTrue(
                run_daily_drill_cycle(
                    session_date="2025-11-24",
                    data_dir=d2_dir,
                    config=cfg,
                    transport=lambda u, h: _mock_market_transport(u, h, withhold_inst="TQQQ"),
                    environ={"TIINGO_API_KEY": "test-key"},
                    ots_transport=_mock_ots_transport,
                    wall_clock=_tick_clock("2025-11-24"),
                    sleeper=lambda _: None,
                )["fail_closed"]
            )
            with self.assertRaisesRegex(RuntimeError, "crash_after_commit"):
                run_daily_drill_cycle(
                    session_date="2025-11-24",
                    data_dir=d2_dir,
                    config=cfg,
                    transport=lambda u, h: _mock_market_transport(u, h),
                    environ={"TIINGO_API_KEY": "test-key"},
                    ots_transport=_mock_ots_transport,
                    wall_clock=_tick_clock("2025-11-24"),
                    sleeper=lambda _: None,
                    _post_record_commit_hook=lambda: (_ for _ in ()).throw(RuntimeError("crash_after_commit")),
                )
            (d2_dir / "ledger" / "seq_000001.record.json").unlink()
            orig_seq1 = [
                (d2_dir / rel).read_bytes()
                for rel in ("replay/replay_bundle.json", "ledger/seq_000001.ots", "ledger/seq_000001.receipt.json")
            ]

            day3_res = run_daily_drill_cycle(
                session_date="2025-11-25",
                data_dir=d2_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-25"),
                sleeper=lambda _: None,
            )
            self.assertFalse(day3_res["appended"])
            self.assertTrue(day3_res["fail_closed"])
            self.assertFalse((d2_dir / "ledger" / "seq_000001.record.json").exists())
            self.assertEqual(
                [
                    (d2_dir / rel).read_bytes()
                    for rel in ("replay/replay_bundle.json", "ledger/seq_000001.ots", "ledger/seq_000001.receipt.json")
                ],
                orig_seq1,
            )

            out_buf = io.StringIO()
            with contextlib.redirect_stdout(out_buf):
                rc2 = sched_mod.main(
                    [
                        "detect-missed",
                        "--start-date",
                        "2025-11-21",
                        "--as-of-utc",
                        "2025-11-26T05:00:00.000Z",
                        "--data-dir",
                        str(d2_dir),
                    ]
                )
            audit2 = json.loads(out_buf.getvalue())
            self.assertEqual(rc2, 2)
            self.assertTrue(audit2["missed_run_detected"])
            self.assertIn("2025-11-24", audit2["silent_missed_days"])
            self.assertIn("2025-11-24", audit2["failed_days"])

    def test_r12_valid_ots_permission_error_and_direct_ledger_append_protection(self) -> None:
        from shadow import ledger as ledger_mod

        cfg = replace(
            load_scheduler_config(DEFAULT_SCHEDULER_CONFIG_PATH),
            treasury_endpoint="https://treasury.example.test/yield{year}.csv",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            def _setup_committed_seq1_missing_record(tag: str) -> tuple[Path, ShadowLedger]:
                d = root / tag
                self.assertTrue(
                    run_daily_drill_cycle(
                        session_date="2025-11-21",
                        data_dir=d,
                        config=cfg,
                        transport=lambda u, h: _mock_market_transport(u, h),
                        environ={"TIINGO_API_KEY": "test-key"},
                        ots_transport=_mock_ots_transport,
                        wall_clock=_tick_clock("2025-11-21"),
                        sleeper=lambda _: None,
                    )["appended"]
                )
                self.assertTrue(
                    run_daily_drill_cycle(
                        session_date="2025-11-24",
                        data_dir=d,
                        config=cfg,
                        transport=lambda u, h: _mock_market_transport(u, h, withhold_inst="TQQQ"),
                        environ={"TIINGO_API_KEY": "test-key"},
                        ots_transport=_mock_ots_transport,
                        wall_clock=_tick_clock("2025-11-24"),
                        sleeper=lambda _: None,
                    )["fail_closed"]
                )
                with self.assertRaisesRegex(RuntimeError, "crash_after_commit"):
                    run_daily_drill_cycle(
                        session_date="2025-11-24",
                        data_dir=d,
                        config=cfg,
                        transport=lambda u, h: _mock_market_transport(u, h),
                        environ={"TIINGO_API_KEY": "test-key"},
                        ots_transport=_mock_ots_transport,
                        wall_clock=_tick_clock("2025-11-24"),
                        sleeper=lambda _: None,
                        _post_record_commit_hook=lambda: (_ for _ in ()).throw(RuntimeError("crash_after_commit")),
                    )
                led = ShadowLedger(d / "ledger")
                led._record_path(1).unlink()
                return d, led

            # 1. R12 #1: Valid .ots proof even WITHOUT .receipt.json blocks Day 3 pruning/reuse of seq=1
            ots_dir, ots_led = _setup_committed_seq1_missing_record("valid_ots_no_receipt")
            ots_led._receipt_path(1).unlink()
            ots_bytes_before = ots_led._ots_path(1).read_bytes()
            bundle_before = (ots_dir / "replay" / "replay_bundle.json").read_bytes()
            d3_ots = run_daily_drill_cycle(
                session_date="2025-11-25",
                data_dir=ots_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-25"),
                sleeper=lambda _: None,
            )
            self.assertFalse(d3_ots["appended"])
            self.assertTrue(d3_ots["fail_closed"])
            self.assertEqual(ots_led._ots_path(1).read_bytes(), ots_bytes_before)
            self.assertEqual((ots_dir / "replay" / "replay_bundle.json").read_bytes(), bundle_before)

            # 2. R12 #2: PermissionError / OSError reading seq_000001.receipt.json aborts BEFORE pruning
            perm_dir, perm_led = _setup_committed_seq1_missing_record("permission_error_receipt")
            denied_rcp = perm_led._receipt_path(1)
            rcp_bytes_before = denied_rcp.read_bytes()
            orig_read_text = Path.read_text

            def _deny_receipt_read(self_path: Path, *args: object, **kwargs: object) -> str:
                if self_path == denied_rcp:
                    raise PermissionError("simulated_read_denied")
                return orig_read_text(self_path, *args, **kwargs)

            with patch.object(Path, "read_text", _deny_receipt_read):
                d3_perm = run_daily_drill_cycle(
                    session_date="2025-11-25",
                    data_dir=perm_dir,
                    config=cfg,
                    transport=lambda u, h: _mock_market_transport(u, h),
                    environ={"TIINGO_API_KEY": "test-key"},
                    ots_transport=_mock_ots_transport,
                    wall_clock=_tick_clock("2025-11-25"),
                    sleeper=lambda _: None,
                )
            self.assertFalse(d3_perm["appended"])
            self.assertTrue(d3_perm["fail_closed"])
            self.assertEqual(denied_rcp.read_bytes(), rcp_bytes_before)

            # 3. R12 #3: Direct ShadowLedger.append_record(allow_unanchored=True) refuses overwriting seq=1 evidence
            direct_dir, direct_led = _setup_committed_seq1_missing_record("direct_append_protected")
            clean_dir = root / "clean_source"
            for ds in ("2025-11-21", "2025-11-25"):
                self.assertTrue(
                    run_daily_drill_cycle(
                        session_date=ds,
                        data_dir=clean_dir,
                        config=cfg,
                        transport=lambda u, h: _mock_market_transport(u, h),
                        environ={"TIINGO_API_KEY": "test-key"},
                        ots_transport=_mock_ots_transport,
                        wall_clock=_tick_clock(ds),
                        sleeper=lambda _: None,
                    )["appended"]
                )
            replacement_body = ShadowLedger(clean_dir / "ledger").head()["record_body"]
            orig_direct_ots = direct_led._ots_path(1).read_bytes()
            orig_direct_rcp = direct_led._receipt_path(1).read_bytes()
            with self.assertRaisesRegex(LedgerAppendError, "committed_evidence_exists_without_record"):
                direct_led.append_record(replacement_body, allow_unanchored=True)
            self.assertFalse(direct_led._record_path(1).exists())
            self.assertEqual(direct_led._ots_path(1).read_bytes(), orig_direct_ots)
            self.assertEqual(direct_led._receipt_path(1).read_bytes(), orig_direct_rcp)
            self.assertTrue(ledger_mod.has_committed_detached_evidence(direct_led, 1)[0])

    def test_r13_post_commit_crash_before_receipt_or_report_unanchored_flags_silent_missed(self) -> None:
        import contextlib
        import io
        from shadow import scheduler as sched_mod

        cfg = replace(
            load_scheduler_config(DEFAULT_SCHEDULER_CONFIG_PATH),
            treasury_endpoint="https://treasury.example.test/yield{year}.csv",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            # 1. R13 #1a: OTS outage (allow_unanchored_on_ots_failure=True) + crash before completeness report write
            unanchored_crash_dir = root / "unanchored_post_commit_crash"
            self.assertTrue(
                run_daily_drill_cycle(
                    session_date="2025-11-21",
                    data_dir=unanchored_crash_dir,
                    config=cfg,
                    transport=lambda u, h: _mock_market_transport(u, h),
                    environ={"TIINGO_API_KEY": "test-key"},
                    ots_transport=_mock_ots_transport,
                    wall_clock=_tick_clock("2025-11-21"),
                    sleeper=lambda _: None,
                )["appended"]
            )
            with self.assertRaisesRegex(RuntimeError, "crash_after_unanchored_commit"):
                run_daily_drill_cycle(
                    session_date="2025-11-24",
                    data_dir=unanchored_crash_dir,
                    config=cfg,
                    transport=lambda u, h: _mock_market_transport(u, h),
                    environ={"TIINGO_API_KEY": "test-key"},
                    ots_transport=lambda *a: (503, b"ots offline"),
                    wall_clock=_tick_clock("2025-11-24"),
                    sleeper=lambda _: None,
                    _post_record_commit_hook=lambda: (_ for _ in ()).throw(
                        RuntimeError("crash_after_unanchored_commit")
                    ),
                )
            self.assertTrue((unanchored_crash_dir / "ledger" / "seq_000001.record.json").is_file())
            self.assertFalse((unanchored_crash_dir / "ledger" / "seq_000001.receipt.json").exists())
            self.assertFalse((unanchored_crash_dir / "reports" / "completeness_2025-11-24.json").exists())

            out1 = io.StringIO()
            with contextlib.redirect_stdout(out1):
                rc1 = sched_mod.main(
                    [
                        "detect-missed",
                        "--start-date",
                        "2025-11-21",
                        "--as-of-utc",
                        "2025-11-25T05:00:00.000Z",
                        "--data-dir",
                        str(unanchored_crash_dir),
                    ]
                )
            a1 = json.loads(out1.getvalue())
            self.assertEqual(rc1, 2)
            self.assertTrue(a1["missed_run_detected"])
            self.assertEqual(a1["silent_missed_days"], ["2025-11-24"])
            self.assertIn("2025-11-24", a1["failed_days"])

            # 2. R13 #1b: Record + .ots committed, crash before .receipt.json & report write -> CLI exits 2 cleanly
            ots_only_dir = root / "record_ots_no_receipt_no_report"
            self.assertTrue(
                run_daily_drill_cycle(
                    session_date="2025-11-21",
                    data_dir=ots_only_dir,
                    config=cfg,
                    transport=lambda u, h: _mock_market_transport(u, h),
                    environ={"TIINGO_API_KEY": "test-key"},
                    ots_transport=_mock_ots_transport,
                    wall_clock=_tick_clock("2025-11-21"),
                    sleeper=lambda _: None,
                )["appended"]
            )
            with self.assertRaisesRegex(RuntimeError, "crash_before_report"):
                run_daily_drill_cycle(
                    session_date="2025-11-24",
                    data_dir=ots_only_dir,
                    config=cfg,
                    transport=lambda u, h: _mock_market_transport(u, h),
                    environ={"TIINGO_API_KEY": "test-key"},
                    ots_transport=_mock_ots_transport,
                    wall_clock=_tick_clock("2025-11-24"),
                    sleeper=lambda _: None,
                    _post_record_commit_hook=lambda: (_ for _ in ()).throw(RuntimeError("crash_before_report")),
                )
            (ots_only_dir / "ledger" / "seq_000001.receipt.json").unlink()
            out2 = io.StringIO()
            with contextlib.redirect_stdout(out2):
                rc2 = sched_mod.main(
                    [
                        "detect-missed",
                        "--start-date",
                        "2025-11-21",
                        "--as-of-utc",
                        "2025-11-25T05:00:00.000Z",
                        "--data-dir",
                        str(ots_only_dir),
                    ]
                )
            a2 = json.loads(out2.getvalue())
            self.assertEqual(rc2, 2)
            self.assertTrue(a2["missed_run_detected"])
            self.assertEqual(a2["silent_missed_days"], ["2025-11-24"])
            self.assertIn("2025-11-24", a2["failed_days"])

            # 3. R14 #1: Detecting any of the above under a tightened cutoff (16:30 / 17:00 ET) MUST still exit 2!
            raw_tight = json.loads(DEFAULT_SCHEDULER_CONFIG_PATH.read_text(encoding="utf-8"))
            raw_tight["decision_instant_et"] = "16:30"
            raw_tight["decision_cutoff_et"] = "17:00"
            raw_tight["treasury_endpoint"] = "https://treasury.example.test/yield{year}.csv"
            tight_cfg_path = root / "tight_1700.json"
            tight_cfg_path.write_text(json.dumps(raw_tight), encoding="utf-8")
            for target_dir in (unanchored_crash_dir, ots_only_dir):
                out_t = io.StringIO()
                with contextlib.redirect_stdout(out_t):
                    rc_t = sched_mod.main(
                        [
                            "detect-missed",
                            "--config",
                            str(tight_cfg_path),
                            "--start-date",
                            "2025-11-24",
                            "--as-of-utc",
                            "2025-11-25T05:00:00.000Z",
                            "--data-dir",
                            str(target_dir),
                        ]
                    )
                at = json.loads(out_t.getvalue())
                self.assertEqual(rc_t, 2)
                self.assertTrue(at["missed_run_detected"])
                self.assertEqual(at["silent_missed_days"], ["2025-11-24"])
                self.assertIn("2025-11-24", at["failed_days"])

    def test_r16_stale_treasury_csv_fails_o1_and_missed_run_audit(self) -> None:
        """P2 regression: stale Treasury CSV (01/04/1954,1.33) fails O1 with 0 appends and non-ok missed-run audit."""
        cfg = load_scheduler_config(DEFAULT_SCHEDULER_CONFIG_PATH)

        def stale_treasury_transport(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
            if "treasury.example.test" in url:
                return 200, b"Date,3 Mo\n01/04/1954,1.33\n"
            return _mock_market_transport(url, headers)

        with tempfile.TemporaryDirectory() as tmpdir:
            stale_dir = Path(tmpdir) / "stale_tbill_dir"
            res = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=stale_dir,
                config=cfg,
                transport=stale_treasury_transport,
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21"),
                treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                sleeper=lambda _: None,
            )
            self.assertFalse(res["appended"])
            self.assertTrue(res["fail_closed"])
            self.assertIn("TBILL_3M", res["reason"])
            self.assertFalse((stale_dir / "ledger" / "seq_000000.record.json").exists())
            comp = json.loads((stale_dir / "reports" / "completeness_2025-11-21.json").read_text(encoding="utf-8"))
            tbill_row = next(r for r in comp["required_series"] if r["instrument"] == "TBILL_3M")
            self.assertEqual(tbill_row["status"], "missing")
            self.assertEqual(tbill_row["reason"], "tbill_stale_effective_date")

            audit = detect_missed_runs(
                data_dir=stale_dir,
                config=cfg,
                window_start_date="2025-11-21",
                as_of_utc="2025-11-22T05:00:00.000Z",
            )
            self.assertFalse(audit["missed_run_detected"])
            self.assertNotIn("2025-11-21", audit["ok_days"])
            self.assertIn("2025-11-21", audit["failed_days"])

    def test_r17_rotate_logs_preserves_active_log_inodes_while_truncating(self) -> None:
        """rotate_logs truncates active daemon logs in place so open file descriptors keep their inode."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logs_dir = Path(tmpdir) / "logs"
            logs_dir.mkdir(parents=True, exist_ok=True)
            drill_log = logs_dir / "shadow-drill.log"
            missed_log = logs_dir / "shadow-missed-run.log"
            extra_log = logs_dir / "old-worker.log"
            with drill_log.open("a", encoding="utf-8") as df, missed_log.open("a", encoding="utf-8") as mf:
                df.write("D" * 120 + "\n")
                df.flush()
                mf.write("M" * 80 + "\n")
                mf.flush()
                extra_log.write_text("E" * 100 + "\n", encoding="utf-8")
                os.utime(drill_log, (1000, 1000))
                os.utime(missed_log, (1001, 1001))
                os.utime(extra_log, (900, 900))
                inode_drill = os.stat(drill_log).st_ino
                inode_missed = os.stat(missed_log).st_ino

                rot = rotate_logs(logs_dir, max_age_days=30, max_total_bytes=60, now_ts=1000 + 35 * 86400)
                self.assertEqual(os.stat(drill_log).st_ino, inode_drill)
                self.assertEqual(os.stat(missed_log).st_ino, inode_missed)
                self.assertFalse(extra_log.exists())
                self.assertEqual(drill_log.stat().st_size, 0)
                self.assertEqual(missed_log.stat().st_size, 0)
                self.assertGreaterEqual(rot["truncated_active"], 2)

                df.write("after-trunc\n")
                df.flush()
                mf.write("missed-after\n")
                mf.flush()
            self.assertIn("after-trunc", drill_log.read_text(encoding="utf-8"))
            self.assertIn("missed-after", missed_log.read_text(encoding="utf-8"))

    def test_r17_tampered_existing_chain_fails_closed_before_appending(self) -> None:
        """Corrupting seq_000000 fails closed with existing_chain_verification_failed and appends 0 records."""
        cfg = load_scheduler_config(DEFAULT_SCHEDULER_CONFIG_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            tamper_dir = Path(tmpdir) / "chain_tamper_dir"
            r0 = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=tamper_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21"),
                treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                sleeper=lambda _: None,
            )
            self.assertTrue(r0["appended"])

            rec_path = tamper_dir / "ledger" / "seq_000000.record.json"
            rec_obj = json.loads(rec_path.read_text(encoding="utf-8"))
            rec_obj["record_body"]["assertions"]["B0"] = "99999"
            rec_path.write_text(json.dumps(rec_obj), encoding="utf-8")

            r1 = run_daily_drill_cycle(
                session_date="2025-11-24",
                data_dir=tamper_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-24"),
                treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                sleeper=lambda _: None,
            )
            self.assertFalse(r1["appended"])
            self.assertTrue(r1["fail_closed"])
            self.assertEqual(r1["reason"], "ledger_integrity")
            fc_comp = json.loads(
                (tamper_dir / "reports" / "completeness_2025-11-24.json").read_text(encoding="utf-8")
            )
            self.assertIn("existing_chain_verification_failed", fc_comp["ledger"]["fail_closed_reason"])
            self.assertFalse((tamper_dir / "ledger" / "seq_000001.record.json").exists())

            # Truncated/unreadable JSON head record also fails closed cleanly without JSONDecodeError
            rec_path.write_text("{", encoding="utf-8")
            r2 = run_daily_drill_cycle(
                session_date="2025-11-24",
                data_dir=tamper_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-24"),
                treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                sleeper=lambda _: None,
            )
            self.assertFalse(r2["appended"])
            self.assertTrue(r2["fail_closed"])
            self.assertEqual(r2["reason"], "ledger_integrity")
            fc_trunc = json.loads(
                (tamper_dir / "reports" / "completeness_2025-11-24.json").read_text(encoding="utf-8")
            )
            self.assertIn("existing_chain_verification_failed", fc_trunc["ledger"]["fail_closed_reason"])
            self.assertFalse((tamper_dir / "ledger" / "seq_000001.record.json").exists())

    def test_r17_partial_or_mismatched_fail_closed_report_flagged_silent_missed(self) -> None:
        """detect_missed_runs treats partial fail-closed JSON or mismatched .md/.receipt as silent_missed."""
        cfg = load_scheduler_config(DEFAULT_SCHEDULER_CONFIG_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            partial_dir = root / "partial_fc"
            (partial_dir / "reports").mkdir(parents=True, exist_ok=True)
            (partial_dir / "reports" / "completeness_2025-11-21.json").write_text(
                json.dumps(
                    {
                        "created_at_utc": "2025-11-21T22:30:00.000Z",
                        "ledger": {"appended": False, "fail_closed_reason": "o1_missing"},
                    }
                ),
                encoding="utf-8",
            )
            audit_partial = detect_missed_runs(
                data_dir=partial_dir,
                config=cfg,
                window_start_date="2025-11-21",
                as_of_utc="2025-11-22T05:00:00.000Z",
            )
            self.assertTrue(audit_partial["missed_run_detected"])
            self.assertEqual(audit_partial["silent_missed_days"], ["2025-11-21"])
            self.assertEqual(audit_partial["evaluated_trading_days"][0]["status"], "missed_run_silent_failure")

            mismatch_dir = root / "mismatch_fc"
            res = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=mismatch_dir,
                config=cfg,
                transport=lambda u, h: _mock_market_transport(u, h, withhold_inst="TBILL_3M"),
                environ={"TIINGO_API_KEY": "test-key"},
                ots_transport=_mock_ots_transport,
                wall_clock=_tick_clock("2025-11-21"),
                treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                sleeper=lambda _: None,
            )
            self.assertFalse(res["appended"])
            self.assertTrue(res["fail_closed"])
            md_path = mismatch_dir / "reports" / "completeness_2025-11-21.md"
            md_path.write_text("# tampered markdown\n", encoding="utf-8")
            audit_md = detect_missed_runs(
                data_dir=mismatch_dir,
                config=cfg,
                window_start_date="2025-11-21",
                as_of_utc="2025-11-22T05:00:00.000Z",
            )
            self.assertTrue(audit_md["missed_run_detected"])
            self.assertEqual(audit_md["silent_missed_days"], ["2025-11-21"])
            self.assertEqual(audit_md["evaluated_trading_days"][0]["status"], "missed_run_silent_failure")

            # Mutated standing_assertions or missing required_series must classify silent failure without KeyError
            rep_path = mismatch_dir / "reports" / "completeness_2025-11-21.json"
            orig_rep = json.loads(rep_path.read_text(encoding="utf-8"))
            from shadow.scheduler import _render_completeness_markdown as _rcm

            bad_assert_rep = json.loads(json.dumps(orig_rep))
            bad_assert_rep["standing_assertions"]["loss_controls_armed"] = True
            rep_path.write_text(json.dumps(bad_assert_rep), encoding="utf-8")
            md_path.write_text(_rcm(bad_assert_rep), encoding="utf-8")
            audit_assert = detect_missed_runs(
                data_dir=mismatch_dir,
                config=cfg,
                window_start_date="2025-11-21",
                as_of_utc="2025-11-22T05:00:00.000Z",
            )
            self.assertTrue(audit_assert["missed_run_detected"])
            self.assertEqual(audit_assert["silent_missed_days"], ["2025-11-21"])
            self.assertEqual(audit_assert["evaluated_trading_days"][0]["status"], "missed_run_silent_failure")

            missing_series_rep = json.loads(json.dumps(orig_rep))
            del missing_series_rep["required_series"]
            rep_path.write_text(json.dumps(missing_series_rep), encoding="utf-8")
            md_path.write_text(_rcm(orig_rep), encoding="utf-8")
            audit_missing = detect_missed_runs(
                data_dir=mismatch_dir,
                config=cfg,
                window_start_date="2025-11-21",
                as_of_utc="2025-11-22T05:00:00.000Z",
            )
            self.assertTrue(audit_missing["missed_run_detected"])
            self.assertEqual(audit_missing["silent_missed_days"], ["2025-11-21"])
            self.assertEqual(audit_missing["evaluated_trading_days"][0]["status"], "missed_run_silent_failure")


if __name__ == "__main__":
    unittest.main()
