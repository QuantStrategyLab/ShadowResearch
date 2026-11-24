"""Tests for SH-2 production lock, 23/5 close isolation, restart snapshots, replay, and D1-D10 demo."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock
from zoneinfo import ZoneInfo

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC = str(_REPO_ROOT / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
_DEMO = str(_REPO_ROOT / "demo")
if _DEMO not in sys.path:
    sys.path.insert(0, _DEMO)

from run_sh2_demo import (  # noqa: E402
    _build_session_fixture_files,
    build_live_mock_transport,
    run_all_demos,
)
from shadow.calendar_consumer import (  # noqa: E402
    classify_us_equity_session_window,
    is_official_close_bar_timestamp,
    load_spec,
    resolve_prior_close_session,
    resolve_session,
)
from shadow.feeder import (  # noqa: E402
    select_tiingo_row,
    select_yahoo_close,
    validate_official_close_market_record,
)
from shadow.fuse_adapter import (  # noqa: E402
    EngineIdentityError,
    PINNED_CSA22_V2_DIGEST,
    PINNED_FUSE_ENGINE_HASH,
    PINNED_FUSE_SPEC_VERSION,
    ProductionChainLockedError,
    VendoredFuseEvaluator,
)
from shadow.ledger import (  # noqa: E402
    GENESIS_PREV_HASH,
    ShadowLedger,
    check_forward_clock_credit,
    load_pending_rejected_state_snapshot,
    load_row_state_snapshot,
    save_row_state_snapshot,
)
from shadow.ots import OpenTimestampsClient, build_synthetic_ots_fixture  # noqa: E402
from shadow.replay import replay_window  # noqa: E402
from shadow.scheduler import (  # noqa: E402
    CalendarCoverageError,
    DrillOnlyViolationError,
    detect_missed_runs,
    load_production_scheduler_config,
    load_scheduler_config,
    run_daily_drill_cycle,
    run_production_cycle,
)

_NY_TZ = ZoneInfo("America/New_York")


def _sh2_drill_cfg():
    return dataclasses.replace(
        load_scheduler_config(_REPO_ROOT / "config" / "drill-scheduler.json"),
        chain_id="drill-v0",
        calendar_version="csa22-v2",
        calendar_spec_digest=PINNED_CSA22_V2_DIGEST,
        fuse_spec_version=PINNED_FUSE_SPEC_VERSION,
        fuse_engine_hash=PINNED_FUSE_ENGINE_HASH,
    )


def _fast_clock():
    state = [1000.0]

    def _next() -> float:
        state[0] += 25.0
        return state[0]

    return _next


class Sh2PipelineTest(unittest.TestCase):
    def test_sh2_d1_through_d10_design_demonstration(self) -> None:
        out = run_all_demos()
        self.assertEqual(out["passed"], 10)
        self.assertEqual(out["total"], 10)
        for did in [f"D{i}" for i in range(1, 11)]:
            self.assertEqual(out["cases"][did]["status"], "PASS", did)

    def test_oa_sh2_2_production_chain_locked_blocks_writes(self) -> None:
        cfg = load_production_scheduler_config(_REPO_ROOT / "config" / "production-scheduler.json")
        self.assertTrue(cfg.production_chain_locked)
        self.assertFalse(cfg.clk01_active)
        self.assertEqual(cfg.calendar_version, "csa22-v2")
        self.assertEqual(cfg.calendar_spec_digest, PINNED_CSA22_V2_DIGEST)
        self.assertEqual(cfg.fuse_engine_hash, PINNED_FUSE_ENGINE_HASH)

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "prod_locked"
            with self.assertRaises(ProductionChainLockedError):
                run_production_cycle(session_date="2026-01-05", data_dir=root)
            self.assertFalse((root / "ledger").exists())

    def test_unlocked_production_cycle_writes_canonical_chain_snapshots_and_replays(self) -> None:
        ev = VendoredFuseEvaluator()
        raw_cfg = json.loads(
            (_REPO_ROOT / "config" / "production-scheduler.json").read_text(encoding="utf-8")
        )
        raw_cfg["production_chain_locked"] = False
        raw_cfg["clk01_active"] = True
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "prod_unlocked"
            unlocked_cfg_path = Path(tmpdir) / "unlocked-production.json"
            unlocked_cfg_path.write_text(json.dumps(raw_cfg), encoding="utf-8")

            # Production mode rejects fixtures_dir explicitly (#3)
            with self.assertRaises(ValueError) as fix_ctx:
                run_production_cycle(
                    session_date="2026-01-05",
                    data_dir=root,
                    config_path=unlocked_cfg_path,
                    fixtures_dir=Path(tmpdir),
                )
            self.assertEqual(str(fix_ctx.exception), "production_mode_forbids_offline_fixtures")

            hdrs: dict[int, dict[str, str]] = {}

            class _FakeOts(OpenTimestampsClient):
                def stamp_digest(self, sha256_hex: str) -> tuple[bytes, str]:
                    ots_b, root_hex = build_synthetic_ots_fixture(sha256_hex, bitcoin_height=880100)
                    assert root_hex is not None
                    hdrs[880100] = {"merkle_root": root_hex, "timestamp": "2026-01-06T03:00:00.000Z"}
                    return ots_b, "https://a.pool.opentimestamps.org"

            r1 = run_production_cycle(
                session_date="2026-01-05",
                data_dir=root,
                config_path=unlocked_cfg_path,
                transport=build_live_mock_transport("2026-01-05"),
                environ={"TIINGO_API_KEY": "test-dummy-token"},
                ots_client=_FakeOts(),
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clock(),
                sleeper=lambda _s: None,
                evaluator=ev,
            )
            self.assertTrue(r1["appended"])
            self.assertEqual(r1["report"]["observation_class"], "decision_time_market")

            # Missing/tampered state snapshot blocks next evaluation
            snap_file = root / "state_snapshots" / "seq_000000.state.json"
            self.assertTrue(snap_file.is_file())

            replay_res = replay_window(
                root / "replay",
                root / "ledger",
                bitcoin_merkle_roots=hdrs,
            )
            self.assertTrue(replay_res["bit_for_bit_match"])
            self.assertTrue(replay_res["chain_ok"])
            self.assertTrue(replay_res["receipts_creditable"])

            ledger = ShadowLedger(root / "ledger")
            spec_v2 = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
            recs = ledger.load_records()
            rcps = ledger.load_receipts()

            # Strict default check_forward_clock_credit (#4): omitted config_path reads locked default config -> fails closed
            ok_default_locked, issues_locked = check_forward_clock_credit(
                recs,
                rcps,
                ledger_dir=ledger.root_dir,
                bitcoin_merkle_roots=hdrs,
                commitment_deadline_et="23:30",
                calendar_spec=spec_v2,
            )
            self.assertFalse(ok_default_locked)
            self.assertTrue(any("production_chain_locked" in str(x) for x in issues_locked))

            # Explicit unlocked config_path verifies locks + pinned engine hash cleanly
            ok_credit, issues = check_forward_clock_credit(
                recs,
                rcps,
                ledger_dir=ledger.root_dir,
                config_path=unlocked_cfg_path,
                bitcoin_merkle_roots=hdrs,
                commitment_deadline_et="23:30",
                calendar_spec=spec_v2,
            )
            self.assertTrue(ok_credit, issues)

    def test_state_snapshot_tamper_or_engine_drift_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            sdir = Path(tmpdir) / "snaps"
            save_row_state_snapshot(
                sdir,
                chain_id="c1",
                sequence=0,
                session_date="2026-01-05",
                record_sha256="a" * 64,
                engine_hash=PINNED_FUSE_ENGINE_HASH,
                spec_version=PINNED_FUSE_SPEC_VERSION,
                next_row_state={"V4": {"TQQQ": {"clearing_counter": 2}}},
            )
            with self.assertRaises(EngineIdentityError):
                load_row_state_snapshot(
                    sdir,
                    expected_chain_id="c1",
                    expected_engine_hash="b" * 64,
                    expected_spec_version=PINNED_FUSE_SPEC_VERSION,
                    expected_sequence=0,
                )
            p = sdir / "seq_000000.state.json"
            obj = json.loads(p.read_text(encoding="utf-8"))
            obj["next_row_state"]["V4"]["TQQQ"]["clearing_counter"] = 99
            p.write_text(json.dumps(obj), encoding="utf-8")
            with self.assertRaises(EngineIdentityError):
                load_row_state_snapshot(
                    sdir,
                    expected_chain_id="c1",
                    expected_engine_hash=PINNED_FUSE_ENGINE_HASH,
                    expected_spec_version=PINNED_FUSE_SPEC_VERSION,
                    expected_sequence=0,
                )

    def test_us_23_5_official_close_isolation_and_post_2100_et_boundary(self) -> None:
        spec_v2 = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal_reg = resolve_session("XNYS", "2026-01-05", spec=spec_v2)
        cal_half = resolve_session("XNYS", "2026-11-27", spec=spec_v2)

        # core_session_open vs extended_overnight_open in O2
        cal_core_agree = resolve_session(
            "XNYS", "2026-01-05", spec=spec_v2, observed_venue_status="core_session_open"
        )
        self.assertFalse(cal_core_agree["blocks_eligibility"])
        cal_overnight = resolve_session(
            "XNYS", "2026-01-05", spec=spec_v2, observed_venue_status="extended_overnight_open"
        )
        self.assertTrue(cal_overnight["blocks_eligibility"])
        self.assertTrue(cal_overnight["venue_disagreement"])

        # classify & bar timestamp boundary checks (including 13:00 ET half-day close)
        self.assertEqual(
            classify_us_equity_session_window("2026-01-05T21:00:00.000Z", cal_reg),
            "core_session_open",
        )
        self.assertEqual(
            classify_us_equity_session_window("2026-01-05T22:00:00.000Z", cal_reg),
            "after_hours_extended",
        )
        self.assertEqual(
            classify_us_equity_session_window("2026-01-06T02:15:00.000Z", cal_reg),
            "extended_overnight_open",
        )
        self.assertEqual(
            is_official_close_bar_timestamp("2026-11-27T18:30:00.000Z", cal_half),
            (False, "extended_hours_overnight_rejected"),
        )

        # Post-21:00 ET prior close resolution resolves T (2026-01-05), never T+1
        prior = resolve_prior_close_session("2026-01-06T02:30:00.000Z", spec=spec_v2)
        self.assertEqual(prior["trade_date"], "2026-01-05")

        # Tiingo overnight/post-21:00 ET contamination rejected cleanly
        bad_tiingo = json.dumps(
            [
                {
                    "date": "2026-01-05T21:15:00-05:00",
                    "close": 52.0,
                    "divCash": 0.0,
                    "splitFactor": 1.0,
                }
            ]
        ).encode()
        with self.assertRaises(ValueError) as ctx:
            select_tiingo_row(bad_tiingo, "2026-01-05", calendar_session=cal_reg)
        self.assertEqual(str(ctx.exception), "next_trade_date_contamination")

        # Half-day 14:00 ET bar rejected cleanly when close is 13:00 ET
        ts_1400 = int(datetime(2026, 11, 27, 14, 0, tzinfo=_NY_TZ).timestamp())
        half_bad_y = json.dumps(
            {"chart": {"result": [{"timestamp": [ts_1400], "indicators": {"quote": [{"close": [29000.0]}]}}]}}
        ).encode()
        with self.assertRaises(ValueError) as yctx:
            select_yahoo_close(half_bad_y, "2026-11-27", calendar_session=cal_half)
        self.assertEqual(str(yctx.exception), "extended_hours_overnight_rejected")

        # Record-level contamination check rejects T+1 effective session for equity series
        ok_rec, err_rec = validate_official_close_market_record(
            {"instrument": "TQQQ", "effective_session": "2026-01-06", "disclosures": {}},
            cal_reg,
        )
        self.assertFalse(ok_rec)
        self.assertEqual(err_rec, "next_trade_date_contamination")

    def test_strict_boolean_config_and_midnight_timestamp_and_non_daily_yahoo_rejected(self) -> None:
        base_cfg = json.loads(
            (_REPO_ROOT / "config" / "production-scheduler.json").read_text(encoding="utf-8")
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            bad_cases = [
                ("production_chain_locked", "false"),
                ("production_chain_locked", 0),
                ("production_chain_locked", None),
                ("clk01_active", "true"),
                ("clk01_active", 1),
                ("clk01_active", None),
                ("production_mode_forbidden", "false"),
            ]
            for idx, (key, bad_val) in enumerate(bad_cases):
                mut = dict(base_cfg)
                mut[key] = bad_val
                p = Path(tmpdir) / f"bad_bool_{idx}.json"
                p.write_text(json.dumps(mut), encoding="utf-8")
                with self.assertRaises(ValueError):
                    load_production_scheduler_config(p)

            forbid_cfg = dict(base_cfg)
            forbid_cfg["production_chain_locked"] = False
            forbid_cfg["clk01_active"] = True
            forbid_cfg["production_mode_forbidden"] = True
            p_forbid = Path(tmpdir) / "forbidden.json"
            p_forbid.write_text(json.dumps(forbid_cfg), encoding="utf-8")
            with self.assertRaises(DrillOnlyViolationError):
                load_production_scheduler_config(p_forbid)

        spec_v2 = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal_reg = resolve_session("XNYS", "2026-01-05", spec=spec_v2)

        # Tiingo row["timestamp"] midnight markers must never slip through (reject UTC or NY midnight)
        for bad_ts_val, expected_err in (
            ("2026-01-05T00:00:00Z", "session_not_in_payload"),
            ("2026-01-05T00:00:00-05:00", "extended_hours_overnight_rejected"),
        ):
            bad_tiingo_ts = json.dumps(
                [
                    {
                        "date": "2026-01-05T00:00:00Z",
                        "timestamp": bad_ts_val,
                        "close": 52.0,
                        "divCash": 0.0,
                        "splitFactor": 1.0,
                    }
                ]
            ).encode()
            with self.assertRaises(ValueError) as t_ctx:
                select_tiingo_row(bad_tiingo_ts, "2026-01-05", calendar_session=cal_reg)
            self.assertEqual(str(t_ctx.exception), expected_err)

        # Yahoo non-daily dataGranularity rejected cleanly even when bar timestamp is 16:00 ET
        ts_1600 = int(datetime(2026, 1, 5, 16, 0, tzinfo=_NY_TZ).timestamp())
        bad_yahoo_intraday = json.dumps(
            {
                "chart": {
                    "result": [
                        {
                            "meta": {"dataGranularity": "1m"},
                            "timestamp": [ts_1600],
                            "indicators": {"quote": [{"close": [29000.0]}]},
                        }
                    ]
                }
            }
        ).encode()
        with self.assertRaises(ValueError) as y_ctx:
            select_yahoo_close(bad_yahoo_intraday, "2026-01-05", calendar_session=cal_reg)
        self.assertEqual(str(y_ctx.exception), "yahoo_non_daily_granularity_rejected")

    def test_live_http_crash_and_split_propagate_to_l6_and_v2_breakers(self) -> None:
        raw_cfg = json.loads(
            (_REPO_ROOT / "config" / "production-scheduler.json").read_text(encoding="utf-8")
        )
        raw_cfg["production_chain_locked"] = False
        raw_cfg["clk01_active"] = True

        class _FakeOts(OpenTimestampsClient):
            def stamp_digest(self, sha256_hex: str) -> tuple[bytes, str]:
                ots_b, _ = build_synthetic_ots_fixture(sha256_hex, bitcoin_height=880100)
                return ots_b, "https://a.pool.opentimestamps.org"

        with tempfile.TemporaryDirectory() as tmpdir:
            cfg_path = Path(tmpdir) / "unlocked.json"
            cfg_path.write_text(json.dumps(raw_cfg), encoding="utf-8")

            # Case 1: -11% XNDX live HTTP drop triggers L6 HALT without static fixtures
            d_crash = Path(tmpdir) / "crash_run"
            res_crash = run_production_cycle(
                session_date="2026-01-05",
                data_dir=d_crash,
                config_path=cfg_path,
                transport=build_live_mock_transport("2026-01-05", xndx_ret_override=-0.11),
                environ={"TIINGO_API_KEY": "test-dummy-token"},
                ots_client=_FakeOts(),
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clock(),
                sleeper=lambda _s: None,
            )
            self.assertTrue(res_crash["appended"])
            recs_c = ShadowLedger(d_crash / "ledger").load_records()
            b_crash = {b["id"]: b for b in recs_c[-1]["record_body"]["evaluator"]["breakers"]}
            self.assertEqual(b_crash["L6"]["state"], "HALT")
            self.assertTrue(b_crash["L6"]["would_have_tripped"])

            # Case 2: Tiingo splitFactor=2.0 in live HTTP response triggers V2 HALT
            d_split = Path(tmpdir) / "split_run"
            res_split = run_production_cycle(
                session_date="2026-01-05",
                data_dir=d_split,
                config_path=cfg_path,
                transport=build_live_mock_transport("2026-01-05", tqqq_split=2.0),
                environ={"TIINGO_API_KEY": "test-dummy-token"},
                ots_client=_FakeOts(),
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clock(),
                sleeper=lambda _s: None,
            )
            self.assertTrue(res_split["appended"])
            recs_s = ShadowLedger(d_split / "ledger").load_records()
            b_split = {b["id"]: b for b in recs_s[-1]["record_body"]["evaluator"]["breakers"]}
            self.assertEqual(b_split["V2"]["state"], "HALT")
            self.assertTrue(b_split["V2"]["would_have_tripped"])

    def test_rejected_day_v4_latch_persists_and_replays_bit_for_bit(self) -> None:
        drill_cfg = _sh2_drill_cfg()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "rej_v4"
            fdir_bad = Path(tmpdir) / "fixtures_missing_treasury"
            _build_session_fixture_files(fdir_bad, "2026-01-05")
            (fdir_bad / "treasury_tbill_2026-01-05.csv").unlink()

            fdir_good = Path(tmpdir) / "fixtures_good"
            _build_session_fixture_files(fdir_good, "2026-01-06")

            # Day 1 fails closed on O1 (missing TBILL_3M) while sigma=0.50 trips V4 latch
            r_rej = run_daily_drill_cycle(
                session_date="2026-01-05",
                data_dir=root,
                config=drill_cfg,
                fixtures_dir=fdir_bad,
                sigma_by_inst={"TQQQ": "0.50", "SOXL": "0.24"},
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
            )
            self.assertFalse(r_rej["appended"])
            self.assertTrue(r_rej["fail_closed"])
            self.assertEqual(len(ShadowLedger(root / "ledger").load_records()), 0)

            pending_res = load_pending_rejected_state_snapshot(
                root / "state_snapshots",
                expected_chain_id="drill-v0",
                expected_base_sequence=-1,
                expected_base_record_sha256=GENESIS_PREV_HASH,
                current_session_date="2026-01-06",
                expected_engine_hash=PINNED_FUSE_ENGINE_HASH,
                expected_spec_version=PINNED_FUSE_SPEC_VERSION,
            )
            self.assertIsNotNone(pending_res)
            pending_state, pending_meta = pending_res  # type: ignore[misc]
            self.assertEqual(pending_meta["rejected_session_date"], "2026-01-05")
            self.assertIn("V4", pending_state)

            # Day 2 succeeds with normal sigma=0.24, carries V4 clearing state (clearing_counter=1), unlinks pending
            r_ok = run_daily_drill_cycle(
                session_date="2026-01-06",
                data_dir=root,
                config=drill_cfg,
                fixtures_dir=fdir_good,
                sigma_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
            )
            self.assertTrue(r_ok["appended"])
            self.assertFalse((root / "state_snapshots" / "pending_rejected_state.json").exists())

            recs = ShadowLedger(root / "ledger").load_records()
            self.assertEqual(len(recs), 1)
            b_v4 = {b["id"]: b for b in recs[0]["record_body"]["evaluator"]["breakers"]}["V4"]
            self.assertEqual(b_v4["state"], "NO-NEW-RISK")
            self.assertEqual(b_v4["reason"], "volatility_below_2x_but_not_yet_cleared")
            self.assertEqual(b_v4["outputs"]["TQQQ"]["clearing_counter"], 1)

            rep = replay_window(root / "replay", root / "ledger")
            self.assertTrue(rep["bit_for_bit_match"])
            self.assertTrue(rep["chain_ok"])

    def test_tampered_spec_digest_and_mid_commit_snapshot_rollback_and_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            # 1. Self-consistent forged spec_dir whose spec_digest != cfg.calendar_spec_digest fails closed
            forged_spec = Path(tmpdir) / "forged_spec"
            shutil.copytree(_REPO_ROOT / "specs" / "csa22-v2", forged_spec)
            spec_md = forged_spec / "SPEC.md"
            spec_md.write_bytes(spec_md.read_bytes() + b"\n")
            manifest = json.loads((forged_spec / "manifest.json").read_text(encoding="utf-8"))
            exc_name = next(k for k in manifest["files"] if k.startswith("exceptions-"))
            for fname in list(manifest["files"].keys()):
                manifest["files"][fname] = hashlib.sha256((forged_spec / fname).read_bytes()).hexdigest()
            manifest["spec_digest"] = hashlib.sha256(
                spec_md.read_bytes() + (forged_spec / exc_name).read_bytes()
            ).hexdigest()
            (forged_spec / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

            root_spec = Path(tmpdir) / "tamper_spec_data"
            fdir = Path(tmpdir) / "fixtures"
            _build_session_fixture_files(fdir, "2026-01-05")
            _build_session_fixture_files(fdir, "2026-01-06")

            drill_cfg = _sh2_drill_cfg()
            with self.assertRaises(CalendarCoverageError):
                run_daily_drill_cycle(
                    session_date="2026-01-05",
                    data_dir=root_spec,
                    config=drill_cfg,
                    spec_dir=forged_spec,
                    fixtures_dir=fdir,
                    wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                )
            with self.assertRaises(CalendarCoverageError):
                detect_missed_runs(
                    window_start_date="2026-01-05",
                    as_of_utc="2026-01-05T23:45:00.000Z",
                    data_dir=root_spec,
                    config=drill_cfg,
                    spec_dir=forged_spec,
                )

            # 2. Rollback of staged state snapshot + replay_bundle when append_record crashes after staging
            root_rec = Path(tmpdir) / "atomic_rec"

            def _failing_append(self, *args, before_record_commit=None, **kwargs):
                if before_record_commit is not None:
                    before_record_commit()
                raise OSError("simulated_crash_before_record_commit")

            with mock.patch.object(ShadowLedger, "append_record", new=_failing_append):
                with self.assertRaises(OSError):
                    run_daily_drill_cycle(
                        session_date="2026-01-05",
                        data_dir=root_rec,
                        config=drill_cfg,
                        fixtures_dir=fdir,
                        wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                    )
            self.assertFalse((root_rec / "replay" / "replay_bundle.json").exists())
            self.assertFalse((root_rec / "state_snapshots" / "seq_000000.state.json").exists())
            self.assertFalse((root_rec / "state_snapshots" / "latest.state.json").exists())
            self.assertEqual(len(ShadowLedger(root_rec / "ledger").load_records()), 0)

            # 3. Missing seq_000000.state.json + stale latest.state.json deterministically reconciled on retry
            r_day0 = run_daily_drill_cycle(
                session_date="2026-01-05",
                data_dir=root_rec,
                config=drill_cfg,
                fixtures_dir=fdir,
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
            )
            self.assertTrue(r_day0["appended"])
            (root_rec / "state_snapshots" / "seq_000000.state.json").unlink()
            (root_rec / "state_snapshots" / "latest.state.json").write_text(
                '{"sequence": 99}', encoding="utf-8"
            )

            r_retry0 = run_daily_drill_cycle(
                session_date="2026-01-05",
                data_dir=root_rec,
                config=drill_cfg,
                fixtures_dir=fdir,
                wall_clock=lambda: datetime(2026, 1, 5, 22, 35, tzinfo=timezone.utc),
            )
            self.assertFalse(r_retry0["appended"])
            self.assertEqual(r_retry0["reason"], "already_committed_session_preserved")
            self.assertTrue((root_rec / "state_snapshots" / "seq_000000.state.json").is_file())
            self.assertTrue((root_rec / "state_snapshots" / "latest.state.json").is_file())
            self.assertEqual(
                json.loads((root_rec / "state_snapshots" / "latest.state.json").read_text(encoding="utf-8"))["sequence"],
                0,
            )

            # Next trading day proceeds cleanly using repaired snapshot (never stranded)
            r_day1 = run_daily_drill_cycle(
                session_date="2026-01-06",
                data_dir=root_rec,
                config=drill_cfg,
                fixtures_dir=fdir,
                wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
            )
            self.assertTrue(r_day1["appended"])
            self.assertEqual(r_day1["sequence"], 1)


if __name__ == "__main__":
    unittest.main()
