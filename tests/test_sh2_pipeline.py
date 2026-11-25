"""Tests for SH-2 production lock, 23/5 close isolation, restart snapshots, replay, and D1-D10 demo."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
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
    resolve_previous_trading_date,
    resolve_prior_close_session,
    resolve_session,
)
from shadow.feeder import (  # noqa: E402
    extract_prior_close_from_tiingo_bytes,
    extract_prior_close_from_yahoo_bytes,
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
    CanonicalizationError,
    GENESIS_PREV_HASH,
    NO_PENDING_REJECTED_STATE_SHA256,
    ShadowLedger,
    _sha256_canonical_state_dict,
    build_record_body,
    check_forward_clock_credit,
    compute_code_manifest_hash,
    compute_pending_rejected_state_sha256,
    load_pending_rejected_state_snapshot,
    load_row_state_snapshot,
    save_pending_rejected_state_snapshot,
    save_row_state_snapshot,
    sha256_canonical,
    validate_record_body,
)
from shadow.ots import OpenTimestampsClient, build_synthetic_ots_fixture  # noqa: E402
from shadow.replay import replay_window  # noqa: E402
from shadow.scheduler import (  # noqa: E402
    CalendarCoverageError,
    DrillOnlyViolationError,
    _retire_superseded_pending_rejected_state,
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
                expected_prior_head_sha256=GENESIS_PREV_HASH,
                expected_pending_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                trusted_head_anchor_path=Path(tmpdir) / "prod_unlocked_anchor.json",
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
                expected_prior_head_sha256=GENESIS_PREV_HASH,
                expected_pending_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                trusted_head_anchor_path=Path(tmpdir) / "crash_anchor.json",
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
                expected_prior_head_sha256=GENESIS_PREV_HASH,
                expected_pending_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                trusted_head_anchor_path=Path(tmpdir) / "split_anchor.json",
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
                long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
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
                long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
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

    def test_immediate_preceding_trading_session_prior_close_and_skip_protection(self) -> None:
        spec_v2 = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        self.assertEqual(resolve_previous_trading_date(spec_v2, "XNYS", "2026-01-05"), "2026-01-02")
        self.assertEqual(resolve_previous_trading_date(spec_v2, "XNYS", "2026-07-06"), "2026-07-02")

        # Prior half-day (2026-11-27 13:00 ET close) validates against 2026-11-27 close cutoff, never falling back to 2026-11-25
        cal_hd = resolve_session("XNYS", "2026-11-27", spec=spec_v2)
        ts_25 = int(datetime(2026, 11, 25, 16, 0, tzinfo=_NY_TZ).timestamp())
        ts_27_1300 = int(datetime(2026, 11, 27, 13, 0, tzinfo=_NY_TZ).timestamp())
        ts_27_1400 = int(datetime(2026, 11, 27, 14, 0, tzinfo=_NY_TZ).timestamp())
        y_good_hd = json.dumps(
            {
                "chart": {
                    "result": [
                        {
                            "meta": {"dataGranularity": "1d"},
                            "timestamp": [ts_25, ts_27_1300],
                            "indicators": {"quote": [{"close": [28000.0, 29000.0]}]},
                        }
                    ]
                }
            }
        ).encode()
        self.assertEqual(
            extract_prior_close_from_yahoo_bytes(
                y_good_hd, "2026-11-30", calendar_session=cal_hd, target_prior_date="2026-11-27"
            ),
            "29000",
        )
        y_bad_hd = json.dumps(
            {
                "chart": {
                    "result": [
                        {
                            "meta": {"dataGranularity": "1d"},
                            "timestamp": [ts_25, ts_27_1400],
                            "indicators": {"quote": [{"close": [28000.0, 29000.0]}]},
                        }
                    ]
                }
            }
        ).encode()
        self.assertIsNone(
            extract_prior_close_from_yahoo_bytes(
                y_bad_hd, "2026-11-30", calendar_session=cal_hd, target_prior_date="2026-11-27"
            )
        )

        # Codex P1 scenario: Jan 5 committed (29500), Jan 6 skipped/rejected, Jan 7 (29500) with Jan 6=33000 in payload -> -10.6% -> L6 HALT
        drill_cfg = _sh2_drill_cfg()
        with tempfile.TemporaryDirectory() as tmpdir:
            fdir_5 = Path(tmpdir) / "fix_05"
            _build_session_fixture_files(fdir_5, "2026-01-05")

            # Jan 7 fixtures containing Jan 6 (33000) -> Jan 7 (29500) = -10.606% on XNDX
            fdir_7_with_6 = Path(tmpdir) / "fix_07_with_06"
            _build_session_fixture_files(fdir_7_with_6, "2026-01-07")
            y7 = json.loads((fdir_7_with_6 / "yahoo_xndx_2026-01-07.json").read_text(encoding="utf-8"))
            y7["chart"]["result"][0]["indicators"]["quote"][0]["close"] = [33000.0, 29500.0]
            (fdir_7_with_6 / "yahoo_xndx_2026-01-07.json").write_text(json.dumps(y7), encoding="utf-8")

            root_a = Path(tmpdir) / "skip_trip_l6"
            r5 = run_daily_drill_cycle(
                session_date="2026-01-05",
                data_dir=root_a,
                config=drill_cfg,
                fixtures_dir=fdir_5,
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
            )
            self.assertTrue(r5["appended"])

            r7_crash = run_daily_drill_cycle(
                session_date="2026-01-07",
                data_dir=root_a,
                config=drill_cfg,
                fixtures_dir=fdir_7_with_6,
                wall_clock=lambda: datetime(2026, 1, 7, 22, 30, tzinfo=timezone.utc),
            )
            self.assertTrue(r7_crash["appended"])
            recs_a = ShadowLedger(root_a / "ledger").load_records()
            b7 = {b["id"]: b for b in recs_a[-1]["record_body"]["evaluator"]["breakers"]}
            self.assertEqual(b7["L6"]["state"], "HALT")
            self.assertTrue(b7["L6"]["would_have_tripped"])

            # If Jan 6 is absent from BOTH bundle AND archive payload (only Jan 5 present), must NEVER reuse Jan 5 -> O1 fails closed!
            fdir_7_stale = Path(tmpdir) / "fix_07_only_05"
            _build_session_fixture_files(fdir_7_stale, "2026-01-07")
            ts_05 = int(datetime(2026, 1, 5, 16, 0, tzinfo=_NY_TZ).timestamp())
            ts_07 = int(datetime(2026, 1, 7, 16, 0, tzinfo=_NY_TZ).timestamp())
            y7_only_05 = json.loads((fdir_7_stale / "yahoo_xndx_2026-01-07.json").read_text(encoding="utf-8"))
            y7_only_05["chart"]["result"][0]["timestamp"] = [ts_05, ts_07]
            y7_only_05["chart"]["result"][0]["indicators"]["quote"][0]["close"] = [29500.0, 29500.0]
            (fdir_7_stale / "yahoo_xndx_2026-01-07.json").write_text(json.dumps(y7_only_05), encoding="utf-8")

            root_b = Path(tmpdir) / "skip_fail_closed"
            run_daily_drill_cycle(
                session_date="2026-01-05",
                data_dir=root_b,
                config=drill_cfg,
                fixtures_dir=fdir_5,
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
            )
            r7_fail = run_daily_drill_cycle(
                session_date="2026-01-07",
                data_dir=root_b,
                config=drill_cfg,
                fixtures_dir=fdir_7_stale,
                wall_clock=lambda: datetime(2026, 1, 7, 22, 30, tzinfo=timezone.utc),
            )
            self.assertFalse(r7_fail["appended"])
            self.assertTrue(r7_fail["fail_closed"])
            self.assertEqual(r7_fail["reason"], "stale_or_missing_input")
            self.assertEqual(len(ShadowLedger(root_b / "ledger").load_records()), 1)

    def test_same_session_retry_after_o1_rejection_and_v4_no_double_advance(self) -> None:
        drill_cfg = _sh2_drill_cfg()
        with tempfile.TemporaryDirectory() as tmpdir:
            # Part 1: Run 2026-01-05 missing Treasury (O1 fail-closed), restore Treasury, rerun 2026-01-05 before cutoff -> commits & clears pending
            root1 = Path(tmpdir) / "same_day_o1"
            fdir_bad = Path(tmpdir) / "fix_missing_treasury"
            _build_session_fixture_files(fdir_bad, "2026-01-05")
            (fdir_bad / "treasury_tbill_2026-01-05.csv").unlink()
            fdir_good = Path(tmpdir) / "fix_good"
            _build_session_fixture_files(fdir_good, "2026-01-05")

            r_fail = run_daily_drill_cycle(
                session_date="2026-01-05",
                data_dir=root1,
                config=drill_cfg,
                fixtures_dir=fdir_bad,
                wall_clock=lambda: datetime(2026, 1, 5, 22, 20, tzinfo=timezone.utc),
            )
            self.assertFalse(r_fail["appended"])
            self.assertTrue((root1 / "state_snapshots" / "pending_rejected_state.json").is_file())

            r_retry = run_daily_drill_cycle(
                session_date="2026-01-05",
                data_dir=root1,
                config=drill_cfg,
                fixtures_dir=fdir_good,
                wall_clock=lambda: datetime(2026, 1, 5, 22, 35, tzinfo=timezone.utc),
            )
            self.assertTrue(r_retry["appended"])
            self.assertEqual(r_retry["sequence"], 0)
            self.assertFalse((root1 / "state_snapshots" / "pending_rejected_state.json").exists())
            recs1 = ShadowLedger(root1 / "ledger").load_records()
            self.assertEqual(len(recs1), 1)
            b_o1 = {b["id"]: b for b in recs1[0]["record_body"]["evaluator"]["breakers"]}["O1"]
            self.assertEqual(b_o1["state"], "NO-NEW-RISK")
            self.assertEqual(b_o1["reason"], "clearing_period")
            rep1 = replay_window(root1 / "replay", root1 / "ledger")
            self.assertTrue(rep1["bit_for_bit_match"])

            # Part 2: V4 clearing mode across two rejected attempts + same-day commit increments clearing_counter ONCE
            root2 = Path(tmpdir) / "same_day_v4"
            fdir_day0 = Path(tmpdir) / "fix_05"
            _build_session_fixture_files(fdir_day0, "2026-01-05")
            r_d0 = run_daily_drill_cycle(
                session_date="2026-01-05",
                data_dir=root2,
                config=drill_cfg,
                fixtures_dir=fdir_day0,
                sigma_by_inst={"TQQQ": "0.50", "SOXL": "0.24"},
                long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
            )
            self.assertTrue(r_d0["appended"])
            d0_hash = r_d0["record_sha256"]

            fdir_d1_bad = Path(tmpdir) / "fix_06_bad"
            _build_session_fixture_files(fdir_d1_bad, "2026-01-06")
            (fdir_d1_bad / "treasury_tbill_2026-01-06.csv").unlink()
            fdir_d1_good = Path(tmpdir) / "fix_06_good"
            _build_session_fixture_files(fdir_d1_good, "2026-01-06")

            # Attempt 1 & Attempt 2 on 2026-01-06 fail O1 while V4 is calm (sigma=0.24)
            for minute in (15, 25):
                rf = run_daily_drill_cycle(
                    session_date="2026-01-06",
                    data_dir=root2,
                    config=drill_cfg,
                    fixtures_dir=fdir_d1_bad,
                    sigma_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                    long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                    wall_clock=lambda m=minute: datetime(2026, 1, 6, 22, m, tzinfo=timezone.utc),
                )
                self.assertFalse(rf["appended"])
                pend_state, pend_meta = load_pending_rejected_state_snapshot(  # type: ignore[misc]
                    root2 / "state_snapshots",
                    expected_chain_id="drill-v0",
                    expected_base_sequence=0,
                    expected_base_record_sha256=d0_hash,
                    current_session_date="2026-01-06",
                    expected_engine_hash=PINNED_FUSE_ENGINE_HASH,
                    expected_spec_version=PINNED_FUSE_SPEC_VERSION,
                )
                self.assertEqual(pend_meta["rejected_session_date"], "2026-01-06")
                # Session-start baseline stays clearing_counter=0 across retries while next_row_state has counter=1 once
                self.assertEqual(pend_meta["base_prior_row_state"]["V4"]["TQQQ"]["clearing_counter"], 0)
                self.assertEqual(pend_state["V4"]["TQQQ"]["clearing_counter"], 1)

            # Attempt 3 restores Treasury same day -> commits seq=1 with clearing_counter == 1 (NOT 2 or 3!)
            r_ok = run_daily_drill_cycle(
                session_date="2026-01-06",
                data_dir=root2,
                config=drill_cfg,
                fixtures_dir=fdir_d1_good,
                sigma_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                wall_clock=lambda: datetime(2026, 1, 6, 22, 40, tzinfo=timezone.utc),
            )
            self.assertTrue(r_ok["appended"])
            self.assertEqual(r_ok["sequence"], 1)
            self.assertFalse((root2 / "state_snapshots" / "pending_rejected_state.json").exists())
            recs2 = ShadowLedger(root2 / "ledger").load_records()
            b_v4_2 = {b["id"]: b for b in recs2[-1]["record_body"]["evaluator"]["breakers"]}["V4"]
            self.assertEqual(b_v4_2["outputs"]["TQQQ"]["clearing_counter"], 1)
            rep2 = replay_window(root2 / "replay", root2 / "ledger")
            self.assertTrue(rep2["bit_for_bit_match"])

    def test_non_production_config_rejected_by_check_forward_clock_credit(self) -> None:
        raw_cfg = json.loads(
            (_REPO_ROOT / "config" / "production-scheduler.json").read_text(encoding="utf-8")
        )
        raw_cfg["production_chain_locked"] = False
        raw_cfg["clk01_active"] = True
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "credit_cfg_check"
            unlocked_cfg_path = Path(tmpdir) / "unlocked.json"
            unlocked_cfg_path.write_text(json.dumps(raw_cfg), encoding="utf-8")
            hdrs: dict[int, dict[str, str]] = {}

            class _FakeOts(OpenTimestampsClient):
                def stamp_digest(self, sha256_hex: str) -> tuple[bytes, str]:
                    ots_b, root_hex = build_synthetic_ots_fixture(sha256_hex, bitcoin_height=880100)
                    assert root_hex is not None
                    hdrs[880100] = {"merkle_root": root_hex, "timestamp": "2026-01-06T03:00:00.000Z"}
                    return ots_b, "https://a.pool.opentimestamps.org"

            run_production_cycle(
                session_date="2026-01-05",
                data_dir=root,
                config_path=unlocked_cfg_path,
                transport=build_live_mock_transport("2026-01-05"),
                environ={"TIINGO_API_KEY": "test-dummy-token"},
                ots_client=_FakeOts(),
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clock(),
                sleeper=lambda _s: None,
                expected_prior_head_sha256=GENESIS_PREV_HASH,
                expected_pending_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                trusted_head_anchor_path=Path(tmpdir) / "prod_credit_anchor.json",
            )
            ledger = ShadowLedger(root / "ledger")
            spec_v2 = load_spec(_REPO_ROOT / "specs" / "csa22-v2")

            ok_drill, issues_drill = check_forward_clock_credit(
                ledger.load_records(),
                ledger.load_receipts(),
                ledger_dir=ledger.root_dir,
                config_path=_REPO_ROOT / "config" / "drill-scheduler.json",
                bitcoin_merkle_roots=hdrs,
                commitment_deadline_et="23:30",
                calendar_spec=spec_v2,
            )
            self.assertFalse(ok_drill)
            self.assertIn("non_production_config_rejected:drill", issues_drill)

            ok_missing, issues_missing = check_forward_clock_credit(
                ledger.load_records(),
                ledger.load_receipts(),
                ledger_dir=ledger.root_dir,
                config_path=Path(tmpdir) / "missing.json",
                bitcoin_merkle_roots=hdrs,
                commitment_deadline_et="23:30",
                calendar_spec=spec_v2,
            )
            self.assertFalse(ok_missing)
            self.assertIn("missing_production_config", issues_missing)

    def test_ledger_bound_state_digest_prevents_forged_halt_downgrade(self) -> None:
        ev = VendoredFuseEvaluator()
        spec_v2 = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal = resolve_session("XNYS", "2026-01-05", spec=spec_v2)
        code_sha = compute_code_manifest_hash(_REPO_ROOT, calendar_version="csa22-v2")

        # 1. Committed non-stub record embeds evaluator.next_row_state_sha256 bound to O8 HALT latch
        inp_o8 = {
            "E": "50000",
            "E_star": "50000",
            "H": "0",
            "L": "0",
            "positions_source_record": {"as_of": "2026-01-05T22:30:00.000Z", "age_sessions": 0},
            "broker_reconciliation_source_record": {
                "as_of": "2026-01-05T22:30:00.000Z",
                "age_sessions": 0,
            },
            "index_one_session_returns": {"NDX": 0.0, "SEMI": 0.0},
            "authorized_events": {"account": [{"type": "kill_switch", "session": "2026-01-05"}]},
            "funds": {
                "TQQQ": {
                    "s": "1",
                    "close_price": "50",
                    "q_pre": 100,
                    "sigma_forecast": "0.24",
                    "fund_return": 0.0,
                    "index_return": 0.0,
                },
                "SOXL": {
                    "s": "1",
                    "close_price": "40",
                    "q_pre": 100,
                    "sigma_forecast": "0.22",
                    "fund_return": 0.0,
                    "index_return": 0.0,
                },
            },
        }
        built_o8 = build_record_body(
            chain_id="prod-o8-bind",
            sequence=0,
            prev_record_sha256=GENESIS_PREV_HASH,
            mode="drill",
            session_date="2026-01-05",
            decision_timestamp_utc="2026-01-05T22:30:00.000Z",
            session_inputs=inp_o8,
            calendar_session=cal,
            code_version_sha=code_sha,
            evaluator=ev,
            calendar_spec=spec_v2,
        )
        self.assertTrue(built_o8["appended"])
        body_o8 = built_o8["record_body"]
        state_o8 = built_o8["next_row_state"]
        self.assertTrue(state_o8["O8"]["latched"])
        committed_nrs_sha = body_o8["evaluator"]["next_row_state_sha256"]
        self.assertEqual(committed_nrs_sha, _sha256_canonical_state_dict(state_o8))

        # Stripping next_row_state_sha256 from a non-stub record fails validate_record_body
        stripped = json.loads(json.dumps(body_o8))
        del stripped["evaluator"]["next_row_state_sha256"]
        with self.assertRaises(CanonicalizationError):
            validate_record_body(stripped)

        with tempfile.TemporaryDirectory() as tmpdir:
            sdir = Path(tmpdir) / "snaps"
            rec_sha = sha256_canonical(body_o8)
            save_row_state_snapshot(
                sdir,
                chain_id="prod-o8-bind",
                sequence=0,
                session_date="2026-01-05",
                record_sha256=rec_sha,
                engine_hash=ev.engine_hash,
                spec_version=ev.spec_version,
                next_row_state=state_o8,
            )

            # 2. Forging O8 HALT -> {} in BOTH seq_000000.state.json AND latest.state.json + recomputing state_sha256 MUST FAIL
            forged_state = json.loads(json.dumps(state_o8))
            forged_state["O8"] = {}
            forged_sha = _sha256_canonical_state_dict(forged_state)
            for fname in ("seq_000000.state.json", "latest.state.json"):
                fp = sdir / fname
                obj = json.loads(fp.read_text(encoding="utf-8"))
                obj["next_row_state"] = forged_state
                obj["state_sha256"] = forged_sha
                fp.write_text(json.dumps(obj), encoding="utf-8")

            with self.assertRaises(EngineIdentityError):
                load_row_state_snapshot(
                    sdir,
                    expected_chain_id="prod-o8-bind",
                    expected_engine_hash=ev.engine_hash,
                    expected_spec_version=ev.spec_version,
                    expected_sequence=0,
                    expected_record_sha256=rec_sha,
                    expected_state_sha256=committed_nrs_sha,
                    allow_missing_at_genesis=False,
                )
            with self.assertRaises(EngineIdentityError):
                load_row_state_snapshot(
                    sdir,
                    expected_chain_id="prod-o8-bind",
                    expected_engine_hash=ev.engine_hash,
                    expected_spec_version=ev.spec_version,
                    expected_sequence=None,
                    expected_record_sha256=rec_sha,
                    expected_state_sha256=committed_nrs_sha,
                    allow_missing_at_genesis=False,
                )

            # 3. Forging pending_rejected_state.json to clear O8 HALT or forge base_prior_row_state + recomputed checksums MUST FAIL
            save_pending_rejected_state_snapshot(
                sdir,
                chain_id="prod-o8-bind",
                base_sequence=0,
                base_record_sha256=rec_sha,
                rejected_session_date="2026-01-06",
                engine_hash=ev.engine_hash,
                spec_version=ev.spec_version,
                next_row_state=forged_state,  # O8 wiped to {} with recomputed state_sha256!
                base_prior_row_state=state_o8,
            )
            with self.assertRaises(EngineIdentityError):
                load_pending_rejected_state_snapshot(
                    sdir,
                    expected_chain_id="prod-o8-bind",
                    expected_base_sequence=0,
                    expected_base_record_sha256=rec_sha,
                    current_session_date="2026-01-06",
                    expected_engine_hash=ev.engine_hash,
                    expected_spec_version=ev.spec_version,
                    expected_base_prior_state_sha256=committed_nrs_sha,
                    committed_base_prior_row_state=state_o8,
                )

            # Forging base_prior_row_state itself to {} fails ledger-digest check
            save_pending_rejected_state_snapshot(
                sdir,
                chain_id="prod-o8-bind",
                base_sequence=0,
                base_record_sha256=rec_sha,
                rejected_session_date="2026-01-06",
                engine_hash=ev.engine_hash,
                spec_version=ev.spec_version,
                next_row_state=forged_state,
                base_prior_row_state={},
            )
            with self.assertRaises(EngineIdentityError):
                load_pending_rejected_state_snapshot(
                    sdir,
                    expected_chain_id="prod-o8-bind",
                    expected_base_sequence=0,
                    expected_base_record_sha256=rec_sha,
                    current_session_date="2026-01-06",
                    expected_engine_hash=ev.engine_hash,
                    expected_spec_version=ev.spec_version,
                    expected_base_prior_state_sha256=committed_nrs_sha,
                    committed_base_prior_row_state=state_o8,
                )

            # 4. End-to-end scheduler rejects forged seq_000000.state.json & forged pending_rejected_state.json, and recovers committed state deterministically
            drill_cfg = _sh2_drill_cfg()
            root = Path(tmpdir) / "e2e_bind"
            f5 = Path(tmpdir) / "fix_05"
            f6 = Path(tmpdir) / "fix_06"
            f6_bad = Path(tmpdir) / "fix_06_bad"
            _build_session_fixture_files(f5, "2026-01-05")
            _build_session_fixture_files(f6, "2026-01-06")
            _build_session_fixture_files(f6_bad, "2026-01-06")
            (f6_bad / "treasury_tbill_2026-01-06.csv").unlink()

            r0 = run_daily_drill_cycle(
                session_date="2026-01-05",
                data_dir=root,
                config=drill_cfg,
                fixtures_dir=f5,
                sigma_by_inst={"TQQQ": "0.50", "SOXL": "0.24"},
                long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
            )
            self.assertTrue(r0["appended"])

            snap0_path = root / "state_snapshots" / "seq_000000.state.json"
            orig_snap0_bytes = snap0_path.read_bytes()
            snap0_obj = json.loads(orig_snap0_bytes.decode("utf-8"))
            snap0_obj["next_row_state"]["V4"] = {}
            snap0_obj["state_sha256"] = _sha256_canonical_state_dict(snap0_obj["next_row_state"])
            snap0_path.write_text(json.dumps(snap0_obj), encoding="utf-8")

            # Attempting Day 1 with forged snapshot (even valid self-hash!) fails closed BEFORE evaluate
            with self.assertRaises(EngineIdentityError):
                run_daily_drill_cycle(
                    session_date="2026-01-06",
                    data_dir=root,
                    config=drill_cfg,
                    fixtures_dir=f6,
                    long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                    wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
                )
            self.assertEqual(len(ShadowLedger(root / "ledger").load_records()), 1)

            # Re-running already-committed Day 0 deterministically repairs forged snapshot from ledger+bundle!
            r0_heal = run_daily_drill_cycle(
                session_date="2026-01-05",
                data_dir=root,
                config=drill_cfg,
                fixtures_dir=f5,
                long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                wall_clock=lambda: datetime(2026, 1, 5, 22, 35, tzinfo=timezone.utc),
            )
            self.assertEqual(r0_heal["reason"], "already_committed_session_preserved")
            repaired_snap0 = json.loads(snap0_path.read_text(encoding="utf-8"))
            self.assertEqual(repaired_snap0["next_row_state"]["V4"]["TQQQ"]["clearing_counter"], 0)

            # Day 1 fails O1 -> saves pending_rejected_state.json; attacker clears V4 + recomputes checksums -> rejected!
            r1_rej = run_daily_drill_cycle(
                session_date="2026-01-06",
                data_dir=root,
                config=drill_cfg,
                fixtures_dir=f6_bad,
                sigma_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                wall_clock=lambda: datetime(2026, 1, 6, 22, 20, tzinfo=timezone.utc),
            )
            self.assertFalse(r1_rej["appended"])
            pend_path = root / "state_snapshots" / "pending_rejected_state.json"
            pend_obj = json.loads(pend_path.read_text(encoding="utf-8"))
            pend_obj["next_row_state"]["V4"] = {}
            pend_obj["state_sha256"] = _sha256_canonical_state_dict(pend_obj["next_row_state"])
            pend_path.write_text(json.dumps(pend_obj), encoding="utf-8")

            with self.assertRaises(EngineIdentityError):
                run_daily_drill_cycle(
                    session_date="2026-01-06",
                    data_dir=root,
                    config=drill_cfg,
                    fixtures_dir=f6,
                    wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
                )

    def test_consecutive_rejected_sessions_preserve_committed_base_and_v4_clearing_progress(self) -> None:
        drill_cfg = _sh2_drill_cfg()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "multi_day_rej"
            f5 = Path(tmpdir) / "fix_05"
            f6_bad = Path(tmpdir) / "fix_06_bad"
            f7_bad = Path(tmpdir) / "fix_07_bad"
            f7_good = Path(tmpdir) / "fix_07_good"
            _build_session_fixture_files(f5, "2026-01-05")
            _build_session_fixture_files(f6_bad, "2026-01-06")
            (f6_bad / "treasury_tbill_2026-01-06.csv").unlink()
            _build_session_fixture_files(f7_bad, "2026-01-07")
            (f7_bad / "treasury_tbill_2026-01-07.csv").unlink()
            _build_session_fixture_files(f7_good, "2026-01-07")

            r0 = run_daily_drill_cycle(
                session_date="2026-01-05",
                data_dir=root,
                config=drill_cfg,
                fixtures_dir=f5,
                sigma_by_inst={"TQQQ": "0.50", "SOXL": "0.24"},
                long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
            )
            self.assertTrue(r0["appended"])
            d0_hash = r0["record_sha256"]
            recs0 = ShadowLedger(root / "ledger").load_records()
            s0_expected_sha = recs0[-1]["record_body"]["evaluator"]["next_row_state_sha256"]
            committed_s0, _ = load_row_state_snapshot(
                root / "state_snapshots",
                expected_chain_id="drill-v0",
                expected_engine_hash=PINNED_FUSE_ENGINE_HASH,
                expected_spec_version=PINNED_FUSE_SPEC_VERSION,
                expected_sequence=0,
                expected_record_sha256=d0_hash,
                expected_state_sha256=s0_expected_sha,
                allow_missing_at_genesis=False,
            )

            # Day 1 (2026-01-06) fails O1 while calm -> V4 clearing_counter steps 0 -> 1
            r1_rej = run_daily_drill_cycle(
                session_date="2026-01-06",
                data_dir=root,
                config=drill_cfg,
                fixtures_dir=f6_bad,
                sigma_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                wall_clock=lambda: datetime(2026, 1, 6, 22, 20, tzinfo=timezone.utc),
            )
            self.assertFalse(r1_rej["appended"])

            # Day 2 (2026-01-07) ALSO fails O1 while calm -> V4 clearing_counter steps 1 -> 2
            # base_prior_row_state MUST stay pinned to committed seq=0 state (counter=0)
            r2_rej = run_daily_drill_cycle(
                session_date="2026-01-07",
                data_dir=root,
                config=drill_cfg,
                fixtures_dir=f7_bad,
                sigma_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                wall_clock=lambda: datetime(2026, 1, 7, 22, 20, tzinfo=timezone.utc),
            )
            self.assertFalse(r2_rej["appended"])
            pend_state, pend_meta = load_pending_rejected_state_snapshot(  # type: ignore[misc]
                root / "state_snapshots",
                expected_chain_id="drill-v0",
                expected_base_sequence=0,
                expected_base_record_sha256=d0_hash,
                current_session_date="2026-01-07",
                expected_engine_hash=PINNED_FUSE_ENGINE_HASH,
                expected_spec_version=PINNED_FUSE_SPEC_VERSION,
                expected_base_prior_state_sha256=s0_expected_sha,
                committed_base_prior_row_state=committed_s0,
                expected_base_session_date="2026-01-05",
            )
            self.assertEqual(pend_meta["rejected_session_date"], "2026-01-07")
            self.assertEqual(
                _sha256_canonical_state_dict(pend_meta["base_prior_row_state"]),
                s0_expected_sha,
            )
            self.assertEqual(pend_meta["base_prior_row_state"]["V4"]["TQQQ"]["clearing_counter"], 0)
            self.assertEqual(pend_state["V4"]["TQQQ"]["clearing_counter"], 2)

            # Negative boundary: counter jump beyond elapsed days (2 -> 5 on 2026-01-07) fails closed
            pend_path = root / "state_snapshots" / "pending_rejected_state.json"
            orig_pend_bytes = pend_path.read_bytes()
            forged_jump = json.loads(orig_pend_bytes.decode("utf-8"))
            forged_jump["next_row_state"]["V4"]["TQQQ"]["clearing_counter"] = 5
            forged_jump["state_sha256"] = _sha256_canonical_state_dict(forged_jump["next_row_state"])
            pend_path.write_text(json.dumps(forged_jump), encoding="utf-8")
            with self.assertRaises(EngineIdentityError):
                load_pending_rejected_state_snapshot(
                    root / "state_snapshots",
                    expected_chain_id="drill-v0",
                    expected_base_sequence=0,
                    expected_base_record_sha256=d0_hash,
                    current_session_date="2026-01-07",
                    expected_engine_hash=PINNED_FUSE_ENGINE_HASH,
                    expected_spec_version=PINNED_FUSE_SPEC_VERSION,
                    expected_base_prior_state_sha256=s0_expected_sha,
                    committed_base_prior_row_state=committed_s0,
                    expected_base_session_date="2026-01-05",
                )
            pend_path.write_bytes(orig_pend_bytes)

            # Same-day retry on 2026-01-07 restores Treasury -> commits seq=1 cleanly (clearing_counter=2)
            r2_ok = run_daily_drill_cycle(
                session_date="2026-01-07",
                data_dir=root,
                config=drill_cfg,
                fixtures_dir=f7_good,
                sigma_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                wall_clock=lambda: datetime(2026, 1, 7, 22, 35, tzinfo=timezone.utc),
            )
            self.assertTrue(r2_ok["appended"])
            self.assertEqual(r2_ok["sequence"], 1)
            self.assertFalse(pend_path.exists())
            recs = ShadowLedger(root / "ledger").load_records()
            b_v4 = {b["id"]: b for b in recs[-1]["record_body"]["evaluator"]["breakers"]}["V4"]
            self.assertEqual(b_v4["outputs"]["TQQQ"]["clearing_counter"], 2)
            rep = replay_window(root / "replay", root / "ledger")
            self.assertTrue(rep["bit_for_bit_match"])

    def test_v4_10th_session_clearing_allowed_while_premature_clearing_rejected(self) -> None:
        drill_cfg = _sh2_drill_cfg()
        spec_v2 = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        trading_days: list[str] = []
        d_cur = datetime(2026, 1, 5, tzinfo=timezone.utc).date()
        while len(trading_days) < 11:
            ds = d_cur.isoformat()
            if resolve_session("XNYS", ds, spec=spec_v2).get("is_trading_day"):
                trading_days.append(ds)
            d_cur = d_cur + timedelta(days=1)

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "v4_clear_10"
            fix_root = Path(tmpdir) / "fixtures"
            for ds in trading_days:
                _build_session_fixture_files(fix_root / ds, ds)

            # Day 0: V4 trip (clearing_counter=0); Days 1..8: 8 calm sessions -> counter=8
            for idx, ds in enumerate(trading_days[:9]):
                sig = "0.50" if idx == 0 else "0.24"
                res = run_daily_drill_cycle(
                    session_date=ds,
                    data_dir=root,
                    config=drill_cfg,
                    fixtures_dir=fix_root / ds,
                    sigma_by_inst={"TQQQ": sig, "SOXL": "0.24"},
                    long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                    wall_clock=lambda d=ds: datetime.fromisoformat(f"{d}T22:30:00+00:00"),
                )
                self.assertTrue(res["appended"])

            ledger = ShadowLedger(root / "ledger")
            recs_8 = ledger.load_records()
            s8_env = recs_8[-1]
            s8_nrs_sha = s8_env["record_body"]["evaluator"]["next_row_state_sha256"]
            s8_state, _ = load_row_state_snapshot(
                root / "state_snapshots",
                expected_chain_id="drill-v0",
                expected_engine_hash=PINNED_FUSE_ENGINE_HASH,
                expected_spec_version=PINNED_FUSE_SPEC_VERSION,
                expected_sequence=8,
                expected_record_sha256=s8_env["record_sha256"],
                expected_state_sha256=s8_nrs_sha,
                allow_missing_at_genesis=False,
            )
            self.assertEqual(s8_state["V4"]["TQQQ"]["clearing_counter"], 8)

            # Negative control: premature V4 clearing at counter=8 on the 9th day (1 day gap) MUST fail
            premature_state = json.loads(json.dumps(s8_state))
            premature_state["V4"] = {}
            save_pending_rejected_state_snapshot(
                root / "state_snapshots",
                chain_id="drill-v0",
                base_sequence=8,
                base_record_sha256=s8_env["record_sha256"],
                rejected_session_date=trading_days[9],
                engine_hash=PINNED_FUSE_ENGINE_HASH,
                spec_version=PINNED_FUSE_SPEC_VERSION,
                next_row_state=premature_state,
                base_prior_row_state=s8_state,
            )
            with self.assertRaises(EngineIdentityError):
                load_pending_rejected_state_snapshot(
                    root / "state_snapshots",
                    expected_chain_id="drill-v0",
                    expected_base_sequence=8,
                    expected_base_record_sha256=s8_env["record_sha256"],
                    current_session_date=trading_days[9],
                    expected_engine_hash=PINNED_FUSE_ENGINE_HASH,
                    expected_spec_version=PINNED_FUSE_SPEC_VERSION,
                    expected_base_prior_state_sha256=s8_nrs_sha,
                    committed_base_prior_row_state=s8_state,
                    expected_base_session_date=trading_days[8],
                )
            (root / "state_snapshots" / "pending_rejected_state.json").unlink()

            # Day 9 commits clearing_counter=9
            d9 = trading_days[9]
            r9 = run_daily_drill_cycle(
                session_date=d9,
                data_dir=root,
                config=drill_cfg,
                fixtures_dir=fix_root / d9,
                sigma_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                wall_clock=lambda d=d9: datetime.fromisoformat(f"{d}T22:30:00+00:00"),
            )
            self.assertTrue(r9["appended"])

            # Day 10 (10th calm session): O1 rejects (missing Treasury) while V4 reaches 10 & clears entry
            d10 = trading_days[10]
            bad_d10 = Path(tmpdir) / "bad_d10"
            _build_session_fixture_files(bad_d10, d10)
            (bad_d10 / f"treasury_tbill_{d10}.csv").unlink()
            r10_rej = run_daily_drill_cycle(
                session_date=d10,
                data_dir=root,
                config=drill_cfg,
                fixtures_dir=bad_d10,
                sigma_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                wall_clock=lambda d=d10: datetime.fromisoformat(f"{d}T22:20:00+00:00"),
            )
            self.assertFalse(r10_rej["appended"])
            self.assertTrue((root / "state_snapshots" / "pending_rejected_state.json").is_file())

            # Restoring Treasury & retrying Day 10 loads cleared V4 state cleanly, commits NORMAL, replays bit-for-bit
            r10_ok = run_daily_drill_cycle(
                session_date=d10,
                data_dir=root,
                config=drill_cfg,
                fixtures_dir=fix_root / d10,
                sigma_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                wall_clock=lambda d=d10: datetime.fromisoformat(f"{d}T22:35:00+00:00"),
            )
            self.assertTrue(r10_ok["appended"])
            self.assertEqual(r10_ok["sequence"], 10)
            self.assertFalse((root / "state_snapshots" / "pending_rejected_state.json").exists())
            recs = ledger.load_records()
            b_v4_10 = {b["id"]: b for b in recs[-1]["record_body"]["evaluator"]["breakers"]}["V4"]
            self.assertEqual(b_v4_10["state"], "NORMAL")
            self.assertFalse(b_v4_10["would_have_tripped"])
            rep = replay_window(root / "replay", root / "ledger")
            self.assertTrue(rep["bit_for_bit_match"])

    def test_retry_committed_session_retires_superseded_pending_rejected_state(self) -> None:
        drill_cfg = _sh2_drill_cfg()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "retire_pending"
            f5_bad = Path(tmpdir) / "fix_05_bad"
            f6 = Path(tmpdir) / "fix_06"
            f7 = Path(tmpdir) / "fix_07"
            _build_session_fixture_files(f5_bad, "2026-01-05")
            (f5_bad / "treasury_tbill_2026-01-05.csv").unlink()
            _build_session_fixture_files(f6, "2026-01-06")
            _build_session_fixture_files(f7, "2026-01-07")

            # Jan 5 rejects -> saves pending_rejected_state.json (base_sequence=-1, rejected_session_date=2026-01-05)
            r5 = run_daily_drill_cycle(
                session_date="2026-01-05",
                data_dir=root,
                config=drill_cfg,
                fixtures_dir=f5_bad,
                sigma_by_inst={"TQQQ": "0.50", "SOXL": "0.24"},
                long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                wall_clock=lambda: datetime(2026, 1, 5, 22, 20, tzinfo=timezone.utc),
            )
            self.assertFalse(r5["appended"])
            pend_path = root / "state_snapshots" / "pending_rejected_state.json"
            stale_pending_bytes = pend_path.read_bytes()

            # Jan 6 commits seq=0 cleanly, then simulate crash right before post-commit unlink of pending file
            r6 = run_daily_drill_cycle(
                session_date="2026-01-06",
                data_dir=root,
                config=drill_cfg,
                fixtures_dir=f6,
                wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
            )
            self.assertTrue(r6["appended"])
            self.assertEqual(r6["sequence"], 0)
            self.assertTrue((root / "state_snapshots" / "seq_000000.state.json").is_file())
            self.assertTrue((root / "state_snapshots" / "latest.state.json").is_file())
            pend_path.write_bytes(stale_pending_bytes)

            # Retrying Jan 6 when state_snapshots are already valid MUST still retire stale pending file
            r6_retry = run_daily_drill_cycle(
                session_date="2026-01-06",
                data_dir=root,
                config=drill_cfg,
                fixtures_dir=f6,
                wall_clock=lambda: datetime(2026, 1, 6, 22, 35, tzinfo=timezone.utc),
            )
            self.assertFalse(r6_retry["appended"])
            self.assertEqual(r6_retry["reason"], "already_committed_session_preserved")
            self.assertFalse(pend_path.exists())

            # Non-superseded pending file (base_sequence >= committed_seq) is preserved untouched
            future_pending = json.loads(stale_pending_bytes.decode("utf-8"))
            future_pending["base_sequence"] = 0
            future_pending["rejected_session_date"] = "2026-01-07"
            pend_path.write_text(json.dumps(future_pending), encoding="utf-8")
            self.assertFalse(
                _retire_superseded_pending_rejected_state(
                    root / "state_snapshots",
                    committed_seq=0,
                    committed_session_date="2026-01-06",
                    expected_chain_id="drill-v0",
                )
            )
            self.assertTrue(pend_path.is_file())
            pend_path.unlink()

            # Next session (Jan 7) now runs and commits seq=1 cleanly
            r7 = run_daily_drill_cycle(
                session_date="2026-01-07",
                data_dir=root,
                config=drill_cfg,
                fixtures_dir=f7,
                wall_clock=lambda: datetime(2026, 1, 7, 22, 30, tzinfo=timezone.utc),
            )
            self.assertTrue(r7["appended"])
            self.assertEqual(r7["sequence"], 1)

    def test_extract_prior_close_from_tiingo_rejects_extended_session_markers(self) -> None:
        spec_v2 = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal_jan5 = resolve_session("XNYS", "2026-01-05", spec=spec_v2)

        for extra_fields in (
            {"session": "extended_overnight_open"},
            {"market_session": "overnight"},
            {"session_window": "after_hours"},
            {"session_time": "pre_market"},
            {"session_time": "18:30"},
            {"session_time": "2026-01-05T21:15:00-05:00"},
            {"publicationTimestamp": "2026-01-06T12:00:00Z"},
        ):
            payload = json.dumps(
                [
                    {
                        "date": "2026-01-05T00:00:00.000Z",
                        "close": 49.0,
                        "divCash": 0.0,
                        "splitFactor": 1.0,
                        **extra_fields,
                    }
                ]
            ).encode("utf-8")
            self.assertIsNone(
                extract_prior_close_from_tiingo_bytes(
                    payload,
                    "2026-01-06",
                    calendar_session=cal_jan5,
                    target_prior_date="2026-01-05",
                ),
                f"Expected None for contaminated prior row {extra_fields}",
            )

        clean_payload = json.dumps(
            [
                {
                    "date": "2026-01-05T00:00:00.000Z",
                    "close": 49.0,
                    "divCash": 0.0,
                    "splitFactor": 1.0,
                    "session": "regular",
                    "session_time": "regular",
                }
            ]
        ).encode("utf-8")
        self.assertEqual(
            extract_prior_close_from_tiingo_bytes(
                clean_payload,
                "2026-01-06",
                calendar_session=cal_jan5,
                target_prior_date="2026-01-05",
            ),
            "49",
        )

        # End-to-end genesis cycle on Jan 6 with extended-session Jan 5 Tiingo print MUST fail O1 closed
        drill_cfg = _sh2_drill_cfg()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "ext_prior"
            f6_ext = Path(tmpdir) / "fix_06_ext"
            _build_session_fixture_files(f6_ext, "2026-01-06")
            tqqq_path = f6_ext / "tiingo_tqqq_2026-01-06.json"
            rows = json.loads(tqqq_path.read_text(encoding="utf-8"))
            rows[0]["session"] = "extended_overnight_open"
            tqqq_path.write_text(json.dumps(rows), encoding="utf-8")
            res = run_daily_drill_cycle(
                session_date="2026-01-06",
                data_dir=root,
                config=drill_cfg,
                fixtures_dir=f6_ext,
                wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
            )
            self.assertFalse(res["appended"])
            self.assertTrue(res["fail_closed"])
            self.assertEqual(len(ShadowLedger(root / "ledger").load_records()), 0)

    def test_production_history_authentication_prevents_forged_prior_chain(self) -> None:
        raw_cfg = json.loads(
            (_REPO_ROOT / "config" / "production-scheduler.json").read_text(encoding="utf-8")
        )
        raw_cfg["production_chain_locked"] = False
        raw_cfg["clk01_active"] = True

        with tempfile.TemporaryDirectory() as tmpdir:
            cfg_path = Path(tmpdir) / "unlocked.json"
            cfg_path.write_text(json.dumps(raw_cfg), encoding="utf-8")
            root = Path(tmpdir) / "prod_auth"
            hdrs: dict[int, dict[str, str]] = {}
            heights_by_date = {"2026-01-05": 880100, "2026-01-06": 880101, "2026-01-07": 880102}

            def _make_ots(block_ts: str, session_date: str):
                class _Ots(OpenTimestampsClient):
                    def stamp_digest(self, sha256_hex: str) -> tuple[bytes, str]:
                        bh = heights_by_date[session_date]
                        ots_b, root_hex = build_synthetic_ots_fixture(sha256_hex, bitcoin_height=bh)
                        assert root_hex is not None
                        hdrs[bh] = {"merkle_root": root_hex, "timestamp": block_ts}
                        return ots_b, "https://a.pool.opentimestamps.org"

                return _Ots()

            # 0. Empty production bucket without GENESIS_PREV_HASH MUST fail closed (with or without BTC roots)
            for btc_arg in (
                None,
                {880100: {"merkle_root": "0" * 64, "timestamp": "2026-01-06T03:00:00.000Z"}},
            ):
                with self.assertRaises(EngineIdentityError) as gen_ctx:
                    run_production_cycle(
                        session_date="2026-01-05",
                        data_dir=root,
                        config_path=cfg_path,
                        transport=build_live_mock_transport("2026-01-05"),
                        environ={"TIINGO_API_KEY": "test-dummy-token"},
                        ots_client=_make_ots("2026-01-06T03:00:00.000Z", "2026-01-05"),
                        wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                        clock=_fast_clock(),
                        sleeper=lambda _s: None,
                        bitcoin_merkle_roots=btc_arg,
                        trusted_head_anchor_path=Path(tmpdir) / "auth_anchor.json",
                    )
                self.assertIn("production_genesis_unauthenticated", str(gen_ctx.exception))
                self.assertEqual(ShadowLedger(root / "ledger").load_records(), [])
                self.assertFalse((root / "replay").exists())

            # 1. Explicit GENESIS_PREV_HASH initializes seq=0 cleanly on empty ledger
            r0 = run_production_cycle(
                session_date="2026-01-05",
                data_dir=root,
                config_path=cfg_path,
                transport=build_live_mock_transport("2026-01-05"),
                environ={"TIINGO_API_KEY": "test-dummy-token"},
                ots_client=_make_ots("2026-01-06T03:00:00.000Z", "2026-01-05"),
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clock(),
                sleeper=lambda _s: None,
                expected_prior_head_sha256=GENESIS_PREV_HASH,
                expected_pending_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                trusted_head_anchor_path=Path(tmpdir) / "auth_anchor.json",
            )
            self.assertTrue(r0["appended"])
            seq0_sha = r0["record_sha256"]

            # 2. Unauthenticated non-empty production ledger fails closed (0 appends),
            # including when bitcoin_merkle_roots is passed WITHOUT expected_prior_head_sha256
            for unauth_kw in (
                {},
                {"bitcoin_merkle_roots": dict(hdrs)},
            ):
                with self.assertRaises(EngineIdentityError) as unauth_ctx:
                    run_production_cycle(
                        session_date="2026-01-06",
                        data_dir=root,
                        config_path=cfg_path,
                        transport=build_live_mock_transport("2026-01-06"),
                        environ={"TIINGO_API_KEY": "test-dummy-token"},
                        ots_client=_make_ots("2026-01-07T03:00:00.000Z", "2026-01-06"),
                        wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
                        clock=_fast_clock(),
                        sleeper=lambda _s: None,
                        trusted_head_anchor_path=Path(tmpdir) / "auth_anchor.json",
                        **unauth_kw,
                    )
                self.assertIn("production_history_unauthenticated", str(unauth_ctx.exception))
            self.assertEqual(len(ShadowLedger(root / "ledger").load_records()), 1)

            # 3. Mismatched trusted head fails closed (0 appends)
            with self.assertRaises(EngineIdentityError):
                run_production_cycle(
                    session_date="2026-01-06",
                    data_dir=root,
                    config_path=cfg_path,
                    transport=build_live_mock_transport("2026-01-06"),
                    environ={"TIINGO_API_KEY": "test-dummy-token"},
                    ots_client=_make_ots("2026-01-07T03:00:00.000Z", "2026-01-06"),
                    wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
                    clock=_fast_clock(),
                    sleeper=lambda _s: None,
                    expected_prior_head_sha256="f" * 64,
                    expected_pending_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                    trusted_head_anchor_path=Path(tmpdir) / "auth_anchor.json",
                )
            with self.assertRaises(EngineIdentityError):
                run_production_cycle(
                    session_date="2026-01-06",
                    data_dir=root,
                    config_path=cfg_path,
                    transport=build_live_mock_transport("2026-01-06"),
                    environ={"TIINGO_API_KEY": "test-dummy-token"},
                    ots_client=_make_ots("2026-01-07T03:00:00.000Z", "2026-01-06"),
                    wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
                    clock=_fast_clock(),
                    sleeper=lambda _s: None,
                    expected_prior_head_sha256=seq0_sha,
                    expected_pending_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                    bitcoin_merkle_roots={880100: {"merkle_root": "a" * 64, "timestamp": "2026-01-06T03:00:00.000Z"}},
                    trusted_head_anchor_path=Path(tmpdir) / "auth_anchor.json",
                )

            # 4. Authenticated with authorized head seq0_sha (+ corroborating BTC roots) -> seq=1 commits
            r1 = run_production_cycle(
                session_date="2026-01-06",
                data_dir=root,
                config_path=cfg_path,
                transport=build_live_mock_transport("2026-01-06"),
                environ={"TIINGO_API_KEY": "test-dummy-token"},
                ots_client=_make_ots("2026-01-07T03:00:00.000Z", "2026-01-06"),
                wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clock(),
                sleeper=lambda _s: None,
                expected_prior_head_sha256=seq0_sha,
                expected_pending_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                bitcoin_merkle_roots=dict(hdrs),
                trusted_head_anchor_path=Path(tmpdir) / "auth_anchor.json",
            )
            self.assertTrue(r1["appended"])
            self.assertEqual(r1["sequence"], 1)
            seq1_sha = r1["record_sha256"]

            # 5. Same-session retry requires exact committed record_sha256 (seq1_sha);
            # seq0_sha fails closed even when bitcoin_merkle_roots is provided
            for extra_btc in (None, dict(hdrs)):
                with self.assertRaises(EngineIdentityError) as stale_retry_ctx:
                    run_production_cycle(
                        session_date="2026-01-06",
                        data_dir=root,
                        config_path=cfg_path,
                        transport=build_live_mock_transport("2026-01-06"),
                        environ={"TIINGO_API_KEY": "test-dummy-token"},
                        wall_clock=lambda: datetime(2026, 1, 6, 22, 35, tzinfo=timezone.utc),
                        expected_prior_head_sha256=seq0_sha,
                        expected_pending_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                        bitcoin_merkle_roots=extra_btc,
                        trusted_head_anchor_path=Path(tmpdir) / "auth_anchor.json",
                    )
                self.assertIn("production_trusted_head_mismatch", str(stale_retry_ctx.exception))

            # Reproduce forged same-session record/receipt + deleted report using unchanged seq0_sha
            forged_root = Path(tmpdir) / "forged_retry_root"
            shutil.copytree(root, forged_root)
            forged_anchor = Path(tmpdir) / "forged_retry_anchor.json"
            shutil.copy2(Path(tmpdir) / "auth_anchor.json", forged_anchor)
            forged_ledger = ShadowLedger(forged_root / "ledger")
            forged_recs = forged_ledger.load_records()
            forged_body = json.loads(json.dumps(forged_recs[1]["record_body"]))
            forged_o8 = next(b for b in forged_body["evaluator"]["breakers"] if b["id"] == "O8")
            forged_o8["state"] = "NO-NEW-RISK"
            forged_o8["latched"] = True
            forged_o8["would_have_tripped"] = True
            forged_o8["reason"] = "forged_o8"
            for p in (
                forged_ledger._record_path(1),
                forged_ledger._receipt_path(1),
                forged_ledger._ots_path(1),
                forged_root / "reports" / "completeness_2026-01-06.json",
                forged_root / "reports" / "completeness_2026-01-06.md",
            ):
                p.unlink(missing_ok=True)
            forged_ots, forged_merkle = build_synthetic_ots_fixture(
                sha256_canonical(forged_body),
                bitcoin_height=880199,
            )
            forged_env = forged_ledger.append_record(
                forged_body,
                obtained_at_utc="2026-01-06T22:30:05.000Z",
                ots_bytes=forged_ots,
                commitment_deadline_utc="2026-01-07T02:00:00.000Z",
            )
            save_row_state_snapshot(
                forged_root / "state_snapshots",
                chain_id=forged_body["chain_id"],
                sequence=1,
                session_date="2026-01-06",
                record_sha256=forged_env["record_sha256"],
                engine_hash=PINNED_FUSE_ENGINE_HASH,
                spec_version=PINNED_FUSE_SPEC_VERSION,
                next_row_state={},
            )
            # Even with a valid creditable BTC root for the forged height, omitted or old trusted head fails closed
            alt_hdrs = {**hdrs, 880199: {"merkle_root": forged_merkle, "timestamp": "2026-01-07T03:00:00.000Z"}}
            for head_arg, btc_arg in (
                (seq0_sha, None),
                (seq0_sha, dict(hdrs)),
                (None, alt_hdrs),
            ):
                with self.assertRaises(EngineIdentityError):
                    run_production_cycle(
                        session_date="2026-01-06",
                        data_dir=forged_root,
                        config_path=cfg_path,
                        transport=build_live_mock_transport("2026-01-06"),
                        environ={"TIINGO_API_KEY": "test-dummy-token"},
                        wall_clock=lambda: datetime(2026, 1, 6, 22, 35, tzinfo=timezone.utc),
                        expected_prior_head_sha256=head_arg,
                        bitcoin_merkle_roots=btc_arg,
                        trusted_head_anchor_path=forged_anchor,
                    )
                self.assertFalse((forged_root / "reports" / "completeness_2026-01-06.json").exists())

            # Legitimate retries succeed with exact committed seq1_sha
            for retry_kw in (
                {"expected_prior_head_sha256": seq1_sha, "expected_pending_state_sha256": NO_PENDING_REJECTED_STATE_SHA256},
                {"expected_prior_head_sha256": seq1_sha, "expected_pending_state_sha256": NO_PENDING_REJECTED_STATE_SHA256, "bitcoin_merkle_roots": dict(hdrs)},
            ):
                r1_retry = run_production_cycle(
                    session_date="2026-01-06",
                    data_dir=root,
                    config_path=cfg_path,
                    transport=build_live_mock_transport("2026-01-06"),
                    environ={"TIINGO_API_KEY": "test-dummy-token"},
                    wall_clock=lambda: datetime(2026, 1, 6, 22, 35, tzinfo=timezone.utc),
                    trusted_head_anchor_path=Path(tmpdir) / "auth_anchor.json",
                    **retry_kw,
                )
                self.assertEqual(r1_retry["reason"], "already_committed_session_preserved")

            # 6. Advancing to seq=2 with stale seq0_sha fails closed; matching seq1_sha commits seq=2
            with self.assertRaises(EngineIdentityError):
                run_production_cycle(
                    session_date="2026-01-07",
                    data_dir=root,
                    config_path=cfg_path,
                    transport=build_live_mock_transport("2026-01-07"),
                    environ={"TIINGO_API_KEY": "test-dummy-token"},
                    ots_client=_make_ots("2026-01-08T03:00:00.000Z", "2026-01-07"),
                    wall_clock=lambda: datetime(2026, 1, 7, 22, 30, tzinfo=timezone.utc),
                    clock=_fast_clock(),
                    sleeper=lambda _s: None,
                    expected_prior_head_sha256=seq0_sha,
                    expected_pending_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                    trusted_head_anchor_path=Path(tmpdir) / "auth_anchor.json",
                )
            r2 = run_production_cycle(
                session_date="2026-01-07",
                data_dir=root,
                config_path=cfg_path,
                transport=build_live_mock_transport("2026-01-07"),
                environ={"TIINGO_API_KEY": "test-dummy-token"},
                ots_client=_make_ots("2026-01-08T03:00:00.000Z", "2026-01-07"),
                wall_clock=lambda: datetime(2026, 1, 7, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clock(),
                sleeper=lambda _s: None,
                expected_prior_head_sha256=seq1_sha,
                expected_pending_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                trusted_head_anchor_path=Path(tmpdir) / "auth_anchor.json",
            )
            self.assertTrue(r2["appended"])
            self.assertEqual(r2["sequence"], 2)

            # 7. Codex wiped-bucket regression: wiping data_dir after commit fails closed (0 appends)
            # whether recurring run uses BTC roots alone or a prior committed head digest
            shutil.rmtree(root)
            for wiped_head, expected_msg in (
                (None, "production_genesis_unauthenticated"),
                (seq1_sha, "production_trusted_head_mismatch:empty_ledger"),
            ):
                with self.assertRaises(EngineIdentityError) as wiped_ctx:
                    run_production_cycle(
                        session_date="2026-01-08",
                        data_dir=root,
                        config_path=cfg_path,
                        transport=build_live_mock_transport("2026-01-08"),
                        environ={"TIINGO_API_KEY": "test-dummy-token"},
                        ots_client=_make_ots("2026-01-09T03:00:00.000Z", "2026-01-07"),
                        wall_clock=lambda: datetime(2026, 1, 8, 22, 30, tzinfo=timezone.utc),
                        clock=_fast_clock(),
                        sleeper=lambda _s: None,
                        expected_prior_head_sha256=wiped_head,
                        bitcoin_merkle_roots=dict(hdrs),
                        trusted_head_anchor_path=Path(tmpdir) / "auth_anchor.json",
                    )
                self.assertIn(expected_msg, str(wiped_ctx.exception))
                self.assertEqual(ShadowLedger(root / "ledger").load_records(), [])
                self.assertFalse((root / "replay").exists())

    def test_interrupted_staging_snapshot_recovers_on_retry_and_subsequent_sessions(self) -> None:
        class _SimSigkill(BaseException):
            pass

        drill_cfg = _sh2_drill_cfg()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "crash_snap_recovery"
            r0 = run_daily_drill_cycle(
                session_date="2026-01-05",
                data_dir=root,
                config=drill_cfg,
                transport=build_live_mock_transport("2026-01-05"),
                environ={"TIINGO_API_KEY": "test-dummy-token"},
                ots_client=None,
                allow_unanchored_on_ots_failure=True,
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clock(),
                sleeper=lambda _s: None,
                evaluator=VendoredFuseEvaluator(),
            )
            self.assertTrue(r0["appended"])
            self.assertEqual(r0["sequence"], 0)

            with self.assertRaises(_SimSigkill):
                run_daily_drill_cycle(
                    session_date="2026-01-06",
                    data_dir=root,
                    config=drill_cfg,
                    transport=build_live_mock_transport("2026-01-06"),
                    environ={"TIINGO_API_KEY": "test-dummy-token"},
                    ots_client=None,
                    allow_unanchored_on_ots_failure=True,
                    wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
                    clock=_fast_clock(),
                    sleeper=lambda _s: None,
                    evaluator=VendoredFuseEvaluator(),
                    _post_ledger_commit_hook=lambda: (_ for _ in ()).throw(
                        _SimSigkill("abrupt kill after snapshot staging before marker")
                    ),
                )

            # Confirm staged seq=1 snapshot + latest.state.json survived while ledger stayed at seq=0
            self.assertEqual(len(ShadowLedger(root / "ledger").load_records()), 1)
            self.assertTrue((root / "state_snapshots" / "seq_000001.state.json").is_file())
            stale_latest = json.loads((root / "state_snapshots" / "latest.state.json").read_text(encoding="utf-8"))
            self.assertEqual(stale_latest["sequence"], 1)

            # Retry 2026-01-06 and advance 2026-01-07 both recover cleanly without mismatch
            r1 = run_daily_drill_cycle(
                session_date="2026-01-06",
                data_dir=root,
                config=drill_cfg,
                transport=build_live_mock_transport("2026-01-06"),
                environ={"TIINGO_API_KEY": "test-dummy-token"},
                ots_client=None,
                allow_unanchored_on_ots_failure=True,
                wall_clock=lambda: datetime(2026, 1, 6, 22, 35, tzinfo=timezone.utc),
                clock=_fast_clock(),
                sleeper=lambda _s: None,
                evaluator=VendoredFuseEvaluator(),
            )
            self.assertTrue(r1["appended"])
            self.assertEqual(r1["sequence"], 1)

            r2 = run_daily_drill_cycle(
                session_date="2026-01-07",
                data_dir=root,
                config=drill_cfg,
                transport=build_live_mock_transport("2026-01-07"),
                environ={"TIINGO_API_KEY": "test-dummy-token"},
                ots_client=None,
                allow_unanchored_on_ots_failure=True,
                wall_clock=lambda: datetime(2026, 1, 7, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clock(),
                sleeper=lambda _s: None,
                evaluator=VendoredFuseEvaluator(),
            )
            self.assertTrue(r2["appended"])
            self.assertEqual(r2["sequence"], 2)

    def test_live_tiingo_lookback_fetches_prior_trading_close_for_fuse_genesis(self) -> None:
        raw_cfg = json.loads(
            (_REPO_ROOT / "config" / "production-scheduler.json").read_text(encoding="utf-8")
        )
        raw_cfg["production_chain_locked"] = False
        raw_cfg["clk01_active"] = True
        seen_tiingo_urls: list[str] = []
        base_transport = build_live_mock_transport("2026-01-05")

        def _window_recording_transport(url: str, headers: dict):
            if "api.tiingo.com" in url.lower():
                seen_tiingo_urls.append(url)
            return base_transport(url, headers)

        class _FakeOts(OpenTimestampsClient):
            def stamp_digest(self, sha256_hex: str) -> tuple[bytes, str]:
                ots_b, _ = build_synthetic_ots_fixture(sha256_hex, bitcoin_height=880100)
                return ots_b, "https://a.pool.opentimestamps.org"

        with tempfile.TemporaryDirectory() as tmpdir:
            cfg_path = Path(tmpdir) / "unlocked.json"
            cfg_path.write_text(json.dumps(raw_cfg), encoding="utf-8")
            root = Path(tmpdir) / "live_genesis"
            r0 = run_production_cycle(
                session_date="2026-01-05",
                data_dir=root,
                config_path=cfg_path,
                transport=_window_recording_transport,
                environ={"TIINGO_API_KEY": "test-dummy-token"},
                ots_client=_FakeOts(),
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clock(),
                sleeper=lambda _s: None,
                expected_prior_head_sha256=GENESIS_PREV_HASH,
                expected_pending_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                trusted_head_anchor_path=Path(tmpdir) / "lookback_anchor.json",
            )
            for u in seen_tiingo_urls:
                self.assertIn("startDate=2026-01-02", u)
                self.assertIn("endDate=2026-01-05", u)

    def test_production_pending_rejected_state_requires_external_authentication(self) -> None:
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
            root = Path(tmpdir) / "prod_pending_auth"

            r0 = run_production_cycle(
                session_date="2026-01-05",
                data_dir=root,
                config_path=cfg_path,
                transport=build_live_mock_transport("2026-01-05"),
                environ={"TIINGO_API_KEY": "test-dummy-token"},
                ots_client=_FakeOts(),
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clock(),
                sleeper=lambda _s: None,
                expected_prior_head_sha256=GENESIS_PREV_HASH,
                expected_pending_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                trusted_head_anchor_path=Path(tmpdir) / "pend_auth_anchor.json",
            )
            self.assertTrue(r0["appended"])
            seq0_sha = str(r0["record_sha256"])

            # 0. Omitting expected_pending_state_sha256 fails closed even when no pending file exists on disk
            with self.assertRaises(EngineIdentityError) as ctx_clean_omit:
                run_production_cycle(
                    session_date="2026-01-06",
                    data_dir=root,
                    config_path=cfg_path,
                    transport=build_live_mock_transport("2026-01-06"),
                    environ={"TIINGO_API_KEY": "test-dummy-token"},
                    ots_client=_FakeOts(),
                    wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
                    clock=_fast_clock(),
                    sleeper=lambda _s: None,
                    expected_prior_head_sha256=seq0_sha,
                    trusted_head_anchor_path=Path(tmpdir) / "pend_auth_anchor.json",
                )
            self.assertIn("production_pending_rejected_state_unauthenticated", str(ctx_clean_omit.exception))

            base_rej_transport = build_live_mock_transport("2026-01-06")

            def _missing_tbill_transport(url: str, headers: dict):
                if "treasury.gov" in url.lower():
                    return 404, b"missing"
                return base_rej_transport(url, headers)

            r_rej = run_production_cycle(
                session_date="2026-01-06",
                data_dir=root,
                config_path=cfg_path,
                transport=_missing_tbill_transport,
                environ={"TIINGO_API_KEY": "test-dummy-token"},
                ots_client=_FakeOts(),
                wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clock(),
                sleeper=lambda _s: None,
                sigma_by_inst={"TQQQ": "0.50", "SOXL": "0.24"},
                long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                expected_prior_head_sha256=seq0_sha,
                expected_pending_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                trusted_head_anchor_path=Path(tmpdir) / "pend_auth_anchor.json",
            )
            self.assertFalse(r_rej["appended"])
            self.assertTrue(r_rej["fail_closed"])
            pending_sha = str(r_rej["pending_rejected_state_sha256"])
            pend_path = root / "state_snapshots" / "pending_rejected_state.json"
            self.assertTrue(pend_path.is_file())
            orig_pend_bytes = pend_path.read_bytes()

            # 1. Omitting expected_pending_state_sha256 fails closed when pending_rejected_state.json exists
            with self.assertRaises(EngineIdentityError) as ctx_unauth:
                run_production_cycle(
                    session_date="2026-01-07",
                    data_dir=root,
                    config_path=cfg_path,
                    transport=build_live_mock_transport("2026-01-07"),
                    environ={"TIINGO_API_KEY": "test-dummy-token"},
                    ots_client=_FakeOts(),
                    wall_clock=lambda: datetime(2026, 1, 7, 22, 30, tzinfo=timezone.utc),
                    clock=_fast_clock(),
                    sleeper=lambda _s: None,
                    expected_prior_head_sha256=seq0_sha,
                    trusted_head_anchor_path=Path(tmpdir) / "pend_auth_anchor.json",
                )
            self.assertIn("production_pending_rejected_state_unauthenticated", str(ctx_unauth.exception))

            # 1b. Passing NO_PENDING_REJECTED_STATE_SHA256 when pending_rejected_state.json exists fails closed
            with self.assertRaises(EngineIdentityError) as ctx_unexp:
                run_production_cycle(
                    session_date="2026-01-07",
                    data_dir=root,
                    config_path=cfg_path,
                    transport=build_live_mock_transport("2026-01-07"),
                    environ={"TIINGO_API_KEY": "test-dummy-token"},
                    ots_client=_FakeOts(),
                    wall_clock=lambda: datetime(2026, 1, 7, 22, 30, tzinfo=timezone.utc),
                    clock=_fast_clock(),
                    sleeper=lambda _s: None,
                    expected_prior_head_sha256=seq0_sha,
                    expected_pending_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                    trusted_head_anchor_path=Path(tmpdir) / "pend_auth_anchor.json",
                )
            self.assertIn("production_pending_rejected_state_unexpected", str(ctx_unexp.exception))

            # 2. Tampering pending_rejected_state.json (with recomputed colocated state_sha256) fails closed
            tampered_obj = json.loads(orig_pend_bytes.decode("utf-8"))
            tampered_obj["next_row_state"] = {}
            tampered_obj["state_sha256"] = _sha256_canonical_state_dict({})
            pend_path.write_text(json.dumps(tampered_obj), encoding="utf-8")
            with self.assertRaises(EngineIdentityError) as ctx_tamper:
                run_production_cycle(
                    session_date="2026-01-07",
                    data_dir=root,
                    config_path=cfg_path,
                    transport=build_live_mock_transport("2026-01-07"),
                    environ={"TIINGO_API_KEY": "test-dummy-token"},
                    ots_client=_FakeOts(),
                    wall_clock=lambda: datetime(2026, 1, 7, 22, 30, tzinfo=timezone.utc),
                    clock=_fast_clock(),
                    sleeper=lambda _s: None,
                    expected_prior_head_sha256=seq0_sha,
                    expected_pending_state_sha256=pending_sha,
                    trusted_head_anchor_path=Path(tmpdir) / "pend_auth_anchor.json",
                )
            self.assertIn("production_pending_rejected_state_mismatch", str(ctx_tamper.exception))

            # 3. Deleting pending_rejected_state.json fails closed BOTH with pinned pending_sha AND stale NO_PENDING_REJECTED_STATE_SHA256
            pend_path.unlink()
            for del_arg, del_err in (
                (pending_sha, "production_pending_rejected_state_missing"),
                (
                    NO_PENDING_REJECTED_STATE_SHA256,
                    "production_trusted_head_rollback_rejected: pending state diverged from trusted anchor",
                ),
            ):
                with self.assertRaises(EngineIdentityError) as ctx_missing:
                    run_production_cycle(
                        session_date="2026-01-07",
                        data_dir=root,
                        config_path=cfg_path,
                        transport=build_live_mock_transport("2026-01-07"),
                        environ={"TIINGO_API_KEY": "test-dummy-token"},
                        ots_client=_FakeOts(),
                        wall_clock=lambda: datetime(2026, 1, 7, 22, 30, tzinfo=timezone.utc),
                        clock=_fast_clock(),
                        sleeper=lambda _s: None,
                        expected_prior_head_sha256=seq0_sha,
                        expected_pending_state_sha256=del_arg,
                        trusted_head_anchor_path=Path(tmpdir) / "pend_auth_anchor.json",
                    )
                self.assertIn(del_err, str(ctx_missing.exception))

            # 4. Restoring intact pending_rejected_state.json + matching pending_sha preserves V4 latch & commits
            pend_path.write_bytes(orig_pend_bytes)
            r_ok = run_production_cycle(
                session_date="2026-01-07",
                data_dir=root,
                config_path=cfg_path,
                transport=build_live_mock_transport("2026-01-07"),
                environ={"TIINGO_API_KEY": "test-dummy-token"},
                ots_client=_FakeOts(),
                wall_clock=lambda: datetime(2026, 1, 7, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clock(),
                sleeper=lambda _s: None,
                long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                expected_prior_head_sha256=seq0_sha,
                expected_pending_state_sha256=pending_sha,
                trusted_head_anchor_path=Path(tmpdir) / "pend_auth_anchor.json",
            )
            self.assertTrue(r_ok["appended"])
            self.assertEqual(r_ok["sequence"], 1)
            self.assertFalse(pend_path.exists())
            recs = ShadowLedger(root / "ledger").load_records()
            v4_row = next(b for b in recs[1]["record_body"]["evaluator"]["breakers"] if b["id"] == "V4")
            self.assertEqual(v4_row["state"], "NO-NEW-RISK")
            self.assertEqual(v4_row["reason"], "volatility_below_2x_but_not_yet_cleared")
            self.assertEqual(v4_row["outputs"]["TQQQ"]["clearing_counter"], 1)

            # 5. Once Jan 7 commits, Jan 8 succeeds with NO_PENDING_REJECTED_STATE_SHA256 (superseded Jan 6 rejected report ignored)
            r_jan8 = run_production_cycle(
                session_date="2026-01-08",
                data_dir=root,
                config_path=cfg_path,
                transport=build_live_mock_transport("2026-01-08"),
                environ={"TIINGO_API_KEY": "test-dummy-token"},
                ots_client=_FakeOts(),
                wall_clock=lambda: datetime(2026, 1, 8, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clock(),
                sleeper=lambda _s: None,
                expected_prior_head_sha256=str(r_ok["record_sha256"]),
                expected_pending_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                trusted_head_anchor_path=Path(tmpdir) / "pend_auth_anchor.json",
            )
            self.assertTrue(r_jan8["appended"])
            self.assertEqual(r_jan8["sequence"], 2)

    def test_post_evaluation_ots_failure_and_deadline_preserve_evaluated_breaker_state(self) -> None:
        raw_cfg = json.loads(
            (_REPO_ROOT / "config" / "production-scheduler.json").read_text(encoding="utf-8")
        )
        raw_cfg["production_chain_locked"] = False
        raw_cfg["clk01_active"] = True

        class _OkOts(OpenTimestampsClient):
            def stamp_digest(self, sha256_hex: str) -> tuple[bytes, str]:
                ots_b, _ = build_synthetic_ots_fixture(sha256_hex, bitcoin_height=880100)
                return ots_b, "https://a.pool.opentimestamps.org"

        class _FailingOts(OpenTimestampsClient):
            def stamp_digest(self, sha256_hex: str) -> tuple[bytes, str]:
                raise RuntimeError("ots calendar unavailable")

        with tempfile.TemporaryDirectory() as tmpdir:
            cfg_path = Path(tmpdir) / "unlocked.json"
            cfg_path.write_text(json.dumps(raw_cfg), encoding="utf-8")

            # Case A: Jan 5 V4 trip followed by OTS outage (allow_unanchored_on_ots_failure=False)
            root_ots = Path(tmpdir) / "prod_ots_fail"
            with self.assertRaises(RuntimeError) as ctx_ots:
                run_production_cycle(
                    session_date="2026-01-05",
                    data_dir=root_ots,
                    config_path=cfg_path,
                    transport=build_live_mock_transport("2026-01-05"),
                    environ={"TIINGO_API_KEY": "test-dummy-token"},
                    ots_client=_FailingOts(),
                    wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
                    clock=_fast_clock(),
                    sleeper=lambda _s: None,
                    sigma_by_inst={"TQQQ": "0.50", "SOXL": "0.24"},
                    long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                    expected_prior_head_sha256=GENESIS_PREV_HASH,
                    expected_pending_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                    trusted_head_anchor_path=Path(tmpdir) / "ots_anchor.json",
                )
            self.assertIn("OTS receipt acquisition failed:", str(ctx_ots.exception))
            self.assertIn("pending_rejected_state_sha256=", str(ctx_ots.exception))
            self.assertEqual(len(ShadowLedger(root_ots / "ledger").load_records()), 0)
            pend_ots = root_ots / "state_snapshots" / "pending_rejected_state.json"
            self.assertTrue(pend_ots.is_file())
            pend_ots_sha = compute_pending_rejected_state_sha256(
                json.loads(pend_ots.read_text(encoding="utf-8"))
            )
            self.assertIn(pend_ots_sha, str(ctx_ots.exception))

            # Jan 6 with calm vol must retain V4 10-session clearing interlock (not NORMAL)
            r_next_ots = run_production_cycle(
                session_date="2026-01-06",
                data_dir=root_ots,
                config_path=cfg_path,
                transport=build_live_mock_transport("2026-01-06"),
                environ={"TIINGO_API_KEY": "test-dummy-token"},
                ots_client=_OkOts(),
                wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clock(),
                sleeper=lambda _s: None,
                sigma_by_inst={"TQQQ": "0.22", "SOXL": "0.22"},
                long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                expected_prior_head_sha256=GENESIS_PREV_HASH,
                expected_pending_state_sha256=pend_ots_sha,
                trusted_head_anchor_path=Path(tmpdir) / "ots_anchor.json",
            )
            self.assertTrue(r_next_ots["appended"])
            self.assertEqual(r_next_ots["sequence"], 0)
            self.assertFalse(pend_ots.exists())
            recs_ots = ShadowLedger(root_ots / "ledger").load_records()
            v4_ots = next(b for b in recs_ots[0]["record_body"]["evaluator"]["breakers"] if b["id"] == "V4")
            self.assertEqual(v4_ots["state"], "NO-NEW-RISK")
            self.assertEqual(v4_ots["reason"], "volatility_below_2x_but_not_yet_cleared")
            self.assertEqual(v4_ots["outputs"]["TQQQ"]["clearing_counter"], 1)

            # Case B: Jan 5 V4 trip followed by post-stamp deadline overrun preserves V4 state
            root_dl = Path(tmpdir) / "prod_deadline_fail"
            stamped = [False]

            class _SlowOts(OpenTimestampsClient):
                def stamp_digest(self, sha256_hex: str) -> tuple[bytes, str]:
                    stamped[0] = True
                    ots_b, _ = build_synthetic_ots_fixture(sha256_hex, bitcoin_height=880101)
                    return ots_b, "https://a.pool.opentimestamps.org"

            def _deadline_clock() -> datetime:
                if not stamped[0]:
                    return datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc)
                return datetime(2026, 1, 6, 5, 0, tzinfo=timezone.utc)

            r_dl = run_production_cycle(
                session_date="2026-01-05",
                data_dir=root_dl,
                config_path=cfg_path,
                transport=build_live_mock_transport("2026-01-05"),
                environ={"TIINGO_API_KEY": "test-dummy-token"},
                ots_client=_SlowOts(),
                wall_clock=_deadline_clock,
                clock=_fast_clock(),
                sleeper=lambda _s: None,
                sigma_by_inst={"TQQQ": "0.50", "SOXL": "0.24"},
                long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                expected_prior_head_sha256=GENESIS_PREV_HASH,
                expected_pending_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                trusted_head_anchor_path=Path(tmpdir) / "dl_anchor.json",
            )
            self.assertFalse(r_dl["appended"])
            self.assertEqual(r_dl["reason"], "late_after_commitment_deadline_backfill_rejected")
            self.assertEqual(len(ShadowLedger(root_dl / "ledger").load_records()), 0)
            pend_dl_sha = str(r_dl["pending_rejected_state_sha256"])

            r_next_dl = run_production_cycle(
                session_date="2026-01-06",
                data_dir=root_dl,
                config_path=cfg_path,
                transport=build_live_mock_transport("2026-01-06"),
                environ={"TIINGO_API_KEY": "test-dummy-token"},
                ots_client=_OkOts(),
                wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
                clock=_fast_clock(),
                sleeper=lambda _s: None,
                sigma_by_inst={"TQQQ": "0.22", "SOXL": "0.22"},
                long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                expected_prior_head_sha256=GENESIS_PREV_HASH,
                expected_pending_state_sha256=pend_dl_sha,
                trusted_head_anchor_path=Path(tmpdir) / "dl_anchor.json",
            )
            self.assertTrue(r_next_dl["appended"])
            recs_dl = ShadowLedger(root_dl / "ledger").load_records()
            v4_dl = next(b for b in recs_dl[0]["record_body"]["evaluator"]["breakers"] if b["id"] == "V4")
            self.assertEqual(v4_dl["state"], "NO-NEW-RISK")
            self.assertEqual(v4_dl["reason"], "volatility_below_2x_but_not_yet_cleared")
            self.assertEqual(v4_dl["outputs"]["TQQQ"]["clearing_counter"], 1)

    def test_production_treasury_endpoint_uses_par_yield_dataset(self) -> None:
        raw_cfg = json.loads(
            (_REPO_ROOT / "config" / "production-scheduler.json").read_text(encoding="utf-8")
        )
        endpoint = str(raw_cfg.get("treasury_endpoint", ""))
        self.assertIn("type=daily_treasury_yield_curve", endpoint)
        self.assertNotIn("daily_treasury_bill_rates", endpoint)

    def test_v2_pending_adjustment_auto_clears_while_latched_mismatch_protected(self) -> None:
        from shadow.fuse_adapter import build_fuse_validated_inputs
        from shadow.ledger import sha256_canonical

        drill_cfg = _sh2_drill_cfg()
        spec_v2 = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        ev = VendoredFuseEvaluator()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "v2_pend"
            f5 = Path(tmpdir) / "fix_05"
            _build_session_fixture_files(f5, "2026-01-05", tqqq_split=2.0)

            r0 = run_daily_drill_cycle(
                session_date="2026-01-05",
                data_dir=root,
                config=drill_cfg,
                fixtures_dir=f5,
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
            )
            self.assertTrue(r0["appended"])
            recs0 = ShadowLedger(root / "ledger").load_records()
            s0_rec_sha = sha256_canonical(recs0[0]["record_body"])
            s0_state, _ = load_row_state_snapshot(
                root / "state_snapshots",
                expected_chain_id=drill_cfg.chain_id,
                expected_engine_hash=PINNED_FUSE_ENGINE_HASH,
                expected_spec_version=PINNED_FUSE_SPEC_VERSION,
                expected_sequence=0,
                expected_record_sha256=s0_rec_sha,
            )
            self.assertIn("TQQQ", s0_state["V2"]["pending_adjustments"])
            s0_sha = _sha256_canonical_state_dict(s0_state)

            cal6 = resolve_session("XNYS", "2026-01-06", spec=spec_v2)
            recon_inputs = {
                "E": "50000",
                "E_star": "50000",
                "H": "0",
                "L": "0",
                "hwm_adjusted": True,
                "funds": {
                    "TQQQ": {
                        "s": "1",
                        "close_price": "25",
                        "fund_return": "0.0",
                        "q_pre": 200,
                        "sigma_forecast": "0.24",
                        "exact_reconciliation": True,
                        "hwm_adjusted": True,
                        "corporate_action": {
                            "type": "split",
                            "ratio": 2.0,
                            "ex_date": "2026-01-05",
                            "adjustment_complete": True,
                            "exact_reconciliation": True,
                            "hwm_adjusted": True,
                        },
                    },
                    "SOXL": {
                        "s": "1",
                        "close_price": "40",
                        "fund_return": "0.0",
                        "q_pre": 100,
                        "sigma_forecast": "0.22",
                    },
                },
                "sources": {
                    "TQQQ": "tiingo",
                    "SOXL": "tiingo",
                    "QQQ": "tiingo",
                    "XNDX": "yahoo",
                    "SEMI": "yahoo",
                    "SOFR": "nyfed",
                    "TBILL_3M": "treasury",
                },
                "sofr_bps": 430,
                "tbill_bps": 420,
                "qqq_close": "500",
                "xndx_return": "0.0",
                "semi_return": "0.0",
                "index_return": "0.0",
            }
            rows6, _, pend_next = ev.evaluate(
                ev.spec_version,
                ev.engine_hash,
                "p",
                "2026-01-06",
                "2026-01-06T22:00:00.000Z",
                "shadow",
                build_fuse_validated_inputs(
                    recon_inputs,
                    cal6,
                    policy_hash="p",
                    session_date="2026-01-06",
                    as_of="2026-01-06T22:00:00.000Z",
                ),
                s0_state,
                None,
            )
            self.assertEqual(rows6["V2"]["state"], "NORMAL")
            self.assertEqual(rows6["V2"]["reason"], "exact_reconciliation_auto_clear")
            self.assertFalse(pend_next.get("V2", {}).get("pending_adjustments"))

            save_pending_rejected_state_snapshot(
                root / "state_snapshots",
                chain_id=drill_cfg.chain_id,
                base_sequence=0,
                base_record_sha256=s0_rec_sha,
                rejected_session_date="2026-01-06",
                engine_hash=PINNED_FUSE_ENGINE_HASH,
                spec_version=PINNED_FUSE_SPEC_VERSION,
                next_row_state=pend_next,
                base_prior_row_state=s0_state,
            )
            loaded_state, _ = load_pending_rejected_state_snapshot(
                root / "state_snapshots",
                expected_chain_id=drill_cfg.chain_id,
                expected_base_sequence=0,
                expected_base_record_sha256=s0_rec_sha,
                current_session_date="2026-01-06",
                expected_engine_hash=PINNED_FUSE_ENGINE_HASH,
                expected_spec_version=PINNED_FUSE_SPEC_VERSION,
                expected_base_prior_state_sha256=s0_sha,
                committed_base_prior_row_state=s0_state,
                expected_base_session_date="2026-01-05",
            )
            self.assertFalse(loaded_state.get("V2", {}).get("pending_adjustments"))

            # Negative control 1 & 2: latched mismatch protects BOTH latched_mismatches and pending_adjustments
            latched_base = json.loads(json.dumps(s0_state))
            latched_base["V2"]["latched_mismatches"] = {"TQQQ": True}
            latched_base_sha = _sha256_canonical_state_dict(latched_base)
            latched_dir = Path(tmpdir) / "v2_latched"
            save_row_state_snapshot(
                latched_dir / "state_snapshots",
                chain_id=drill_cfg.chain_id,
                sequence=0,
                session_date="2026-01-05",
                record_sha256=s0_rec_sha,
                engine_hash=PINNED_FUSE_ENGINE_HASH,
                spec_version=PINNED_FUSE_SPEC_VERSION,
                next_row_state=latched_base,
            )

            clear_latched = json.loads(json.dumps(latched_base))
            clear_latched["V2"]["latched_mismatches"] = {}
            save_pending_rejected_state_snapshot(
                latched_dir / "state_snapshots",
                chain_id=drill_cfg.chain_id,
                base_sequence=0,
                base_record_sha256=s0_rec_sha,
                rejected_session_date="2026-01-06",
                engine_hash=PINNED_FUSE_ENGINE_HASH,
                spec_version=PINNED_FUSE_SPEC_VERSION,
                next_row_state=clear_latched,
                base_prior_row_state=latched_base,
            )
            with self.assertRaises(EngineIdentityError):
                load_pending_rejected_state_snapshot(
                    latched_dir / "state_snapshots",
                    expected_chain_id=drill_cfg.chain_id,
                    expected_base_sequence=0,
                    expected_base_record_sha256=s0_rec_sha,
                    current_session_date="2026-01-06",
                    expected_engine_hash=PINNED_FUSE_ENGINE_HASH,
                    expected_spec_version=PINNED_FUSE_SPEC_VERSION,
                    expected_base_prior_state_sha256=latched_base_sha,
                    committed_base_prior_row_state=latched_base,
                    expected_base_session_date="2026-01-05",
                )

            clear_pend_while_latched = json.loads(json.dumps(latched_base))
            clear_pend_while_latched["V2"]["pending_adjustments"] = {}
            save_pending_rejected_state_snapshot(
                latched_dir / "state_snapshots",
                chain_id=drill_cfg.chain_id,
                base_sequence=0,
                base_record_sha256=s0_rec_sha,
                rejected_session_date="2026-01-06",
                engine_hash=PINNED_FUSE_ENGINE_HASH,
                spec_version=PINNED_FUSE_SPEC_VERSION,
                next_row_state=clear_pend_while_latched,
                base_prior_row_state=latched_base,
            )
            with self.assertRaises(EngineIdentityError):
                load_pending_rejected_state_snapshot(
                    latched_dir / "state_snapshots",
                    expected_chain_id=drill_cfg.chain_id,
                    expected_base_sequence=0,
                    expected_base_record_sha256=s0_rec_sha,
                    current_session_date="2026-01-06",
                    expected_engine_hash=PINNED_FUSE_ENGINE_HASH,
                    expected_spec_version=PINNED_FUSE_SPEC_VERSION,
                    expected_base_prior_state_sha256=latched_base_sha,
                    committed_base_prior_row_state=latched_base,
                    expected_base_session_date="2026-01-05",
                )

    def test_o5_unlatched_rejection_count_resets_across_sessions_while_latch_protected(self) -> None:
        drill_cfg = _sh2_drill_cfg()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "o5_reset"
            f5 = Path(tmpdir) / "fix_05"
            _build_session_fixture_files(f5, "2026-01-05")
            r0 = run_daily_drill_cycle(
                session_date="2026-01-05",
                data_dir=root,
                config=drill_cfg,
                fixtures_dir=f5,
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
            )
            self.assertTrue(r0["appended"])
            recs0 = ShadowLedger(root / "ledger").load_records()
            from shadow.ledger import sha256_canonical

            s0_rec_sha = sha256_canonical(recs0[0]["record_body"])
            s0_state, _ = load_row_state_snapshot(
                root / "state_snapshots",
                expected_chain_id=drill_cfg.chain_id,
                expected_engine_hash=PINNED_FUSE_ENGINE_HASH,
                expected_spec_version=PINNED_FUSE_SPEC_VERSION,
                expected_sequence=0,
                expected_record_sha256=s0_rec_sha,
            )

            # 1. Committed unlatched O5 count=2 on 2026-01-05 legitimately resets to count=1 on 2026-01-06
            base_unlatched = json.loads(json.dumps(s0_state))
            base_unlatched["O5"] = {
                "rejection_count": 2,
                "last_rejection_session": "2026-01-05",
                "latched": False,
            }
            base_unlatched_sha = _sha256_canonical_state_dict(base_unlatched)
            pend_dir = Path(tmpdir) / "o5_cross_day"
            save_row_state_snapshot(
                pend_dir / "state_snapshots",
                chain_id=drill_cfg.chain_id,
                sequence=0,
                session_date="2026-01-05",
                record_sha256=s0_rec_sha,
                engine_hash=PINNED_FUSE_ENGINE_HASH,
                spec_version=PINNED_FUSE_SPEC_VERSION,
                next_row_state=base_unlatched,
            )
            pend_next = json.loads(json.dumps(s0_state))
            pend_next["O5"] = {
                "rejection_count": 1,
                "last_rejection_session": "2026-01-06",
                "latched": False,
            }
            save_pending_rejected_state_snapshot(
                pend_dir / "state_snapshots",
                chain_id=drill_cfg.chain_id,
                base_sequence=0,
                base_record_sha256=s0_rec_sha,
                rejected_session_date="2026-01-06",
                engine_hash=PINNED_FUSE_ENGINE_HASH,
                spec_version=PINNED_FUSE_SPEC_VERSION,
                next_row_state=pend_next,
                base_prior_row_state=base_unlatched,
            )
            loaded, _ = load_pending_rejected_state_snapshot(
                pend_dir / "state_snapshots",
                expected_chain_id=drill_cfg.chain_id,
                expected_base_sequence=0,
                expected_base_record_sha256=s0_rec_sha,
                current_session_date="2026-01-06",
                expected_engine_hash=PINNED_FUSE_ENGINE_HASH,
                expected_spec_version=PINNED_FUSE_SPEC_VERSION,
                expected_base_prior_state_sha256=base_unlatched_sha,
                committed_base_prior_row_state=base_unlatched,
                expected_base_session_date="2026-01-05",
            )
            self.assertEqual(loaded["O5"]["rejection_count"], 1)

            # 2. Same-session retry MUST NOT reduce committed O5 rejection_count
            same_day_bad = json.loads(json.dumps(s0_state))
            same_day_bad["O5"] = {
                "rejection_count": 1,
                "last_rejection_session": "2026-01-05",
                "latched": False,
            }
            save_pending_rejected_state_snapshot(
                pend_dir / "state_snapshots",
                chain_id=drill_cfg.chain_id,
                base_sequence=0,
                base_record_sha256=s0_rec_sha,
                rejected_session_date="2026-01-05",
                engine_hash=PINNED_FUSE_ENGINE_HASH,
                spec_version=PINNED_FUSE_SPEC_VERSION,
                next_row_state=same_day_bad,
                base_prior_row_state=base_unlatched,
            )
            with self.assertRaises(EngineIdentityError):
                load_pending_rejected_state_snapshot(
                    pend_dir / "state_snapshots",
                    expected_chain_id=drill_cfg.chain_id,
                    expected_base_sequence=0,
                    expected_base_record_sha256=s0_rec_sha,
                    current_session_date="2026-01-05",
                    expected_engine_hash=PINNED_FUSE_ENGINE_HASH,
                    expected_spec_version=PINNED_FUSE_SPEC_VERSION,
                    expected_base_prior_state_sha256=base_unlatched_sha,
                    committed_base_prior_row_state=base_unlatched,
                    expected_base_session_date="2026-01-05",
                )

            # 3. Latched O5 on 2026-01-05 still rejects cross-session latch clear & count drop on 2026-01-06
            base_latched = json.loads(json.dumps(s0_state))
            base_latched["O5"] = {
                "rejection_count": 3,
                "last_rejection_session": "2026-01-05",
                "latched": True,
            }
            base_latched_sha = _sha256_canonical_state_dict(base_latched)
            lat_dir = Path(tmpdir) / "o5_latched"
            save_row_state_snapshot(
                lat_dir / "state_snapshots",
                chain_id=drill_cfg.chain_id,
                sequence=0,
                session_date="2026-01-05",
                record_sha256=s0_rec_sha,
                engine_hash=PINNED_FUSE_ENGINE_HASH,
                spec_version=PINNED_FUSE_SPEC_VERSION,
                next_row_state=base_latched,
            )
            forged_lat = json.loads(json.dumps(s0_state))
            forged_lat["O5"] = {
                "rejection_count": 1,
                "last_rejection_session": "2026-01-06",
                "latched": False,
            }
            save_pending_rejected_state_snapshot(
                lat_dir / "state_snapshots",
                chain_id=drill_cfg.chain_id,
                base_sequence=0,
                base_record_sha256=s0_rec_sha,
                rejected_session_date="2026-01-06",
                engine_hash=PINNED_FUSE_ENGINE_HASH,
                spec_version=PINNED_FUSE_SPEC_VERSION,
                next_row_state=forged_lat,
                base_prior_row_state=base_latched,
            )
            with self.assertRaises(EngineIdentityError):
                load_pending_rejected_state_snapshot(
                    lat_dir / "state_snapshots",
                    expected_chain_id=drill_cfg.chain_id,
                    expected_base_sequence=0,
                    expected_base_record_sha256=s0_rec_sha,
                    current_session_date="2026-01-06",
                    expected_engine_hash=PINNED_FUSE_ENGINE_HASH,
                    expected_spec_version=PINNED_FUSE_SPEC_VERSION,
                    expected_base_prior_state_sha256=base_latched_sha,
                    committed_base_prior_row_state=base_latched,
                    expected_base_session_date="2026-01-05",
                )

    def test_o4_terminal_clearing_during_o1_rejection_preserved_in_pending_anchor(self) -> None:
        from shadow.ledger import build_pending_rejected_state_payload

        ev = VendoredFuseEvaluator()
        spec_v2 = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal5 = resolve_session("XNYS", "2026-01-05", spec=spec_v2)
        cal6 = resolve_session("XNYS", "2026-01-06", spec=spec_v2)
        cfg = load_scheduler_config(_REPO_ROOT / "config" / "drill-scheduler.json")
        code_sha = compute_code_manifest_hash()

        def _base_si(ts: str) -> dict:
            return {
                "E": "50000",
                "E_star": "50000",
                "H": "0",
                "L": "0",
                "positions_source_record": {"as_of": ts, "age_sessions": 0},
                "broker_reconciliation_source_record": {"as_of": ts, "age_sessions": 0},
                "index_one_session_returns": {"NDX": 0.0, "SEMI": 0.0},
                "market_records": [
                    {
                        "instrument": inst,
                        "normalized_value": 50.0,
                        "calendar_spec_digest": str(cal5["spec_digest"]),
                        "eligibility": {"eligible": True},
                    }
                    for inst in ("TQQQ", "SOXL", "QQQ", "XNDX", "SEMI", "SOFR", "TBILL_3M")
                ],
                "funds": {
                    "TQQQ": {
                        "s": "1",
                        "close_price": "50",
                        "q_pre": 100,
                        "sigma_forecast": "0.24",
                        "fund_return": 0.0,
                        "index_return": 0.0,
                    },
                    "SOXL": {
                        "s": "1",
                        "close_price": "40",
                        "q_pre": 100,
                        "sigma_forecast": "0.22",
                        "fund_return": 0.0,
                        "index_return": 0.0,
                    },
                },
            }

        si5 = _base_si("2026-01-05T22:00:00.000Z")
        si5["pending_orders"] = {
            "TQQQ": {"order_id": "ord-123", "status": "unknown", "timed_out": True}
        }
        b0 = build_record_body(
            chain_id=cfg.chain_id,
            sequence=0,
            prev_record_sha256=GENESIS_PREV_HASH,
            mode="drill",
            session_date="2026-01-05",
            decision_timestamp_utc="2026-01-05T22:00:00.000Z",
            session_inputs=si5,
            calendar_session=cal5,
            code_version_sha=code_sha,
            evaluator=ev,
            prior_row_state={},
            calendar_spec=spec_v2,
        )
        self.assertTrue(b0["appended"])
        committed_state = b0["next_row_state"]
        self.assertIn("TQQQ", committed_state["O4"])
        committed_sha = _sha256_canonical_state_dict(committed_state)
        rec0_sha = sha256_canonical(b0["record_body"])

        # Jan 6: TQQQ terminal status="filled" clears O4.TQQQ while missing positions triggers O1 reject
        si6 = _base_si("2026-01-06T22:00:00.000Z")
        si6["positions_source_record"] = None
        si6["pending_orders"] = {"TQQQ": {"order_id": "ord-123", "status": "filled"}}
        b1_rej = build_record_body(
            chain_id=cfg.chain_id,
            sequence=1,
            prev_record_sha256=rec0_sha,
            mode="drill",
            session_date="2026-01-06",
            decision_timestamp_utc="2026-01-06T22:00:00.000Z",
            session_inputs=si6,
            calendar_session=cal6,
            code_version_sha=code_sha,
            evaluator=ev,
            prior_row_state=committed_state,
            calendar_spec=spec_v2,
        )
        self.assertFalse(b1_rej["appended"])
        self.assertEqual(b1_rej["o4_resolved_symbols"], ["TQQQ"])
        self.assertEqual(b1_rej["next_row_state"].get("O4", {}), {})

        with tempfile.TemporaryDirectory() as tmpdir:
            sdir = Path(tmpdir) / "state_snapshots"
            pending_payload = build_pending_rejected_state_payload(
                chain_id=cfg.chain_id,
                base_sequence=0,
                base_record_sha256=rec0_sha,
                rejected_session_date="2026-01-06",
                engine_hash=ev.engine_hash,
                spec_version=ev.spec_version,
                next_row_state=b1_rej["next_row_state"],
                base_prior_row_state=committed_state,
                o4_resolved_symbols=b1_rej["o4_resolved_symbols"],
            )
            p_path = save_pending_rejected_state_snapshot(
                sdir,
                chain_id=cfg.chain_id,
                base_sequence=0,
                base_record_sha256=rec0_sha,
                rejected_session_date="2026-01-06",
                engine_hash=ev.engine_hash,
                spec_version=ev.spec_version,
                next_row_state=b1_rej["next_row_state"],
                base_prior_row_state=committed_state,
                o4_resolved_symbols=b1_rej["o4_resolved_symbols"],
            )
            self.assertEqual(
                compute_pending_rejected_state_sha256(pending_payload),
                compute_pending_rejected_state_sha256(
                    json.loads(p_path.read_text(encoding="utf-8"))
                ),
            )
            loaded_state, loaded_payload = load_pending_rejected_state_snapshot(
                sdir,
                expected_chain_id=cfg.chain_id,
                expected_base_sequence=0,
                expected_base_record_sha256=rec0_sha,
                current_session_date="2026-01-07",
                expected_engine_hash=ev.engine_hash,
                expected_spec_version=ev.spec_version,
                expected_base_prior_state_sha256=committed_sha,
                committed_base_prior_row_state=committed_state,
                expected_base_session_date="2026-01-05",
            )
            self.assertEqual(loaded_state.get("O4", {}), {})
            self.assertEqual(loaded_payload.get("o4_resolved_symbols"), ["TQQQ"])

            # Without o4_resolved_symbols, dropping O4.TQQQ is still rejected as tampering
            save_pending_rejected_state_snapshot(
                sdir,
                chain_id=cfg.chain_id,
                base_sequence=0,
                base_record_sha256=rec0_sha,
                rejected_session_date="2026-01-06",
                engine_hash=ev.engine_hash,
                spec_version=ev.spec_version,
                next_row_state=b1_rej["next_row_state"],
                base_prior_row_state=committed_state,
            )
            with self.assertRaises(EngineIdentityError):
                load_pending_rejected_state_snapshot(
                    sdir,
                    expected_chain_id=cfg.chain_id,
                    expected_base_sequence=0,
                    expected_base_record_sha256=rec0_sha,
                    current_session_date="2026-01-07",
                    expected_engine_hash=ev.engine_hash,
                    expected_spec_version=ev.spec_version,
                    expected_base_prior_state_sha256=committed_sha,
                    committed_base_prior_row_state=committed_state,
                    expected_base_session_date="2026-01-05",
                )

    def test_authorized_l4_rearm_during_o1_rejection_persists_and_blocks_forged_clearing(self) -> None:
        from shadow.ledger import build_pending_rejected_state_payload

        ev = VendoredFuseEvaluator()
        spec_v2 = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal5 = resolve_session("XNYS", "2026-01-05", spec=spec_v2)
        cal6 = resolve_session("XNYS", "2026-01-06", spec=spec_v2)
        cal7 = resolve_session("XNYS", "2026-01-07", spec=spec_v2)
        cfg = load_scheduler_config(_REPO_ROOT / "config" / "drill-scheduler.json")
        code_sha = compute_code_manifest_hash()

        def _make_si(ts: str, *, e_val: str, prior_e_val: str, e_star_val: str) -> dict:
            return {
                "E": e_val,
                "E_star": e_star_val,
                "prior_E": prior_e_val,
                "H": "0",
                "L": "0",
                "positions_source_record": {"as_of": ts, "age_sessions": 0},
                "broker_reconciliation_source_record": {"as_of": ts, "age_sessions": 0},
                "index_one_session_returns": {"NDX": 0.0, "SEMI": 0.0},
                "market_records": [
                    {
                        "instrument": inst,
                        "normalized_value": 50.0,
                        "calendar_spec_digest": str(cal5["spec_digest"]),
                        "eligibility": {"eligible": True},
                    }
                    for inst in ("TQQQ", "SOXL", "QQQ", "XNDX", "SEMI", "SOFR", "TBILL_3M")
                ],
                "funds": {
                    "TQQQ": {
                        "s": "1",
                        "close_price": "50",
                        "q_pre": 100,
                        "sigma_forecast": "0.24",
                        "fund_return": 0.0,
                        "index_return": 0.0,
                    },
                    "SOXL": {
                        "s": "1",
                        "close_price": "40",
                        "q_pre": 100,
                        "sigma_forecast": "0.22",
                        "fund_return": 0.0,
                        "index_return": 0.0,
                    },
                },
            }

        # Jan 5: one-session loss >= D/10 (E=48000 vs prior_E=50000 -> -4%) commits L4 latch
        si5 = _make_si(
            "2026-01-05T22:00:00.000Z", e_val="48000", prior_e_val="50000", e_star_val="50000"
        )
        b0 = build_record_body(
            chain_id=cfg.chain_id,
            sequence=0,
            prev_record_sha256=GENESIS_PREV_HASH,
            mode="drill",
            session_date="2026-01-05",
            decision_timestamp_utc="2026-01-05T22:00:00.000Z",
            session_inputs=si5,
            calendar_session=cal5,
            code_version_sha=code_sha,
            evaluator=ev,
            prior_row_state={},
            calendar_spec=spec_v2,
        )
        self.assertTrue(b0["appended"])
        committed_state = b0["next_row_state"]
        self.assertTrue(committed_state["L4"]["latched"])
        committed_sha = _sha256_canonical_state_dict(committed_state)
        rec0_sha = sha256_canonical(b0["record_body"])

        # Jan 6: missing positions triggers O1 reject while owner rearms L4 (account scope)
        si6 = _make_si(
            "2026-01-06T22:00:00.000Z", e_val="48000", prior_e_val="48000", e_star_val="50000"
        )
        si6["positions_source_record"] = None
        valid_rearm = {"account": [{"type": "rearm", "row": "L4", "session": "2026-01-06"}]}
        si6["authorized_events"] = valid_rearm
        b1_rej = build_record_body(
            chain_id=cfg.chain_id,
            sequence=1,
            prev_record_sha256=rec0_sha,
            mode="drill",
            session_date="2026-01-06",
            decision_timestamp_utc="2026-01-06T22:00:00.000Z",
            session_inputs=si6,
            calendar_session=cal6,
            code_version_sha=code_sha,
            evaluator=ev,
            prior_row_state=committed_state,
            calendar_spec=spec_v2,
        )
        self.assertFalse(b1_rej["appended"])
        self.assertEqual(b1_rej["next_row_state"].get("L4", {}), {})

        with tempfile.TemporaryDirectory() as tmpdir:
            sdir = Path(tmpdir) / "state_snapshots"
            pending_payload = build_pending_rejected_state_payload(
                chain_id=cfg.chain_id,
                base_sequence=0,
                base_record_sha256=rec0_sha,
                rejected_session_date="2026-01-06",
                engine_hash=ev.engine_hash,
                spec_version=ev.spec_version,
                next_row_state=b1_rej["next_row_state"],
                base_prior_row_state=committed_state,
                authorized_events=valid_rearm,
            )
            p_path = save_pending_rejected_state_snapshot(
                sdir,
                chain_id=cfg.chain_id,
                base_sequence=0,
                base_record_sha256=rec0_sha,
                rejected_session_date="2026-01-06",
                engine_hash=ev.engine_hash,
                spec_version=ev.spec_version,
                next_row_state=b1_rej["next_row_state"],
                base_prior_row_state=committed_state,
                authorized_events=valid_rearm,
            )
            self.assertEqual(
                compute_pending_rejected_state_sha256(pending_payload),
                compute_pending_rejected_state_sha256(
                    json.loads(p_path.read_text(encoding="utf-8"))
                ),
            )
            for cur_ds in ("2026-01-06", "2026-01-07"):
                loaded_state, loaded_payload = load_pending_rejected_state_snapshot(
                    sdir,
                    expected_chain_id=cfg.chain_id,
                    expected_base_sequence=0,
                    expected_base_record_sha256=rec0_sha,
                    current_session_date=cur_ds,
                    expected_engine_hash=ev.engine_hash,
                    expected_spec_version=ev.spec_version,
                    expected_base_prior_state_sha256=committed_sha,
                    committed_base_prior_row_state=committed_state,
                    expected_base_session_date="2026-01-05",
                )
                self.assertFalse(bool(loaded_state.get("L4", {}).get("latched")))
                self.assertIsInstance(loaded_payload.get("authorized_events"), list)

            # Jan 7 commits cleanly from loaded pending state with L4 cleared
            si7 = _make_si(
                "2026-01-07T22:00:00.000Z", e_val="48000", prior_e_val="48000", e_star_val="50000"
            )
            b1_ok = build_record_body(
                chain_id=cfg.chain_id,
                sequence=1,
                prev_record_sha256=rec0_sha,
                mode="drill",
                session_date="2026-01-07",
                decision_timestamp_utc="2026-01-07T22:00:00.000Z",
                session_inputs=si7,
                calendar_session=cal7,
                code_version_sha=code_sha,
                evaluator=ev,
                prior_row_state=loaded_state,
                calendar_spec=spec_v2,
            )
            self.assertTrue(b1_ok["appended"])
            self.assertFalse(bool(b1_ok["next_row_state"].get("L4", {}).get("latched")))

            # Fail-closed tamper cases: missing authorized_events, stale session, wrong scope, malformed
            bad_cases: list[tuple[dict | list | None, str]] = [
                (None, "Pending state downgraded or cleared committed L4 latch"),
                (
                    {"account": [{"type": "rearm", "row": "L4", "session": "2026-01-05"}]},
                    "Pending state downgraded or cleared committed L4 latch",
                ),
                (
                    {"fund:TQQQ": [{"type": "rearm", "row": "L4", "session": "2026-01-06"}]},
                    "Pending state downgraded or cleared committed L4 latch",
                ),
                (
                    [{"type": "rearm", "row": "L4", "fund": "TQQQ", "session": "2026-01-06"}],
                    "Pending state downgraded or cleared committed L4 latch",
                ),
            ]
            for bad_auth, expected_msg in bad_cases:
                save_pending_rejected_state_snapshot(
                    sdir,
                    chain_id=cfg.chain_id,
                    base_sequence=0,
                    base_record_sha256=rec0_sha,
                    rejected_session_date="2026-01-06",
                    engine_hash=ev.engine_hash,
                    spec_version=ev.spec_version,
                    next_row_state=b1_rej["next_row_state"],
                    base_prior_row_state=committed_state,
                    authorized_events=bad_auth,
                )
                with self.assertRaisesRegex(EngineIdentityError, expected_msg):
                    load_pending_rejected_state_snapshot(
                        sdir,
                        expected_chain_id=cfg.chain_id,
                        expected_base_sequence=0,
                        expected_base_record_sha256=rec0_sha,
                        current_session_date="2026-01-07",
                        expected_engine_hash=ev.engine_hash,
                        expected_spec_version=ev.spec_version,
                        expected_base_prior_state_sha256=committed_sha,
                        committed_base_prior_row_state=committed_state,
                        expected_base_session_date="2026-01-05",
                    )

            # Malformed authorized_events list type in snapshot file rejected cleanly
            malformed_payload = build_pending_rejected_state_payload(
                chain_id=cfg.chain_id,
                base_sequence=0,
                base_record_sha256=rec0_sha,
                rejected_session_date="2026-01-06",
                engine_hash=ev.engine_hash,
                spec_version=ev.spec_version,
                next_row_state=b1_rej["next_row_state"],
                base_prior_row_state=committed_state,
            )
            malformed_payload["authorized_events"] = ["not-a-dict"]
            (sdir / "pending_rejected_state.json").write_text(
                json.dumps(malformed_payload) + "\n", encoding="utf-8"
            )
            with self.assertRaisesRegex(
                EngineIdentityError, "Pending state authorized_events must be a list of dicts"
            ):
                load_pending_rejected_state_snapshot(
                    sdir,
                    expected_chain_id=cfg.chain_id,
                    expected_base_sequence=0,
                    expected_base_record_sha256=rec0_sha,
                    current_session_date="2026-01-07",
                    expected_engine_hash=ev.engine_hash,
                    expected_spec_version=ev.spec_version,
                    expected_base_prior_state_sha256=committed_sha,
                    committed_base_prior_row_state=committed_state,
                    expected_base_session_date="2026-01-05",
                )

            # L3 cooling-off enforcement: premature rearm rejected, on/after cooling_off_until allowed
            l3_committed = {
                "L3": {
                    "latched": True,
                    "trip_session": "2026-01-05",
                    "cooling_off_until": "2026-02-04",
                }
            }
            l3_committed_sha = _sha256_canonical_state_dict(l3_committed)
            l3_cleared = {"L3": {"latched": False}}
            save_pending_rejected_state_snapshot(
                sdir,
                chain_id=cfg.chain_id,
                base_sequence=0,
                base_record_sha256=rec0_sha,
                rejected_session_date="2026-02-03",
                engine_hash=ev.engine_hash,
                spec_version=ev.spec_version,
                next_row_state=l3_cleared,
                base_prior_row_state=l3_committed,
                authorized_events={
                    "account": [{"type": "rearm", "row": "L3", "session": "2026-02-03"}]
                },
            )
            with self.assertRaisesRegex(
                EngineIdentityError, "Pending state downgraded or cleared committed L3 latch"
            ):
                load_pending_rejected_state_snapshot(
                    sdir,
                    expected_chain_id=cfg.chain_id,
                    expected_base_sequence=0,
                    expected_base_record_sha256=rec0_sha,
                    current_session_date="2026-02-03",
                    expected_engine_hash=ev.engine_hash,
                    expected_spec_version=ev.spec_version,
                    expected_base_prior_state_sha256=l3_committed_sha,
                    committed_base_prior_row_state=l3_committed,
                    expected_base_session_date="2026-01-05",
                )

            save_pending_rejected_state_snapshot(
                sdir,
                chain_id=cfg.chain_id,
                base_sequence=0,
                base_record_sha256=rec0_sha,
                rejected_session_date="2026-02-04",
                engine_hash=ev.engine_hash,
                spec_version=ev.spec_version,
                next_row_state=l3_cleared,
                base_prior_row_state=l3_committed,
                authorized_events={
                    "account": [{"type": "rearm", "row": "L3", "session": "2026-02-04"}]
                },
            )
            l3_loaded, _ = load_pending_rejected_state_snapshot(
                sdir,
                expected_chain_id=cfg.chain_id,
                expected_base_sequence=0,
                expected_base_record_sha256=rec0_sha,
                current_session_date="2026-02-04",
                expected_engine_hash=ev.engine_hash,
                expected_spec_version=ev.spec_version,
                expected_base_prior_state_sha256=l3_committed_sha,
                committed_base_prior_row_state=l3_committed,
                expected_base_session_date="2026-01-05",
            )
            self.assertFalse(bool(l3_loaded.get("L3", {}).get("latched")))

    def test_scheduler_authorized_events_pending_clear_replays_bit_for_bit(self) -> None:
        drill_cfg = _sh2_drill_cfg()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sched_auth_clear"
            f5 = Path(tmpdir) / "f5"
            _build_session_fixture_files(f5, "2026-01-05")
            f6_bad = Path(tmpdir) / "f6_bad"
            _build_session_fixture_files(f6_bad, "2026-01-06")
            (f6_bad / "treasury_tbill_2026-01-06.csv").unlink()
            f7 = Path(tmpdir) / "f7"
            _build_session_fixture_files(f7, "2026-01-07")

            # Jan 5: commit O8 kill_switch latch
            r0 = run_daily_drill_cycle(
                session_date="2026-01-05",
                data_dir=root,
                config=drill_cfg,
                fixtures_dir=f5,
                authorized_events={"account": [{"type": "kill_switch", "session": "2026-01-05"}]},
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
            )
            self.assertTrue(r0["appended"])
            recs0 = ShadowLedger(root / "ledger").load_records()
            b0_o8 = {b["id"]: b for b in recs0[-1]["record_body"]["evaluator"]["breakers"]}["O8"]
            self.assertTrue(b0_o8["latched"])

            # Jan 6: O1 missing treasury rejection + valid kill_switch_clear clears O8 in pending
            r1_rej = run_daily_drill_cycle(
                session_date="2026-01-06",
                data_dir=root,
                config=drill_cfg,
                fixtures_dir=f6_bad,
                authorized_events={
                    "account": [{"type": "kill_switch_clear", "session": "2026-01-06"}]
                },
                wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
            )
            self.assertFalse(r1_rej["appended"])
            self.assertTrue(r1_rej["fail_closed"])

            # Jan 7: clean session loads pending state (preserving prior O8 clear) and replays cleanly
            r1_ok = run_daily_drill_cycle(
                session_date="2026-01-07",
                data_dir=root,
                config=drill_cfg,
                fixtures_dir=f7,
                wall_clock=lambda: datetime(2026, 1, 7, 22, 30, tzinfo=timezone.utc),
            )
            self.assertTrue(r1_ok["appended"])
            recs1 = ShadowLedger(root / "ledger").load_records()
            self.assertEqual(len(recs1), 2)
            b1_o8 = {b["id"]: b for b in recs1[-1]["record_body"]["evaluator"]["breakers"]}["O8"]
            self.assertFalse(b1_o8["latched"])

            rep = replay_window(root / "replay", root / "ledger")
            self.assertTrue(rep["bit_for_bit_match"])

    def test_scheduler_default_omits_long_run_volatility_v4_fails_closed(self) -> None:
        drill_cfg = _sh2_drill_cfg()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sched_missing_lrv"
            f5 = Path(tmpdir) / "f5"
            _build_session_fixture_files(f5, "2026-01-05")
            res = run_daily_drill_cycle(
                session_date="2026-01-05",
                data_dir=root,
                config=drill_cfg,
                fixtures_dir=f5,
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
            )
            self.assertTrue(res["appended"])
            recs = ShadowLedger(root / "ledger").load_records()
            v4_row = next(
                b for b in recs[-1]["record_body"]["evaluator"]["breakers"] if b["id"] == "V4"
            )
            self.assertEqual(v4_row["state"], "NO-NEW-RISK")
            self.assertEqual(v4_row["reason"], "missing_input")
            rep = replay_window(root / "replay", root / "ledger")
            self.assertTrue(rep["bit_for_bit_match"])

    def test_extract_prior_close_rejects_huge_exponents_and_out_of_range_closes(self) -> None:
        cal_spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal_jan5 = resolve_session("XNYS", "2026-01-05", spec=cal_spec)
        ts_02 = int(datetime(2026, 1, 2, 16, 0, tzinfo=_NY_TZ).timestamp())
        ts_05 = int(datetime(2026, 1, 5, 16, 0, tzinfo=_NY_TZ).timestamp())

        for bad_val in ("1e200000000", "1e-200000000", "0e200000000", "0", "-5", "1e10", 0, -5):
            t_payload = json.dumps(
                [
                    {
                        "date": "2026-01-05T00:00:00.000Z",
                        "close": bad_val,
                        "divCash": 0.0,
                        "splitFactor": 1.0,
                        "session": "regular",
                        "session_time": "regular",
                    }
                ]
            ).encode("utf-8")
            self.assertIsNone(
                extract_prior_close_from_tiingo_bytes(
                    t_payload,
                    "2026-01-06",
                    calendar_session=cal_jan5,
                    target_prior_date="2026-01-05",
                ),
                f"Tiingo prior close must reject {bad_val!r}",
            )

            y_payload = json.dumps(
                {
                    "chart": {
                        "result": [
                            {
                                "meta": {"dataGranularity": "1d"},
                                "timestamp": [ts_05],
                                "indicators": {"quote": [{"close": [bad_val]}]},
                            }
                        ]
                    }
                }
            ).encode("utf-8")
            self.assertIsNone(
                extract_prior_close_from_yahoo_bytes(
                    y_payload,
                    "2026-01-06",
                    calendar_session=cal_jan5,
                    target_prior_date="2026-01-05",
                ),
                f"Yahoo prior close must reject {bad_val!r}",
            )

        # Raw JSON with unquoted huge JSON number (`1e200000000`) plus an earlier valid row
        t_mixed = (
            b'[{"date":"2026-01-02T00:00:00.000Z","close":48.5,"divCash":0.0,"splitFactor":1.0,'
            b'"session":"regular","session_time":"regular"},'
            b'{"date":"2026-01-05T00:00:00.000Z","close":"1e200000000","divCash":0.0,"splitFactor":1.0,'
            b'"session":"regular","session_time":"regular"}]'
        )
        self.assertEqual(
            extract_prior_close_from_tiingo_bytes(
                t_mixed,
                "2026-01-06",
                calendar_session=cal_jan5,
            ),
            "48.5",
        )

        y_mixed = json.dumps(
            {
                "chart": {
                    "result": [
                        {
                            "meta": {"dataGranularity": "1d"},
                            "timestamp": [ts_02, ts_05],
                            "indicators": {"quote": [{"close": [28000.0, "1e200000000"]}]},
                        }
                    ]
                }
            }
        ).encode("utf-8")
        self.assertEqual(
            extract_prior_close_from_yahoo_bytes(
                y_mixed,
                "2026-01-06",
                calendar_session=cal_jan5,
            ),
            "28000",
        )


if __name__ == "__main__":
    unittest.main()
