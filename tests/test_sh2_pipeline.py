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
    ShadowLedger,
    _sha256_canonical_state_dict,
    build_record_body,
    check_forward_clock_credit,
    compute_code_manifest_hash,
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
                    wall_clock=lambda: datetime(2026, 1, 6, 22, 30, tzinfo=timezone.utc),
                )
            self.assertEqual(len(ShadowLedger(root / "ledger").load_records()), 1)

            # Re-running already-committed Day 0 deterministically repairs forged snapshot from ledger+bundle!
            r0_heal = run_daily_drill_cycle(
                session_date="2026-01-05",
                data_dir=root,
                config=drill_cfg,
                fixtures_dir=f5,
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
            )
            self.assertTrue(r0["appended"])
            seq0_sha = r0["record_sha256"]

            # 2. Unauthenticated non-empty production ledger fails closed (0 appends)
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
                )
            self.assertIn("production_history_unauthenticated", str(unauth_ctx.exception))
            self.assertEqual(len(ShadowLedger(root / "ledger").load_records()), 1)

            # 3. Mismatched trusted head or non-creditable Bitcoin roots fail closed (0 appends)
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
                    bitcoin_merkle_roots={880100: {"merkle_root": "a" * 64, "timestamp": "2026-01-06T03:00:00.000Z"}},
                )

            # 4. Authenticated with valid bitcoin_merkle_roots -> seq=1 commits cleanly
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
                bitcoin_merkle_roots=dict(hdrs),
            )
            self.assertTrue(r1["appended"])
            self.assertEqual(r1["sequence"], 1)
            seq1_sha = r1["record_sha256"]

            # 5. Idempotent retry of seq=1 accepts either pre-run seq0_sha or seq1_sha
            for trusted_sha in (seq0_sha, seq1_sha):
                r1_retry = run_production_cycle(
                    session_date="2026-01-06",
                    data_dir=root,
                    config_path=cfg_path,
                    transport=build_live_mock_transport("2026-01-06"),
                    environ={"TIINGO_API_KEY": "test-dummy-token"},
                    wall_clock=lambda: datetime(2026, 1, 6, 22, 35, tzinfo=timezone.utc),
                    expected_prior_head_sha256=trusted_sha,
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
                    )
                self.assertIn(expected_msg, str(wiped_ctx.exception))
                self.assertEqual(ShadowLedger(root / "ledger").load_records(), [])
                self.assertFalse((root / "replay").exists())


if __name__ == "__main__":
    unittest.main()
