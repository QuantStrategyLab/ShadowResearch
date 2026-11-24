"""Tests for SH-2 production lock, 23/5 close isolation, restart snapshots, replay, and D1-D10 demo."""

from __future__ import annotations

import dataclasses
import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC = str(_REPO_ROOT / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
_DEMO = str(_REPO_ROOT / "demo")
if _DEMO not in sys.path:
    sys.path.insert(0, _DEMO)

from run_sh2_demo import _build_session_fixture_files, run_all_demos  # noqa: E402
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
    ShadowLedger,
    check_forward_clock_credit,
    load_row_state_snapshot,
    save_row_state_snapshot,
)
from shadow.ots import OpenTimestampsClient, build_synthetic_ots_fixture  # noqa: E402
from shadow.replay import replay_window  # noqa: E402
from shadow.scheduler import (  # noqa: E402
    load_production_scheduler_config,
    run_daily_drill_cycle,
    run_production_cycle,
)

_NY_TZ = ZoneInfo("America/New_York")


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
        cfg = dataclasses.replace(
            load_production_scheduler_config(_REPO_ROOT / "config" / "production-scheduler.json"),
            production_chain_locked=False,
            clk01_active=True,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "prod_unlocked"
            fdir = Path(tmpdir) / "fixtures"
            _build_session_fixture_files(fdir, "2026-01-05")
            _build_session_fixture_files(fdir, "2026-01-06")

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
                config=cfg,
                fixtures_dir=fdir,
                ots_client=_FakeOts(),
                wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
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
            ok_credit, issues = check_forward_clock_credit(
                ledger.load_records(),
                ledger.load_receipts(),
                ledger_dir=ledger.root_dir,
                bitcoin_merkle_roots=hdrs,
                commitment_deadline_et="23:30",
                calendar_spec=load_spec(_REPO_ROOT / "specs" / "csa22-v2"),
                expected_engine_hash=PINNED_FUSE_ENGINE_HASH,
                expected_spec_version=PINNED_FUSE_SPEC_VERSION,
                production_chain_locked=False,
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


if __name__ == "__main__":
    unittest.main()
