"""Unit tests for C-SA-22 published calendar spec and consumer."""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

_SRC = str(Path(__file__).resolve().parents[1] / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from shadow.calendar_consumer import (
    CalendarDigestError,
    classify_us_equity_session_window,
    load_spec,
    resolve_prior_close_session,
    resolve_session,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SPEC_DIR = REPO_ROOT / "specs" / "csa22-v1"


class CalendarConsumerTest(unittest.TestCase):
    """Verify full-year 2025 vectors, digest verification, DST, and error cases."""

    def test_full_2025_vectors_match_expected(self) -> None:
        spec = load_spec(SPEC_DIR)
        expected_doc = json.loads((SPEC_DIR / "expected-2025.json").read_text(encoding="utf-8"))
        sessions = expected_doc["sessions"]
        self.assertEqual(len(sessions), 365)

        for trade_date, expected in sessions.items():
            actual = resolve_session("XNYS", trade_date, spec=spec)
            self.assertEqual(actual, expected, msg=f"mismatch on {trade_date}")

    def test_specific_boundaries_dst_early_closes_and_unscheduled(self) -> None:
        spec = load_spec(SPEC_DIR)

        # Unscheduled closure: 2025-01-09
        carter = resolve_session("XNYS", "2025-01-09", spec=spec)
        self.assertEqual(carter["session_state"], "unscheduled_closure")
        self.assertFalse(carter["is_trading_day"])
        self.assertIn("Jimmy Carter", carter["reason"])
        self.assertIsNone(carter["open_utc"])
        self.assertIsNone(carter["close_utc"])
        self.assertIsNone(carter["order_cutoff_utc"])

        # Spring DST transition week: Fri 2025-03-07 (EST) -> Mon 2025-03-10 (EDT)
        fri_est = resolve_session("XNYS", "2025-03-07", spec=spec)
        mon_edt = resolve_session("XNYS", "2025-03-10", spec=spec)
        self.assertFalse(fri_est["is_dst"])
        self.assertEqual(fri_est["open_utc"], "2025-03-07T14:30:00.000Z")
        self.assertEqual(fri_est["close_utc"], "2025-03-07T21:00:00.000Z")
        self.assertTrue(mon_edt["is_dst"])
        self.assertEqual(mon_edt["open_utc"], "2025-03-10T13:30:00.000Z")
        self.assertEqual(mon_edt["close_utc"], "2025-03-10T20:00:00.000Z")

        # Fall DST transition week: Fri 2025-10-31 (EDT) -> Mon 2025-11-03 (EST)
        fri_edt = resolve_session("XNYS", "2025-10-31", spec=spec)
        mon_est = resolve_session("XNYS", "2025-11-03", spec=spec)
        self.assertTrue(fri_edt["is_dst"])
        self.assertEqual(fri_edt["close_utc"], "2025-10-31T20:00:00.000Z")
        self.assertFalse(mon_est["is_dst"])
        self.assertEqual(mon_est["close_utc"], "2025-11-03T21:00:00.000Z")

        # Early closes in EDT (2025-07-03) and EST (2025-11-28, 2025-12-24)
        jul3 = resolve_session("XNYS", "2025-07-03", spec=spec)
        nov28 = resolve_session("XNYS", "2025-11-28", spec=spec)
        dec24 = resolve_session("XNYS", "2025-12-24", spec=spec)
        self.assertEqual(jul3["session_state"], "half_day")
        self.assertEqual(jul3["close_utc"], "2025-07-03T17:00:00.000Z")
        self.assertEqual(nov28["session_state"], "half_day")
        self.assertEqual(nov28["close_utc"], "2025-11-28T18:00:00.000Z")
        self.assertEqual(dec24["session_state"], "half_day")
        self.assertEqual(dec24["close_utc"], "2025-12-24T18:00:00.000Z")

    def test_unsupported_venue_out_of_range_and_disagreement(self) -> None:
        spec = load_spec(SPEC_DIR)

        bad_venue = resolve_session("XHKG", "2025-06-02", spec=spec)
        self.assertEqual(bad_venue["session_state"], "unknown")
        self.assertEqual(bad_venue["reason"], "unsupported_venue")
        self.assertTrue(bad_venue["blocks_orders"])
        self.assertIsNone(bad_venue["open_utc"])

        before_range = resolve_session("XNYS", "2024-12-31", spec=spec)
        after_range = resolve_session("XNYS", "2026-01-01", spec=spec)
        self.assertEqual(before_range["session_state"], "unknown")
        self.assertEqual(before_range["reason"], "out_of_range")
        self.assertEqual(after_range["session_state"], "unknown")
        self.assertEqual(after_range["reason"], "out_of_range")

        disagree = resolve_session("XNYS", "2025-06-02", spec=spec, observed_venue_status="closed")
        self.assertTrue(disagree["blocks_orders"])
        self.assertTrue(disagree["blocks_eligibility"])
        self.assertTrue(disagree["venue_disagreement"])

        # Cutoff earlier than close caps order_window end_utc
        early_cut = resolve_session("XNYS", "2025-06-02", spec=spec, cutoff_local="15:30")
        self.assertEqual(early_cut["order_cutoff_utc"], "2025-06-02T19:30:00.000Z")
        self.assertEqual(early_cut["order_window"]["end_utc"], "2025-06-02T19:30:00.000Z")

        # Cutoff later than half-day close is capped by close
        capped = resolve_session("XNYS", "2025-11-28", spec=spec, cutoff_local="16:00")
        self.assertEqual(capped["order_cutoff_utc"], "2025-11-28T18:00:00.000Z")

    def test_digest_tampering_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_dir = Path(tmpdir) / "csa22-v1"
            shutil.copytree(SPEC_DIR, copy_dir)
            exc_file = copy_dir / "exceptions-2025.json"
            exc_file.write_text(exc_file.read_text(encoding="utf-8") + " ", encoding="utf-8")
            with self.assertRaises(CalendarDigestError):
                load_spec(copy_dir)

    def test_resolve_prior_close_session_and_dynamic_exceptions_filename(self) -> None:
        spec = load_spec(SPEC_DIR)
        # Before Friday regular close -> resolves Thursday 2025-11-20
        before_fri = resolve_prior_close_session("2025-11-21T20:55:00.000Z", spec=spec)
        self.assertEqual(before_fri["trade_date"], "2025-11-20")
        # At/after Friday regular close -> resolves Friday 2025-11-21
        at_fri = resolve_prior_close_session("2025-11-21T21:00:00.000Z", spec=spec)
        self.assertEqual(at_fri["trade_date"], "2025-11-21")
        # Saturday morning UTC/ET -> resolves Friday 2025-11-21
        sat = resolve_prior_close_session("2025-11-22T05:55:00.000Z", spec=spec)
        self.assertEqual(sat["trade_date"], "2025-11-21")
        # Thanksgiving afternoon -> resolves Wednesday 2025-11-26
        thx = resolve_prior_close_session("2025-11-27T22:00:00.000Z", spec=spec)
        self.assertEqual(thx["trade_date"], "2025-11-26")
        # Half-day 2025-11-28 after 13:00 EST (18:00Z) -> resolves 2025-11-28
        half_after = resolve_prior_close_session("2025-11-28T18:05:00.000Z", spec=spec)
        self.assertEqual(half_after["trade_date"], "2025-11-28")

    def test_classify_session_window_checks_closure_and_date_before_overnight_hours(self) -> None:
        spec = load_spec(SPEC_DIR)
        sat_sess = resolve_session("XNYS", "2025-11-22", spec=spec)
        hol_sess = resolve_session("XNYS", "2025-12-25", spec=spec)
        mon_sess = resolve_session("XNYS", "2025-11-24", spec=spec)
        self.assertEqual(classify_us_equity_session_window("2025-11-23T02:00:00.000Z", sat_sess), "closed")
        self.assertEqual(classify_us_equity_session_window("2025-12-26T02:00:00.000Z", hol_sess), "closed")
        self.assertEqual(classify_us_equity_session_window("2025-11-25T06:00:00.000Z", mon_sess), "next_trade_date_window")
        self.assertEqual(classify_us_equity_session_window("2025-11-25T02:15:00.000Z", mon_sess), "extended_overnight_open")


if __name__ == "__main__":
    unittest.main()
