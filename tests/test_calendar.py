"""Unit tests for C-SA-22 provisional calendar spec and consumer."""

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

from shadow.calendar_consumer import CalendarDigestError, load_spec, resolve_session

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


if __name__ == "__main__":
    unittest.main()
