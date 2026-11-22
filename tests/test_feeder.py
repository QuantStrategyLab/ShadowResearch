"""Unit tests for C-SH-MKT feeder fixtures, raw-byte hashing, revisions, and Y2 reconstruction."""

from __future__ import annotations

import hashlib
import sys
import tempfile
import unittest
from pathlib import Path

_SRC = str(Path(__file__).resolve().parents[1] / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from shadow.feeder import (
    FeederIntegrityError,
    SOXL_PROXY_LABEL,
    VINTAGE_Y2,
    _parse_y2_csv_bytes,
    emit_market_records_from_fixtures,
    emit_y2_reconstruction_records,
    read_verified_archived_bytes,
    run_reconstruction_window,
)
from shadow.ledger import check_forward_clock_credit

REPO_ROOT = Path(__file__).resolve().parents[1]
FEEDER_FIXTURES = REPO_ROOT / "tests" / "fixtures" / "feeder"
Y2_OVERLAP_DIR = REPO_ROOT / "tests" / "fixtures" / "y2overlap"


class FeederTest(unittest.TestCase):
    """Verify fixture archival, digest integrity, revisions, and Y2 reconstruction."""

    def test_recorded_fixtures_raw_bytes_and_sofr_revision(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_dir = Path(tmpdir) / "archive"
            recs_v0 = emit_market_records_from_fixtures(
                fixture_dir=FEEDER_FIXTURES,
                session_date="2025-11-21",
                retrieval_time_utc="2025-11-21T20:55:00.000Z",
                archive_dir=archive_dir,
            )
            self.assertEqual(len(recs_v0), 6)
            for rec in recs_v0:
                self.assertTrue(rec["eligibility"]["eligible"], rec)
                raw = read_verified_archived_bytes(
                    archive_dir,
                    rec["raw_bytes_ref"],
                    rec["raw_bytes_sha256"],
                )
                self.assertEqual(hashlib.sha256(raw).hexdigest(), rec["raw_bytes_sha256"])

            by_inst = {r["instrument"]: r for r in recs_v0}
            self.assertEqual(by_inst["SOXL"]["price_basis"], "close (proxy, NAV unverified)")
            self.assertTrue(by_inst["SEMI"]["disclosures"]["xsox_for_icesemit_substitution"])
            self.assertEqual(by_inst["SOFR"]["normalized_value"], "0.0391")
            self.assertEqual(by_inst["SOFR"]["disclosures"]["accrual_basis"], "days/365")

            # Revision appends a new record with a new digest and preserves original v0 raw bytes
            recs_v1 = emit_market_records_from_fixtures(
                fixture_dir=FEEDER_FIXTURES,
                session_date="2025-11-21",
                retrieval_time_utc="2025-11-21T20:58:00.000Z",
                archive_dir=archive_dir,
                revision_seq=1,
                sofr_filename="nyfed_sofr_2025-11-21_rev.json",
            )
            sofr_v1 = next(r for r in recs_v1 if r["instrument"] == "SOFR")
            self.assertEqual(sofr_v1["normalized_value"], "0.0393")
            self.assertEqual(sofr_v1["disclosures"]["revision_indicator"], "R")
            self.assertNotEqual(sofr_v1["raw_bytes_sha256"], by_inst["SOFR"]["raw_bytes_sha256"])

            # Tampering archived raw bytes fails SHA-256 verification
            tampered_path = archive_dir / by_inst["TQQQ"]["raw_bytes_ref"]
            tampered_path.write_bytes(b"corrupted")
            with self.assertRaises(FeederIntegrityError):
                read_verified_archived_bytes(
                    archive_dir,
                    by_inst["TQQQ"]["raw_bytes_ref"],
                    by_inst["TQQQ"]["raw_bytes_sha256"],
                )

    def test_y2_overlap_reconstruction_and_modelled_flag(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            res = run_reconstruction_window(
                y2_dir=Y2_OVERLAP_DIR,
                ledger_dir=root / "ledger",
                archive_dir=root / "archive",
            )
            self.assertEqual(res["session_count"], 10)
            ledger = res["ledger"]
            records = ledger.load_records()
            receipts = ledger.load_receipts()
            self.assertEqual(len(records), 10)

            for env in records:
                body = env["record_body"]
                self.assertEqual(body["mode"], "reconstruction")
                self.assertEqual(body["observation_class"], "historical_reconstruction")
                labels = body["discipline_labels"]
                self.assertEqual(labels["vintage_qualification"], VINTAGE_Y2)
                self.assertEqual(labels["soxl_basis"], SOXL_PROXY_LABEL)
                self.assertEqual(labels["semi_substitution"], "native_xsox_sub_for_icesemit")
                self.assertFalse(labels["forward_clock_credit_eligible"])
                self.assertFalse(labels["per_series_is_modelled"]["XNDX"])
                self.assertFalse(labels["per_series_is_modelled"]["SEMI"])

            eligible, reasons = check_forward_clock_credit(records, receipts)
            self.assertFalse(eligible)
            self.assertTrue(len(reasons) > 0)

    def test_pre_tr_modelled_source_row_preserves_is_modelled_true(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            synth_y2 = Path(tmpdir) / "y2_modelled"
            synth_y2.mkdir()
            # Copy 2025 structure for TQQQ/SOXL/RF and synthesize a pre-TR modelled row on 2025-11-14
            for fname in ("tqqq_daily.csv", "soxl_daily.csv", "risk_free_rates_daily.csv"):
                (synth_y2 / fname).write_bytes((Y2_OVERLAP_DIR / fname).read_bytes())

            ndx_lines = (Y2_OVERLAP_DIR / "ndx_index_daily.csv").read_text(encoding="utf-8").splitlines()
            semi_lines = (Y2_OVERLAP_DIR / "semi_index_daily.csv").read_text(encoding="utf-8").splitlines()
            # Replace first data row's is_modelled=false with true
            ndx_hdr_idx = next(i for i, ln in enumerate(ndx_lines) if ln.startswith("date,"))
            semi_hdr_idx = next(i for i, ln in enumerate(semi_lines) if ln.startswith("date,"))
            ndx_lines[ndx_hdr_idx + 1] = ndx_lines[ndx_hdr_idx + 1].replace(",false,", ",true,")
            semi_lines[semi_hdr_idx + 1] = semi_lines[semi_hdr_idx + 1].replace(",false,", ",true,")
            (synth_y2 / "ndx_index_daily.csv").write_text("\n".join(ndx_lines) + "\n", encoding="utf-8")
            (synth_y2 / "semi_index_daily.csv").write_text("\n".join(semi_lines) + "\n", encoding="utf-8")

            recs = emit_y2_reconstruction_records(synth_y2, Path(tmpdir) / "arch")
            first_day = {r["instrument"]: r for r in recs["2025-11-14"]}
            self.assertTrue(first_day["XNDX"]["is_modelled"])
            self.assertTrue(first_day["SEMI"]["is_modelled"])
            second_day = {r["instrument"]: r for r in recs["2025-11-17"]}
            self.assertFalse(second_day["XNDX"]["is_modelled"])


if __name__ == "__main__":
    unittest.main()
