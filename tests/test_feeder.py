"""Unit tests for C-SH-MKT feeder fixtures, raw-byte hashing, revisions, and Y2 reconstruction."""

from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

_SRC = str(Path(__file__).resolve().parents[1] / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from shadow.calendar_consumer import resolve_session
from shadow.feeder import (
    FeederIntegrityError,
    SOXL_PROXY_LABEL,
    SeriesSpec,
    VINTAGE_Y2,
    _parse_y2_csv_bytes,
    emit_market_records_from_fixtures,
    emit_market_records_from_payloads,
    emit_y2_reconstruction_records,
    expected_prior_sofr_business_day,
    read_verified_archived_bytes,
    run_reconstruction_window,
    select_nyfed_sofr_entry,
    select_tiingo_row,
    select_treasury_tbill_row,
    select_yahoo_close,
    validate_official_close_market_record,
    verify_market_record_raw_integrity,
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
                retrieval_time_utc="2025-11-21T21:30:00.000Z",
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
                retrieval_time_utc="2025-11-21T21:35:00.000Z",
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

    def test_normalization_honesty_t_minus_1_sofr_ca_fields_and_yahoo_timestamps(self) -> None:
        # Item #10 & #2: SOFR requires effectiveDate < session_date (strict T-1)
        same_day_only = json.dumps(
            {"refRates": [{"effectiveDate": "2025-11-24", "type": "SOFR", "percentRate": 3.95}]}
        ).encode("utf-8")
        with self.assertRaises(ValueError):
            select_nyfed_sofr_entry(same_day_only, "2025-11-24")

        with_prior = json.dumps(
            {
                "refRates": [
                    {"effectiveDate": "2025-11-21", "type": "SOFR", "percentRate": 3.92},
                    {"effectiveDate": "2025-11-24", "type": "SOFR", "percentRate": 3.95},
                ]
            }
        ).encode("utf-8")
        picked = select_nyfed_sofr_entry(with_prior, "2025-11-24")
        self.assertEqual(picked["effectiveDate"], "2025-11-21")

        # Item #10: Tiingo refuses synthesizing missing divCash or splitFactor
        missing_ca_bytes = json.dumps([{"date": "2025-11-21T00:00:00.000Z", "close": 50.0}]).encode("utf-8")
        with self.assertRaises(ValueError) as ctx_ca:
            select_tiingo_row(missing_ca_bytes, "2025-11-21")
        self.assertEqual(str(ctx_ca.exception), "tiingo_missing_corporate_action_fields")

        # Daily Tiingo row without publication timestamp leaves source_publication_time_utc=None
        valid_tiingo = json.dumps(
            [{"date": "2025-11-21T00:00:00.000Z", "close": 50.0, "divCash": 0.0, "splitFactor": 1.0}]
        ).encode("utf-8")
        with tempfile.TemporaryDirectory() as tmpdir:
            recs = emit_market_records_from_payloads(
                [("TQQQ", "TQQQ", "etf_tiingo"), ("SOXL", "SOXL", "etf_tiingo")],
                {"TQQQ": valid_tiingo, "SOXL": missing_ca_bytes},
                session_date="2025-11-21",
                retrieval_time_utc="2025-11-21T21:30:00.000Z",
                archive_dir=Path(tmpdir) / "arch",
                vintage="recorded_fixture",
                observation_class="synthetic",
                provider_for={"TQQQ": "tiingo_fixture", "SOXL": "tiingo_fixture"},
                missing_reason_for=lambda inst: f"missing:{inst}",
            )
            self.assertIsNone(recs[0]["source_publication_time_utc"])
            self.assertFalse(recs[1]["eligibility"]["eligible"])
            self.assertEqual(recs[1]["eligibility"]["reason"], "tiingo_missing_corporate_action_fields")

            # Pre-close call MUST fail session_close_utc lower bound even when session_close_utc param is omitted
            pre_close_recs = emit_market_records_from_payloads(
                [("TQQQ", "TQQQ", "etf_tiingo")],
                {"TQQQ": valid_tiingo},
                session_date="2025-11-21",
                retrieval_time_utc="2025-11-21T20:55:00.000Z",
                archive_dir=Path(tmpdir) / "arch_pre",
                vintage="recorded_fixture",
                observation_class="synthetic",
                provider_for={"TQQQ": "tiingo_fixture"},
                missing_reason_for=lambda inst: f"missing:{inst}",
            )
            self.assertFalse(pre_close_recs[0]["eligibility"]["eligible"])
            self.assertEqual(pre_close_recs[0]["eligibility"]["reason"], "session_not_yet_closed")

        # Item #2/#12: Yahoo without bar timestamps is rejected even with 1 close value
        no_ts_yahoo = json.dumps(
            {"chart": {"result": [{"timestamp": [], "indicators": {"quote": [{"close": [29500.0]}]}}]}}
        ).encode("utf-8")
        with self.assertRaises(ValueError):
            select_yahoo_close(no_ts_yahoo, "2025-11-21")

        # R3 #7 regression: SOFR T-1 business day checks & stale SOFR rejection
        self.assertEqual(expected_prior_sofr_business_day("2025-11-24"), "2025-11-21")
        # US federal bond-only holidays: Veterans Day (2025-11-11 Tue) & Columbus Day (2025-10-13 Mon)
        self.assertEqual(expected_prior_sofr_business_day("2025-11-12"), "2025-11-10")
        self.assertEqual(expected_prior_sofr_business_day("2025-10-14"), "2025-10-10")

        sofr_series = [("SOFR", "SOFR", "rate_sofr")]
        stale_sofr_payload = json.dumps(
            {"refRates": [{"effectiveDate": "2025-11-17", "type": "SOFR", "percentRate": 3.91, "revisionIndicator": ""}]}
        ).encode("utf-8")
        fresh_sofr_payload = json.dumps(
            {"refRates": [{"effectiveDate": "2025-11-21", "type": "SOFR", "percentRate": 3.92, "revisionIndicator": ""}]}
        ).encode("utf-8")
        bond_gap_payload = json.dumps(
            {"refRates": [{"effectiveDate": "2025-11-10", "type": "SOFR", "percentRate": 3.95, "revisionIndicator": ""}]}
        ).encode("utf-8")
        with tempfile.TemporaryDirectory() as tmpdir:
            stale_rec = emit_market_records_from_payloads(
                sofr_series,
                {"SOFR": stale_sofr_payload},
                session_date="2025-11-24",
                retrieval_time_utc="2025-11-24T21:30:00.000Z",
                archive_dir=Path(tmpdir) / "sofr_stale",
                vintage="live_retrieval",
                observation_class="live",
                provider_for={"SOFR": "nyfed_sofr"},
                missing_reason_for=lambda inst: f"missing:{inst}",
            )[0]
            self.assertFalse(stale_rec["eligibility"]["eligible"])
            self.assertEqual(stale_rec["eligibility"]["reason"], "sofr_stale_effective_date")
            self.assertEqual(stale_rec["disclosures"]["vintage_lag"], "stale")
            self.assertEqual(stale_rec["disclosures"]["actual_effective_date"], "2025-11-17")
            self.assertEqual(stale_rec["disclosures"]["expected_effective_date"], "2025-11-21")

            fresh_rec = emit_market_records_from_payloads(
                sofr_series,
                {"SOFR": fresh_sofr_payload},
                session_date="2025-11-24",
                retrieval_time_utc="2025-11-24T21:30:00.000Z",
                archive_dir=Path(tmpdir) / "sofr_fresh",
                vintage="live_retrieval",
                observation_class="live",
                provider_for={"SOFR": "nyfed_sofr"},
                missing_reason_for=lambda inst: f"missing:{inst}",
            )[0]
            self.assertTrue(fresh_rec["eligibility"]["eligible"])
            self.assertEqual(fresh_rec["disclosures"]["vintage_lag"], "T-1")

            bond_gap_rec = emit_market_records_from_payloads(
                sofr_series,
                {"SOFR": bond_gap_payload},
                session_date="2025-11-12",
                retrieval_time_utc="2025-11-12T21:30:00.000Z",
                archive_dir=Path(tmpdir) / "sofr_bond",
                vintage="live_retrieval",
                observation_class="live",
                provider_for={"SOFR": "nyfed_sofr"},
                missing_reason_for=lambda inst: f"missing:{inst}",
            )[0]
            self.assertTrue(bond_gap_rec["eligibility"]["eligible"])
            self.assertEqual(bond_gap_rec["disclosures"]["vintage_lag"], "T-1")
            self.assertEqual(bond_gap_rec["disclosures"]["expected_effective_date"], "2025-11-10")

            # R4 #7 regression: XNYS unscheduled closure (2025-01-09 Carter Mourning) STILL publishes NY Fed SOFR
            self.assertEqual(expected_prior_sofr_business_day("2025-01-10"), "2025-01-09")
            jan09_sofr_payload = json.dumps(
                {"refRates": [{"effectiveDate": "2025-01-09", "type": "SOFR", "percentRate": 4.29, "revisionIndicator": ""}]}
            ).encode("utf-8")
            jan08_stale_payload = json.dumps(
                {"refRates": [{"effectiveDate": "2025-01-08", "type": "SOFR", "percentRate": 4.30, "revisionIndicator": ""}]}
            ).encode("utf-8")
            jan09_rec = emit_market_records_from_payloads(
                sofr_series,
                {"SOFR": jan09_sofr_payload},
                session_date="2025-01-10",
                retrieval_time_utc="2025-01-10T21:30:00.000Z",
                archive_dir=Path(tmpdir) / "sofr_jan09",
                vintage="live_retrieval",
                observation_class="live",
                provider_for={"SOFR": "nyfed_sofr"},
                missing_reason_for=lambda inst: f"missing:{inst}",
            )[0]
            self.assertTrue(jan09_rec["eligibility"]["eligible"])
            self.assertEqual(jan09_rec["disclosures"]["vintage_lag"], "T-1")
            self.assertEqual(jan09_rec["disclosures"]["expected_effective_date"], "2025-01-09")

            jan08_rec = emit_market_records_from_payloads(
                sofr_series,
                {"SOFR": jan08_stale_payload},
                session_date="2025-01-10",
                retrieval_time_utc="2025-01-10T21:30:00.000Z",
                archive_dir=Path(tmpdir) / "sofr_jan08_stale",
                vintage="live_retrieval",
                observation_class="live",
                provider_for={"SOFR": "nyfed_sofr"},
                missing_reason_for=lambda inst: f"missing:{inst}",
            )[0]
            self.assertFalse(jan08_rec["eligibility"]["eligible"])
            self.assertEqual(jan08_rec["eligibility"]["reason"], "sofr_stale_effective_date")
            self.assertEqual(jan08_rec["disclosures"]["expected_effective_date"], "2025-01-09")

    def test_parsers_reject_nonnumeric_and_nonfinite_values(self) -> None:
        """Close PR #6 P2: reject nonnumeric/nonfinite Tiingo & sibling market fields cleanly."""
        for bad_row in (
            {"date": "2025-11-21T00:00:00.000Z", "close": 47.48, "divCash": 0.0, "splitFactor": "N/A"},
            {"date": "2025-11-21T00:00:00.000Z", "close": "NaN", "divCash": 0.0, "splitFactor": 1.0},
            {"date": "2025-11-21T00:00:00.000Z", "close": 47.48, "divCash": "Infinity", "splitFactor": 1.0},
            {"date": "2025-11-21T00:00:00.000Z", "close": True, "divCash": 0.0, "splitFactor": 1.0},
        ):
            with self.assertRaises(ValueError):
                select_tiingo_row(json.dumps([bad_row]).encode("utf-8"), "2025-11-21")

        bad_yahoo = json.dumps(
            {
                "chart": {
                    "result": [
                        {
                            "timestamp": [int(datetime(2025, 11, 21, 21, 0, tzinfo=timezone.utc).timestamp())],
                            "indicators": {"quote": [{"close": ["NaN"]}]},
                        }
                    ]
                }
            }
        ).encode("utf-8")
        with self.assertRaises(ValueError):
            select_yahoo_close(bad_yahoo, "2025-11-21")

        bad_sofr = json.dumps(
            {"refRates": [{"effectiveDate": "2025-11-20", "type": "SOFR", "percentRate": "N/A"}]}
        ).encode("utf-8")
        with self.assertRaises(ValueError):
            select_nyfed_sofr_entry(bad_sofr, "2025-11-21")

        bad_treasury = b"Date,3 Mo\n2025-11-20,invalid_rate\n"
        with self.assertRaises(ValueError):
            select_treasury_tbill_row(bad_treasury, "2025-11-21")

    def test_tiingo_rejects_non_utc_midnight_marker_and_naive_publication_timestamp(self) -> None:
        non_utc_midnight = json.dumps(
            [{"date": "2025-11-21T00:00:00+09:00", "close": 47.48, "divCash": 0.0, "splitFactor": 1.0}]
        ).encode("utf-8")
        with self.assertRaisesRegex(ValueError, "session_not_in_payload"):
            select_tiingo_row(non_utc_midnight, "2025-11-21")

        naive_pub = json.dumps(
            [
                {
                    "date": "2025-11-21T00:00:00.000Z",
                    "close": 47.48,
                    "divCash": 0.0,
                    "splitFactor": 1.0,
                    "publicationTimestamp": "2025-11-21T16:00:00",
                }
            ]
        ).encode("utf-8")
        with self.assertRaisesRegex(ValueError, "tiingo_invalid_timestamp"):
            select_tiingo_row(naive_pub, "2025-11-21")

    def test_verify_market_record_raw_integrity_uses_half_day_calendar(self) -> None:
        cal = resolve_session("XNYS", "2025-11-28")
        self.assertEqual(cal["session_state"], "half_day")
        yahoo_payload = json.dumps(
            {
                "chart": {
                    "result": [
                        {
                            "meta": {"dataGranularity": "1d"},
                            "timestamp": [
                                int(datetime(2025, 11, 28, 18, 0, tzinfo=timezone.utc).timestamp()),  # 13:00 EST
                                int(datetime(2025, 11, 28, 19, 0, tzinfo=timezone.utc).timestamp()),  # 14:00 EST
                            ],
                            "indicators": {"quote": [{"close": [110.0, 120.0]}]},
                        }
                    ]
                }
            }
        ).encode("utf-8")
        with tempfile.TemporaryDirectory() as tmpdir:
            arch = Path(tmpdir) / "arch"
            rec = emit_market_records_from_payloads(
                (SeriesSpec("XNDX", "^XNDX", "idx_yahoo"),),
                {"XNDX": yahoo_payload},
                session_date="2025-11-28",
                retrieval_time_utc="2025-11-28T19:30:00.000Z",
                archive_dir=arch,
                vintage="live_retrieval",
                observation_class="live",
                provider_for={"XNDX": "yahoo"},
                missing_reason_for=lambda inst: f"missing:{inst}",
            )[0]
            self.assertEqual(rec["normalized_value"], "110")
            ok_with_cal, err_with_cal = verify_market_record_raw_integrity(
                arch, rec, session_date="2025-11-28", calendar_session=cal
            )
            self.assertTrue(ok_with_cal, err_with_cal)

    def test_tiingo_and_record_reject_next_trade_date_window_and_closed_markers(self) -> None:
        cal = resolve_session("XNYS", "2025-11-21")
        for marker, expected_err in (
            ("next_trade_date_window", "next_trade_date_contamination"),
            ("closed", "extended_hours_overnight_rejected"),
        ):
            payload = json.dumps(
                [
                    {
                        "date": "2025-11-21T00:00:00.000Z",
                        "close": 47.48,
                        "divCash": 0.0,
                        "splitFactor": 1.0,
                        "session_window": marker,
                    }
                ]
            ).encode("utf-8")
            with self.assertRaisesRegex(ValueError, expected_err):
                select_tiingo_row(payload, "2025-11-21", calendar_session=cal)

            ok_rec, err_rec = validate_official_close_market_record(
                {
                    "instrument": "TQQQ",
                    "effective_session": "2025-11-21",
                    "disclosures": {"session_window": marker},
                },
                cal,
            )
            self.assertFalse(ok_rec)
            self.assertEqual(err_rec, expected_err)

    def test_tiingo_rejects_blank_or_numeric_timestamp_and_parses_offset_session_time(self) -> None:
        for bad_ts in ("", "   ", 1763762400):
            payload = json.dumps(
                [
                    {
                        "date": "2025-11-21T00:00:00.000Z",
                        "close": 47.48,
                        "divCash": 0.0,
                        "splitFactor": 1.0,
                        "timestamp": bad_ts,
                    }
                ]
            ).encode("utf-8")
            with self.assertRaisesRegex(ValueError, "tiingo_invalid_timestamp"):
                select_tiingo_row(payload, "2025-11-21")

        half_cal = resolve_session("XNYS", "2025-11-28")
        ok_offset = json.dumps(
            [
                {
                    "date": "2025-11-28T00:00:00.000Z",
                    "close": 50.0,
                    "divCash": 0.0,
                    "splitFactor": 1.0,
                    "session_time": "13:00:00-05:00",
                }
            ]
        ).encode("utf-8")
        self.assertEqual(
            select_tiingo_row(ok_offset, "2025-11-28", calendar_session=half_cal)["close"],
            50.0,
        )

        late_offset = json.dumps(
            [
                {
                    "date": "2025-11-28T00:00:00.000Z",
                    "close": 50.0,
                    "divCash": 0.0,
                    "splitFactor": 1.0,
                    "session_time": "14:00:00-05:00",
                }
            ]
        ).encode("utf-8")
        with self.assertRaisesRegex(ValueError, "extended_hours_overnight_rejected"):
            select_tiingo_row(late_offset, "2025-11-28", calendar_session=half_cal)


if __name__ == "__main__":
    unittest.main()
