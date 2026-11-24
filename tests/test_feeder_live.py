"""Offline unit and end-to-end tests for live C-SH-MKT source adapters."""

from __future__ import annotations

import base64
import copy
import hashlib
import http.server
import json
import re
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

_SRC = str(Path(__file__).resolve().parents[1] / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from shadow.calendar_consumer import DEFAULT_SPEC_DIR, load_spec
from shadow.feeder import (
    DEMO_SERIES,
    SOXL_PROXY_LABEL,
    emit_market_records_from_payloads,
    read_verified_archived_bytes,
)
from shadow.feeder_live import (
    BROWSER_USER_AGENT,
    TREASURY_ENDPOINT_VERIFICATION,
    YAHOO_SYMBOL_VERIFICATION,
    CredentialAbsentError,
    FredDtb3Adapter,
    InstrumentMismatchError,
    NyFedSofrAdapter,
    SessionNotInPayloadError,
    SourceChallengeGatedError,
    SourceThrottledError,
    SourceUnavailableError,
    StooqDailyAdapter,
    TiingoDailyAdapter,
    TreasuryYieldAdapter,
    YahooChartAdapter,
    _NoCrossHostAuthRedirectHandler,
    emit_live_market_records,
    urllib_fetch_bytes,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
LIVE_FIXTURES = REPO_ROOT / "tests" / "fixtures" / "live"


class FakeClock:
    """Deterministic monotonic clock + sleeper pair for spacing/backoff tests."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _build_valid_synthetic_2026_calendar(root: Path) -> dict:
    """Create a digest-verified synthetic calendar package for 2026 fixture unit tests."""
    spec_dir = root / "synthetic_cal_2026"
    spec_dir.mkdir(parents=True, exist_ok=True)
    spec_md = (DEFAULT_SPEC_DIR / "SPEC.md").read_bytes()
    orig_exc = json.loads((DEFAULT_SPEC_DIR / "exceptions-2025.json").read_text(encoding="utf-8"))
    syn_exc = {
        "calendar_version": "synthetic-test-2026",
        "exchange": orig_exc["exchange"],
        "timezone": orig_exc["timezone"],
        "coverage_start_date": "2026-01-01",
        "coverage_end_date": "2026-12-31",
        "regular_open_local": orig_exc["regular_open_local"],
        "regular_close_local": orig_exc["regular_close_local"],
        "holidays": {"2026-11-26": "Thanksgiving Day"},
        "early_closes": {"2026-11-27": {"name": "Day After Thanksgiving", "close_local": "13:00"}},
        "unscheduled_closures": {},
    }
    exc_bytes = json.dumps(syn_exc, indent=2, sort_keys=True).encode("utf-8")
    (spec_dir / "SPEC.md").write_bytes(spec_md)
    (spec_dir / "exceptions-2026-synthetic.json").write_bytes(exc_bytes)
    manifest = {
        "calendar_version": "synthetic-test-2026",
        "spec_digest": hashlib.sha256(spec_md + exc_bytes).hexdigest(),
        "files": {
            "SPEC.md": hashlib.sha256(spec_md).hexdigest(),
            "exceptions-2026-synthetic.json": hashlib.sha256(exc_bytes).hexdigest(),
        },
    }
    (spec_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return load_spec(spec_dir)


def _ticking_utc_clock(session_date: str, hhmmss: str = "22:30:00", step_seconds: int = 1) -> Callable[[], datetime]:
    base = datetime.fromisoformat(f"{session_date}T{hhmmss}+00:00")
    state = {"n": 0}

    def _next() -> datetime:
        state["n"] += 1
        return base + timedelta(seconds=step_seconds * state["n"])

    return _next


def _yahoo_index_payload(symbol: str, session_date: str, close_val: float) -> bytes:
    dt = datetime.fromisoformat(f"{session_date}T21:00:00+00:00")
    ts = int(dt.replace(tzinfo=timezone.utc).timestamp())
    return json.dumps(
        {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "symbol": symbol,
                            "instrumentType": "INDEX",
                            "currency": "USD",
                        },
                        "timestamp": [ts],
                        "indicators": {"quote": [{"close": [close_val]}]},
                    }
                ],
                "error": None,
            }
        }
    ).encode("utf-8")


class LiveFeederAdaptersTest(unittest.TestCase):
    """Offline tests using real A02 probe captures and scripted transports."""

    def test_tiingo_missing_key_emits_credential_absent_without_transport(self) -> None:
        calls: list[tuple[str, Mapping[str, str]]] = []

        def failing_transport(url: str, headers: Mapping[str, str]) -> tuple[int, bytes]:
            calls.append((url, dict(headers)))
            return 200, b"[]"

        adapter = TiingoDailyAdapter(transport=failing_transport, environ={})
        with self.assertRaises(CredentialAbsentError):
            adapter.fetch_daily("TQQQ", "2025-11-21")
        self.assertEqual(calls, [])

        with tempfile.TemporaryDirectory() as tmpdir:
            clk = FakeClock()
            records = emit_live_market_records(
                session_date="2025-11-21",
                archive_dir=Path(tmpdir) / "arch",
                transport=failing_transport,
                environ={},
                clock=clk.clock,
                sleeper=clk.sleep,
                utc_clock=_ticking_utc_clock("2025-11-21"),
            )
            by_inst = {r["instrument"]: r for r in records}
            for inst in ("TQQQ", "SOXL", "QQQ"):
                self.assertEqual(by_inst[inst]["provider"], "tiingo")
                self.assertIsNone(by_inst[inst]["normalized_value"])
                self.assertFalse(by_inst[inst]["eligibility"]["eligible"])
                self.assertEqual(by_inst[inst]["eligibility"]["reason"], "credential_absent")
            # Tiingo never called transport
            self.assertFalse(any("tiingo.com" in u for u, _ in calls))

    def test_tiingo_payload_selects_session_with_div_and_split_and_missing_session(self) -> None:
        seen_urls: list[str] = []
        seen_headers: list[dict[str, str]] = []
        payload_by_ticker = {
            "TQQQ": json.dumps(
                [
                    {
                        "date": "2025-11-19T00:00:00.000Z",
                        "close": 94.0,
                        "divCash": 0.0,
                        "splitFactor": 1.0,
                    },
                    {
                        "date": "2025-11-20T00:00:00.000Z",
                        "close": 47.25,
                        "divCash": 0.125,
                        "splitFactor": 2.0,
                    },
                ]
            ).encode("utf-8"),
            "SOXL": json.dumps(
                [
                    {
                        "date": "2025-11-20T00:00:00.000Z",
                        "close": 31.73,
                        "divCash": 0.0,
                        "splitFactor": 1.0,
                    }
                ]
            ).encode("utf-8"),
            "QQQ": json.dumps(
                [
                    {
                        "date": "2025-11-20T00:00:00.000Z",
                        "close": 510.4,
                        "divCash": 0.65,
                        "splitFactor": 1.0,
                    }
                ]
            ).encode("utf-8"),
        }

        def scripted(url: str, headers: Mapping[str, str]) -> tuple[int, bytes]:
            seen_urls.append(url)
            seen_headers.append(dict(headers))
            for t, body in payload_by_ticker.items():
                if f"/daily/{t}/prices" in url:
                    return 200, body
            return 404, b"not found"

        token = "dummy-tiingo-token-for-offline-test"
        adapter = TiingoDailyAdapter(transport=scripted, environ={"TIINGO_API_KEY": token})
        tqqq_row = adapter.fetch_daily("TQQQ", "2025-11-20", lookback_days=2)
        self.assertEqual(tqqq_row["close"], 47.25)
        self.assertEqual(tqqq_row["divCash"], 0.125)
        self.assertEqual(tqqq_row["splitFactor"], 2.0)
        self.assertNotIn(token, seen_urls[0])
        self.assertEqual(seen_headers[0]["Authorization"], f"Token {token}")

        with self.assertRaises(SessionNotInPayloadError):
            adapter.fetch_daily("SOXL", "2025-11-21")

        with tempfile.TemporaryDirectory() as tmpdir:
            clk = FakeClock()
            records = emit_live_market_records(
                session_date="2025-11-20",
                archive_dir=Path(tmpdir) / "arch",
                transport=scripted,
                environ={"TIINGO_API_KEY": token},
                clock=clk.clock,
                sleeper=clk.sleep,
                utc_clock=_ticking_utc_clock("2025-11-20"),
            )
            by_inst = {r["instrument"]: r for r in records}
            self.assertEqual(by_inst["TQQQ"]["normalized_value"], "47.25")
            self.assertEqual(by_inst["TQQQ"]["corporate_action"]["cash_per_share_usd"], "0.125")
            self.assertEqual(by_inst["TQQQ"]["corporate_action"]["split_ratio"], "2")
            self.assertEqual(by_inst["SOXL"]["normalized_value"], "31.73")
            self.assertEqual(by_inst["SOXL"]["disclosures"]["proxy_label"], SOXL_PROXY_LABEL)
            self.assertEqual(by_inst["QQQ"]["normalized_value"], "510.4")
            self.assertEqual(by_inst["QQQ"]["corporate_action"]["cash_per_share_usd"], "0.65")
            self.assertTrue(by_inst["TQQQ"]["eligibility"]["eligible"])
            self.assertTrue(by_inst["SOXL"]["eligibility"]["eligible"])
            self.assertTrue(by_inst["QQQ"]["eligibility"]["eligible"])

    def test_yahoo_real_etf_capture_rejected_as_instrument_mismatch(self) -> None:
        self.assertEqual(YAHOO_SYMBOL_VERIFICATION, "unverified")
        etf_body = (LIVE_FIXTURES / "yahoo_xndx_etf_2026-11-21.json").read_bytes()
        seen_urls: list[str] = []

        def transport(url: str, headers: Mapping[str, str]) -> tuple[int, bytes]:
            seen_urls.append(url)
            self.assertEqual(headers.get("User-Agent"), BROWSER_USER_AGENT)
            return 200, etf_body

        adapter = YahooChartAdapter(transport=transport)
        with self.assertRaises(InstrumentMismatchError):
            adapter.fetch_daily("XNDX", "2026-11-20")
        self.assertEqual(
            seen_urls[0],
            "https://query1.finance.yahoo.com/v8/finance/chart/%5EXNDX?range=5d&interval=1d",
        )

        # Bare symbol mapping for XNDX is also rejected before transport
        bare_adapter = YahooChartAdapter(
            transport=transport,
            symbol_map={"XNDX": "XNDX", "XSOX": "XSOX"},
        )
        with self.assertRaises(InstrumentMismatchError):
            bare_adapter.fetch_daily("XNDX", "2026-11-20")

        with tempfile.TemporaryDirectory() as tmpdir:
            clk = FakeClock()
            records = emit_live_market_records(
                session_date="2026-11-20",
                archive_dir=Path(tmpdir) / "arch",
                transport=transport,
                environ={},
                clock=clk.clock,
                sleeper=clk.sleep,
                utc_clock=_ticking_utc_clock("2026-11-20"),
                calendar_spec=_build_valid_synthetic_2026_calendar(Path(tmpdir)),
            )
            xndx = next(r for r in records if r["instrument"] == "XNDX")
            self.assertIsNone(xndx["normalized_value"])
            self.assertEqual(xndx["eligibility"]["eligible"], False)
            self.assertEqual(xndx["eligibility"]["reason"], "instrument_mismatch")

    def test_yahoo_spacing_and_429_retry_with_fake_clock(self) -> None:
        clk = FakeClock(start=100.0)
        attempts: list[tuple[str, float]] = []
        xndx_ok = _yahoo_index_payload("^XNDX", "2026-11-20", 29500.5)
        xsox_ok = _yahoo_index_payload("XSOX", "2026-11-20", 8200.25)

        def transport(url: str, headers: Mapping[str, str]) -> tuple[int, bytes]:
            attempts.append((url, clk.clock()))
            if "%5EXNDX" in url:
                return 200, xndx_ok
            # First XSOX request returns 429, second succeeds
            xsox_calls = sum(1 for u, _ in attempts if "XSOX" in u)
            if xsox_calls == 1:
                return 429, b"Edge: Too Many Requests"
            return 200, xsox_ok

        adapter = YahooChartAdapter(
            transport=transport,
            clock=clk.clock,
            sleeper=clk.sleep,
        )
        v1 = adapter.fetch_daily("XNDX", "2026-11-20")
        v2 = adapter.fetch_daily("XSOX", "2026-11-20")
        self.assertEqual(v1, Decimal("29500.5"))
        self.assertEqual(v2, Decimal("8200.25"))
        # 20s spacing before XSOX + 30s backoff after 429
        self.assertEqual(clk.sleeps, [20.0, 30.0])
        self.assertEqual([t for _, t in attempts], [100.0, 120.0, 150.0])

        # Persistent 429 after single retry raises SourceThrottledError
        clk2 = FakeClock(start=0.0)
        throttled_calls: list[str] = []

        def always_429(url: str, headers: Mapping[str, str]) -> tuple[int, bytes]:
            throttled_calls.append(url)
            return 429, b"Edge: Too Many Requests"

        adapter2 = YahooChartAdapter(
            transport=always_429,
            clock=clk2.clock,
            sleeper=clk2.sleep,
        )
        with self.assertRaises(SourceThrottledError):
            adapter2.fetch_daily("XSOX", "2026-11-20")
        self.assertEqual(len(throttled_calls), 2)
        self.assertEqual(clk2.sleeps, [30.0])

    def test_nyfed_real_capture_returns_effective_2026_11_19_sofr(self) -> None:
        nyfed_body = (LIVE_FIXTURES / "nyfed_sofr_2026-11-21.json").read_bytes()
        seen_urls: list[str] = []

        def transport(url: str, headers: Mapping[str, str]) -> tuple[int, bytes]:
            seen_urls.append(url)
            return 200, nyfed_body

        adapter = NyFedSofrAdapter(transport=transport)
        parsed = adapter.fetch_daily("2026-11-20")
        self.assertIn("startDate=2026-11-06&endDate=2026-11-21", seen_urls[0])
        self.assertIn("typeCode=fr&sort=postDt:1", seen_urls[0])
        self.assertEqual(parsed["effective_date"], "2026-11-19")
        self.assertEqual(parsed["rate_decimal"], "0.0391")
        self.assertEqual(parsed["revision_indicator"], "")

        with tempfile.TemporaryDirectory() as tmpdir:
            clk = FakeClock()
            records = emit_live_market_records(
                session_date="2026-11-20",
                archive_dir=Path(tmpdir) / "arch",
                transport=transport,
                environ={},
                clock=clk.clock,
                sleeper=clk.sleep,
                utc_clock=_ticking_utc_clock("2026-11-20"),
                calendar_spec=_build_valid_synthetic_2026_calendar(Path(tmpdir)),
            )
            sofr = next(r for r in records if r["instrument"] == "SOFR")
            self.assertEqual(sofr["effective_session"], "2026-11-19")
            self.assertEqual(sofr["normalized_value"], "0.0391")
            self.assertEqual(sofr["disclosures"]["sofr_effective_lag"], "T-1")
            self.assertEqual(sofr["disclosures"]["revision_indicator"], "")
            self.assertTrue(sofr["eligibility"]["eligible"])

    def test_treasury_full_shape_and_trimmed_csv_and_unverified_endpoint(self) -> None:
        self.assertEqual(TREASURY_ENDPOINT_VERIFICATION, "unverified")
        calls: list[str] = []

        def transport(url: str, headers: Mapping[str, str]) -> tuple[int, bytes]:
            calls.append(url)
            if "full" in url:
                return (
                    200,
                    (
                        "Date,1 Mo,2 Mo,3 Mo,4 Mo,6 Mo,1 Yr,2 Yr,10 Yr,30 Yr\n"
                        "11/18/2026,4.22,4.19,4.17,4.15,4.12,4.05,3.95,4.10,4.35\n"
                        "11/19/2026,4.20,4.18,4.16,4.14,4.11,4.04,3.94,4.09,4.34\n"
                        "11/20/2026,4.21,4.19,N/A,4.15,4.12,4.05,3.96,4.11,4.36\n"
                    ).encode("utf-8"),
                )
            return 200, b"Date,3 Mo\n2026-11-19,4.16\n"

        # Default unverified endpoint fails closed without calling transport
        unverified = TreasuryYieldAdapter(transport=transport)
        with self.assertRaises(SourceUnavailableError) as ctx:
            unverified.fetch_daily("2026-11-20")
        self.assertIn("treasury_endpoint_unverified", str(ctx.exception))
        self.assertEqual(calls, [])

        # Override with full multi-column Treasury shape skips N/A row and picks 2026-11-19
        full_adapter = TreasuryYieldAdapter(
            transport=transport,
            endpoint="https://example.test/treasury/full_{year}.csv",
        )
        eff_full, val_full = full_adapter.fetch_daily("2026-11-20")
        self.assertEqual(eff_full, "2026-11-19")
        self.assertEqual(val_full, Decimal("0.0416"))

        # Trimmed 2-column fixture shape also parses cleanly
        trimmed_adapter = TreasuryYieldAdapter(
            transport=transport,
            endpoint="https://example.test/treasury/trimmed_{year}.csv",
        )
        eff_trim, val_trim = trimmed_adapter.fetch_daily("2026-11-20")
        self.assertEqual(eff_trim, "2026-11-19")
        self.assertEqual(val_trim, Decimal("0.0416"))

        # Unverified endpoint in emit_live_market_records yields missing-with-reason without transport
        calls.clear()
        with tempfile.TemporaryDirectory() as tmpdir:
            clk = FakeClock()
            records = emit_live_market_records(
                session_date="2026-11-20",
                archive_dir=Path(tmpdir) / "arch",
                transport=transport,
                environ={},
                clock=clk.clock,
                sleeper=clk.sleep,
                utc_clock=_ticking_utc_clock("2026-11-20"),
                calendar_spec=_build_valid_synthetic_2026_calendar(Path(tmpdir)),
            )
            tbill = next(r for r in records if r["instrument"] == "TBILL_3M")
            self.assertIsNone(tbill["normalized_value"])
            self.assertEqual(tbill["eligibility"]["reason"], "source_unavailable:treasury")
            self.assertFalse(any("treasury" in u for u in calls))

    def test_fred_excerpt_and_dot_missing_handling(self) -> None:
        fred_bytes = (LIVE_FIXTURES / "fred_dtb3_excerpt_2026-11-21.csv").read_bytes()

        def transport(url: str, headers: Mapping[str, str]) -> tuple[int, bytes]:
            if "with-dot" in url:
                return (
                    200,
                    b"observation_date,DTB3\n2026-11-18,4.15\n2026-11-19,.\n2026-11-20,\n",
                )
            return 200, fred_bytes

        adapter = FredDtb3Adapter(transport=transport)
        eff_date, rate = adapter.fetch_daily("2026-11-20")
        self.assertEqual(eff_date, "2026-11-19")
        self.assertEqual(rate, Decimal("0.0416"))

        dot_adapter = FredDtb3Adapter(
            transport=transport,
            endpoint="https://example.test/fred/with-dot",
        )
        eff_dot, rate_dot = dot_adapter.fetch_daily("2026-11-20")
        self.assertEqual(eff_dot, "2026-11-18")
        self.assertEqual(rate_dot, Decimal("0.0415"))

    def test_stooq_challenge_body_raises_and_synthetic_csv_parses(self) -> None:
        challenge_body = (LIVE_FIXTURES / "stooq_challenge_2026-11-21.html").read_bytes()
        challenge_text = challenge_body.decode("utf-8")
        self.assertIn('nonce="SYNTHETIC_NONCE_TEST01"', challenge_text)
        m_c = re.search(r'const c="([^"]+)"', challenge_text)
        assert m_c is not None
        c_tok = m_c.group(1)
        c_bytes = base64.urlsafe_b64decode(c_tok + "=" * (-len(c_tok) % 4))
        self.assertEqual(len(c_bytes), 44)
        self.assertEqual(c_bytes[:24], b"\x00" * 24)
        self.assertEqual(c_bytes[24:28], bytes([198, 51, 100, 1]))
        self.assertEqual(c_bytes[28:], b"\x00" * 16)

        synthetic_csv = (
            b"Date,Open,High,Low,Close,Volume\n"
            b"2026-11-19,46.10,47.50,45.90,47.20,51234000\n"
            b"2026-11-20,47.00,48.10,46.80,47.85,49800000\n"
        )

        def transport(url: str, headers: Mapping[str, str]) -> tuple[int, bytes]:
            if "tqqq.us" in url:
                return 200, challenge_body
            return 200, synthetic_csv

        adapter = StooqDailyAdapter(transport=transport)
        with self.assertRaises(SourceChallengeGatedError):
            adapter.fetch_daily("tqqq.us")

        rows = adapter.fetch_daily("soxl.us")
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[-1]["Date"], "2026-11-20")
        self.assertEqual(rows[-1]["Close"], "47.85")

    def test_emit_live_market_records_end_to_end_seven_series_and_mixed_failures(self) -> None:
        nyfed_body = (LIVE_FIXTURES / "nyfed_sofr_2026-11-21.json").read_bytes()
        yahoo_etf_body = (LIVE_FIXTURES / "yahoo_xndx_etf_2026-11-21.json").read_bytes()
        yahoo_xsox_ok = _yahoo_index_payload("XSOX", "2026-11-20", 8123.45)
        yahoo_xndx_ok = _yahoo_index_payload("^XNDX", "2026-11-20", 29500.5)
        tiingo_tqqq_2025 = json.dumps(
            [{"date": "2025-11-21T00:00:00.000Z", "close": 47.48, "divCash": 0.0, "splitFactor": 1.0}]
        ).encode("utf-8")
        tiingo_tqqq_2026 = json.dumps(
            [{"date": "2026-11-20T00:00:00.000Z", "close": 52.10, "divCash": 0.0, "splitFactor": 1.0}]
        ).encode("utf-8")
        tiingo_soxl_2026 = json.dumps(
            [{"date": "2026-11-20T00:00:00.000Z", "close": 33.40, "divCash": 0.05, "splitFactor": 1.0}]
        ).encode("utf-8")
        tiingo_qqq_2026 = json.dumps(
            [{"date": "2026-11-20T00:00:00.000Z", "close": 511.25, "divCash": 0.0, "splitFactor": 1.0}]
        ).encode("utf-8")
        # QQQ intentionally missing 2026-11-20 on rev 0 to exercise session_not_in_payload
        tiingo_qqq_empty = b"[]"
        clk = FakeClock(start=500.0)

        def scripted(url: str, headers: Mapping[str, str]) -> tuple[int, bytes]:
            if "/daily/TQQQ/prices" in url:
                return 200, tiingo_tqqq_2026
            if "/daily/SOXL/prices" in url:
                return 200, tiingo_soxl_2026
            if "/daily/QQQ/prices" in url:
                return 200, tiingo_qqq_empty
            if "%5EXNDX" in url:
                return 200, yahoo_etf_body
            if "finance.yahoo.com" in url and "XSOX" in url:
                return 200, yahoo_xsox_ok
            if "markets.newyorkfed.org" in url:
                return 200, nyfed_body
            raise AssertionError(f"Unexpected URL: {url}")

        with tempfile.TemporaryDirectory() as tmpdir:
            archive_dir = Path(tmpdir) / "live_archive"
            syn_cal_2026 = _build_valid_synthetic_2026_calendar(Path(tmpdir))
            records = emit_live_market_records(
                session_date="2026-11-20",
                archive_dir=archive_dir,
                transport=scripted,
                environ={"TIINGO_API_KEY": "dummy-tiingo-token"},
                clock=clk.clock,
                sleeper=clk.sleep,
                utc_clock=_ticking_utc_clock("2026-11-20"),
                calendar_spec=syn_cal_2026,
            )
            self.assertEqual(len(records), 7)
            self.assertEqual(
                [r["instrument"] for r in records],
                ["TQQQ", "SOXL", "QQQ", "XNDX", "SEMI", "SOFR", "TBILL_3M"],
            )
            by_inst = {r["instrument"]: r for r in records}

            # Successful series archived & hash-verified
            for inst, exp_val in (("TQQQ", "52.1"), ("SOXL", "33.4"), ("SEMI", "8123.45"), ("SOFR", "0.0391")):
                rec = by_inst[inst]
                self.assertEqual(rec["vintage"], "live_retrieval")
                self.assertEqual(rec["observation_class"], "live")
                self.assertEqual(rec["normalized_value"], exp_val)
                self.assertTrue(rec["eligibility"]["eligible"], rec)
                archived = read_verified_archived_bytes(
                    archive_dir,
                    rec["raw_bytes_ref"],
                    rec["raw_bytes_sha256"],
                )
                self.assertEqual(hashlib.sha256(archived).hexdigest(), rec["raw_bytes_sha256"])

            self.assertTrue(by_inst["SEMI"]["disclosures"]["xsox_for_icesemit_substitution"])

            # Failures become missing-with-reason records, never raising or fabricating
            self.assertIsNone(by_inst["QQQ"]["normalized_value"])
            self.assertEqual(by_inst["QQQ"]["eligibility"]["reason"], "session_not_in_payload")
            self.assertIsNone(by_inst["XNDX"]["normalized_value"])
            self.assertEqual(by_inst["XNDX"]["eligibility"]["reason"], "instrument_mismatch")
            self.assertIsNone(by_inst["TBILL_3M"]["normalized_value"])
            self.assertEqual(by_inst["TBILL_3M"]["eligibility"]["reason"], "source_unavailable:treasury")
            self.assertEqual(clk.sleeps, [20.0])

            # Revision rev=1 with SOFR 'R', Yahoo 429 throttle on XSOX, and verified Treasury endpoint
            nyfed_rev = json.dumps(
                {
                    "refRates": [
                        {
                            "effectiveDate": "2026-11-19",
                            "type": "SOFR",
                            "percentRate": 3.94,
                            "revisionIndicator": "R",
                        }
                    ]
                }
            ).encode("utf-8")

            def scripted_rev1(url: str, headers: Mapping[str, str]) -> tuple[int, bytes]:
                if "/daily/TQQQ/prices" in url:
                    return 200, tiingo_tqqq_2026
                if "/daily/SOXL/prices" in url:
                    return 200, tiingo_soxl_2026
                if "/daily/QQQ/prices" in url:
                    return 200, tiingo_qqq_2026
                if "%5EXNDX" in url:
                    return 200, yahoo_xndx_ok
                if "finance.yahoo.com" in url and "XSOX" in url:
                    return 429, b"Edge: Too Many Requests"
                if "markets.newyorkfed.org" in url:
                    return 200, nyfed_rev
                if "treasury.example.test" in url:
                    return 200, b"Date,1 Mo,3 Mo,10 Yr\n2026-11-20,4.20,4.17,4.11\n"
                raise AssertionError(f"Unexpected URL: {url}")

            clk_rev1 = FakeClock(start=700.0)
            records_rev1 = emit_live_market_records(
                session_date="2026-11-20",
                archive_dir=archive_dir,
                transport=scripted_rev1,
                environ={"TIINGO_API_KEY": "dummy-tiingo-token"},
                clock=clk_rev1.clock,
                sleeper=clk_rev1.sleep,
                utc_clock=_ticking_utc_clock("2026-11-20", "22:32:00"),
                calendar_spec=syn_cal_2026,
                revision_seq=1,
                treasury_endpoint="https://treasury.example.test/yield{year}.csv",
            )
            by_inst_rev1 = {r["instrument"]: r for r in records_rev1}
            self.assertEqual(by_inst_rev1["QQQ"]["normalized_value"], "511.25")
            self.assertEqual(by_inst_rev1["XNDX"]["normalized_value"], "29500.5")
            self.assertIsNone(by_inst_rev1["SEMI"]["normalized_value"])
            self.assertEqual(by_inst_rev1["SEMI"]["eligibility"]["reason"], "source_throttled")
            self.assertEqual(by_inst_rev1["SOFR"]["normalized_value"], "0.0394")
            self.assertEqual(by_inst_rev1["SOFR"]["disclosures"]["revision_indicator"], "R")
            self.assertNotEqual(
                by_inst_rev1["SOFR"]["raw_bytes_sha256"],
                by_inst["SOFR"]["raw_bytes_sha256"],
            )
            self.assertEqual(by_inst_rev1["TBILL_3M"]["normalized_value"], "0.0417")
            self.assertTrue(by_inst_rev1["TBILL_3M"]["eligibility"]["eligible"])
            # Original rev0 archived SOFR bytes remain intact and verifiable
            read_verified_archived_bytes(
                archive_dir,
                by_inst["SOFR"]["raw_bytes_ref"],
                by_inst["SOFR"]["raw_bytes_sha256"],
            )

        # Also verify shared emit_market_records_from_payloads handles direct None payloads cleanly
        with tempfile.TemporaryDirectory() as tmpdir:
            recs = emit_market_records_from_payloads(
                DEMO_SERIES,
                {"TQQQ": tiingo_tqqq_2025},
                session_date="2025-11-21",
                retrieval_time_utc="2025-11-21T21:30:00.000Z",
                archive_dir=Path(tmpdir) / "arch",
                vintage="live_retrieval",
                observation_class="live",
                provider_for={s.instrument: "test_provider" for s in DEMO_SERIES},
                missing_reason_for=lambda inst: (f"custom_missing:{inst}", {"note": inst}),
            )
            self.assertEqual(len(recs), 6)
            self.assertEqual(recs[0]["normalized_value"], "47.48")
            self.assertEqual(recs[1]["eligibility"]["reason"], "custom_missing:SOXL")
            self.assertEqual(recs[1]["disclosures"]["note"], "SOXL")

    def test_cross_host_redirect_credential_guard(self) -> None:
        # Item #9: redirect handler refuses forwarding Authorization cross-origin & strips on cross-origin
        handler = _NoCrossHostAuthRedirectHandler()
        auth_req = urllib.request.Request(
            "https://api.tiingo.com/tiingo/daily/TQQQ/prices",
            headers={"Authorization": "Token super-secret"},
        )
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            handler.redirect_request(auth_req, None, 302, "Found", {}, "https://evil.example/steal")
        self.assertIn("cross_host_redirect_with_credentials_blocked", str(ctx.exception))

        # End-to-end localhost HTTP server check: cross-port redirect from origin A -> B receives 0 requests at B
        b_headers_seen: list[dict[str, str]] = []

        class TargetBHandler(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                b_headers_seen.append(dict(self.headers))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"[]")

            def log_message(self, fmt: str, *args: object) -> None:
                return

        srv_b = http.server.HTTPServer(("127.0.0.1", 0), TargetBHandler)
        b_port = srv_b.server_address[1]

        class RedirectAHandler(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                self.send_response(302)
                self.send_header("Location", f"http://127.0.0.1:{b_port}/steal")
                self.end_headers()

            def log_message(self, fmt: str, *args: object) -> None:
                return

        srv_a = http.server.HTTPServer(("127.0.0.1", 0), RedirectAHandler)
        a_port = srv_a.server_address[1]
        t_a = threading.Thread(target=srv_a.serve_forever, daemon=True)
        t_b = threading.Thread(target=srv_b.serve_forever, daemon=True)
        t_a.start()
        t_b.start()
        try:
            status, _ = urllib_fetch_bytes(
                f"http://127.0.0.1:{a_port}/tiingo/daily/TQQQ/prices",
                headers={"Authorization": "Token secret-token"},
            )
            self.assertEqual(status, 302)
            self.assertEqual(b_headers_seen, [])
        finally:
            srv_a.shutdown()
            srv_b.shutdown()
            srv_a.server_close()
            srv_b.server_close()

    def test_per_provider_post_response_timestamps_enforce_decision_cutoff(self) -> None:
        # Item #2: each provider gets its own post-response UTC timestamp; crossing decision_cutoff fails late one
        tiingo_payload = json.dumps(
            [{"date": "2025-11-21T00:00:00.000Z", "close": 47.48, "divCash": 0.0, "splitFactor": 1.0}]
        ).encode("utf-8")
        yahoo_ok = _yahoo_index_payload("^XNDX", "2025-11-21", 29462.35)
        yahoo_xsox = _yahoo_index_payload("XSOX", "2025-11-21", 8179.87)
        nyfed_ok = json.dumps(
            {"refRates": [{"effectiveDate": "2025-11-20", "type": "SOFR", "percentRate": 3.91, "revisionIndicator": ""}]}
        ).encode("utf-8")
        treasury_ok = b"Date,3 Mo\n2025-11-20,3.90\n"

        def scripted(url: str, headers: Mapping[str, str]) -> tuple[int, bytes]:
            if "/daily/" in url:
                return 200, tiingo_payload
            if "%5EXNDX" in url:
                return 200, yahoo_ok
            if "XSOX" in url:
                return 200, yahoo_xsox
            if "markets.newyorkfed.org" in url:
                return 200, nyfed_ok
            if "treasury.example.test" in url:
                return 200, treasury_ok
            return 404, b""

        # First 6 responses land before 23:00:00Z (18:00 EST); 7th response (TBILL_3M) lands at 23:00:05Z
        per_call_times = iter(
            [
                datetime(2025, 11, 21, 22, 30, 1, tzinfo=timezone.utc),
                datetime(2025, 11, 21, 22, 30, 2, tzinfo=timezone.utc),
                datetime(2025, 11, 21, 22, 30, 3, tzinfo=timezone.utc),
                datetime(2025, 11, 21, 22, 30, 4, tzinfo=timezone.utc),
                datetime(2025, 11, 21, 22, 30, 25, tzinfo=timezone.utc),
                datetime(2025, 11, 21, 22, 59, 58, tzinfo=timezone.utc),
                datetime(2025, 11, 21, 23, 0, 5, tzinfo=timezone.utc),
            ]
        )
        clk = FakeClock()
        with tempfile.TemporaryDirectory() as tmpdir:
            recs = emit_live_market_records(
                session_date="2025-11-21",
                archive_dir=Path(tmpdir) / "arch",
                transport=scripted,
                environ={"TIINGO_API_KEY": "tok"},
                clock=clk.clock,
                sleeper=clk.sleep,
                utc_clock=lambda: next(per_call_times),
                treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                session_close_utc="2025-11-21T21:00:00.000Z",
                decision_cutoff_utc="2025-11-21T23:00:00.000Z",
            )
            by_inst = {r["instrument"]: r for r in recs}
            self.assertEqual(len({r["retrieval_time_utc"] for r in recs}), 7)
            for inst in ("TQQQ", "SOXL", "QQQ", "XNDX", "SEMI", "SOFR"):
                self.assertTrue(by_inst[inst]["eligibility"]["eligible"], inst)
            self.assertFalse(by_inst["TBILL_3M"]["eligibility"]["eligible"])
            self.assertEqual(by_inst["TBILL_3M"]["eligibility"]["reason"], "late_after_decision_cutoff")


if __name__ == "__main__":
    unittest.main()
