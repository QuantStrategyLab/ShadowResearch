"""Live decision-time market-data source adapters for C-SH-MKT."""

from __future__ import annotations

import csv
import io
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from shadow.feeder import (
    DEMO_SERIES,
    SeriesSpec,
    emit_market_records_from_payloads,
    parse_flexible_iso_date,
    select_nyfed_sofr_entry,
    select_tiingo_row,
    select_treasury_tbill_row,
    select_yahoo_close,
)
from shadow.sizing import canonical_decimal_str

Transport = Callable[[str, Mapping[str, str]], tuple[int, bytes]]

YAHOO_SYMBOL_VERIFICATION = "unverified"
TREASURY_ENDPOINT_VERIFICATION = "unverified"

DEFAULT_YAHOO_SYMBOL_MAP: dict[str, str] = {
    "XNDX": "^XNDX",
    "XSOX": "XSOX",
}

DEFAULT_TREASURY_URL_TEMPLATE = (
    "https://home.treasury.gov/sites/default/files/interest-rates/yield{year}.csv"
)
FRED_DTB3_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DTB3"
NYFED_SOFR_BASE_URL = "https://markets.newyorkfed.org/api/rates/secured/sofr/search.json"
TIINGO_DAILY_URL_TEMPLATE = (
    "https://api.tiingo.com/tiingo/daily/{ticker}/prices"
    "?startDate={start_date}&endDate={end_date}&format=json"
)
STOOQ_DAILY_URL_TEMPLATE = "https://stooq.com/q/d/l/?s={symbol}&i=d"

BROWSER_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

LIVE_SERIES: tuple[SeriesSpec, ...] = (
    DEMO_SERIES[0],
    DEMO_SERIES[1],
    SeriesSpec("QQQ", "QQQ", "etf_tiingo"),
    DEMO_SERIES[2],
    DEMO_SERIES[3],
    DEMO_SERIES[4],
    DEMO_SERIES[5],
)

LIVE_PROVIDERS: dict[str, str] = {
    "TQQQ": "tiingo",
    "SOXL": "tiingo",
    "QQQ": "tiingo",
    "XNDX": "yahoo",
    "SEMI": "yahoo",
    "SOFR": "nyfed",
    "TBILL_3M": "treasury",
}


class LiveFeederError(Exception):
    """Base error raised by live market-data source adapters."""

    reason_code = "source_unavailable"

    def __init__(self, message: str = "", *, reason_code: str | None = None) -> None:
        if reason_code is not None:
            self.reason_code = reason_code
        super().__init__(message or self.reason_code)


class CredentialAbsentError(LiveFeederError):
    """Raised when a required source API key is absent from the environment."""

    reason_code = "credential_absent"


class SourceUnavailableError(LiveFeederError):
    """Raised when a source endpoint is unverified, unreachable, or returns invalid data."""

    reason_code = "source_unavailable"


class SourceChallengeGatedError(LiveFeederError):
    """Raised when a source serves a JavaScript/CAPTCHA browser-verification challenge."""

    reason_code = "source_challenge_gated"


class InstrumentMismatchError(LiveFeederError):
    """Raised when returned instrument metadata disagrees with the expected series mapping."""

    reason_code = "instrument_mismatch"


class SourceThrottledError(LiveFeederError):
    """Raised when HTTP 429 rate limiting persists after bounded retry backoff."""

    reason_code = "source_throttled"


class SessionNotInPayloadError(LiveFeederError):
    """Raised when a source payload succeeds at transport level but lacks the session."""

    reason_code = "session_not_in_payload"


def urllib_fetch_bytes(
    url: str,
    headers: Mapping[str, str] | None = None,
    *,
    timeout: float = 30.0,
) -> tuple[int, bytes]:
    """Perform an HTTP GET via stdlib urllib, returning (status_code, body_bytes)."""
    req = urllib.request.Request(url, headers=dict(headers or {}), method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return int(resp.status), resp.read()
    except urllib.error.HTTPError as exc:
        body = exc.read() if exc.fp is not None else b""
        return int(exc.code), body
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise SourceUnavailableError(f"transport_error:{type(exc).__name__}") from exc


def _resolve_transport(transport: Transport | None) -> Transport:
    return transport if transport is not None else urllib_fetch_bytes


class TiingoDailyAdapter:
    """Adapter for Tiingo daily prices + corporate actions (TQQQ, SOXL, QQQ)."""

    provider = "tiingo"

    def __init__(
        self,
        *,
        transport: Transport | None = None,
        environ: Mapping[str, str] | None = None,
        env_var: str = "TIINGO_API_KEY",
    ) -> None:
        self._transport = _resolve_transport(transport)
        self._environ = environ if environ is not None else os.environ
        self._env_var = env_var

    @staticmethod
    def build_url(ticker: str, start_date: str, end_date: str) -> str:
        """Build the Tiingo daily prices endpoint URL (never carrying credentials)."""
        encoded_ticker = urllib.parse.quote(ticker, safe="")
        return TIINGO_DAILY_URL_TEMPLATE.format(
            ticker=encoded_ticker,
            start_date=start_date,
            end_date=end_date,
        )

    @staticmethod
    def parse_payload(raw_bytes: bytes, session_date: str) -> dict:
        """Select and validate the Tiingo daily bar for session_date."""
        try:
            row = select_tiingo_row(raw_bytes, session_date)
        except ValueError as exc:
            if str(exc) == "session_not_in_payload":
                raise SessionNotInPayloadError(
                    f"tiingo_session_not_in_payload:{session_date}"
                ) from exc
            raise SourceUnavailableError(f"tiingo_parse_error:{exc}") from exc
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SourceUnavailableError(f"tiingo_invalid_json:{type(exc).__name__}") from exc
        return row

    def fetch_bytes_for_session(
        self,
        ticker: str,
        session_date: str,
        *,
        lookback_days: int = 0,
    ) -> bytes:
        """Fetch and validate raw Tiingo JSON bytes for ticker and session_date."""
        token = str(self._environ.get(self._env_var, "")).strip()
        if not token:
            raise CredentialAbsentError(f"missing_env:{self._env_var}")

        end_d = date.fromisoformat(session_date)
        start_d = end_d - timedelta(days=max(0, lookback_days))
        url = self.build_url(ticker, start_d.isoformat(), end_d.isoformat())
        headers = {
            "Authorization": f"Token {token}",
            "Accept": "application/json",
        }
        status, body = self._transport(url, headers)
        if status == 429:
            raise SourceThrottledError("tiingo_http_429")
        if status != 200:
            raise SourceUnavailableError(f"tiingo_http_{status}")
        self.parse_payload(body, session_date)
        return body

    def fetch_daily(
        self,
        ticker: str,
        session_date: str,
        *,
        lookback_days: int = 0,
    ) -> dict:
        """Fetch and return the Tiingo row dict for ticker on session_date."""
        raw = self.fetch_bytes_for_session(ticker, session_date, lookback_days=lookback_days)
        return self.parse_payload(raw, session_date)


class YahooChartAdapter:
    """Adapter for Yahoo Finance v8 chart API (total-return index series)."""

    provider = "yahoo"
    verification_status = YAHOO_SYMBOL_VERIFICATION

    def __init__(
        self,
        *,
        transport: Transport | None = None,
        symbol_map: Mapping[str, str] | None = None,
        clock: Callable[[], float] | None = None,
        sleeper: Callable[[float], None] | None = None,
        min_spacing_seconds: float = 20.0,
        retry_backoff_seconds: float = 30.0,
    ) -> None:
        self._transport = _resolve_transport(transport)
        self.symbol_map = dict(symbol_map) if symbol_map is not None else dict(DEFAULT_YAHOO_SYMBOL_MAP)
        self._clock = clock if clock is not None else time.monotonic
        self._sleeper = sleeper if sleeper is not None else time.sleep
        self.min_spacing_seconds = float(min_spacing_seconds)
        self.retry_backoff_seconds = float(retry_backoff_seconds)
        self._last_request_ts: float | None = None

    def resolve_symbol(self, series_id: str) -> str:
        """Resolve configured Yahoo symbol, rejecting unmapped or bare XNDX index symbols."""
        if series_id not in self.symbol_map:
            raise InstrumentMismatchError(f"unmapped_yahoo_series:{series_id}")
        yahoo_sym = self.symbol_map[series_id]
        if series_id == "XNDX" and yahoo_sym == "XNDX":
            raise InstrumentMismatchError("bare_index_symbol_disallowed:XNDX")
        return yahoo_sym

    def build_url(self, series_id: str) -> str:
        """Build URL-encoded Yahoo chart URL for the mapped series symbol."""
        yahoo_sym = self.resolve_symbol(series_id)
        encoded = urllib.parse.quote(yahoo_sym, safe="")
        return f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}?range=5d&interval=1d"

    def validate_and_parse(self, raw_bytes: bytes, series_id: str, session_date: str) -> Decimal:
        """Validate meta.symbol and meta.instrumentType=='INDEX' before extracting close."""
        expected_sym = self.resolve_symbol(series_id)
        try:
            payload = json.loads(raw_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SourceUnavailableError(f"yahoo_invalid_json:{type(exc).__name__}") from exc

        results = ((payload.get("chart") or {}).get("result")) if isinstance(payload, dict) else None
        if not isinstance(results, list) or not results or not isinstance(results[0], dict):
            raise SourceUnavailableError("yahoo_empty_result")

        meta = results[0].get("meta")
        if not isinstance(meta, dict):
            raise InstrumentMismatchError("yahoo_missing_meta")

        actual_sym = meta.get("symbol")
        actual_type = meta.get("instrumentType")
        if actual_sym != expected_sym or actual_type != "INDEX":
            raise InstrumentMismatchError(
                f"yahoo_instrument_mismatch:expected=({expected_sym},INDEX),"
                f"got=({actual_sym!r},{actual_type!r})"
            )

        try:
            return select_yahoo_close(raw_bytes, session_date)
        except ValueError as exc:
            if str(exc) == "session_not_in_payload":
                raise SessionNotInPayloadError(
                    f"yahoo_session_not_in_payload:{series_id}:{session_date}"
                ) from exc
            raise SourceUnavailableError(f"yahoo_parse_error:{exc}") from exc

    def _wait_spacing(self) -> None:
        if self._last_request_ts is not None:
            elapsed = self._clock() - self._last_request_ts
            remaining = self.min_spacing_seconds - elapsed
            if remaining > 0:
                self._sleeper(remaining)

    def _request(self, url: str, headers: Mapping[str, str]) -> tuple[int, bytes]:
        self._wait_spacing()
        self._last_request_ts = self._clock()
        return self._transport(url, headers)

    def fetch_bytes_for_session(self, series_id: str, session_date: str) -> bytes:
        """Fetch Yahoo chart payload with spacing, single 429 retry, and meta validation."""
        url = self.build_url(series_id)
        headers = {
            "User-Agent": BROWSER_USER_AGENT,
            "Accept": "application/json",
        }
        status, body = self._request(url, headers)
        if status == 429:
            self._sleeper(self.retry_backoff_seconds)
            self._last_request_ts = self._clock()
            status, body = self._transport(url, headers)
            if status == 429:
                raise SourceThrottledError(f"yahoo_http_429:{series_id}")
        if status != 200:
            raise SourceUnavailableError(f"yahoo_http_{status}:{series_id}")
        self.validate_and_parse(body, series_id, session_date)
        return body

    def fetch_daily(self, series_id: str, session_date: str) -> Decimal:
        """Fetch, validate, and return the close Decimal for series_id on session_date."""
        raw = self.fetch_bytes_for_session(series_id, session_date)
        return self.validate_and_parse(raw, series_id, session_date)


class NyFedSofrAdapter:
    """Adapter for New York Fed Markets Data API (SOFR)."""

    provider = "nyfed"

    def __init__(self, *, transport: Transport | None = None) -> None:
        self._transport = _resolve_transport(transport)

    @staticmethod
    def build_url(session_date: str) -> str:
        """Build NY Fed search.json URL covering [session-14d, session+1d]."""
        d = date.fromisoformat(session_date)
        start_d = (d - timedelta(days=14)).isoformat()
        end_d = (d + timedelta(days=1)).isoformat()
        return f"{NYFED_SOFR_BASE_URL}?startDate={start_d}&endDate={end_d}&typeCode=fr&sort=postDt:1"

    @staticmethod
    def parse_payload(raw_bytes: bytes, session_date: str) -> dict:
        """Select entry with greatest effectiveDate <= session_date and carry revisionIndicator."""
        try:
            entry = select_nyfed_sofr_entry(raw_bytes, session_date)
        except ValueError as exc:
            if str(exc) == "session_not_in_payload":
                raise SessionNotInPayloadError(
                    f"nyfed_session_not_in_payload:{session_date}"
                ) from exc
            raise SourceUnavailableError(f"nyfed_parse_error:{exc}") from exc
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SourceUnavailableError(f"nyfed_invalid_json:{type(exc).__name__}") from exc

        rate_dec = Decimal(str(entry["percentRate"])) / Decimal("100")
        return {
            "effective_date": entry["effectiveDate"],
            "percent_rate": canonical_decimal_str(Decimal(str(entry["percentRate"]))),
            "rate_decimal": canonical_decimal_str(rate_dec),
            "revision_indicator": entry.get("revisionIndicator", ""),
        }

    def fetch_bytes_for_session(self, session_date: str) -> bytes:
        """Fetch and validate NY Fed SOFR JSON bytes for session_date."""
        url = self.build_url(session_date)
        status, body = self._transport(url, {"Accept": "application/json"})
        if status != 200:
            raise SourceUnavailableError(f"nyfed_http_{status}")
        self.parse_payload(body, session_date)
        return body

    def fetch_daily(self, session_date: str) -> dict:
        """Fetch and return parsed SOFR observation dict for session_date."""
        raw = self.fetch_bytes_for_session(session_date)
        return self.parse_payload(raw, session_date)


class TreasuryYieldAdapter:
    """Adapter for U.S. Treasury Daily Par Yield Curve Rates (3 Mo)."""

    provider = "treasury"
    endpoint_verification = TREASURY_ENDPOINT_VERIFICATION
    default_endpoint_template = DEFAULT_TREASURY_URL_TEMPLATE

    def __init__(
        self,
        *,
        transport: Transport | None = None,
        endpoint: str | None = None,
    ) -> None:
        self._transport = _resolve_transport(transport)
        self.endpoint = endpoint

    def build_url(self, session_date: str, *, endpoint: str | None = None) -> str:
        """Return configured override URL or fail closed without fetching if unverified."""
        chosen = endpoint if endpoint is not None else self.endpoint
        if not chosen:
            raise SourceUnavailableError("treasury_endpoint_unverified")
        return chosen.format(year=session_date[:4])

    @staticmethod
    def parse_payload(raw_bytes: bytes, session_date: str) -> tuple[str, Decimal]:
        """Parse full or trimmed Treasury par-yield CSV and return (Date, 3_Mo_decimal_rate)."""
        try:
            return select_treasury_tbill_row(raw_bytes, session_date)
        except ValueError as exc:
            if str(exc) == "session_not_in_payload":
                raise SessionNotInPayloadError(
                    f"treasury_session_not_in_payload:{session_date}"
                ) from exc
            raise SourceUnavailableError(f"treasury_parse_error:{exc}") from exc
        except UnicodeDecodeError as exc:
            raise SourceUnavailableError("treasury_invalid_encoding") from exc

    def fetch_bytes_for_session(
        self,
        session_date: str,
        *,
        endpoint: str | None = None,
    ) -> bytes:
        """Fetch Treasury CSV when an override endpoint is supplied; otherwise fail without fetch."""
        url = self.build_url(session_date, endpoint=endpoint)
        status, body = self._transport(url, {"Accept": "text/csv,text/plain"})
        if status != 200:
            raise SourceUnavailableError(f"treasury_http_{status}")
        self.parse_payload(body, session_date)
        return body

    def fetch_daily(
        self,
        session_date: str,
        *,
        endpoint: str | None = None,
    ) -> tuple[str, Decimal]:
        """Fetch and return (effective_date, 3_Mo_decimal_rate)."""
        raw = self.fetch_bytes_for_session(session_date, endpoint=endpoint)
        return self.parse_payload(raw, session_date)


class FredDtb3Adapter:
    """Cross-check adapter for FRED DTB3 CSV (keyless)."""

    provider = "fred"

    def __init__(
        self,
        *,
        transport: Transport | None = None,
        endpoint: str = FRED_DTB3_URL,
    ) -> None:
        self._transport = _resolve_transport(transport)
        self.endpoint = endpoint

    @staticmethod
    def parse_payload(raw_bytes: bytes, session_date: str) -> tuple[str, Decimal]:
        """Parse fredgraph CSV (skipping '#' comments and '.'/empty) up to session_date."""
        try:
            text = raw_bytes.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise SourceUnavailableError("fred_invalid_encoding") from exc

        data_lines = [
            ln for ln in text.splitlines() if ln.strip() and not ln.lstrip().startswith("#")
        ]
        if not data_lines:
            raise SourceUnavailableError("fred_empty_csv")

        reader = csv.DictReader(io.StringIO("\n".join(data_lines)))
        if reader.fieldnames is None:
            raise SourceUnavailableError("fred_missing_header")
        cols = [c.strip() for c in reader.fieldnames]
        date_col = "observation_date" if "observation_date" in cols else ("DATE" if "DATE" in cols else None)
        if date_col is None or "DTB3" not in cols:
            raise SourceUnavailableError("fred_missing_columns")

        best_date: str | None = None
        best_val: Decimal | None = None
        for row in reader:
            cleaned = {k.strip(): (v.strip() if v is not None else "") for k, v in row.items() if k is not None}
            raw_d = cleaned.get(date_col, "")
            raw_v = cleaned.get("DTB3", "")
            if not raw_d or raw_v in ("", ".", "N/A"):
                continue
            iso_d = parse_flexible_iso_date(raw_d)
            if iso_d <= session_date and (best_date is None or iso_d > best_date):
                best_date = iso_d
                best_val = Decimal(raw_v) / Decimal("100")

        if best_date is None or best_val is None:
            raise SessionNotInPayloadError(f"fred_session_not_in_payload:{session_date}")
        return best_date, best_val

    def fetch_bytes_for_session(self, session_date: str) -> bytes:
        """Fetch and validate FRED DTB3 CSV bytes."""
        status, body = self._transport(self.endpoint, {"Accept": "text/csv,text/plain"})
        if status != 200:
            raise SourceUnavailableError(f"fred_http_{status}")
        self.parse_payload(body, session_date)
        return body

    def fetch_daily(self, session_date: str) -> tuple[str, Decimal]:
        """Fetch and return (effective_date, dtb3_decimal_rate)."""
        raw = self.fetch_bytes_for_session(session_date)
        return self.parse_payload(raw, session_date)


class StooqDailyAdapter:
    """Cross-check adapter for Stooq daily OHLCV CSV (detects JS verification gate)."""

    provider = "stooq"

    def __init__(self, *, transport: Transport | None = None) -> None:
        self._transport = _resolve_transport(transport)

    @staticmethod
    def build_url(symbol: str) -> str:
        """Build Stooq per-symbol daily CSV URL."""
        encoded = urllib.parse.quote(symbol, safe=".")
        return STOOQ_DAILY_URL_TEMPLATE.format(symbol=encoded)

    @staticmethod
    def is_challenge_page(raw_bytes: bytes) -> bool:
        """Return True when raw_bytes matches Stooq's HTML/JS browser verification page."""
        head = raw_bytes[:2048].decode("utf-8", errors="replace").lstrip().lower()
        return (
            head.startswith("<!doctype html")
            or "<html" in head[:512]
            or "requires javascript" in head
            or "/__verify" in head
        )

    @classmethod
    def parse_payload(cls, raw_bytes: bytes) -> list[dict[str, str]]:
        """Parse Stooq daily CSV rows, raising SourceChallengeGatedError on JS challenge pages."""
        if cls.is_challenge_page(raw_bytes):
            raise SourceChallengeGatedError("stooq_js_verification_challenge")
        try:
            text = raw_bytes.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise SourceUnavailableError("stooq_invalid_encoding") from exc

        lines = [ln for ln in text.splitlines() if ln.strip()]
        if not lines:
            raise SourceUnavailableError("stooq_empty_response")

        reader = csv.DictReader(io.StringIO("\n".join(lines)))
        cols = [c.strip() for c in (reader.fieldnames or [])]
        if not {"Date", "Open", "High", "Low", "Close", "Volume"}.issubset(set(cols)):
            raise SourceUnavailableError("stooq_invalid_csv_header")

        rows: list[dict[str, str]] = []
        for row in reader:
            cleaned = {k.strip(): (v.strip() if v is not None else "") for k, v in row.items() if k is not None}
            if cleaned.get("Date") and cleaned.get("Close"):
                rows.append(cleaned)
        if not rows:
            raise SessionNotInPayloadError("stooq_no_rows")
        return rows

    def fetch_daily(self, symbol: str) -> list[dict[str, str]]:
        """Fetch and parse daily OHLCV rows for symbol (e.g., 'tqqq.us')."""
        url = self.build_url(symbol)
        status, body = self._transport(url, {"Accept": "text/csv,text/plain"})
        if status != 200:
            raise SourceUnavailableError(f"stooq_http_{status}")
        return self.parse_payload(body)


def _failure_reason_and_disclosures(provider: str, exc: Exception) -> tuple[str, dict]:
    if isinstance(exc, CredentialAbsentError):
        return "credential_absent", {"detail": str(exc)}
    if isinstance(exc, SourceChallengeGatedError):
        return "source_challenge_gated", {"detail": str(exc)}
    if isinstance(exc, InstrumentMismatchError):
        return "instrument_mismatch", {"detail": str(exc)}
    if isinstance(exc, SourceThrottledError):
        return "source_throttled", {"detail": str(exc)}
    if isinstance(exc, SessionNotInPayloadError):
        return "session_not_in_payload", {"detail": str(exc)}
    return f"source_unavailable:{provider}", {"detail": str(exc)}


def emit_live_market_records(
    *,
    session_date: str,
    retrieval_time_utc: str,
    archive_dir: str | Path,
    transport: Transport | None = None,
    environ: Mapping[str, str] | None = None,
    clock: Callable[[], float] | None = None,
    sleeper: Callable[[float], None] | None = None,
    calendar_spec: dict | None = None,
    revision_seq: int = 0,
    treasury_endpoint: str | None = None,
    yahoo_symbol_map: Mapping[str, str] | None = None,
) -> list[dict]:
    """Fetch all 7 live series (or record missing-with-reason) and emit C-SH-MKT records."""
    resolved_transport = _resolve_transport(transport)
    tiingo = TiingoDailyAdapter(transport=resolved_transport, environ=environ)
    yahoo = YahooChartAdapter(
        transport=resolved_transport,
        symbol_map=yahoo_symbol_map,
        clock=clock,
        sleeper=sleeper,
    )
    nyfed = NyFedSofrAdapter(transport=resolved_transport)
    treasury = TreasuryYieldAdapter(transport=resolved_transport, endpoint=treasury_endpoint)

    payloads: dict[str, bytes | None] = {}
    missing_reasons: dict[str, tuple[str, dict]] = {}

    for inst, series_id, kind in LIVE_SERIES:
        provider = LIVE_PROVIDERS[inst]
        try:
            if kind == "etf_tiingo":
                payloads[inst] = tiingo.fetch_bytes_for_session(series_id, session_date)
            elif kind == "idx_yahoo":
                payloads[inst] = yahoo.fetch_bytes_for_session(series_id, session_date)
            elif kind == "rate_sofr":
                payloads[inst] = nyfed.fetch_bytes_for_session(session_date)
            elif kind == "rate_tbill":
                payloads[inst] = treasury.fetch_bytes_for_session(session_date)
            else:
                raise SourceUnavailableError(f"unsupported_kind:{kind}")
        except Exception as exc:
            payloads[inst] = None
            missing_reasons[inst] = _failure_reason_and_disclosures(provider, exc)

    archive_names = {
        inst: (
            f"rev{revision_seq}_{LIVE_PROVIDERS[inst]}_{inst.lower()}_{session_date}."
            f"{'csv' if kind == 'rate_tbill' else 'json'}"
        )
        for inst, _, kind in LIVE_SERIES
    }

    return emit_market_records_from_payloads(
        LIVE_SERIES,
        payloads,
        session_date=session_date,
        retrieval_time_utc=retrieval_time_utc,
        archive_dir=archive_dir,
        calendar_spec=calendar_spec,
        revision_seq=revision_seq,
        vintage="live_retrieval",
        observation_class="live",
        provider_for=LIVE_PROVIDERS,
        missing_reason_for=lambda inst: missing_reasons.get(
            inst,
            (f"source_unavailable:{LIVE_PROVIDERS[inst]}", {}),
        ),
        archive_name_for=archive_names,
    )
