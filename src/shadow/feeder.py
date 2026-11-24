"""C-SH-MKT decision-time and Y2 reconstruction market-data feeder."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, time as dt_time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import NamedTuple
from zoneinfo import ZoneInfo

from shadow.calendar_consumer import (
    DEFAULT_SPEC_DIR,
    is_official_close_bar_timestamp,
    load_spec,
    resolve_session,
)
from shadow.evaluator import StubEvaluator
from shadow.ledger import (
    GENESIS_PREV_HASH,
    ShadowLedger,
    build_record_body,
    compute_code_manifest_hash,
    sha256_canonical,
)
from shadow.sizing import canonical_decimal_str

VINTAGE_Y2 = "as-retrieved reconstruction; not point-in-time inputs"
SOXL_PROXY_LABEL = "market-close proxy; NAV criterion unverified"
XSOX_SUBSTITUTION_CUTOFF = "2021-08-25"
_NY_TZ = ZoneInfo("America/New_York")


def default_eod_decision_cutoff_utc(session_date: str, cal: Mapping[str, object]) -> str:
    """Compute pinned EOD retrieval cutoff UTC-ms (18:00 ET regular, 15:00 ET half-day)."""
    hhmm = "15:00" if cal.get("session_state") == "half_day" else "18:00"
    hour, minute = map(int, hhmm.split(":"))
    local_dt = datetime.combine(date.fromisoformat(session_date), dt_time(hour, minute), tzinfo=_NY_TZ)
    return local_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def is_us_federal_bond_holiday(d: date) -> bool:
    """Return True on US federal bond-only holidays when NY Fed SOFR does not publish."""
    # Columbus Day: 2nd Monday in October
    if d.month == 10 and d.weekday() == 0 and 8 <= d.day <= 14:
        return True
    # Veterans Day: November 11 weekday, or observed Monday November 12 if Nov 11 is Sunday
    if d.month == 11 and ((d.day == 11 and d.weekday() < 5) or (d.day == 12 and d.weekday() == 0)):
        return True
    return False


def expected_prior_sofr_business_day(
    session_date: str,
    calendar_spec: dict | None = None,
) -> str:
    """Compute expected prior NY Fed SOFR publication business day (skipping weekends, scheduled NYSE holidays, & US bond holidays)."""
    spec = calendar_spec if calendar_spec is not None else load_spec(DEFAULT_SPEC_DIR)
    holidays = spec.get("rules", {}).get("holidays", {}) if isinstance(spec, dict) else {}
    cur = date.fromisoformat(session_date) - timedelta(days=1)
    for _ in range(30):
        if cur.weekday() >= 5 or is_us_federal_bond_holiday(cur):
            cur -= timedelta(days=1)
            continue
        cur_iso = cur.isoformat()
        cal = resolve_session("XNYS", cur_iso, spec=spec)
        if cal.get("session_state") != "unknown":
            if cur_iso in holidays:
                cur -= timedelta(days=1)
                continue
        else:
            # Outside loaded spec coverage fallback: skip Jan 1 / Jul 4 / Dec 25 observed fixed US holidays
            if (cur.month, cur.day) in ((1, 1), (7, 4), (12, 25)):
                cur -= timedelta(days=1)
                continue
        return cur_iso
    raise ValueError(f"Unable to resolve prior SOFR business day for {session_date}")


class SeriesSpec(NamedTuple):
    """Descriptor for a C-SH-MKT market series."""

    instrument: str
    series_id: str
    kind: str


DEMO_SERIES: tuple[SeriesSpec, ...] = (
    SeriesSpec("TQQQ", "TQQQ", "etf_tiingo"),
    SeriesSpec("SOXL", "SOXL", "etf_tiingo"),
    SeriesSpec("XNDX", "XNDX", "idx_yahoo"),
    SeriesSpec("SEMI", "XSOX", "idx_yahoo"),
    SeriesSpec("SOFR", "SOFR", "rate_sofr"),
    SeriesSpec("TBILL_3M", "DTB3_PAR_3M", "rate_tbill"),
)


class FeederIntegrityError(ValueError):
    """Raised when archived raw bytes fail SHA-256 verification."""


def archive_raw_bytes(raw_bytes: bytes, archive_dir: Path, rel_name: str) -> tuple[str, str]:
    """Store raw bytes immutably under archive_dir and return (rel_name, sha256)."""
    archive_dir.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(raw_bytes).hexdigest()
    target = archive_dir / rel_name
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        existing_digest = hashlib.sha256(target.read_bytes()).hexdigest()
        if existing_digest != digest:
            raise FeederIntegrityError(
                f"Immutable raw byte conflict for {rel_name}: {existing_digest} != {digest}"
            )
    else:
        target.write_bytes(raw_bytes)
    return rel_name, digest


def read_verified_archived_bytes(
    archive_dir: Path,
    raw_bytes_ref: str,
    expected_sha256: str,
) -> bytes:
    """Read archived raw bytes and verify exact SHA-256 match."""
    target = archive_dir / raw_bytes_ref
    if not target.is_file():
        raise FeederIntegrityError(f"Missing archived raw bytes: {raw_bytes_ref}")
    data = target.read_bytes()
    actual = hashlib.sha256(data).hexdigest()
    if actual != expected_sha256:
        raise FeederIntegrityError(
            f"Raw byte SHA-256 mismatch for {raw_bytes_ref}: {actual} != {expected_sha256}"
        )
    return data


def _build_mkt_record(
    *,
    instrument: str,
    provider: str,
    series_id: str,
    price_basis: str,
    units: str,
    effective_session: str,
    source_publication_time_utc: str | None,
    retrieval_time_utc: str,
    calendar_session: dict,
    vintage: str,
    observation_class: str,
    raw_bytes_ref: str,
    raw_bytes_sha256: str,
    normalized_value: str | None,
    is_modelled: bool,
    disclosures: dict,
    corporate_action: dict | None = None,
    revision_seq: int = 0,
    missing_reason: str | None = None,
    decision_cutoff_utc: str | None = None,
    session_close_utc: str | None = None,
) -> dict:
    cutoff = decision_cutoff_utc if decision_cutoff_utc is not None else calendar_session.get("order_cutoff_utc")
    eff_close = session_close_utc if session_close_utc is not None else calendar_session.get("close_utc")
    post_close_ok = bool(eff_close is not None and retrieval_time_utc >= eff_close)
    eligible = (
        missing_reason is None
        and normalized_value is not None
        and bool(calendar_session.get("is_trading_day"))
        and not bool(calendar_session.get("blocks_eligibility"))
        and cutoff is not None
        and post_close_ok
        and retrieval_time_utc <= cutoff
    )
    elig_reason = missing_reason
    if eligible:
        elig_reason = None
    elif elig_reason is None:
        if normalized_value is None:
            elig_reason = "missing_normalized_value"
        elif cutoff is None or not calendar_session.get("is_trading_day"):
            elig_reason = "non_trading_session"
        elif eff_close is None:
            elig_reason = "missing_session_close_utc"
        elif not post_close_ok:
            elig_reason = "session_not_yet_closed"
        elif retrieval_time_utc > cutoff:
            elig_reason = "late_after_decision_cutoff"

    return {
        "instrument": instrument,
        "provider": provider,
        "series_id": series_id,
        "price_basis": price_basis,
        "units": units,
        "effective_session": effective_session,
        "source_publication_time_utc": source_publication_time_utc,
        "retrieval_time_utc": retrieval_time_utc,
        "decision_cutoff_utc": cutoff,
        "calendar_version": calendar_session["calendar_version"],
        "calendar_spec_digest": calendar_session["spec_digest"],
        "vintage": vintage,
        "observation_class": observation_class,
        "raw_bytes_ref": raw_bytes_ref,
        "raw_bytes_sha256": raw_bytes_sha256,
        "normalized_value": normalized_value,
        "is_modelled": is_modelled,
        "revision_seq": revision_seq,
        "eligibility": {"eligible": eligible, "reason": elig_reason},
        "disclosures": disclosures,
        "corporate_action": corporate_action,
    }


def _resolve_lookup(
    source: Mapping[str, object] | Callable[[str], object],
    key: str,
) -> object:
    return source(key) if callable(source) else source[key]


def parse_flexible_iso_date(raw: str) -> str:
    """Parse YYYY-MM-DD, MM/DD/YYYY, or MM/DD/YY into ISO YYYY-MM-DD."""
    cleaned = raw.strip()
    try:
        return datetime.strptime(cleaned, "%Y-%m-%d").date().isoformat()
    except ValueError:
        pass
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(cleaned, fmt).date().isoformat()
        except ValueError:
            continue
    raise ValueError(f"invalid_date:{raw!r}")


def _parse_finite_decimal(raw: object, field_name: str) -> Decimal:
    """Parse a finite non-boolean numeric value into Decimal or raise ValueError."""
    if isinstance(raw, bool) or not isinstance(raw, (int, float, Decimal, str)):
        raise ValueError(f"invalid_numeric:{field_name}")
    if isinstance(raw, float) and not math.isfinite(raw):
        raise ValueError(f"nonfinite_numeric:{field_name}")
    text = str(raw).strip()
    if not text:
        raise ValueError(f"invalid_numeric:{field_name}")
    try:
        dec = Decimal(text)
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"invalid_numeric:{field_name}") from exc
    if not dec.is_finite():
        raise ValueError(f"nonfinite_numeric:{field_name}")
    return dec


_EXTENDED_SESSION_MARKERS = frozenset(
    {
        "extended",
        "after_hours",
        "after_hours_extended",
        "overnight",
        "extended_overnight_open",
        "pre_market",
        "pre_market_extended",
        "blue_ocean_overnight",
    }
)


def _official_close_time_for_calendar(calendar_session: Mapping[str, object] | None) -> dt_time:
    if isinstance(calendar_session, Mapping):
        close_local = calendar_session.get("close_local")
        if isinstance(close_local, str) and ":" in close_local:
            hh, mm = map(int, close_local.split(":"))
            return dt_time(hh, mm)
        if calendar_session.get("session_state") == "half_day":
            return dt_time(13, 0)
    return dt_time(16, 0)


def _validate_core_rth_bar_timestamp(
    raw_ts: object,
    session_date: str,
    calendar_session: Mapping[str, object] | None = None,
    *,
    allow_midnight_date_marker: bool = False,
) -> None:
    if not isinstance(raw_ts, str) or not raw_ts.strip():
        return
    txt = raw_ts.strip()
    if allow_midnight_date_marker:
        if len(txt) == 10:
            if txt > session_date:
                raise ValueError("next_trade_date_contamination")
            return
        if "T00:00:00" in txt and (txt.endswith("Z") or "+" in txt or txt.endswith("-00:00")):
            # Canonical Tiingo midnight-UTC daily session marker ("YYYY-MM-DDT00:00:00.000Z")
            row_d = txt[:10]
            if row_d > session_date:
                raise ValueError("next_trade_date_contamination")
            return
    iso_norm = txt[:-1] + "+00:00" if txt.endswith("Z") else txt
    try:
        dt_ny = datetime.fromisoformat(iso_norm).astimezone(_NY_TZ)
    except ValueError as exc:
        raise ValueError("tiingo_invalid_timestamp") from exc
    dt_d = dt_ny.date().isoformat()
    if dt_d > session_date:
        raise ValueError("next_trade_date_contamination")
    if dt_d < session_date:
        raise ValueError("session_not_in_payload")
    close_t = _official_close_time_for_calendar(calendar_session)
    if dt_ny.time() >= dt_time(21, 0):
        raise ValueError("next_trade_date_contamination")
    if dt_ny.time() < dt_time(9, 30) or dt_ny.time() > close_t:
        raise ValueError("extended_hours_overnight_rejected")


def select_tiingo_row(
    raw_bytes: bytes,
    session_date: str,
    *,
    calendar_session: Mapping[str, object] | None = None,
) -> dict:
    """Parse Tiingo daily JSON array and return Official Close row (rejecting 23/5 overnight/T+1)."""
    rows = json.loads(raw_bytes.decode("utf-8"))
    if not isinstance(rows, list):
        raise ValueError("tiingo_payload_not_list")
    extended_err: str | None = None
    for row in rows:
        if not isinstance(row, dict):
            continue
        raw_date = str(row.get("date", ""))
        if not raw_date.startswith(session_date) or row.get("close") is None:
            continue
        for marker_key in ("session", "market_session", "session_window", "session_time"):
            marker_val = row.get(marker_key)
            if isinstance(marker_val, str) and marker_val.strip().lower() in _EXTENDED_SESSION_MARKERS:
                extended_err = "extended_hours_overnight_rejected"
                break
        else:
            if row.get("extendedHours") is True or row.get("isExtendedHours") is True:
                extended_err = "extended_hours_overnight_rejected"
                continue
            try:
                _validate_core_rth_bar_timestamp(
                    raw_date,
                    session_date,
                    calendar_session,
                    allow_midnight_date_marker=True,
                )
                if "timestamp" in row and row.get("timestamp") is not None:
                    _validate_core_rth_bar_timestamp(
                        row.get("timestamp"),
                        session_date,
                        calendar_session,
                        allow_midnight_date_marker=False,
                    )
                if "publicationTimestamp" in row and row.get("publicationTimestamp") is not None:
                    pub = str(row["publicationTimestamp"]).strip()
                    iso_pub = pub[:-1] + "+00:00" if pub.endswith("Z") else pub
                    pub_ny = datetime.fromisoformat(iso_pub).astimezone(_NY_TZ)
                    if pub_ny.date().isoformat() > session_date:
                        raise ValueError("next_trade_date_contamination")
            except ValueError as exc:
                extended_err = str(exc)
                continue
            if row.get("divCash") is None or row.get("splitFactor") is None:
                raise ValueError("tiingo_missing_corporate_action_fields")
            _parse_finite_decimal(row["close"], "tiingo_close")
            _parse_finite_decimal(row["divCash"], "tiingo_divCash")
            _parse_finite_decimal(row["splitFactor"], "tiingo_splitFactor")
            return row
    if extended_err is not None:
        raise ValueError(extended_err)
    raise ValueError("session_not_in_payload")


def select_yahoo_close(
    raw_bytes: bytes,
    session_date: str,
    *,
    calendar_session: Mapping[str, object] | None = None,
) -> Decimal:
    """Parse Yahoo chart JSON and return core RTH Official Close Decimal matching session_date."""
    payload = json.loads(raw_bytes.decode("utf-8"))
    results = (payload.get("chart") or {}).get("result") if isinstance(payload, dict) else None
    if not isinstance(results, list) or not results or not isinstance(results[0], dict):
        raise ValueError("yahoo_empty_result")
    result0 = results[0]
    meta = result0.get("meta") if isinstance(result0.get("meta"), dict) else {}
    if "dataGranularity" in meta and meta.get("dataGranularity") not in ("1d", "1day", "daily"):
        raise ValueError("yahoo_non_daily_granularity_rejected")
    timestamps = result0.get("timestamp") or []
    quote = ((result0.get("indicators") or {}).get("quote") or [{}])[0]
    closes = quote.get("close") or []
    if not timestamps:
        raise ValueError("session_not_in_payload")
    close_t = _official_close_time_for_calendar(calendar_session)
    selected: Decimal | None = None
    extended_err: str | None = None
    for ts, close in zip(timestamps, closes):
        if ts is None or close is None:
            continue
        if isinstance(ts, bool):
            raise ValueError("yahoo_invalid_timestamp")
        try:
            ts_int = int(ts)
            bar_dt = datetime.fromtimestamp(ts_int, tz=_NY_TZ)
            bar_date = bar_dt.date().isoformat()
        except (ValueError, TypeError, OverflowError, OSError) as exc:
            raise ValueError("yahoo_invalid_timestamp") from exc
        if bar_date == session_date:
            bar_time = bar_dt.time()
            if bar_time >= dt_time(21, 0):
                extended_err = "next_trade_date_contamination"
                continue
            if bar_time < dt_time(9, 30) or bar_time > close_t:
                extended_err = "extended_hours_overnight_rejected"
                continue
            selected = _parse_finite_decimal(close, "yahoo_close")
    if selected is not None:
        return selected
    if extended_err is not None:
        raise ValueError(extended_err)
    raise ValueError("session_not_in_payload")


def select_nyfed_sofr_entry(raw_bytes: bytes, session_date: str) -> dict:
    """Parse NY Fed SOFR JSON and select T-1 / prior business day entry (effectiveDate < session_date)."""
    payload = json.loads(raw_bytes.decode("utf-8"))
    ref_rates = payload.get("refRates") if isinstance(payload, dict) else None
    if not isinstance(ref_rates, list):
        raise ValueError("nyfed_missing_refRates")
    eligible = [
        entry
        for entry in ref_rates
        if isinstance(entry, dict)
        and entry.get("type") in (None, "SOFR")
        and isinstance(entry.get("effectiveDate"), str)
        and entry["effectiveDate"] < session_date
        and entry.get("percentRate") is not None
    ]
    if not eligible:
        raise ValueError("session_not_in_payload")
    best = max(
        eligible,
        key=lambda e: (e["effectiveDate"], 1 if e.get("revisionIndicator") else 0),
    )
    _parse_finite_decimal(best["percentRate"], "nyfed_percentRate")
    return best


def select_treasury_tbill_row(raw_bytes: bytes, session_date: str) -> tuple[str, Decimal]:
    """Parse Treasury par-yield CSV and return (Date_ISO, 3_Mo_decimal_rate) <= session_date."""
    text = raw_bytes.decode("utf-8-sig")
    data_lines = [
        ln for ln in text.splitlines() if ln.strip() and not ln.lstrip().startswith("#")
    ]
    if not data_lines:
        raise ValueError("treasury_empty_csv")
    reader = csv.DictReader(io.StringIO("\n".join(data_lines)))
    if reader.fieldnames is None:
        raise ValueError("treasury_missing_header")
    cols = [c.strip() for c in reader.fieldnames]
    if "Date" not in cols or "3 Mo" not in cols:
        raise ValueError("treasury_missing_columns")

    best_date: str | None = None
    best_rate: Decimal | None = None
    for row in reader:
        cleaned = {
            k.strip(): (v.strip() if v is not None else "")
            for k, v in row.items()
            if k is not None
        }
        raw_d = cleaned.get("Date", "")
        raw_v = cleaned.get("3 Mo", "")
        if not raw_d or raw_v in ("", ".", "N/A"):
            continue
        iso_d = parse_flexible_iso_date(raw_d)
        if iso_d <= session_date and (best_date is None or iso_d > best_date):
            best_rate = _parse_finite_decimal(raw_v, "treasury_3m") / Decimal("100")
            best_date = iso_d

    if best_date is None or best_rate is None:
        raise ValueError("session_not_in_payload")
    return best_date, best_rate


def _build_missing_record(
    *,
    inst: str,
    provider: str,
    series_id: str,
    session_date: str,
    retrieval_time_utc: str,
    cal: dict,
    vintage: str,
    observation_class: str,
    disclosures: dict,
    revision_seq: int,
    missing_reason: str,
    raw_bytes_ref: str = "",
    raw_bytes_sha256: str = "",
    decision_cutoff_utc: str | None = None,
    session_close_utc: str | None = None,
) -> dict:
    return _build_mkt_record(
        instrument=inst,
        provider=provider,
        series_id=series_id,
        price_basis="unavailable",
        units="USD",
        effective_session=session_date,
        source_publication_time_utc=None,
        retrieval_time_utc=retrieval_time_utc,
        calendar_session=cal,
        vintage=vintage,
        observation_class=observation_class,
        raw_bytes_ref=raw_bytes_ref,
        raw_bytes_sha256=raw_bytes_sha256,
        normalized_value=None,
        is_modelled=False,
        disclosures=disclosures,
        revision_seq=revision_seq,
        missing_reason=missing_reason,
        decision_cutoff_utc=decision_cutoff_utc,
        session_close_utc=session_close_utc,
    )


def _record_from_verified_bytes(
    *,
    inst: str,
    provider: str,
    series_id: str,
    kind: str,
    verified_bytes: bytes,
    ref: str,
    digest: str,
    session_date: str,
    retrieval_time_utc: str,
    cal: dict,
    calendar_spec: dict,
    vintage: str,
    observation_class: str,
    revision_seq: int,
    decision_cutoff_utc: str | None = None,
    session_close_utc: str | None = None,
) -> dict:
    if kind == "etf_tiingo":
        row = select_tiingo_row(verified_bytes, session_date, calendar_session=cal)
        close_str = canonical_decimal_str(Decimal(str(row["close"])))
        div_cash = canonical_decimal_str(Decimal(str(row["divCash"])))
        split_ratio = canonical_decimal_str(Decimal(str(row["splitFactor"])))
        disclosures = {
            "nav_proxy": True,
            "proxy_label": SOXL_PROXY_LABEL if inst == "SOXL" else "close (proxy, NAV unverified)",
        }
        ca = {
            "ex_date": session_date,
            "split_ratio": split_ratio,
            "cash_per_share_usd": div_cash,
            "broker_reconciliation_claim": False,
        }
        return _build_mkt_record(
            instrument=inst,
            provider=provider,
            series_id=series_id,
            price_basis="close (proxy, NAV unverified)",
            units="USD/share",
            effective_session=session_date,
            source_publication_time_utc=row.get("publicationTimestamp"),
            retrieval_time_utc=retrieval_time_utc,
            calendar_session=cal,
            vintage=vintage,
            observation_class=observation_class,
            raw_bytes_ref=ref,
            raw_bytes_sha256=digest,
            normalized_value=close_str,
            is_modelled=False,
            disclosures=disclosures,
            corporate_action=ca,
            revision_seq=revision_seq,
            decision_cutoff_utc=decision_cutoff_utc,
            session_close_utc=session_close_utc,
        )

    if kind == "idx_yahoo":
        close_dec = select_yahoo_close(verified_bytes, session_date, calendar_session=cal)
        disclosures: dict[str, object] = {}
        if inst == "SEMI" and session_date >= XSOX_SUBSTITUTION_CUTOFF:
            disclosures["xsox_for_icesemit_substitution"] = True
            disclosures["substitution_note"] = "post-2021-08-25 XSOX substitution for ICESEMIT"
        return _build_mkt_record(
            instrument=inst,
            provider=provider,
            series_id=series_id,
            price_basis="gross_total_return",
            units="index_points",
            effective_session=session_date,
            source_publication_time_utc=None,
            retrieval_time_utc=retrieval_time_utc,
            calendar_session=cal,
            vintage=vintage,
            observation_class=observation_class,
            raw_bytes_ref=ref,
            raw_bytes_sha256=digest,
            normalized_value=canonical_decimal_str(close_dec),
            is_modelled=False,
            disclosures=disclosures,
            revision_seq=revision_seq,
            decision_cutoff_utc=decision_cutoff_utc,
            session_close_utc=session_close_utc,
        )

    if kind == "rate_sofr":
        entry = select_nyfed_sofr_entry(verified_bytes, session_date)
        rate_dec = Decimal(str(entry["percentRate"])) / Decimal("100")
        actual_eff = str(entry["effectiveDate"])
        expected_eff = expected_prior_sofr_business_day(session_date, calendar_spec=calendar_spec)
        is_exact_t_minus_1 = actual_eff == expected_eff
        vintage_lag = "T-1" if is_exact_t_minus_1 else "stale"
        sofr_reason = None if is_exact_t_minus_1 else "sofr_stale_effective_date"
        return _build_mkt_record(
            instrument=inst,
            provider=provider,
            series_id=series_id,
            price_basis="overnight_rate_days_365",
            units="annual_rate_decimal",
            effective_session=actual_eff,
            source_publication_time_utc=None,
            retrieval_time_utc=retrieval_time_utc,
            calendar_session=cal,
            vintage=vintage,
            observation_class=observation_class,
            raw_bytes_ref=ref,
            raw_bytes_sha256=digest,
            normalized_value=canonical_decimal_str(rate_dec),
            is_modelled=False,
            disclosures={
                "accrual_basis": "days/365",
                "sofr_effective_lag": vintage_lag,
                "vintage_lag": vintage_lag,
                "actual_effective_date": actual_eff,
                "expected_effective_date": expected_eff,
                "revision_indicator": entry.get("revisionIndicator", ""),
            },
            revision_seq=revision_seq,
            missing_reason=sofr_reason,
            decision_cutoff_utc=decision_cutoff_utc,
            session_close_utc=session_close_utc,
        )

    if kind == "rate_tbill":
        eff_date, rate_dec = select_treasury_tbill_row(verified_bytes, session_date)
        expected_prior = expected_prior_sofr_business_day(session_date, calendar_spec=calendar_spec)
        is_fresh_tbill = eff_date in (session_date, expected_prior)
        tbill_lag = "T-0" if eff_date == session_date else ("T-1" if eff_date == expected_prior else "stale")
        tbill_reason = None if is_fresh_tbill else "tbill_stale_effective_date"
        return _build_mkt_record(
            instrument=inst,
            provider=provider,
            series_id=series_id,
            price_basis="tbill_3m_days_365",
            units="annual_rate_decimal",
            effective_session=eff_date,
            source_publication_time_utc=None,
            retrieval_time_utc=retrieval_time_utc,
            calendar_session=cal,
            vintage=vintage,
            observation_class=observation_class,
            raw_bytes_ref=ref,
            raw_bytes_sha256=digest,
            normalized_value=canonical_decimal_str(rate_dec),
            is_modelled=False,
            disclosures={
                "accrual_basis": "days/365",
                "tbill_effective_lag": tbill_lag,
                "vintage_lag": tbill_lag,
                "actual_effective_date": eff_date,
                "expected_prior_business_day": expected_prior,
            },
            revision_seq=revision_seq,
            missing_reason=tbill_reason,
            decision_cutoff_utc=decision_cutoff_utc,
            session_close_utc=session_close_utc,
        )

    raise ValueError(f"Unsupported series kind: {kind!r}")


def _infer_series_kind(rec: dict) -> str | None:
    provider = str(rec.get("provider", ""))
    inst = str(rec.get("instrument", ""))
    if provider.startswith("tiingo"):
        return "etf_tiingo"
    if provider.startswith("yahoo"):
        return "idx_yahoo"
    if provider.startswith("nyfed"):
        return "rate_sofr"
    if provider.startswith("treasury"):
        return "rate_tbill"
    if provider == "y2_reconstruction_csv":
        return "y2_csv"
    if inst in ("TQQQ", "SOXL", "QQQ"):
        return "etf_tiingo"
    if inst in ("XNDX", "SEMI"):
        return "idx_yahoo"
    if inst == "SOFR":
        return "rate_sofr"
    if inst == "TBILL_3M":
        return "rate_tbill"
    return None


def _select_sofr_entry_for_effective_date(raw_bytes: bytes, eff_date: str) -> dict:
    payload = json.loads(raw_bytes.decode("utf-8"))
    ref_rates = payload.get("refRates") if isinstance(payload, dict) else None
    if not isinstance(ref_rates, list):
        raise ValueError("nyfed_missing_refRates")
    eligible = [
        entry
        for entry in ref_rates
        if isinstance(entry, dict)
        and entry.get("type") in (None, "SOFR")
        and entry.get("effectiveDate") == eff_date
        and entry.get("percentRate") is not None
    ]
    if not eligible:
        raise ValueError("session_not_in_payload")
    return max(eligible, key=lambda e: 1 if e.get("revisionIndicator") else 0)


def verify_market_record_raw_integrity(
    archive_dir: str | Path,
    rec: dict,
    *,
    session_date: str | None = None,
    calendar_session: Mapping[str, object] | None = None,
) -> tuple[bool, str | None]:
    """Verify SHA-256 of archived raw bytes and byte-for-byte re-derivation of normalized_value."""
    a_dir = Path(archive_dir)
    ref = rec.get("raw_bytes_ref") or ""
    digest = rec.get("raw_bytes_sha256") or ""
    if not ref:
        if rec.get("normalized_value") is None:
            return True, None
        return False, f"missing_raw_bytes_ref_for_populated_record:{rec.get('instrument')}"
    try:
        verified = read_verified_archived_bytes(a_dir, ref, digest)
    except FeederIntegrityError as exc:
        return False, str(exc)

    if rec.get("normalized_value") is None:
        return True, None

    kind = _infer_series_kind(rec)
    eff_sess = str(rec.get("effective_session", ""))
    try:
        if kind == "etf_tiingo":
            if session_date is not None and eff_sess != session_date:
                return False, f"effective_session_mismatch:{rec.get('instrument')}:{eff_sess}!={session_date}"
            row = select_tiingo_row(verified, eff_sess, calendar_session=calendar_session)
            rederived = canonical_decimal_str(Decimal(str(row["close"])))
        elif kind == "idx_yahoo":
            if session_date is not None and eff_sess != session_date:
                return False, f"effective_session_mismatch:{rec.get('instrument')}:{eff_sess}!={session_date}"
            rederived = canonical_decimal_str(
                select_yahoo_close(verified, eff_sess, calendar_session=calendar_session)
            )
        elif kind == "rate_sofr":
            if session_date is not None:
                entry = select_nyfed_sofr_entry(verified, session_date)
                if entry["effectiveDate"] != eff_sess:
                    return False, f"sofr_effective_session_mismatch:{entry['effectiveDate']}!={eff_sess}"
            else:
                entry = _select_sofr_entry_for_effective_date(verified, eff_sess)
            rederived = canonical_decimal_str(Decimal(str(entry["percentRate"])) / Decimal("100"))
        elif kind == "rate_tbill":
            target = session_date if session_date is not None else eff_sess
            eff_date, rate_dec = select_treasury_tbill_row(verified, target)
            if eff_date != eff_sess:
                return False, f"tbill_effective_session_mismatch:{eff_date}!={eff_sess}"
            rederived = canonical_decimal_str(rate_dec)
        elif kind == "y2_csv":
            if session_date is not None and eff_sess != session_date:
                return False, f"effective_session_mismatch:{rec.get('instrument')}:{eff_sess}!={session_date}"
            rows = _parse_y2_csv_bytes(verified)
            row = rows.get(eff_sess)
            if row is None:
                return False, f"y2_session_missing:{eff_sess}"
            inst = rec.get("instrument")
            if inst in ("TQQQ", "SOXL"):
                rederived = canonical_decimal_str(Decimal(row["mkt_close_split_adj"]))
            elif inst in ("XNDX", "SEMI"):
                rederived = canonical_decimal_str(Decimal(row["native_tr_close"] or row["idx_tr_index"]))
            elif inst == "SOFR":
                rederived = canonical_decimal_str(Decimal(row["sofr_rate"] or row["rf_rate"]))
            elif inst == "TBILL_3M":
                rederived = canonical_decimal_str(Decimal(row["tbill_3m_rate"]))
            else:
                return False, f"unknown_y2_instrument:{inst}"
        else:
            return False, f"unknown_series_kind:{kind}"
    except (ValueError, json.JSONDecodeError) as exc:
        return False, f"raw_rederivation_failed:{rec.get('instrument')}:{exc}"

    if rederived != rec.get("normalized_value"):
        return (
            False,
            f"rederived_value_mismatch:{rec.get('instrument')}:{rederived}!={rec.get('normalized_value')}",
        )
    return True, None


def emit_market_records_from_payloads(
    series: Sequence[SeriesSpec | tuple[str, str, str]],
    payloads: Mapping[str, bytes | None],
    *,
    session_date: str,
    retrieval_time_utc: str,
    archive_dir: str | Path,
    calendar_spec: dict | None = None,
    revision_seq: int = 0,
    vintage: str,
    observation_class: str,
    provider_for: Mapping[str, str] | Callable[[str], str],
    missing_reason_for: (
        Mapping[str, str | tuple[str, dict]] | Callable[[str], str | tuple[str, dict]]
    ),
    missing_disclosures_for: Mapping[str, dict] | Callable[[str], dict] | None = None,
    archive_name_for: Mapping[str, str] | Callable[[str], str] | None = None,
    decision_cutoff_utc: str | None = None,
    session_close_utc: str | None = None,
    retrieval_times_by_inst: Mapping[str, str] | None = None,
    auto_advance_revision: bool = False,
) -> list[dict]:
    """Archive raw payloads (or record missing-with-reason) and derive C-SH-MKT records."""
    a_dir = Path(archive_dir)
    spec = calendar_spec if calendar_spec is not None else load_spec(DEFAULT_SPEC_DIR)
    cal = resolve_session("XNYS", session_date, spec=spec)
    eff_session_close = session_close_utc if session_close_utc is not None else cal.get("close_utc")
    eff_decision_cutoff = decision_cutoff_utc
    if eff_decision_cutoff is None and cal.get("is_trading_day"):
        eff_decision_cutoff = default_eod_decision_cutoff_utc(session_date, cal)
    records: list[dict] = []

    for inst, series_id, kind in series:
        provider = str(_resolve_lookup(provider_for, inst))
        inst_retrieval_utc = (
            str(retrieval_times_by_inst[inst])
            if retrieval_times_by_inst is not None and inst in retrieval_times_by_inst
            else retrieval_time_utc
        )
        raw_bytes = payloads.get(inst)
        if raw_bytes is None:
            missing_entry = _resolve_lookup(missing_reason_for, inst)
            if isinstance(missing_entry, tuple):
                reason, disc = missing_entry[0], dict(missing_entry[1])
            else:
                reason = str(missing_entry)
                disc = (
                    dict(_resolve_lookup(missing_disclosures_for, inst))  # type: ignore[arg-type]
                    if missing_disclosures_for is not None
                    else {}
                )
            records.append(
                _build_missing_record(
                    inst=inst,
                    provider=provider,
                    series_id=series_id,
                    session_date=session_date,
                    retrieval_time_utc=inst_retrieval_utc,
                    cal=cal,
                    vintage=vintage,
                    observation_class=observation_class,
                    disclosures=disc,
                    revision_seq=revision_seq,
                    missing_reason=reason,
                    decision_cutoff_utc=eff_decision_cutoff,
                    session_close_utc=eff_session_close,
                )
            )
            continue

        digest_candidate = hashlib.sha256(raw_bytes).hexdigest()
        eff_revision_seq = revision_seq
        while True:
            if archive_name_for is not None:
                base_rel = str(_resolve_lookup(archive_name_for, inst))
                if eff_revision_seq == revision_seq:
                    rel = base_rel
                elif base_rel.startswith(f"rev{revision_seq}_"):
                    rel = f"rev{eff_revision_seq}_" + base_rel[len(f"rev{revision_seq}_") :]
                else:
                    rel = f"rev{eff_revision_seq}_{base_rel}"
            else:
                ext = "csv" if kind == "rate_tbill" else "json"
                rel = f"rev{eff_revision_seq}_{provider}_{inst.lower()}_{session_date}.{ext}"

            existing_path = a_dir / rel
            if auto_advance_revision and existing_path.exists():
                existing_digest = hashlib.sha256(existing_path.read_bytes()).hexdigest()
                if existing_digest != digest_candidate:
                    eff_revision_seq += 1
                    continue
            break

        ref, digest = archive_raw_bytes(raw_bytes, a_dir, rel)
        verified_bytes = read_verified_archived_bytes(a_dir, ref, digest)

        try:
            rec = _record_from_verified_bytes(
                inst=inst,
                provider=provider,
                series_id=series_id,
                kind=kind,
                verified_bytes=verified_bytes,
                ref=ref,
                digest=digest,
                session_date=session_date,
                retrieval_time_utc=inst_retrieval_utc,
                cal=cal,
                calendar_spec=spec,
                vintage=vintage,
                observation_class=observation_class,
                revision_seq=eff_revision_seq,
                decision_cutoff_utc=eff_decision_cutoff,
                session_close_utc=eff_session_close,
            )
        except ValueError as exc:
            err_msg = str(exc)
            if (
                err_msg
                not in (
                    "session_not_in_payload",
                    "tiingo_missing_corporate_action_fields",
                    "extended_hours_overnight_rejected",
                    "next_trade_date_contamination",
                    "yahoo_invalid_timestamp",
                    "yahoo_non_daily_granularity_rejected",
                    "tiingo_invalid_timestamp",
                )
                and not err_msg.startswith(("invalid_numeric:", "nonfinite_numeric:"))
            ):
                raise
            rec = _build_missing_record(
                inst=inst,
                provider=provider,
                series_id=series_id,
                session_date=session_date,
                retrieval_time_utc=inst_retrieval_utc,
                cal=cal,
                vintage=vintage,
                observation_class=observation_class,
                disclosures={err_msg: session_date},
                revision_seq=eff_revision_seq,
                missing_reason=err_msg,
                raw_bytes_ref=ref,
                raw_bytes_sha256=digest,
                decision_cutoff_utc=eff_decision_cutoff,
                session_close_utc=eff_session_close,
            )
        records.append(rec)

    return records


def emit_market_records_from_fixtures(
    fixture_dir: str | Path,
    session_date: str,
    retrieval_time_utc: str,
    archive_dir: str | Path,
    calendar_spec: dict | None = None,
    revision_seq: int = 0,
    sofr_filename: str | None = None,
    *,
    series: Sequence[SeriesSpec | tuple[str, str, str]] = DEMO_SERIES,
    decision_cutoff_utc: str | None = None,
    session_close_utc: str | None = None,
    auto_advance_revision: bool = False,
) -> list[dict]:
    """Archive recorded provider responses and derive C-SH-MKT records from archived bytes."""
    f_dir = Path(fixture_dir)

    fixture_names = {
        "TQQQ": f"tiingo_tqqq_{session_date}.json",
        "SOXL": f"tiingo_soxl_{session_date}.json",
        "QQQ": f"tiingo_qqq_{session_date}.json",
        "XNDX": f"yahoo_xndx_{session_date}.json",
        "SEMI": f"yahoo_xsox_{session_date}.json",
        "SOFR": sofr_filename or f"nyfed_sofr_{session_date}.json",
        "TBILL_3M": f"treasury_tbill_{session_date}.csv",
    }
    providers = {
        "TQQQ": "tiingo_fixture",
        "SOXL": "tiingo_fixture",
        "QQQ": "tiingo_fixture",
        "XNDX": "yahoo_fixture",
        "SEMI": "yahoo_fixture",
        "SOFR": "nyfed_fixture",
        "TBILL_3M": "treasury_fixture",
    }
    payloads: dict[str, bytes | None] = {}
    for spec_item in series:
        inst = spec_item[0] if isinstance(spec_item, tuple) else spec_item.instrument
        fpath = f_dir / fixture_names[inst]
        payloads[inst] = fpath.read_bytes() if fpath.is_file() else None

    return emit_market_records_from_payloads(
        series,
        payloads,
        session_date=session_date,
        retrieval_time_utc=retrieval_time_utc,
        archive_dir=archive_dir,
        calendar_spec=calendar_spec,
        revision_seq=revision_seq,
        vintage="recorded_fixture",
        observation_class="synthetic",
        provider_for=providers,
        missing_reason_for={inst: f"missing_fixture:{fname}" for inst, fname in fixture_names.items()},
        missing_disclosures_for={
            inst: {"missing_fixture": fname} for inst, fname in fixture_names.items()
        },
        archive_name_for={
            inst: f"rev{revision_seq}_{fname}" for inst, fname in fixture_names.items()
        },
        decision_cutoff_utc=decision_cutoff_utc,
        session_close_utc=session_close_utc,
        auto_advance_revision=auto_advance_revision,
    )


def _parse_y2_csv_bytes(raw_bytes: bytes) -> dict[str, dict[str, str]]:
    text = raw_bytes.decode("utf-8")
    data_lines = [ln for ln in text.splitlines() if ln.strip() and not ln.startswith("#")]
    reader = csv.DictReader(io.StringIO("\n".join(data_lines)))
    by_date: dict[str, dict[str, str]] = {}
    for row in reader:
        by_date[row["date"]] = row
    return by_date


def emit_y2_reconstruction_records(
    y2_dir: str | Path,
    archive_dir: str | Path,
    calendar_spec: dict | None = None,
) -> dict[str, list[dict]]:
    """Archive all 5 Y2 CSVs and derive per-session C-SH-MKT reconstruction records."""
    src = Path(y2_dir)
    a_dir = Path(archive_dir)
    spec = calendar_spec if calendar_spec is not None else load_spec(DEFAULT_SPEC_DIR)

    csv_names = (
        "tqqq_daily.csv",
        "soxl_daily.csv",
        "ndx_index_daily.csv",
        "semi_index_daily.csv",
        "risk_free_rates_daily.csv",
    )
    parsed: dict[str, dict[str, dict[str, str]]] = {}
    refs: dict[str, tuple[str, str]] = {}
    for fname in csv_names:
        ref, digest = archive_raw_bytes((src / fname).read_bytes(), a_dir, f"y2/{fname}")
        verified = read_verified_archived_bytes(a_dir, ref, digest)
        refs[fname] = (ref, digest)
        parsed[fname] = _parse_y2_csv_bytes(verified)

    sessions = sorted(parsed["tqqq_daily.csv"].keys())
    per_session_records: dict[str, list[dict]] = {}

    for ds in sessions:
        cal = resolve_session("XNYS", ds, spec=spec)
        ret_utc = cal["close_utc"] or f"{ds}T21:00:00.000Z"

        tqqq_row = parsed["tqqq_daily.csv"][ds]
        soxl_row = parsed["soxl_daily.csv"][ds]
        ndx_row = parsed["ndx_index_daily.csv"][ds]
        semi_row = parsed["semi_index_daily.csv"][ds]
        rf_row = parsed["risk_free_rates_daily.csv"][ds]

        ndx_modelled = ndx_row["is_modelled"].strip().lower() == "true"
        semi_modelled = semi_row["is_modelled"].strip().lower() == "true"

        tqqq_rec = _build_mkt_record(
            instrument="TQQQ",
            provider="y2_reconstruction_csv",
            series_id="TQQQ",
            price_basis="mkt_close_split_adj",
            units="USD/share",
            effective_session=ds,
            source_publication_time_utc=None,
            retrieval_time_utc=ret_utc,
            calendar_session=cal,
            vintage=VINTAGE_Y2,
            observation_class="historical_reconstruction",
            raw_bytes_ref=refs["tqqq_daily.csv"][0],
            raw_bytes_sha256=refs["tqqq_daily.csv"][1],
            normalized_value=canonical_decimal_str(Decimal(tqqq_row["mkt_close_split_adj"])),
            is_modelled=False,
            disclosures={
                "source": tqqq_row["source"],
                "quality_flag": tqqq_row["quality_flag"],
                "nav_split_adj": canonical_decimal_str(Decimal(tqqq_row["nav_split_adj"])),
            },
            corporate_action={
                "ex_date": ds,
                "split_ratio": "1",
                "cash_per_share_usd": canonical_decimal_str(Decimal(tqqq_row["div_declared"])),
                "broker_reconciliation_claim": False,
            },
        )
        soxl_rec = _build_mkt_record(
            instrument="SOXL",
            provider="y2_reconstruction_csv",
            series_id="SOXL",
            price_basis=SOXL_PROXY_LABEL,
            units="USD/share",
            effective_session=ds,
            source_publication_time_utc=None,
            retrieval_time_utc=ret_utc,
            calendar_session=cal,
            vintage=VINTAGE_Y2,
            observation_class="historical_reconstruction",
            raw_bytes_ref=refs["soxl_daily.csv"][0],
            raw_bytes_sha256=refs["soxl_daily.csv"][1],
            normalized_value=canonical_decimal_str(Decimal(soxl_row["mkt_close_split_adj"])),
            is_modelled=False,
            disclosures={
                "source": soxl_row["source"],
                "quality_flag": soxl_row["quality_flag"],
                "proxy_label": SOXL_PROXY_LABEL,
            },
            corporate_action={
                "ex_date": ds,
                "split_ratio": "1",
                "cash_per_share_usd": canonical_decimal_str(Decimal(soxl_row["div_declared"])),
                "broker_reconciliation_claim": False,
            },
        )
        ndx_rec = _build_mkt_record(
            instrument="XNDX",
            provider="y2_reconstruction_csv",
            series_id="XNDX",
            price_basis="idx_tr_index" if not ndx_modelled else "path_c_modelled_tr",
            units="index_points",
            effective_session=ds,
            source_publication_time_utc=None,
            retrieval_time_utc=ret_utc,
            calendar_session=cal,
            vintage=VINTAGE_Y2,
            observation_class="historical_reconstruction",
            raw_bytes_ref=refs["ndx_index_daily.csv"][0],
            raw_bytes_sha256=refs["ndx_index_daily.csv"][1],
            normalized_value=canonical_decimal_str(
                Decimal(ndx_row["native_tr_close"] or ndx_row["idx_tr_index"])
            ),
            is_modelled=ndx_modelled,
            disclosures={
                "source": ndx_row["source"],
                "tr_regime": ndx_row["tr_regime"],
                "quality_flag": ndx_row["quality_flag"],
            },
        )
        semi_disclosures = {
            "source": semi_row["source"],
            "tr_regime": semi_row["tr_regime"],
            "quality_flag": semi_row["quality_flag"],
        }
        if ds >= XSOX_SUBSTITUTION_CUTOFF:
            semi_disclosures["xsox_for_icesemit_substitution"] = True
            semi_disclosures["substitution_note"] = "post-2021-08-25 XSOX substitution for ICESEMIT"
        semi_rec = _build_mkt_record(
            instrument="SEMI",
            provider="y2_reconstruction_csv",
            series_id="XSOX",
            price_basis="idx_tr_index" if not semi_modelled else "path_c_modelled_tr",
            units="index_points",
            effective_session=ds,
            source_publication_time_utc=None,
            retrieval_time_utc=ret_utc,
            calendar_session=cal,
            vintage=VINTAGE_Y2,
            observation_class="historical_reconstruction",
            raw_bytes_ref=refs["semi_index_daily.csv"][0],
            raw_bytes_sha256=refs["semi_index_daily.csv"][1],
            normalized_value=canonical_decimal_str(
                Decimal(semi_row["native_tr_close"] or semi_row["idx_tr_index"])
            ),
            is_modelled=semi_modelled,
            disclosures=semi_disclosures,
        )
        sofr_rec = _build_mkt_record(
            instrument="SOFR",
            provider="y2_reconstruction_csv",
            series_id="SOFR",
            price_basis="overnight_rate_days_365",
            units="annual_rate_decimal",
            effective_session=ds,
            source_publication_time_utc=None,
            retrieval_time_utc=ret_utc,
            calendar_session=cal,
            vintage=VINTAGE_Y2,
            observation_class="historical_reconstruction",
            raw_bytes_ref=refs["risk_free_rates_daily.csv"][0],
            raw_bytes_sha256=refs["risk_free_rates_daily.csv"][1],
            normalized_value=canonical_decimal_str(Decimal(rf_row["sofr_rate"] or rf_row["rf_rate"])),
            is_modelled=False,
            disclosures={
                "accrual_basis": "days/365",
                "rf_regime": rf_row["rf_regime"],
                "quality_flag": rf_row["quality_flag"],
            },
        )
        tbill_rec = _build_mkt_record(
            instrument="TBILL_3M",
            provider="y2_reconstruction_csv",
            series_id="TBILL_3M",
            price_basis="tbill_3m_days_365",
            units="annual_rate_decimal",
            effective_session=ds,
            source_publication_time_utc=None,
            retrieval_time_utc=ret_utc,
            calendar_session=cal,
            vintage=VINTAGE_Y2,
            observation_class="historical_reconstruction",
            raw_bytes_ref=refs["risk_free_rates_daily.csv"][0],
            raw_bytes_sha256=refs["risk_free_rates_daily.csv"][1],
            normalized_value=canonical_decimal_str(Decimal(rf_row["tbill_3m_rate"])),
            is_modelled=False,
            disclosures={
                "accrual_basis": "days/365",
                "rf_regime": rf_row["rf_regime"],
                "quality_flag": rf_row["quality_flag"],
            },
        )
        per_session_records[ds] = [tqqq_rec, soxl_rec, ndx_rec, semi_rec, sofr_rec, tbill_rec]

    return per_session_records


def run_reconstruction_window(
    y2_dir: str | Path,
    ledger_dir: str | Path,
    archive_dir: str | Path,
    spec_dir: str | Path | None = None,
) -> dict:
    """Run the 10-session Y2 overlap window in reconstruction mode and append to ledger."""
    spec = load_spec(spec_dir or DEFAULT_SPEC_DIR)
    mkt_by_session = emit_y2_reconstruction_records(y2_dir, archive_dir, calendar_spec=spec)
    ledger = ShadowLedger(ledger_dir)
    ev = StubEvaluator()
    code_sha = compute_code_manifest_hash()
    prev = GENESIS_PREV_HASH
    prior_state: dict = {}

    for seq, ds in enumerate(sorted(mkt_by_session.keys())):
        cal = resolve_session("XNYS", ds, spec=spec)
        mkt_recs = mkt_by_session[ds]
        by_inst = {r["instrument"]: r for r in mkt_recs}

        per_series_modelled = {r["instrument"]: r["is_modelled"] for r in mkt_recs}
        discipline_labels = {
            "observation_class": "historical_reconstruction",
            "vintage_qualification": VINTAGE_Y2,
            "soxl_basis": SOXL_PROXY_LABEL,
            "semi_substitution": by_inst["SEMI"]["disclosures"].get("tr_regime"),
            "per_series_is_modelled": per_series_modelled,
            "any_series_modelled": any(per_series_modelled.values()),
            "forward_clock_credit_eligible": False,
        }

        session_inputs = {
            "E": "50000",
            "E_star": "50000",
            "H": "0",
            "L": "0",
            "funds": {
                "TQQQ": {
                    "s": "1",
                    "close_price": by_inst["TQQQ"]["normalized_value"],
                    "q_pre": 0,
                    "sigma_forecast": "0.24",
                },
                "SOXL": {
                    "s": "1",
                    "close_price": by_inst["SOXL"]["normalized_value"],
                    "q_pre": 0,
                    "sigma_forecast": "0.24",
                },
            },
        }
        built = build_record_body(
            chain_id="y2-overlap-reconstruction",
            sequence=seq,
            prev_record_sha256=prev,
            mode="reconstruction",
            session_date=ds,
            decision_timestamp_utc=cal["close_utc"],
            session_inputs=session_inputs,
            calendar_session=cal,
            code_version_sha=code_sha,
            evaluator=ev,
            prior_row_state=prior_state,
            discipline_labels=discipline_labels,
            market_records_digest=sha256_canonical(mkt_recs),
        )
        env = ledger.append_record(built["record_body"], obtained_at_utc=cal["close_utc"])
        prev = env["record_sha256"]
        prior_state = built["next_row_state"]

    return {
        "session_count": len(mkt_by_session),
        "ledger": ledger,
        "market_records": mkt_by_session,
    }


def validate_official_close_market_record(
    rec: Mapping[str, object],
    calendar_session: Mapping[str, object],
) -> tuple[bool, str | None]:
    """Enforce US 23/5 16:00 ET Official Close isolation & non-contamination for equity/index prints."""
    inst = str(rec.get("instrument", ""))
    disclosures = rec.get("disclosures") if isinstance(rec.get("disclosures"), Mapping) else {}
    session_marker = str(disclosures.get("session_window") or disclosures.get("market_session") or "").lower()
    if session_marker in _EXTENDED_SESSION_MARKERS:
        return False, "extended_hours_overnight_rejected"
    if inst in ("TQQQ", "SOXL", "QQQ", "XNDX", "SEMI"):
        trade_date = str(calendar_session.get("trade_date") or "")
        eff = str(rec.get("effective_session") or "")
        if trade_date and eff > trade_date:
            return False, "next_trade_date_contamination"
        bar_ts = disclosures.get("bar_timestamp_utc")
        if isinstance(bar_ts, str) and bar_ts.strip():
            ok_ts, err = is_official_close_bar_timestamp(bar_ts, calendar_session)
            if not ok_ts:
                return False, err
    return True, None


def extract_prior_close_from_tiingo_bytes(
    raw_bytes: bytes,
    session_date: str,
    *,
    calendar_session: Mapping[str, object] | None = None,
) -> str | None:
    """Return latest valid prior-session (< session_date) close string from multi-day Tiingo JSON."""
    try:
        rows = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(rows, list):
        return None
    candidates: list[tuple[str, Decimal]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        raw_date = str(row.get("date", ""))
        row_d = raw_date[:10]
        if len(row_d) != 10 or row_d >= session_date or row.get("close") is None:
            continue
        if row.get("extendedHours") is True or row.get("isExtendedHours") is True:
            continue
        try:
            _validate_core_rth_bar_timestamp(
                raw_date,
                row_d,
                calendar_session,
                allow_midnight_date_marker=True,
            )
            if "timestamp" in row and row.get("timestamp") is not None:
                _validate_core_rth_bar_timestamp(
                    row.get("timestamp"),
                    row_d,
                    calendar_session,
                    allow_midnight_date_marker=False,
                )
            close_dec = _parse_finite_decimal(row["close"], "tiingo_prior_close")
        except ValueError:
            continue
        candidates.append((row_d, close_dec))
    if not candidates:
        return None
    _, best_close = max(candidates, key=lambda item: item[0])
    return canonical_decimal_str(best_close)


def extract_prior_close_from_yahoo_bytes(
    raw_bytes: bytes,
    session_date: str,
    *,
    calendar_session: Mapping[str, object] | None = None,
) -> str | None:
    """Return latest valid prior-session (< session_date) close string from multi-day Yahoo JSON."""
    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    results = (payload.get("chart") or {}).get("result") if isinstance(payload, dict) else None
    if not isinstance(results, list) or not results or not isinstance(results[0], dict):
        return None
    result0 = results[0]
    meta = result0.get("meta") if isinstance(result0.get("meta"), dict) else {}
    if "dataGranularity" in meta and meta.get("dataGranularity") not in ("1d", "1day", "daily"):
        return None
    timestamps = result0.get("timestamp") or []
    quote = ((result0.get("indicators") or {}).get("quote") or [{}])[0]
    closes = quote.get("close") or []
    close_t = _official_close_time_for_calendar(calendar_session)
    best: tuple[str, int, Decimal] | None = None
    for ts, close in zip(timestamps, closes):
        if ts is None or close is None or isinstance(ts, bool):
            continue
        try:
            ts_int = int(ts)
            bar_dt = datetime.fromtimestamp(ts_int, tz=_NY_TZ)
            bar_date = bar_dt.date().isoformat()
        except (ValueError, TypeError, OverflowError, OSError):
            continue
        if bar_date >= session_date:
            continue
        bar_t = bar_dt.time()
        if bar_t < dt_time(9, 30) or bar_t > close_t or bar_t >= dt_time(21, 0):
            continue
        try:
            c_dec = _parse_finite_decimal(close, "yahoo_prior_close")
        except ValueError:
            continue
        cand = (bar_date, ts_int, c_dec)
        if best is None or (cand[0], cand[1]) > (best[0], best[1]):
            best = cand
    return canonical_decimal_str(best[2]) if best is not None else None
