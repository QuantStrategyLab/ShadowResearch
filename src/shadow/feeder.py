"""C-SH-MKT decision-time and Y2 reconstruction market-data feeder."""

from __future__ import annotations

import csv
import hashlib
import io
import json
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import NamedTuple
from zoneinfo import ZoneInfo

from shadow.calendar_consumer import DEFAULT_SPEC_DIR, load_spec, resolve_session
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
) -> dict:
    cutoff = calendar_session.get("order_cutoff_utc")
    eligible = (
        normalized_value is not None
        and bool(calendar_session.get("is_trading_day"))
        and not bool(calendar_session.get("blocks_eligibility"))
        and cutoff is not None
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


def select_tiingo_row(raw_bytes: bytes, session_date: str) -> dict:
    """Parse Tiingo daily JSON array and return the row matching session_date."""
    rows = json.loads(raw_bytes.decode("utf-8"))
    if not isinstance(rows, list):
        raise ValueError("tiingo_payload_not_list")
    for row in rows:
        if (
            isinstance(row, dict)
            and str(row.get("date", "")).startswith(session_date)
            and row.get("close") is not None
        ):
            return row
    raise ValueError("session_not_in_payload")


def select_yahoo_close(raw_bytes: bytes, session_date: str) -> Decimal:
    """Parse Yahoo chart JSON and return close Decimal for America/New_York session_date."""
    payload = json.loads(raw_bytes.decode("utf-8"))
    results = (payload.get("chart") or {}).get("result") if isinstance(payload, dict) else None
    if not isinstance(results, list) or not results or not isinstance(results[0], dict):
        raise ValueError("yahoo_empty_result")
    result0 = results[0]
    timestamps = result0.get("timestamp") or []
    quote = ((result0.get("indicators") or {}).get("quote") or [{}])[0]
    closes = quote.get("close") or []
    if timestamps:
        selected: Decimal | None = None
        for ts, close in zip(timestamps, closes):
            if ts is None or close is None:
                continue
            bar_date = datetime.fromtimestamp(int(ts), tz=_NY_TZ).date().isoformat()
            if bar_date == session_date:
                selected = Decimal(str(close))
        if selected is not None:
            return selected
        raise ValueError("session_not_in_payload")
    if len(closes) == 1 and closes[0] is not None:
        return Decimal(str(closes[0]))
    raise ValueError("session_not_in_payload")


def select_nyfed_sofr_entry(raw_bytes: bytes, session_date: str) -> dict:
    """Parse NY Fed SOFR JSON and select entry with greatest effectiveDate <= session_date."""
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
        and entry["effectiveDate"] <= session_date
        and entry.get("percentRate") is not None
    ]
    if not eligible:
        raise ValueError("session_not_in_payload")
    return max(
        eligible,
        key=lambda e: (e["effectiveDate"], 1 if e.get("revisionIndicator") else 0),
    )


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
            best_date = iso_d
            best_rate = Decimal(raw_v) / Decimal("100")

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
    vintage: str,
    observation_class: str,
    revision_seq: int,
) -> dict:
    if kind == "etf_tiingo":
        row = select_tiingo_row(verified_bytes, session_date)
        close_str = canonical_decimal_str(Decimal(str(row["close"])))
        div_cash = canonical_decimal_str(Decimal(str(row.get("divCash", "0"))))
        split_ratio = canonical_decimal_str(Decimal(str(row.get("splitFactor", "1"))))
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
            source_publication_time_utc=row.get("date"),
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
        )

    if kind == "idx_yahoo":
        close_dec = select_yahoo_close(verified_bytes, session_date)
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
        )

    if kind == "rate_sofr":
        entry = select_nyfed_sofr_entry(verified_bytes, session_date)
        rate_dec = Decimal(str(entry["percentRate"])) / Decimal("100")
        return _build_mkt_record(
            instrument=inst,
            provider=provider,
            series_id=series_id,
            price_basis="overnight_rate_days_365",
            units="annual_rate_decimal",
            effective_session=entry["effectiveDate"],
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
                "sofr_effective_lag": "T-1",
                "revision_indicator": entry.get("revisionIndicator", ""),
            },
            revision_seq=revision_seq,
        )

    if kind == "rate_tbill":
        eff_date, rate_dec = select_treasury_tbill_row(verified_bytes, session_date)
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
            disclosures={"accrual_basis": "days/365"},
            revision_seq=revision_seq,
        )

    raise ValueError(f"Unsupported series kind: {kind!r}")


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
) -> list[dict]:
    """Archive raw payloads (or record missing-with-reason) and derive C-SH-MKT records."""
    a_dir = Path(archive_dir)
    spec = calendar_spec if calendar_spec is not None else load_spec(DEFAULT_SPEC_DIR)
    cal = resolve_session("XNYS", session_date, spec=spec)
    records: list[dict] = []

    for inst, series_id, kind in series:
        provider = str(_resolve_lookup(provider_for, inst))
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
                    retrieval_time_utc=retrieval_time_utc,
                    cal=cal,
                    vintage=vintage,
                    observation_class=observation_class,
                    disclosures=disc,
                    revision_seq=revision_seq,
                    missing_reason=reason,
                )
            )
            continue

        if archive_name_for is not None:
            rel = str(_resolve_lookup(archive_name_for, inst))
        else:
            ext = "csv" if kind == "rate_tbill" else "json"
            rel = f"rev{revision_seq}_{provider}_{inst.lower()}_{session_date}.{ext}"

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
                retrieval_time_utc=retrieval_time_utc,
                cal=cal,
                vintage=vintage,
                observation_class=observation_class,
                revision_seq=revision_seq,
            )
        except ValueError as exc:
            if str(exc) != "session_not_in_payload":
                raise
            rec = _build_missing_record(
                inst=inst,
                provider=provider,
                series_id=series_id,
                session_date=session_date,
                retrieval_time_utc=retrieval_time_utc,
                cal=cal,
                vintage=vintage,
                observation_class=observation_class,
                disclosures={"session_not_in_payload": session_date},
                revision_seq=revision_seq,
                missing_reason="session_not_in_payload",
                raw_bytes_ref=ref,
                raw_bytes_sha256=digest,
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
) -> list[dict]:
    """Archive recorded provider responses and derive C-SH-MKT records from archived bytes."""
    f_dir = Path(fixture_dir)

    fixture_names = {
        "TQQQ": f"tiingo_tqqq_{session_date}.json",
        "SOXL": f"tiingo_soxl_{session_date}.json",
        "XNDX": f"yahoo_xndx_{session_date}.json",
        "SEMI": f"yahoo_xsox_{session_date}.json",
        "SOFR": sofr_filename or f"nyfed_sofr_{session_date}.json",
        "TBILL_3M": f"treasury_tbill_{session_date}.csv",
    }
    providers = {
        "TQQQ": "tiingo_fixture",
        "SOXL": "tiingo_fixture",
        "XNDX": "yahoo_fixture",
        "SEMI": "yahoo_fixture",
        "SOFR": "nyfed_fixture",
        "TBILL_3M": "treasury_fixture",
    }
    payloads: dict[str, bytes | None] = {}
    for spec_item in DEMO_SERIES:
        fpath = f_dir / fixture_names[spec_item.instrument]
        payloads[spec_item.instrument] = fpath.read_bytes() if fpath.is_file() else None

    return emit_market_records_from_payloads(
        DEMO_SERIES,
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
        env = ledger.append_record(built["record_body"])
        prev = env["record_sha256"]
        prior_state = built["next_row_state"]

    return {
        "session_count": len(mkt_by_session),
        "ledger": ledger,
        "market_records": mkt_by_session,
    }
