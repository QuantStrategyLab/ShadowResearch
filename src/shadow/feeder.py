"""C-SH-MKT decision-time and Y2 reconstruction market-data feeder."""

from __future__ import annotations

import csv
import hashlib
import io
import json
from decimal import Decimal
from pathlib import Path

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
    a_dir = Path(archive_dir)
    spec = calendar_spec if calendar_spec is not None else load_spec(DEFAULT_SPEC_DIR)
    cal = resolve_session("XNYS", session_date, spec=spec)

    records: list[dict] = []

    specs_list = [
        ("TQQQ", "tiingo_fixture", "TQQQ", f"tiingo_tqqq_{session_date}.json", "etf_tiingo"),
        ("SOXL", "tiingo_fixture", "SOXL", f"tiingo_soxl_{session_date}.json", "etf_tiingo"),
        ("XNDX", "yahoo_fixture", "XNDX", f"yahoo_xndx_{session_date}.json", "idx_yahoo"),
        ("SEMI", "yahoo_fixture", "XSOX", f"yahoo_xsox_{session_date}.json", "idx_yahoo"),
        (
            "SOFR",
            "nyfed_fixture",
            "SOFR",
            sofr_filename or f"nyfed_sofr_{session_date}.json",
            "rate_sofr",
        ),
        ("TBILL_3M", "treasury_fixture", "DTB3_PAR_3M", f"treasury_tbill_{session_date}.csv", "rate_tbill"),
    ]

    for inst, provider, series_id, fname, kind in specs_list:
        fpath = f_dir / fname
        if not fpath.is_file():
            records.append(
                _build_mkt_record(
                    instrument=inst,
                    provider=provider,
                    series_id=series_id,
                    price_basis="unavailable",
                    units="USD",
                    effective_session=session_date,
                    source_publication_time_utc=None,
                    retrieval_time_utc=retrieval_time_utc,
                    calendar_session=cal,
                    vintage="recorded_fixture",
                    observation_class="synthetic",
                    raw_bytes_ref="",
                    raw_bytes_sha256="",
                    normalized_value=None,
                    is_modelled=False,
                    disclosures={"missing_fixture": fname},
                    revision_seq=revision_seq,
                    missing_reason=f"missing_fixture:{fname}",
                )
            )
            continue

        rel = f"rev{revision_seq}_{fname}"
        ref, digest = archive_raw_bytes(fpath.read_bytes(), a_dir, rel)
        verified_bytes = read_verified_archived_bytes(a_dir, ref, digest)

        if kind == "etf_tiingo":
            row = json.loads(verified_bytes.decode("utf-8"))[0]
            close_str = canonical_decimal_str(Decimal(str(row["close"])))
            div_cash = canonical_decimal_str(Decimal(str(row.get("divCash", "0"))))
            split_ratio = canonical_decimal_str(Decimal(str(row.get("splitFactor", "1"))))
            basis = "close (proxy, NAV unverified)"
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
            records.append(
                _build_mkt_record(
                    instrument=inst,
                    provider=provider,
                    series_id=series_id,
                    price_basis=basis,
                    units="USD/share",
                    effective_session=session_date,
                    source_publication_time_utc=row.get("date"),
                    retrieval_time_utc=retrieval_time_utc,
                    calendar_session=cal,
                    vintage="recorded_fixture",
                    observation_class="synthetic",
                    raw_bytes_ref=ref,
                    raw_bytes_sha256=digest,
                    normalized_value=close_str,
                    is_modelled=False,
                    disclosures=disclosures,
                    corporate_action=ca,
                    revision_seq=revision_seq,
                )
            )
        elif kind == "idx_yahoo":
            payload = json.loads(verified_bytes.decode("utf-8"))
            val = payload["chart"]["result"][0]["indicators"]["quote"][0]["close"][0]
            close_str = canonical_decimal_str(Decimal(str(val)))
            disclosures = {}
            if inst == "SEMI" and session_date >= XSOX_SUBSTITUTION_CUTOFF:
                disclosures["xsox_for_icesemit_substitution"] = True
                disclosures["substitution_note"] = "post-2021-08-25 XSOX substitution for ICESEMIT"
            records.append(
                _build_mkt_record(
                    instrument=inst,
                    provider=provider,
                    series_id=series_id,
                    price_basis="gross_total_return",
                    units="index_points",
                    effective_session=session_date,
                    source_publication_time_utc=None,
                    retrieval_time_utc=retrieval_time_utc,
                    calendar_session=cal,
                    vintage="recorded_fixture",
                    observation_class="synthetic",
                    raw_bytes_ref=ref,
                    raw_bytes_sha256=digest,
                    normalized_value=close_str,
                    is_modelled=False,
                    disclosures=disclosures,
                    revision_seq=revision_seq,
                )
            )
        elif kind == "rate_sofr":
            payload = json.loads(verified_bytes.decode("utf-8"))
            entry = payload["refRates"][0]
            rate_dec = Decimal(str(entry["percentRate"])) / Decimal("100")
            records.append(
                _build_mkt_record(
                    instrument=inst,
                    provider=provider,
                    series_id=series_id,
                    price_basis="overnight_rate_days_365",
                    units="annual_rate_decimal",
                    effective_session=entry["effectiveDate"],
                    source_publication_time_utc=None,
                    retrieval_time_utc=retrieval_time_utc,
                    calendar_session=cal,
                    vintage="recorded_fixture",
                    observation_class="synthetic",
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
            )
        elif kind == "rate_tbill":
            text = verified_bytes.decode("utf-8")
            reader = csv.DictReader(io.StringIO(text))
            row = next(reader)
            rate_dec = Decimal(str(row["3 Mo"])) / Decimal("100")
            records.append(
                _build_mkt_record(
                    instrument=inst,
                    provider=provider,
                    series_id=series_id,
                    price_basis="tbill_3m_days_365",
                    units="annual_rate_decimal",
                    effective_session=row["Date"],
                    source_publication_time_utc=None,
                    retrieval_time_utc=retrieval_time_utc,
                    calendar_session=cal,
                    vintage="recorded_fixture",
                    observation_class="synthetic",
                    raw_bytes_ref=ref,
                    raw_bytes_sha256=digest,
                    normalized_value=canonical_decimal_str(rate_dec),
                    is_modelled=False,
                    disclosures={"accrual_basis": "days/365"},
                    revision_seq=revision_seq,
                )
            )

    return records


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
