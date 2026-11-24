"""Deterministic replay CLI: re-derives records bit-for-bit and verifies chain + receipts."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping
from datetime import date, datetime, time as dt_time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from shadow.calendar_consumer import DEFAULT_SPEC_DIR, load_spec, resolve_session
from shadow.evaluator import (
    REQUIRED_MARKET_SERIES,
    StubEvaluator,
    StubProductionForbiddenError,
    Y2_REQUIRED_MARKET_SERIES,
)
from shadow.feeder import (
    VINTAGE_Y2,
    default_eod_decision_cutoff_utc,
    validate_official_close_market_record,
    verify_market_record_raw_integrity,
)
from shadow.fuse_adapter import (
    PINNED_FUSE_ENGINE_HASH,
    PINNED_FUSE_SPEC_VERSION,
    VendoredFuseEvaluator,
    is_stub_evaluator,
)
from shadow.ledger import (
    GENESIS_PREV_HASH,
    ShadowLedger,
    build_record_body,
    canonical_json_bytes,
    is_utc_ms_timestamp,
    sha256_canonical,
)
from shadow.manifest import compute_code_manifest_hash, load_effective_runtime_config
from shadow.ots import load_trusted_bitcoin_headers
from shadow.sizing import canonical_decimal_str, policy_config_hash

_NY_TZ = ZoneInfo("America/New_York")


def _resolve_spec_dir(bundle: dict) -> Path:
    raw = bundle.get("calendar_spec_dir")
    if raw:
        cand = Path(raw)
        if cand.is_dir():
            return cand
    ver = bundle.get("calendar_version")
    if ver:
        repo_spec = Path(__file__).resolve().parents[2] / "specs" / str(ver)
        if repo_spec.is_dir():
            return repo_spec
    return DEFAULT_SPEC_DIR


def replay_window(
    bundle_dir: str | Path,
    ledger_dir: str | Path,
    *,
    bitcoin_merkle_roots: Mapping[int, str] | None = None,
    evaluator: object | None = None,
) -> dict:
    """Re-derive every record bit-for-bit from retained inputs and verify chain + receipts separately."""
    b_dir = Path(bundle_dir)
    bundle = json.loads((b_dir / "replay_bundle.json").read_text(encoding="utf-8"))

    spec_dir = _resolve_spec_dir(bundle)
    spec = load_spec(spec_dir)
    if spec["spec_digest"] != bundle["calendar_spec_digest"]:
        return {
            "bit_for_bit_match": False,
            "chain_ok": False,
            "first_bad_sequence": None,
            "chain_error": "calendar_spec_digest_mismatch",
            "receipts_ok": False,
            "bad_receipt_sequence": None,
            "receipt_error": None,
            "receipts_creditable": False,
            "receipt_credit_issues": [],
            "archived_inputs_ok": False,
            "mismatches": ["calendar_spec_digest_mismatch"],
            "error": "calendar_spec_digest_mismatch",
            "replayed_count": 0,
        }

    expected_policy_hash = bundle.get("policy_config_hash")
    actual_policy_hash = policy_config_hash(
        runtime_config=bundle.get("runtime_config"),
        calendar_spec=spec,
        code_manifest_hash=bundle.get("code_version_sha"),
    )
    if expected_policy_hash and expected_policy_hash != actual_policy_hash:
        return {
            "bit_for_bit_match": False,
            "chain_ok": False,
            "first_bad_sequence": None,
            "chain_error": "policy_config_hash_mismatch",
            "receipts_ok": False,
            "bad_receipt_sequence": None,
            "receipt_error": None,
            "receipts_creditable": False,
            "receipt_credit_issues": [],
            "archived_inputs_ok": False,
            "mismatches": ["policy_config_hash_mismatch"],
            "error": "policy_config_hash_mismatch",
            "replayed_count": 0,
        }

    ledger = ShadowLedger(ledger_dir)
    chain_ok, first_bad_seq, chain_err = ledger.verify_chain()
    receipts_ok, bad_rec_seq, rec_err = ledger.verify_all_receipts(
        bitcoin_merkle_roots=bitcoin_merkle_roots
    )
    runtime_cfg = bundle.get("runtime_config")
    if isinstance(runtime_cfg, dict):
        raw_deadline = runtime_cfg.get("commitment_deadline_et")
        deadline_et: object | None = "__MISSING__" if raw_deadline is None else raw_deadline
    else:
        deadline_et = None
    receipts_creditable, receipt_credit_issues = ledger.assess_receipt_creditability(
        bitcoin_merkle_roots=bitcoin_merkle_roots,
        commitment_deadline_et=deadline_et,
    )

    record_files = ledger._record_files()
    stored_records = ledger.load_records()
    sessions = bundle["sessions"]
    if len(stored_records) != len(sessions):
        return {
            "bit_for_bit_match": False,
            "chain_ok": chain_ok,
            "first_bad_sequence": first_bad_seq,
            "chain_error": chain_err,
            "receipts_ok": receipts_ok,
            "bad_receipt_sequence": bad_rec_seq,
            "receipt_error": rec_err,
            "receipts_creditable": receipts_creditable,
            "receipt_credit_issues": receipt_credit_issues,
            "archived_inputs_ok": False,
            "mismatches": [
                f"record_count_mismatch: stored={len(stored_records)}, bundle={len(sessions)}"
            ],
            "error": f"record_count_mismatch: stored={len(stored_records)}, bundle={len(sessions)}",
            "replayed_count": len(stored_records),
        }

    raw_archive_dir = bundle.get("archive_dir")
    declared_archive = "archive_dir" in bundle and raw_archive_dir is not None
    archive_dir = Path(str(raw_archive_dir)) if raw_archive_dir else None
    if declared_archive and (archive_dir is None or not archive_dir.is_dir()):
        return {
            "bit_for_bit_match": False,
            "chain_ok": chain_ok,
            "first_bad_sequence": first_bad_seq,
            "chain_error": chain_err,
            "receipts_ok": receipts_ok,
            "bad_receipt_sequence": bad_rec_seq,
            "receipt_error": rec_err,
            "receipts_creditable": receipts_creditable,
            "receipt_credit_issues": receipt_credit_issues,
            "archived_inputs_ok": False,
            "archive_errors": [f"missing_archive_dir:{raw_archive_dir}"],
            "mismatches": ["missing_archive_dir"],
            "error": "missing_archive_dir",
            "replayed_count": 0,
        }

    bundle_engine_hash = bundle.get("engine_hash")
    rec0_body = (
        stored_records[0]["record_body"]
        if stored_records
        and isinstance(stored_records[0], dict)
        and isinstance(stored_records[0].get("record_body"), dict)
        else {}
    )
    rec0_engine_hash = rec0_body.get("engine_hash")
    is_prod_bundle = bundle.get("mode") == "production"

    if is_prod_bundle and evaluator is not None and is_stub_evaluator(evaluator):
        raise StubProductionForbiddenError(
            "StubEvaluator is structurally forbidden when replaying production mode"
        )
    if is_prod_bundle and (
        str(bundle_engine_hash or "").startswith("stub-")
        or str(rec0_engine_hash or "").startswith("stub-")
    ):
        return {
            "bit_for_bit_match": False,
            "chain_ok": chain_ok,
            "first_bad_sequence": first_bad_seq,
            "chain_error": chain_err,
            "receipts_ok": receipts_ok,
            "bad_receipt_sequence": bad_rec_seq,
            "receipt_error": rec_err,
            "receipts_creditable": receipts_creditable,
            "receipt_credit_issues": receipt_credit_issues,
            "archived_inputs_ok": False,
            "mismatches": ["stub_forbidden_in_production"],
            "error": "stub_forbidden_in_production",
            "replayed_count": 0,
        }

    if evaluator is not None:
        ev = evaluator
    elif is_prod_bundle or bundle_engine_hash == PINNED_FUSE_ENGINE_HASH or rec0_engine_hash == PINNED_FUSE_ENGINE_HASH:
        ev = VendoredFuseEvaluator()
    else:
        ev = StubEvaluator()

    if is_prod_bundle and (
        is_stub_evaluator(ev)
        or getattr(ev, "engine_hash", None) != PINNED_FUSE_ENGINE_HASH
        or getattr(ev, "spec_version", None) != PINNED_FUSE_SPEC_VERSION
    ):
        return {
            "bit_for_bit_match": False,
            "chain_ok": chain_ok,
            "first_bad_sequence": first_bad_seq,
            "chain_error": chain_err,
            "receipts_ok": receipts_ok,
            "bad_receipt_sequence": bad_rec_seq,
            "receipt_error": rec_err,
            "receipts_creditable": receipts_creditable,
            "receipt_credit_issues": receipt_credit_issues,
            "archived_inputs_ok": False,
            "mismatches": ["invalid_production_evaluator_identity"],
            "error": "invalid_production_evaluator_identity",
            "replayed_count": 0,
        }

    if bundle_engine_hash is not None and bundle_engine_hash != getattr(ev, "engine_hash", None):
        return {
            "bit_for_bit_match": False,
            "chain_ok": chain_ok,
            "first_bad_sequence": first_bad_seq,
            "chain_error": chain_err,
            "receipts_ok": receipts_ok,
            "bad_receipt_sequence": bad_rec_seq,
            "receipt_error": rec_err,
            "receipts_creditable": receipts_creditable,
            "receipt_credit_issues": receipt_credit_issues,
            "archived_inputs_ok": False,
            "mismatches": [
                f"bundle_engine_hash_mismatch:bundle={bundle_engine_hash},ev={getattr(ev, 'engine_hash', None)}"
            ],
            "error": "bundle_engine_hash_mismatch",
            "replayed_count": 0,
        }

    prior_state: dict = {}
    prev_hash = GENESIS_PREV_HASH
    mismatches: list[str] = []
    archive_errors: list[str] = []
    any_requires_market_records = False

    for seq, sess in enumerate(sessions):
        cal = resolve_session("XNYS", sess["session_date"], spec=spec)
        stored_body = (
            stored_records[seq]["record_body"]
            if seq < len(stored_records)
            and isinstance(stored_records[seq], dict)
            and isinstance(stored_records[seq].get("record_body"), dict)
            else {}
        )
        sess_labels = sess.get("discipline_labels") if isinstance(sess.get("discipline_labels"), dict) else {}
        stored_labels = (
            stored_body.get("discipline_labels")
            if isinstance(stored_body.get("discipline_labels"), dict)
            else {}
        )
        stored_assertions = (
            stored_body.get("assertions") if isinstance(stored_body.get("assertions"), dict) else {}
        )
        is_noncreditable_y2_session = (
            bundle.get("mode") == "reconstruction"
            and stored_body.get("mode") == "reconstruction"
            and stored_body.get("observation_class") == "historical_reconstruction"
            and sess_labels.get("vintage_qualification") == VINTAGE_Y2
            and stored_labels.get("vintage_qualification") == VINTAGE_Y2
            and sess_labels.get("forward_clock_credit_eligible") is False
            and stored_labels.get("forward_clock_credit_eligible") is False
            and stored_assertions.get("forward_clock_credit_eligible") is False
        )
        has_mkt_key = "market_records" in sess
        declared_digest = sess.get("market_records_digest")
        requires_market_records = declared_archive or has_mkt_key or (declared_digest is not None)
        computed_mkt_digest: str | None = None

        if requires_market_records:
            any_requires_market_records = True
            mkt_records = sess.get("market_records")
            if archive_dir is None or not archive_dir.is_dir():
                archive_errors.append(f"seq_{seq}:missing_archive_dir")
            if not isinstance(mkt_records, list) or len(mkt_records) == 0:
                archive_errors.append(f"seq_{seq}:missing_or_empty_market_records")
            else:
                cal_close_utc = cal.get("close_utc")
                rt_eff = runtime_cfg if isinstance(runtime_cfg, dict) else load_effective_runtime_config()
                is_half_day = cal.get("session_state") == "half_day"
                cutoff_et_raw = rt_eff.get(
                    "decision_cutoff_half_day_et" if is_half_day else "decision_cutoff_et"
                )
                if isinstance(cutoff_et_raw, str) and ":" in cutoff_et_raw:
                    hh, mm = map(int, cutoff_et_raw.split(":"))
                    expected_cutoff_utc = (
                        datetime.combine(date.fromisoformat(sess["session_date"]), dt_time(hh, mm), tzinfo=_NY_TZ)
                        .astimezone(timezone.utc)
                        .strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    )
                else:
                    expected_cutoff_utc = (
                        default_eod_decision_cutoff_utc(sess["session_date"], cal)
                        if cal.get("is_trading_day")
                        else None
                    )

                expected_required_series = (
                    Y2_REQUIRED_MARKET_SERIES if is_noncreditable_y2_session else REQUIRED_MARKET_SERIES
                )
                by_inst: dict[str, dict] = {}
                seen_instruments: set[str] = set()
                for idx, mrec in enumerate(mkt_records):
                    if not isinstance(mrec, dict):
                        archive_errors.append(f"seq_{seq}:invalid_market_record_type_{idx}")
                        continue
                    m_inst = mrec.get("instrument")
                    if not isinstance(m_inst, str) or not m_inst:
                        archive_errors.append(f"seq_{seq}:missing_instrument_in_market_record_{idx}")
                        continue
                    if m_inst in seen_instruments:
                        archive_errors.append(f"seq_{seq}:duplicate_market_record_instrument:{m_inst}")
                    else:
                        seen_instruments.add(m_inst)
                        by_inst[m_inst] = mrec

                    if archive_dir is not None and archive_dir.is_dir():
                        ok_raw, raw_err = verify_market_record_raw_integrity(
                            archive_dir,
                            mrec,
                            session_date=sess.get("session_date"),
                            calendar_session=cal,
                        )
                        if not ok_raw:
                            archive_errors.append(f"seq_{seq}:{raw_err}")
                    if not is_noncreditable_y2_session:
                        ok_close, close_err = validate_official_close_market_record(mrec, cal)
                        if not ok_close:
                            archive_errors.append(f"seq_{seq}:{close_err}:{m_inst}")

                    if m_inst in expected_required_series:
                        elig = mrec.get("eligibility")
                        if mrec.get("normalized_value") is None or not (
                            isinstance(elig, dict) and elig.get("eligible") is True
                        ):
                            archive_errors.append(
                                f"seq_{seq}:ineligible_or_null_required_series:{m_inst}"
                            )
                    if not (is_noncreditable_y2_session and mrec.get("vintage") == VINTAGE_Y2):
                        if not cal.get("is_trading_day") or cal.get("blocks_eligibility"):
                            archive_errors.append(
                                f"seq_{seq}:non_trading_session_market_record:{m_inst}"
                            )
                        elif not is_utc_ms_timestamp(cal_close_utc) or expected_cutoff_utc is None:
                            archive_errors.append(
                                f"seq_{seq}:missing_calendar_close_or_cutoff:{m_inst}"
                            )
                        else:
                            ret_utc = mrec.get("retrieval_time_utc")
                            dec_utc = sess.get("decision_timestamp_utc")
                            if not is_utc_ms_timestamp(ret_utc):
                                archive_errors.append(
                                    f"seq_{seq}:invalid_retrieval_time_utc:{m_inst}"
                                )
                            elif not is_utc_ms_timestamp(dec_utc):
                                archive_errors.append(
                                    f"seq_{seq}:invalid_decision_timestamp_utc:{m_inst}"
                                )
                            elif ret_utc < cal_close_utc:
                                archive_errors.append(
                                    f"seq_{seq}:retrieval_before_session_close:{m_inst}:{ret_utc}<{cal_close_utc}"
                                )
                            elif ret_utc > expected_cutoff_utc:
                                archive_errors.append(
                                    f"seq_{seq}:retrieval_after_decision_cutoff:{m_inst}:{ret_utc}>{expected_cutoff_utc}"
                                )
                            elif ret_utc > dec_utc:
                                archive_errors.append(
                                    f"seq_{seq}:retrieval_after_decision_timestamp:{m_inst}:{ret_utc}>{dec_utc}"
                                )

                computed_mkt_digest = sha256_canonical(mkt_records)
                if declared_digest is not None and declared_digest != computed_mkt_digest:
                    archive_errors.append(
                        f"seq_{seq}:market_records_digest_mismatch("
                        f"computed={computed_mkt_digest}, declared={declared_digest})"
                    )

                missing_required = [inst for inst in expected_required_series if inst not in by_inst]
                if missing_required:
                    archive_errors.append(
                        f"seq_{seq}:missing_required_market_series:{missing_required}"
                    )
                funds_in = sess.get("inputs", {}).get("funds", {})
                if isinstance(funds_in, dict):
                    for inst, fcfg in funds_in.items():
                        mrec = by_inst.get(inst)
                        if mrec is None or mrec.get("normalized_value") is None:
                            archive_errors.append(f"seq_{seq}:missing_market_record_for_{inst}")
                        else:
                            try:
                                if canonical_decimal_str(fcfg["close_price"]) != canonical_decimal_str(
                                    mrec["normalized_value"]
                                ):
                                    archive_errors.append(
                                        f"seq_{seq}:sizing_input_close_price_mismatch_{inst}"
                                    )
                            except Exception as exc:
                                archive_errors.append(
                                    f"seq_{seq}:invalid_close_price_{inst}:{exc}"
                                )

        replay_inputs = dict(sess["inputs"])
        eff_prior_state = prior_state
        if not is_stub_evaluator(ev):
            if "market_records" in sess and "market_records" not in replay_inputs:
                replay_inputs["market_records"] = sess["market_records"]
                replay_inputs.setdefault("required_series", expected_required_series)
            fctx = sess.get("fuse_context")
            if isinstance(fctx, Mapping):
                for ctx_key in (
                    "prior_closes",
                    "positions_source_record",
                    "broker_reconciliation_source_record",
                    "index_one_session_returns",
                ):
                    if ctx_key in fctx and fctx[ctx_key] is not None and ctx_key not in replay_inputs:
                        replay_inputs[ctx_key] = fctx[ctx_key]
                if fctx.get("prior_row_state_used") is not None:
                    eff_prior_state = fctx["prior_row_state_used"]

        res = build_record_body(
            chain_id=bundle["chain_id"],
            sequence=seq,
            prev_record_sha256=prev_hash,
            mode=bundle["mode"],
            session_date=sess["session_date"],
            decision_timestamp_utc=sess["decision_timestamp_utc"],
            session_inputs=replay_inputs,
            calendar_session=cal,
            code_version_sha=bundle["code_version_sha"],
            evaluator=ev,
            prior_row_state=eff_prior_state,
            discipline_labels=sess.get("discipline_labels"),
            market_records_digest=computed_mkt_digest,
            runtime_config=bundle.get("runtime_config"),
            calendar_spec=spec,
        )
        if not res["appended"]:
            mismatches.append(f"seq_{seq}:unexpected_fail_closed")
            break

        replayed_body = res["record_body"]
        replayed_hash = sha256_canonical(replayed_body)
        replayed_envelope = {
            "record_body": replayed_body,
            "record_sha256": replayed_hash,
        }
        replayed_file_bytes = canonical_json_bytes(replayed_envelope)

        _, rec_path = record_files[seq]
        stored_file_bytes = rec_path.read_bytes()
        stored_env = stored_records[seq]
        stored_hash = stored_env["record_sha256"]

        if replayed_file_bytes != stored_file_bytes or replayed_hash != stored_hash:
            mismatches.append(
                f"seq_{seq}:bit_for_bit_mismatch(replayed={replayed_hash}, stored={stored_hash})"
            )

        prior_state = res["next_row_state"]
        prev_hash = replayed_hash

    archived_inputs_ok = (len(archive_errors) == 0) if any_requires_market_records else False
    return {
        "bit_for_bit_match": len(mismatches) == 0 and len(archive_errors) == 0,
        "chain_ok": chain_ok,
        "first_bad_sequence": first_bad_seq,
        "chain_error": chain_err,
        "receipts_ok": receipts_ok,
        "bad_receipt_sequence": bad_rec_seq,
        "receipt_error": rec_err,
        "receipts_creditable": receipts_creditable,
        "receipt_credit_issues": receipt_credit_issues,
        "archived_inputs_ok": archived_inputs_ok,
        "archive_errors": archive_errors,
        "policy_config_hash": actual_policy_hash,
        "running_code_matches_bundle": bundle.get("code_version_sha") == compute_code_manifest_hash(),
        "mismatches": mismatches,
        "replayed_count": len(sessions),
    }


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint: python3 -m shadow.replay --bundle <dir> --ledger <dir>."""
    parser = argparse.ArgumentParser(description="Replay and verify SHADOW-N ledger window")
    parser.add_argument("--bundle", required=True, help="Directory holding replay_bundle.json")
    parser.add_argument("--ledger", required=True, help="Ledger directory")
    parser.add_argument(
        "--trusted-headers",
        help=(
            "Optional JSON file mapping integer Bitcoin block height -> 80-byte raw block header hex "
            "or {'merkle_root': <64-hex>, 'timestamp': <UTC ISO or epoch seconds>} / {'raw_header': <160-hex>}"
        ),
    )
    parser.add_argument(
        "--require-creditable-receipts",
        action="store_true",
        help="Require non-stub Bitcoin-verified timely OpenTimestamps receipts for exit code 0",
    )
    args = parser.parse_args(argv)

    trusted_roots = (
        load_trusted_bitcoin_headers(args.trusted_headers) if args.trusted_headers else None
    )
    result = replay_window(args.bundle, args.ledger, bitcoin_merkle_roots=trusted_roots)
    print(json.dumps(result, indent=2, sort_keys=True))
    ok = result["bit_for_bit_match"] and result["chain_ok"] and result["receipts_ok"]
    if args.require_creditable_receipts:
        ok = ok and bool(result["receipts_creditable"])
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
