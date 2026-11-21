"""Deterministic replay CLI: re-derives records and verifies chain + receipts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from shadow.calendar_consumer import load_spec, resolve_session
from shadow.evaluator import StubEvaluator
from shadow.ledger import (
    GENESIS_PREV_HASH,
    ShadowLedger,
    build_record_body,
    canonical_json_bytes,
    sha256_canonical,
)


def replay_window(bundle_dir: str | Path, ledger_dir: str | Path) -> dict:
    """Re-derive every record from retained inputs and compare bit-for-bit."""
    b_dir = Path(bundle_dir)
    bundle = json.loads((b_dir / "replay_bundle.json").read_text(encoding="utf-8"))

    spec_dir = bundle.get("calendar_spec_dir")
    spec = load_spec(spec_dir)
    if spec["spec_digest"] != bundle["calendar_spec_digest"]:
        return {
            "bit_for_bit_match": False,
            "chain_ok": False,
            "receipts_ok": False,
            "error": "calendar_spec_digest_mismatch",
            "replayed_count": 0,
        }

    ledger = ShadowLedger(ledger_dir)
    chain_ok, first_bad_seq, chain_err = ledger.verify_chain()
    receipts_ok, bad_rec_seq, rec_err = ledger.verify_all_receipts()

    stored_records = ledger.load_records()
    sessions = bundle["sessions"]
    if len(stored_records) != len(sessions):
        return {
            "bit_for_bit_match": False,
            "chain_ok": chain_ok,
            "receipts_ok": receipts_ok,
            "error": f"record_count_mismatch: stored={len(stored_records)}, bundle={len(sessions)}",
            "replayed_count": len(stored_records),
        }

    ev = StubEvaluator()
    prior_state: dict = {}
    prev_hash = GENESIS_PREV_HASH
    mismatches: list[str] = []

    for seq, sess in enumerate(sessions):
        cal = resolve_session("XNYS", sess["session_date"], spec=spec)
        res = build_record_body(
            chain_id=bundle["chain_id"],
            sequence=seq,
            prev_record_sha256=prev_hash,
            mode=bundle["mode"],
            session_date=sess["session_date"],
            decision_timestamp_utc=sess["decision_timestamp_utc"],
            session_inputs=sess["inputs"],
            calendar_session=cal,
            code_version_sha=bundle["code_version_sha"],
            evaluator=ev,
            prior_row_state=prior_state,
            discipline_labels=sess.get("discipline_labels"),
            market_records_digest=sess.get("market_records_digest"),
        )
        if not res["appended"]:
            mismatches.append(f"seq_{seq}:unexpected_fail_closed")
            break

        replayed_body = res["record_body"]
        replayed_bytes = canonical_json_bytes(replayed_body)
        replayed_hash = sha256_canonical(replayed_body)

        stored_env = stored_records[seq]
        stored_bytes = canonical_json_bytes(stored_env["record_body"])
        stored_hash = stored_env["record_sha256"]

        if replayed_bytes != stored_bytes or replayed_hash != stored_hash:
            mismatches.append(
                f"seq_{seq}:hash_mismatch(replayed={replayed_hash}, stored={stored_hash})"
            )

        prior_state = res["next_row_state"]
        prev_hash = replayed_hash

    return {
        "bit_for_bit_match": len(mismatches) == 0,
        "chain_ok": chain_ok,
        "first_bad_sequence": first_bad_seq,
        "chain_error": chain_err,
        "receipts_ok": receipts_ok,
        "bad_receipt_sequence": bad_rec_seq,
        "receipt_error": rec_err,
        "mismatches": mismatches,
        "replayed_count": len(sessions),
    }


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint: python3 -m shadow.replay --bundle <dir> --ledger <dir>."""
    parser = argparse.ArgumentParser(description="Replay and verify SHADOW-N ledger window")
    parser.add_argument("--bundle", required=True, help="Directory holding replay_bundle.json")
    parser.add_argument("--ledger", required=True, help="Ledger directory")
    args = parser.parse_args(argv)

    result = replay_window(args.bundle, args.ledger)
    print(json.dumps(result, indent=2, sort_keys=True))
    ok = result["bit_for_bit_match"] and result["chain_ok"] and result["receipts_ok"]
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
