"""C-SH-LEDGER canonicalization (restricted RFC 8785), hash chain, and detached receipts."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import threading
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from datetime import date, datetime, time as dt_time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from shadow.calendar_consumer import DEFAULT_SPEC_DIR, load_spec, resolve_session
from shadow.evaluator import ALL_BREAKER_IDS, StubEvaluator
from shadow.manifest import compute_code_manifest_hash, load_effective_runtime_config
from shadow.ots import verify_ots_bytes
from shadow.sizing import (
    INSTRUMENT_ORDER,
    TIER_ORDER,
    canonical_decimal_str,
    compute_session_sizing,
    policy_config_hash,
)

GENESIS_PREV_HASH = "0" * 64
SCHEMA_VERSION = "c-sh-ledger-v1"
_NY_TZ = ZoneInfo("America/New_York")

_DECIMAL_RE = re.compile(r"^(0|-?(0\.[0-9]*[1-9]|[1-9][0-9]*(\.[0-9]*[1-9])?))$")
_DATE_YYYY_MM_DD_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$", re.ASCII)
_STRICT_HHMM_RE = re.compile(r"^[0-9]{2}:[0-9]{2}$", re.ASCII)
_UTC_MS_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$", re.ASCII)
_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_SPECS_ROOT = Path(__file__).resolve().parents[2] / "specs"

_CONTROL_ESCAPES = {
    0x08: r"\b",
    0x09: r"\t",
    0x0A: r"\n",
    0x0C: r"\f",
    0x0D: r"\r",
}

_REQUIRED_BODY_KEYS = frozenset(
    {
        "assertions",
        "bankroll",
        "calendar_spec_digest",
        "calendar_version",
        "chain_id",
        "code_version_sha",
        "decision_timestamp_utc",
        "engine_hash",
        "evaluator",
        "inputs_digest",
        "ledger_mode",
        "mode",
        "observation_class",
        "policy_config_hash",
        "policy_id",
        "prev_record_sha256",
        "schema_version",
        "sequence",
        "session_date",
        "tiers",
    }
)


class CanonicalizationError(ValueError):
    """Raised when a payload violates the restricted RFC 8785 / decimal profile."""


class LedgerAppendError(ValueError):
    """Raised when an append violates chain rules (duplicate, fork, gap, invalid body)."""


def is_canonical_decimal_str(value: object) -> bool:
    """Return True iff value is a canonical ordinary base-ten decimal string."""
    return isinstance(value, str) and bool(_DECIMAL_RE.fullmatch(value))


def is_utc_ms_timestamp(value: object) -> bool:
    """Return True iff value is a valid UTC ISO-8601 timestamp with millisecond precision."""
    if not isinstance(value, str) or not _UTC_MS_RE.fullmatch(value):
        return False
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return False
    return True


def parse_json_no_duplicates(raw_text: str) -> object:
    """Parse JSON rejecting duplicate object keys and non-finite constants."""

    def _no_dups_hook(pairs: list[tuple[str, object]]) -> dict[str, object]:
        obj: dict[str, object] = {}
        for k, v in pairs:
            if k in obj:
                raise CanonicalizationError(f"Duplicate JSON key disallowed: {k!r}")
            obj[k] = v
        return obj

    def _reject_constant(token: str) -> object:
        raise CanonicalizationError(f"Non-finite JSON constant disallowed: {token!r}")

    return json.loads(
        raw_text,
        object_pairs_hook=_no_dups_hook,
        parse_constant=_reject_constant,
    )


def _encode_rfc8785_string(text: str) -> str:
    out: list[str] = ['"']
    for ch in text:
        code = ord(ch)
        if 0xD800 <= code <= 0xDFFF:
            raise CanonicalizationError("Lone UTF-16 surrogate disallowed in RFC 8785 string")
        if ch == '"':
            out.append(r'\"')
        elif ch == "\\":
            out.append(r"\\")
        elif code in _CONTROL_ESCAPES:
            out.append(_CONTROL_ESCAPES[code])
        elif code < 0x20:
            out.append(f"\\u{code:04x}")
        else:
            out.append(ch)
    out.append('"')
    return "".join(out)


def _encode_rfc8785(obj: object) -> str:
    if obj is None:
        return "null"
    if isinstance(obj, bool):
        return "true" if obj else "false"
    if isinstance(obj, int):
        return str(obj)
    if isinstance(obj, float):
        raise CanonicalizationError(f"Floats are forbidden in ledger bodies: {obj!r}")
    if isinstance(obj, str):
        return _encode_rfc8785_string(obj)
    if isinstance(obj, (list, tuple)):
        return "[" + ",".join(_encode_rfc8785(item) for item in obj) + "]"
    if isinstance(obj, dict):
        items: list[tuple[bytes, str, object]] = []
        for k, v in obj.items():
            if not isinstance(k, str):
                raise CanonicalizationError(f"Object keys must be strings, got {type(k).__name__}")
            items.append((k.encode("utf-16-be"), k, v))
        items.sort(key=lambda item: item[0])
        parts = [f"{_encode_rfc8785_string(k)}:{_encode_rfc8785(v)}" for _, k, v in items]
        return "{" + ",".join(parts) + "}"
    raise CanonicalizationError(f"Unsupported type in canonical JSON: {type(obj).__name__}")


def canonical_json_bytes(obj: object) -> bytes:
    """Serialize obj to UTF-8 bytes under the restricted RFC 8785 profile."""
    return _encode_rfc8785(obj).encode("utf-8")


def sha256_canonical(obj: object) -> str:
    """Return lowercase hex SHA-256 over canonical RFC 8785 UTF-8 bytes."""
    return hashlib.sha256(canonical_json_bytes(obj)).hexdigest()


def validate_record_body(body: dict) -> None:
    """Validate required fields, types, decimals, timestamps, ordering, and invariants."""
    if not isinstance(body, dict):
        raise CanonicalizationError("record_body must be a JSON object")

    missing = _REQUIRED_BODY_KEYS - set(body.keys())
    if missing:
        raise CanonicalizationError(f"Missing required record_body fields: {sorted(missing)}")
    if "record_sha256" in body or "receipt" in body:
        raise CanonicalizationError("record_sha256 and external receipt are excluded from preimage")

    if body["mode"] not in ("drill", "reconstruction", "production"):
        raise CanonicalizationError(f"Unsupported mode: {body['mode']!r}")
    if body["mode"] == "production" and str(body.get("engine_hash", "")).startswith("stub-"):
        raise CanonicalizationError("Stub engine_hash is structurally forbidden in production mode")
    ev_obj = body.get("evaluator")
    if (
        not isinstance(ev_obj, dict)
        or not isinstance(ev_obj.get("engine_hash"), str)
        or not ev_obj.get("engine_hash")
        or not isinstance(ev_obj.get("spec_version"), str)
        or not ev_obj.get("spec_version")
    ):
        raise CanonicalizationError("evaluator must specify non-empty spec_version and engine_hash")
    if ev_obj["engine_hash"] != body.get("engine_hash"):
        raise CanonicalizationError("evaluator.engine_hash must match record engine_hash")
    if body["mode"] == "production" and ev_obj["engine_hash"].startswith("stub-"):
        raise CanonicalizationError("Stub evaluator.engine_hash is structurally forbidden in production mode")
    if not isinstance(body["sequence"], int) or isinstance(body["sequence"], bool) or body["sequence"] < 0:
        raise CanonicalizationError(f"Invalid sequence: {body['sequence']!r}")
    for hex_field in (
        "prev_record_sha256",
        "policy_config_hash",
        "code_version_sha",
        "calendar_spec_digest",
        "inputs_digest",
    ):
        val = body.get(hex_field)
        if not isinstance(val, str) or not _HEX64_RE.fullmatch(val):
            raise CanonicalizationError(f"{hex_field} must be 64 lowercase hex chars")
    if not is_utc_ms_timestamp(body["decision_timestamp_utc"]):
        raise CanonicalizationError(
            f"decision_timestamp_utc must be UTC ms ISO-8601: {body['decision_timestamp_utc']!r}"
        )
    sess_date_body = body.get("session_date")
    if not isinstance(sess_date_body, str) or not _DATE_YYYY_MM_DD_RE.fullmatch(sess_date_body):
        raise CanonicalizationError(f"session_date must be canonical YYYY-MM-DD: {sess_date_body!r}")
    try:
        date.fromisoformat(sess_date_body)
    except ValueError as exc:
        raise CanonicalizationError(f"Invalid session_date: {sess_date_body!r}") from exc

    bankroll = body["bankroll"]
    if not isinstance(bankroll, dict):
        raise CanonicalizationError("bankroll must be a dict")
    for bk in ("E", "E_star", "H", "W", "W_star", "W_ref", "L"):
        if not is_canonical_decimal_str(bankroll.get(bk)):
            raise CanonicalizationError(f"bankroll.{bk} invalid canonical decimal: {bankroll.get(bk)!r}")

    tiers = body["tiers"]
    if not isinstance(tiers, list) or [t.get("tier_id") for t in tiers] != list(TIER_ORDER):
        raise CanonicalizationError(f"tiers must appear once in stable ID order {TIER_ORDER}")
    for tier in tiers:
        for tk in ("D", "k", "x_cap", "x_joint", "w_max", "sigma_target", "c", "x_ML", "x_eff_joint", "alpha_joint"):
            if not is_canonical_decimal_str(tier.get(tk)):
                raise CanonicalizationError(f"tier {tier.get('tier_id')}.{tk} invalid decimal: {tier.get(tk)!r}")
        tier_orders = tier.get("orders_placed")
        if isinstance(tier_orders, bool) or not isinstance(tier_orders, int) or tier_orders != 0:
            raise CanonicalizationError("tier orders_placed must be integer 0 in Stage N")
        funds = tier.get("funds")
        if not isinstance(funds, list) or [f.get("instrument") for f in funds] != list(INSTRUMENT_ORDER):
            raise CanonicalizationError(f"funds must appear once in stable ID order {INSTRUMENT_ORDER}")
        for fund in funds:
            for fk in (
                "s",
                "x_cap",
                "x_ML",
                "x_K_struct",
                "x_K_cond",
                "kelly_counterfactual_x",
                "v",
                "x_pre",
                "x_post_joint",
                "w_target",
            ):
                if not is_canonical_decimal_str(fund.get(fk)):
                    raise CanonicalizationError(
                        f"{tier.get('tier_id')}.{fund.get('instrument')}.{fk} invalid decimal: {fund.get(fk)!r}"
                    )
            if fund.get("kelly_applied") is not False:
                raise CanonicalizationError("kelly_applied must be false")
            fund_orders = fund.get("orders_placed")
            if isinstance(fund_orders, bool) or not isinstance(fund_orders, int) or fund_orders != 0:
                raise CanonicalizationError("fund orders_placed must be integer 0 in Stage N")

    ev = body["evaluator"]
    if not isinstance(ev, dict):
        raise CanonicalizationError("evaluator must be a dict")
    breakers = ev.get("breakers")
    if not isinstance(breakers, list) or [b.get("id") for b in breakers] != list(ALL_BREAKER_IDS):
        raise CanonicalizationError("evaluator.breakers must contain all 24 named IDs in sorted order")

    assertions = body["assertions"]
    if not isinstance(assertions, dict):
        raise CanonicalizationError("assertions must be a dict")
    orders_assert = assertions.get("orders_placed")
    if (
        assertions.get("kelly_applied") is not False
        or isinstance(orders_assert, bool)
        or orders_assert != 0
        or assertions.get("B0") != "50000"
        or assertions.get("H") != "0"
        or bankroll.get("H") != "0"
        or assertions.get("stop_loss_return_rule") != "OFF"
        or assertions.get("loss_controls_armed") is not False
    ):
        raise CanonicalizationError("Standing Stage N assertions violated (requires kelly_applied=false, orders_placed=0, B0=50000, H=0)")

    if "raw_vs_adjusted" in body:
        rva = body["raw_vs_adjusted"]
        if not isinstance(rva, dict):
            raise CanonicalizationError("raw_vs_adjusted must be a dict")
        for rvk in (
            "external_flow_usd",
            "raw_asset_change_usd",
            "raw_asset_change_pct",
            "adjusted_pnl_usd",
            "adjusted_return_pct",
        ):
            if rva.get(rvk) is not None and not is_canonical_decimal_str(rva.get(rvk)):
                raise CanonicalizationError(f"raw_vs_adjusted.{rvk} invalid canonical decimal")

    if "monthly_diagnostic" in body:
        md = body["monthly_diagnostic"]
        if not isinstance(md, dict) or md.get("unread_by_sizing") is not True:
            raise CanonicalizationError("monthly_diagnostic must be a dict with unread_by_sizing=true")
        for mdk in ("win_rate", "payoff_ratio"):
            if md.get(mdk) is not None and not is_canonical_decimal_str(md.get(mdk)):
                raise CanonicalizationError(f"monthly_diagnostic.{mdk} invalid canonical decimal")
        ci = md.get("win_rate_bayes_ci")
        if ci is not None:
            if not isinstance(ci, dict):
                raise CanonicalizationError("win_rate_bayes_ci must be a dict or null")
            for cik in ("level", "mean", "lower", "upper"):
                if not is_canonical_decimal_str(ci.get(cik)):
                    raise CanonicalizationError(f"win_rate_bayes_ci.{cik} invalid canonical decimal")

    # Verify zero floats anywhere via canonicalization
    canonical_json_bytes(body)


def create_synthetic_receipt(
    record_envelope: dict,
    obtained_at_utc: str,
    *,
    commitment_deadline_utc: str | None = None,
) -> dict:
    """Create a detached synthetic-stub timestamp receipt for an appended record."""
    if not is_utc_ms_timestamp(obtained_at_utc):
        raise CanonicalizationError(f"Invalid obtained_at_utc: {obtained_at_utc!r}")
    if commitment_deadline_utc is not None and not is_utc_ms_timestamp(commitment_deadline_utc):
        raise CanonicalizationError(f"Invalid commitment_deadline_utc: {commitment_deadline_utc!r}")
    body = record_envelope["record_body"]
    within_deadline = (
        True if commitment_deadline_utc is None else (obtained_at_utc <= commitment_deadline_utc)
    )
    out = {
        "anchor": "synthetic-stub",
        "chain_id": body["chain_id"],
        "obtained_at_utc": obtained_at_utc,
        "record_sha256": record_envelope["record_sha256"],
        "sequence": body["sequence"],
        "session_date": body["session_date"],
    }
    if commitment_deadline_utc is not None:
        out["commitment_deadline_utc"] = commitment_deadline_utc
        out["within_deadline"] = within_deadline
    return out


def create_ots_receipt(
    record_envelope: dict,
    ots_bytes: bytes,
    obtained_at_utc: str,
    *,
    commitment_deadline_utc: str | None = None,
    calendar_urls: list[str] | tuple[str, ...] | None = None,
    bitcoin_merkle_roots: Mapping[int, object] | None = None,
) -> dict:
    """Create and verify detached OpenTimestamps receipt metadata binding .ots bytes."""
    if not is_utc_ms_timestamp(obtained_at_utc):
        raise CanonicalizationError(f"Invalid obtained_at_utc: {obtained_at_utc!r}")
    if commitment_deadline_utc is not None and not is_utc_ms_timestamp(commitment_deadline_utc):
        raise CanonicalizationError(f"Invalid commitment_deadline_utc: {commitment_deadline_utc!r}")

    ok, info = verify_ots_bytes(
        ots_bytes,
        record_envelope["record_sha256"],
        bitcoin_merkle_roots=bitcoin_merkle_roots,
    )
    if not ok or not isinstance(info, dict):
        raise LedgerAppendError(f"invalid_ots_proof:{info}")

    body = record_envelope["record_body"]
    seq = body["sequence"]
    within_deadline = (
        False if commitment_deadline_utc is None else (obtained_at_utc <= commitment_deadline_utc)
    )
    return {
        "anchor": "opentimestamps",
        "chain_id": body["chain_id"],
        "sequence": seq,
        "session_date": body["session_date"],
        "record_sha256": record_envelope["record_sha256"],
        "ots_file": f"seq_{seq:06d}.ots",
        "ots_sha256": hashlib.sha256(ots_bytes).hexdigest(),
        "obtained_at_utc": obtained_at_utc,
        "commitment_deadline_utc": commitment_deadline_utc,
        "within_deadline": within_deadline,
        "upgrade_state": info["upgrade_state"],
        "bitcoin_verified": info["bitcoin_verified"],
        "pending_calendars": info["pending_calendars"],
        "bitcoin_heights": info["bitcoin_heights"],
        "calendar_urls": list(calendar_urls) if calendar_urls is not None else info["pending_calendars"],
    }


def compute_session_commitment_deadline_utc(
    session_date: str,
    commitment_deadline_et: object | None = None,
    *,
    repo_root: str | Path | None = None,
    config_path: str | Path | None = None,
) -> str:
    """Compute pinned UTC-ms commitment deadline from XNYS session_date and ET wall time."""
    if not isinstance(session_date, str):
        raise ValueError("session_date must be a string")
    if commitment_deadline_et is None:
        try:
            eff_cfg = load_effective_runtime_config(repo_root, config_path=config_path)
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            raise ValueError(f"missing_or_invalid_commitment_deadline_et:{exc}") from exc
        if not isinstance(eff_cfg, dict) or "commitment_deadline_et" not in eff_cfg:
            raise ValueError("missing_or_invalid_commitment_deadline_et:missing_key")
        eff_et = eff_cfg["commitment_deadline_et"]
    else:
        eff_et = commitment_deadline_et
    if isinstance(eff_et, bool) or not isinstance(eff_et, str) or not eff_et.isascii() or not _STRICT_HHMM_RE.fullmatch(eff_et):
        raise ValueError(f"missing_or_invalid_commitment_deadline_et:{eff_et!r}")
    hour, minute = int(eff_et[:2]), int(eff_et[3:])
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"missing_or_invalid_commitment_deadline_et:{eff_et!r}")
    d = date.fromisoformat(session_date)
    local_dt = datetime.combine(d, dt_time(hour, minute), tzinfo=_NY_TZ)
    return local_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def is_receipt_timely(
    receipt: dict,
    *,
    expected_commitment_deadline_utc: str | None = None,
    decision_timestamp_utc: str | None = None,
    require_expected_deadline: bool = False,
) -> tuple[bool, str | None]:
    """Return whether receipt met its pinned session commitment deadline."""
    if not isinstance(receipt, dict):
        return False, "receipt_not_dict"
    deadline = receipt.get("commitment_deadline_utc")
    obtained = receipt.get("obtained_at_utc")
    if not is_utc_ms_timestamp(deadline):
        return False, "missing_or_invalid_pinned_commitment_deadline"
    if not is_utc_ms_timestamp(obtained):
        return False, "missing_or_invalid_obtained_at_utc"
    if require_expected_deadline and expected_commitment_deadline_utc is None:
        return False, "missing_expected_commitment_deadline_utc"
    if expected_commitment_deadline_utc is not None:
        if not is_utc_ms_timestamp(expected_commitment_deadline_utc):
            return False, "invalid_expected_commitment_deadline_utc"
        if deadline != expected_commitment_deadline_utc:
            return False, "commitment_deadline_mismatch_with_pinned_session_deadline"
        if obtained > expected_commitment_deadline_utc:  # type: ignore[operator]
            return False, "late_after_commitment_deadline"
    if decision_timestamp_utc is not None:
        if not is_utc_ms_timestamp(decision_timestamp_utc):
            return False, "invalid_decision_timestamp_utc"
        if obtained < decision_timestamp_utc:  # type: ignore[operator]
            return False, "receipt_obtained_before_decision_timestamp"
    if obtained > deadline:  # type: ignore[operator]
        return False, "late_after_commitment_deadline"
    if receipt.get("within_deadline") is not True:
        return False, "late_or_unverified_deadline"
    return True, None


def verify_receipt(
    record_envelope: dict,
    receipt: dict,
    *,
    ots_bytes: bytes | None = None,
    bitcoin_merkle_roots: Mapping[int, object] | None = None,
    require_bitcoin_verification: bool = False,
    require_bitcoin_timestamp: bool = False,
    expected_commitment_deadline_utc: str | None = None,
) -> tuple[bool, str | None]:
    """Verify detached timestamp receipt separately AND bind it to recomputed canonical record_body hash."""
    if not isinstance(record_envelope, dict):
        return False, "record_envelope_not_dict"
    if not isinstance(receipt, dict):
        return False, "receipt_not_dict"
    body = record_envelope.get("record_body")
    if not isinstance(body, dict):
        return False, "record_envelope_invalid_body"
    try:
        actual_sha = sha256_canonical(body)
    except CanonicalizationError as exc:
        return False, f"record_body_non_canonical:{exc}"
    if record_envelope.get("record_sha256") != actual_sha:
        return False, "record_envelope_sha256_mismatch"

    anchor = receipt.get("anchor")
    if anchor not in ("synthetic-stub", "opentimestamps"):
        return False, f"unexpected_anchor:{anchor!r}"
    if receipt.get("record_sha256") != actual_sha:
        return False, "receipt_record_hash_mismatch"
    if receipt.get("chain_id") != body.get("chain_id"):
        return False, "receipt_chain_id_mismatch"
    if receipt.get("sequence") != body.get("sequence"):
        return False, "receipt_sequence_mismatch"
    if receipt.get("session_date") != body.get("session_date"):
        return False, "receipt_session_date_mismatch"
    if not is_utc_ms_timestamp(receipt.get("obtained_at_utc")):
        return False, "receipt_invalid_obtained_at_utc"

    if anchor == "opentimestamps":
        seq = body.get("sequence")
        expected_ots_file = f"seq_{seq:06d}.ots" if isinstance(seq, int) and not isinstance(seq, bool) else None
        if receipt.get("ots_file") != expected_ots_file:
            return False, "receipt_ots_filename_mismatch"
        expected_ots_sha = receipt.get("ots_sha256")
        if not isinstance(expected_ots_sha, str) or not _HEX64_RE.fullmatch(expected_ots_sha):
            return False, "receipt_invalid_ots_sha256"
        if ots_bytes is None:
            return False, "missing_ots_bytes"
        if hashlib.sha256(ots_bytes).hexdigest() != expected_ots_sha:
            return False, "ots_bytes_sha256_mismatch"
        ok_ots, info_or_err = verify_ots_bytes(
            ots_bytes,
            actual_sha,
            bitcoin_merkle_roots=bitcoin_merkle_roots,
            require_bitcoin_verification=require_bitcoin_verification,
            require_bitcoin_timestamp=require_bitcoin_timestamp,
            max_block_time_utc=expected_commitment_deadline_utc,
        )
        if not ok_ots:
            return False, str(info_or_err)

    return True, None


def compute_inputs_digest(
    session_inputs: Mapping[str, object],
    *,
    w_ref: object,
    active_control_state: object,
    market_records_digest: str | None,
    session_date: str,
) -> str:
    """Compute canonical SHA-256 inputs_digest matching build_record_body preimage."""
    if not isinstance(session_inputs, Mapping):
        raise CanonicalizationError("session_inputs must be a mapping")
    funds = session_inputs.get("funds")
    if not isinstance(funds, Mapping):
        raise CanonicalizationError("session_inputs.funds must be a mapping")
    fund_snap: dict[str, object] = {}
    for inst in INSTRUMENT_ORDER:
        fcfg = funds.get(inst)
        if not isinstance(fcfg, Mapping):
            raise CanonicalizationError(f"session_inputs.funds.{inst} must be a mapping")
        try:
            close_canon = canonical_decimal_str(fcfg["close_price"])  # type: ignore[arg-type]
            s_canon = canonical_decimal_str(fcfg["s"])  # type: ignore[arg-type]
            sigma_canon = canonical_decimal_str(fcfg["sigma_forecast"])  # type: ignore[arg-type]
        except Exception as exc:
            raise CanonicalizationError(f"invalid fund input decimal for {inst}: {exc}") from exc
        if "q_pre" not in fcfg:
            raise CanonicalizationError(f"missing q_pre for {inst}")
        fund_snap[inst] = {
            "close_price": close_canon,
            "q_pre": fcfg["q_pre"],
            "s": s_canon,
            "sigma_forecast": sigma_canon,
        }
    try:
        canonical_inputs_snapshot: dict[str, object] = {
            "E": canonical_decimal_str(session_inputs["E"]),  # type: ignore[arg-type]
            "E_star": canonical_decimal_str(session_inputs["E_star"]),  # type: ignore[arg-type]
            "H": canonical_decimal_str(session_inputs.get("H", "0")),  # type: ignore[arg-type]
            "L": canonical_decimal_str(session_inputs["L"]),  # type: ignore[arg-type]
            "W_ref": w_ref,
            "active_control_state": active_control_state,
            "funds": fund_snap,
            "market_records_digest": market_records_digest,
            "session_date": session_date,
        }
        if session_inputs.get("prior_E") is not None:
            canonical_inputs_snapshot["prior_E"] = canonical_decimal_str(session_inputs["prior_E"])  # type: ignore[arg-type]
        if session_inputs.get("external_flow") is not None:
            canonical_inputs_snapshot["external_flow"] = canonical_decimal_str(session_inputs["external_flow"])  # type: ignore[arg-type]
        if session_inputs.get("monthly_returns") is not None:
            canonical_inputs_snapshot["monthly_returns"] = [
                canonical_decimal_str(x) for x in session_inputs["monthly_returns"]  # type: ignore[attr-defined]
            ]
    except Exception as exc:
        if isinstance(exc, CanonicalizationError):
            raise
        raise CanonicalizationError(f"invalid session_inputs for inputs_digest: {exc}") from exc
    return sha256_canonical(canonical_inputs_snapshot)


def build_record_body(
    *,
    chain_id: str,
    sequence: int,
    prev_record_sha256: str,
    mode: str,
    session_date: str,
    decision_timestamp_utc: str,
    session_inputs: dict,
    calendar_session: dict,
    code_version_sha: str,
    evaluator: StubEvaluator | None = None,
    prior_row_state: dict | None = None,
    discipline_labels: dict | None = None,
    market_records_digest: str | None = None,
    runtime_config: dict[str, object] | None = None,
    calendar_spec: dict[str, object] | None = None,
) -> dict:
    """Run evaluator + sizing and assemble a canonical record_body (or O1/O2 fail-closed dict)."""
    ev = evaluator if evaluator is not None else StubEvaluator()
    p_hash = policy_config_hash(
        runtime_config=runtime_config,
        calendar_spec=calendar_spec or calendar_session,
        code_manifest_hash=code_version_sha,
    )
    prior = prior_row_state if prior_row_state is not None else {}

    eval_inputs = dict(session_inputs)
    eval_inputs["calendar"] = calendar_session
    rows_by_id, eff_scope, next_state = ev.evaluate(
        spec_version=ev.spec_version,
        engine_hash=ev.engine_hash,
        policy_hash=p_hash,
        session=session_date,
        as_of=decision_timestamp_utc,
        mode=mode,
        validated_inputs=eval_inputs,
        prior_row_state=prior,
        authorized_events=None,
    )

    if rows_by_id["O1"]["would_have_tripped"] or rows_by_id["O2"]["would_have_tripped"]:
        return {
            "appended": False,
            "fail_closed": True,
            "o1_output": rows_by_id["O1"],
            "o2_output": rows_by_id["O2"],
            "effective_state_by_scope": eff_scope,
            "next_row_state": next_state,
        }

    if canonical_decimal_str(session_inputs.get("H", "0")) != "0":
        raise CanonicalizationError("Stage N ledger boundary requires H == '0'")

    sizing_res = compute_session_sizing(
        session_inputs,
        active_control_state=eff_scope["aggregate"],
    )

    inputs_digest = compute_inputs_digest(
        session_inputs,
        w_ref=sizing_res["bankroll"]["W_ref"],
        active_control_state=eff_scope["aggregate"],
        market_records_digest=market_records_digest,
        session_date=session_date,
    )

    obs_class = "historical_reconstruction" if mode == "reconstruction" else "synthetic"
    ledger_mode = "stage_n_reconstruction" if mode == "reconstruction" else "stage_n_notional"

    body: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "chain_id": chain_id,
        "sequence": sequence,
        "prev_record_sha256": prev_record_sha256,
        "mode": mode,
        "ledger_mode": ledger_mode,
        "policy_id": sizing_res["policy_id"],
        "policy_config_hash": p_hash,
        "engine_hash": ev.engine_hash,
        "code_version_sha": code_version_sha,
        "calendar_version": calendar_session["calendar_version"],
        "calendar_spec_digest": calendar_session["spec_digest"],
        "session_date": session_date,
        "decision_timestamp_utc": decision_timestamp_utc,
        "observation_class": obs_class,
        "inputs_digest": inputs_digest,
        "unavailable_broker_inputs": sizing_res["unavailable_broker_inputs"],
        "bankroll": sizing_res["bankroll"],
        "raw_vs_adjusted": sizing_res["raw_vs_adjusted"],
        "monthly_diagnostic": sizing_res["monthly_diagnostic"],
        "tiers": sizing_res["tiers"],
        "evaluator": {
            "spec_version": ev.spec_version,
            "engine_hash": ev.engine_hash,
            "effective_state_by_scope": eff_scope,
            "breakers": [rows_by_id[bid] for bid in ALL_BREAKER_IDS],
        },
        "assertions": {
            "B0": "50000",
            "H": sizing_res["bankroll"]["H"],
            "forward_clock_credit_eligible": False,
            "kelly_applied": False,
            "loss_controls_armed": False,
            "orders_placed": 0,
            "stop_loss_return_rule": "OFF",
        },
    }
    if discipline_labels is not None:
        body["discipline_labels"] = discipline_labels

    validate_record_body(body)
    return {
        "appended": True,
        "fail_closed": False,
        "record_body": body,
        "next_row_state": next_state,
    }


_CHAIN_LOCK_TLS = threading.local()


def _atomic_write_bytes(target_path: Path, data: bytes) -> None:
    tmp_path = target_path.with_name(
        f".{target_path.name}.tmp.{os.getpid()}.{threading.get_ident()}"
    )
    try:
        with open(tmp_path, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, target_path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise


def check_forward_clock_credit(
    records: list[dict],
    receipts: list[dict] | None = None,
    *,
    ots_bytes_by_seq: Mapping[int, bytes] | None = None,
    ledger_dir: str | Path | None = None,
    bitcoin_merkle_roots: Mapping[int, object] | None = None,
    commitment_deadline_et: object | None = None,
    repo_root: str | Path | None = None,
    config_path: str | Path | None = None,
    calendar_spec: dict | None = None,
    spec_dir: str | Path | None = None,
) -> tuple[bool, list[str]]:
    """Determine whether a ledger record sequence is eligible for CLK-01..04 forward credit."""
    reasons: list[str] = []
    if not records:
        reasons.append("empty_chain")
        return False, reasons

    if receipts is None or len(receipts) == 0:
        reasons.append("missing_receipts")
    elif len(receipts) != len(records):
        reasons.append(f"receipt_count_mismatch:records={len(records)},receipts={len(receipts)}")

    if not bitcoin_merkle_roots:
        reasons.append("missing_trusted_bitcoin_headers")

    l_dir = Path(ledger_dir) if ledger_dir is not None else None
    receipt_by_seq: dict[int, dict] = {}
    if receipts:
        for idx, rec in enumerate(receipts):
            seq_k = rec.get("sequence", idx) if isinstance(rec, dict) else idx
            if isinstance(seq_k, int) and not isinstance(seq_k, bool) and seq_k not in receipt_by_seq:
                receipt_by_seq[seq_k] = rec

    expected_prev = GENESIS_PREV_HASH
    expected_identity: dict[str, str] = {}
    seen_session_dates: set[str] = set()
    base_spec = calendar_spec if calendar_spec is not None else load_spec(spec_dir or DEFAULT_SPEC_DIR)
    spec_cache: dict[str, dict] = {
        str(base_spec["rules"]["calendar_version"]): base_spec
    }

    for idx, env in enumerate(records):
        if not isinstance(env, dict):
            reasons.append(f"seq_{idx}:invalid_record_envelope")
            continue
        has_wrapped_body = "record_body" in env
        body = env["record_body"] if has_wrapped_body else env
        if not isinstance(body, dict):
            reasons.append(f"seq_{idx}:invalid_record_body")
            continue
        seq = body.get("sequence", idx)
        try:
            actual_sha = sha256_canonical(body)
        except CanonicalizationError as exc:
            reasons.append(f"seq_{seq}:record_body_non_canonical:{exc}")
            actual_sha = None

        if has_wrapped_body and actual_sha is not None and env.get("record_sha256") != actual_sha:
            reasons.append(f"seq_{seq}:record_envelope_sha256_mismatch")
        if seq != idx or isinstance(seq, bool):
            reasons.append(f"seq_{seq}:sequence_gap_or_mismatch:expected_{idx}")
        if body.get("prev_record_sha256") != expected_prev:
            reasons.append(f"seq_{seq}:prev_record_sha256_mismatch")

        sess_date = body.get("session_date")
        parsed_sess_date: date | None = None
        canonical_ds: str | None = None
        if not isinstance(sess_date, str) or not sess_date:
            reasons.append(f"seq_{seq}:missing_session_date")
        elif not _DATE_YYYY_MM_DD_RE.fullmatch(sess_date):
            reasons.append(f"seq_{seq}:non_canonical_session_date:{sess_date}")
        else:
            try:
                parsed_sess_date = date.fromisoformat(sess_date)
                canonical_ds = parsed_sess_date.isoformat()
            except ValueError:
                reasons.append(f"seq_{seq}:invalid_session_date:{sess_date}")
            if canonical_ds is not None:
                if canonical_ds in seen_session_dates:
                    reasons.append(f"seq_{seq}:duplicate_session_date:{canonical_ds}")
                else:
                    seen_session_dates.add(canonical_ds)

        decision_ts = body.get("decision_timestamp_utc")
        if not is_utc_ms_timestamp(decision_ts):
            reasons.append(f"seq_{seq}:invalid_decision_timestamp_utc")
        elif canonical_ds is not None and parsed_sess_date is not None:
            dec_dt = datetime.fromisoformat(decision_ts[:-1] + "+00:00")
            dec_ny_date = dec_dt.astimezone(_NY_TZ).date()
            if dec_ny_date < parsed_sess_date:
                reasons.append(
                    f"seq_{seq}:decision_timestamp_before_session_date:"
                    f"{dec_ny_date.isoformat()}<{parsed_sess_date.isoformat()}"
                )
            else:
                body_cal_ver = body.get("calendar_version")
                rec_spec: dict | None = None
                if isinstance(body_cal_ver, str) and body_cal_ver in spec_cache:
                    rec_spec = spec_cache[body_cal_ver]
                elif calendar_spec is None and spec_dir is None and isinstance(body_cal_ver, str) and body_cal_ver:
                    cand_spec_dir = _SPECS_ROOT / body_cal_ver
                    if cand_spec_dir.is_dir():
                        rec_spec = load_spec(cand_spec_dir)
                        spec_cache[body_cal_ver] = rec_spec
                if rec_spec is None:
                    reasons.append(f"seq_{seq}:unknown_calendar_version:{body_cal_ver}")
                else:
                    cal = resolve_session("XNYS", canonical_ds, spec=rec_spec)
                    if cal.get("session_state") == "unknown":
                        reasons.append(f"seq_{seq}:unknown_calendar_session:{canonical_ds}:{cal.get('reason')}")
                    elif not cal.get("is_trading_day") or cal.get("blocks_eligibility"):
                        reasons.append(f"seq_{seq}:non_trading_session:{canonical_ds}")
                    else:
                        cal_close_utc = cal.get("close_utc")
                        if not is_utc_ms_timestamp(cal_close_utc):
                            reasons.append(f"seq_{seq}:missing_calendar_close_utc:{canonical_ds}")
                        elif decision_ts < cal_close_utc:
                            reasons.append(
                                f"seq_{seq}:decision_timestamp_before_session_close:"
                                f"{decision_ts}<{cal_close_utc}"
                            )

        top_engine_hash = body.get("engine_hash")
        if not isinstance(top_engine_hash, str) or not top_engine_hash:
            reasons.append(f"seq_{seq}:missing_engine_hash")
        elif top_engine_hash.startswith("stub-"):
            reasons.append(f"seq_{seq}:stub_engine_hash")

        ev_obj = body.get("evaluator")
        ev_spec: object = None
        if not isinstance(ev_obj, dict):
            reasons.append(f"seq_{seq}:invalid_evaluator_metadata")
        else:
            ev_hash = ev_obj.get("engine_hash")
            ev_spec = ev_obj.get("spec_version")
            if not isinstance(ev_hash, str) or not ev_hash:
                reasons.append(f"seq_{seq}:missing_evaluator_engine_hash")
            else:
                if ev_hash.startswith("stub-"):
                    reasons.append(f"seq_{seq}:nested_evaluator_stub_engine_hash")
                if isinstance(top_engine_hash, str) and top_engine_hash and ev_hash != top_engine_hash:
                    reasons.append(f"seq_{seq}:evaluator_engine_hash_mismatch")
            if not isinstance(ev_spec, str) or not ev_spec:
                reasons.append(f"seq_{seq}:missing_evaluator_spec_version")

        identity_checks = (
            ("chain_id", body.get("chain_id")),
            ("policy_config_hash", body.get("policy_config_hash")),
            ("code_version_sha", body.get("code_version_sha")),
            ("calendar_spec_digest", body.get("calendar_spec_digest")),
            ("engine_hash", top_engine_hash),
            ("spec_version", ev_spec),
        )
        for field_name, field_val in identity_checks:
            if not isinstance(field_val, str) or not field_val:
                if field_name not in ("engine_hash", "spec_version"):
                    reasons.append(f"seq_{seq}:missing_{field_name}")
                continue
            if field_name not in expected_identity:
                expected_identity[field_name] = field_val
            elif field_val != expected_identity[field_name]:
                reasons.append(f"seq_{seq}:{field_name}_mismatch")

        if actual_sha is not None:
            expected_prev = actual_sha

        if body.get("mode") != "production":
            reasons.append(f"seq_{seq}:non_production_mode:{body.get('mode')}")
        if body.get("observation_class") != "decision_time_market":
            reasons.append(f"seq_{seq}:non_decision_time_observation_class:{body.get('observation_class')}")
        labels = body.get("discipline_labels", {})
        if isinstance(labels, dict):
            if labels.get("forward_clock_credit_eligible") is False:
                reasons.append(f"seq_{seq}:discipline_labels_disallows_forward_credit")
            if labels.get("vintage_qualification") == "as-retrieved reconstruction; not point-in-time inputs":
                reasons.append(f"seq_{seq}:y2_as_retrieved_reconstruction")
            if labels.get("any_series_modelled"):
                reasons.append(f"seq_{seq}:modelled_series_present")
        if body.get("assertions", {}).get("forward_clock_credit_eligible") is False:
            reasons.append(f"seq_{seq}:assertions_forward_clock_credit_eligible_false")

        if receipts:
            rec = receipt_by_seq.get(seq) if isinstance(seq, int) and not isinstance(seq, bool) else (receipts[idx] if idx < len(receipts) else None)
            if rec is None:
                reasons.append(f"receipt_{idx}:missing_receipt_for_seq_{seq}")
                continue
            anchor = rec.get("anchor")
            if anchor == "synthetic-stub":
                reasons.append(f"receipt_{idx}:synthetic_stub_anchor")
            elif anchor != "opentimestamps":
                reasons.append(f"receipt_{idx}:non_opentimestamps_anchor:{anchor}")

            expected_deadline: str | None = None
            try:
                if not isinstance(sess_date, str) or not sess_date:
                    raise ValueError("missing_session_date")
                expected_deadline = compute_session_commitment_deadline_utc(
                    sess_date,
                    commitment_deadline_et,
                    repo_root=repo_root,
                    config_path=config_path,
                )
            except Exception as exc:
                reasons.append(f"receipt_{idx}:invalid_commitment_deadline_config:{exc}")

            if expected_deadline is not None:
                timely, err = is_receipt_timely(
                    rec,
                    expected_commitment_deadline_utc=expected_deadline,
                    decision_timestamp_utc=decision_ts if isinstance(decision_ts, str) else None,
                    require_expected_deadline=True,
                )
                if not timely:
                    reasons.append(f"receipt_{idx}:{err}")

            if anchor == "opentimestamps":
                ots_raw: bytes | None = None
                if ots_bytes_by_seq is not None and isinstance(seq, int) and not isinstance(seq, bool):
                    ots_raw = ots_bytes_by_seq.get(seq)
                elif l_dir is not None and isinstance(seq, int) and not isinstance(seq, bool):
                    opath = l_dir / f"seq_{seq:06d}.ots"
                    if opath.is_file():
                        ots_raw = opath.read_bytes()
                if actual_sha is None:
                    reasons.append(f"receipt_{idx}:unhashable_record_body")
                    continue
                rec_env = env if has_wrapped_body else {
                    "record_body": body,
                    "record_sha256": actual_sha,
                }
                ok_crypto, crypto_err = verify_receipt(
                    rec_env,
                    rec,
                    ots_bytes=ots_raw,
                    bitcoin_merkle_roots=bitcoin_merkle_roots,
                    require_bitcoin_verification=True,
                    require_bitcoin_timestamp=True,
                    expected_commitment_deadline_utc=expected_deadline,
                )
                if not ok_crypto:
                    reasons.append(f"receipt_{idx}:{crypto_err}")

    return len(reasons) == 0, reasons


class ShadowLedger:
    """Append-only file-backed SHA-256 hash-chained ledger with detached receipts."""

    def __init__(self, root_dir: str | Path) -> None:
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def chain_lock(self) -> Iterator[None]:
        """Acquire an exclusive per-chain file lock, safely re-entrant within the same thread."""
        self.root_dir.mkdir(parents=True, exist_ok=True)
        key = str(self.root_dir.resolve())
        locks = getattr(_CHAIN_LOCK_TLS, "locks", None)
        if locks is None:
            locks = {}
            _CHAIN_LOCK_TLS.locks = locks
        entry = locks.get(key)
        if entry is not None:
            entry["depth"] += 1
            try:
                yield
            finally:
                entry["depth"] -= 1
            return

        lock_path = self.root_dir / ".chain.lock"
        fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            locks[key] = {"fd": fd, "depth": 1}
            try:
                yield
            finally:
                locks.pop(key, None)
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def _record_files(self) -> list[tuple[int, Path]]:
        pairs: list[tuple[int, Path]] = []
        for p in self.root_dir.glob("seq_*.record.json"):
            stem = p.name[len("seq_") : -len(".record.json")]
            if stem.isdigit():
                pairs.append((int(stem), p))
        pairs.sort(key=lambda item: item[0])
        return pairs

    def _receipt_path(self, seq: int) -> Path:
        return self.root_dir / f"seq_{seq:06d}.receipt.json"

    def _ots_path(self, seq: int) -> Path:
        return self.root_dir / f"seq_{seq:06d}.ots"

    def _record_path(self, seq: int) -> Path:
        return self.root_dir / f"seq_{seq:06d}.record.json"

    def load_records(self) -> list[dict]:
        """Load all record envelopes sorted by filename sequence."""
        out: list[dict] = []
        for _, path in self._record_files():
            parsed = parse_json_no_duplicates(path.read_text(encoding="utf-8"))
            if not isinstance(parsed, dict):
                raise CanonicalizationError(f"Invalid record file {path.name}")
            out.append(parsed)
        return out

    def load_receipts(self) -> list[dict]:
        """Load all receipt files sorted by sequence."""
        out: list[dict] = []
        for seq, _ in self._record_files():
            rpath = self._receipt_path(seq)
            if not rpath.is_file():
                raise FileNotFoundError(f"Missing detached receipt for seq {seq}")
            parsed = parse_json_no_duplicates(rpath.read_text(encoding="utf-8"))
            if not isinstance(parsed, dict):
                raise CanonicalizationError(f"Invalid receipt file {rpath.name}")
            out.append(parsed)
        return out

    def head(self) -> dict | None:
        """Return the last appended record envelope or None if empty."""
        files = self._record_files()
        if not files:
            return None
        last_path = files[-1][1]
        parsed = parse_json_no_duplicates(last_path.read_text(encoding="utf-8"))
        return parsed if isinstance(parsed, dict) else None

    def append_record(
        self,
        record_body: dict,
        receipt: dict | None = None,
        obtained_at_utc: str | None = None,
        *,
        ots_bytes: bytes | None = None,
        commitment_deadline_utc: str | None = None,
        bitcoin_merkle_roots: Mapping[int, object] | None = None,
        allow_unanchored: bool = False,
        before_record_commit: Callable[[], None] | None = None,
    ) -> dict:
        """Validate and atomically append one record plus its detached receipt (and .ots proof)."""
        with self.chain_lock():
            try:
                validate_record_body(record_body)
            except CanonicalizationError as exc:
                raise LedgerAppendError(f"invalid_record_body: {exc}") from exc

            existing_files = self._record_files()
            existing_records = self.load_records() if existing_files else []
            expected_seq = len(existing_records)
            if existing_files and [seq for seq, _ in existing_files] != list(range(expected_seq)):
                raise LedgerAppendError("existing_chain_has_sequence_gap")

            seq = record_body["sequence"]
            if seq != expected_seq:
                raise LedgerAppendError(f"sequence_gap_or_mismatch: expected {expected_seq}, got {seq}")

            expected_prev = GENESIS_PREV_HASH if expected_seq == 0 else existing_records[-1]["record_sha256"]
            if record_body["prev_record_sha256"] != expected_prev:
                raise LedgerAppendError(
                    f"forked_previous_hash: expected {expected_prev}, got {record_body['prev_record_sha256']}"
                )

            chain_identity = (
                record_body["chain_id"],
                record_body["mode"],
                record_body["ledger_mode"],
                record_body["policy_id"],
                record_body["policy_config_hash"],
                record_body["code_version_sha"],
                record_body["calendar_spec_digest"],
            )
            dedup_key = (
                record_body["mode"],
                record_body["policy_id"],
                record_body["policy_config_hash"],
                record_body["session_date"],
            )
            for prior in existing_records:
                pb = prior["record_body"]
                prior_identity = (
                    pb["chain_id"],
                    pb["mode"],
                    pb["ledger_mode"],
                    pb["policy_id"],
                    pb["policy_config_hash"],
                    pb["code_version_sha"],
                    pb["calendar_spec_digest"],
                )
                if prior_identity != chain_identity:
                    raise LedgerAppendError(
                        f"distinct_chain_required: expected {prior_identity}, got {chain_identity}"
                    )
                prior_key = (pb["mode"], pb["policy_id"], pb["policy_config_hash"], pb["session_date"])
                if prior_key == dedup_key:
                    raise LedgerAppendError(
                        f"duplicate_session_append: {dedup_key}"
                    )

            record_sha256 = sha256_canonical(record_body)
            envelope = {
                "record_body": record_body,
                "record_sha256": record_sha256,
            }

            rec_path = self._record_path(seq)
            rcp_path = self._receipt_path(seq)
            ots_path = self._ots_path(seq)
            # Only committed record marker indicates an existing sequence; orphan .ots/.receipt files from an abrupt kill are cleanly overwritten on retry.
            if rec_path.exists():
                raise LedgerAppendError(f"target_sequence_already_exists: {seq}")

            pre_marker_writes: list[tuple[Path, bytes]] = []
            if not allow_unanchored:
                if receipt is None:
                    if obtained_at_utc is None:
                        raise LedgerAppendError("obtained_at_utc_required_when_receipt_omitted")
                    obt = obtained_at_utc
                    if ots_bytes is not None:
                        receipt_obj = create_ots_receipt(
                            envelope,
                            ots_bytes,
                            obt,
                            commitment_deadline_utc=commitment_deadline_utc,
                            bitcoin_merkle_roots=bitcoin_merkle_roots,
                        )
                    else:
                        receipt_obj = create_synthetic_receipt(
                            envelope,
                            obt,
                            commitment_deadline_utc=commitment_deadline_utc,
                        )
                else:
                    receipt_obj = receipt

                ok_rec, err_rec = verify_receipt(
                    envelope,
                    receipt_obj,
                    ots_bytes=ots_bytes,
                    bitcoin_merkle_roots=bitcoin_merkle_roots,
                )
                if not ok_rec:
                    raise LedgerAppendError(f"invalid_receipt: {err_rec}")
                if ots_bytes is not None:
                    pre_marker_writes.append((ots_path, ots_bytes))
                pre_marker_writes.append((rcp_path, canonical_json_bytes(receipt_obj)))

            self.root_dir.mkdir(parents=True, exist_ok=True)
            if allow_unanchored:
                # Purge any stale orphan .receipt/.ots from an earlier crashed run before committing unanchored.
                for orphan_path in (rcp_path, ots_path):
                    if orphan_path.exists():
                        orphan_path.unlink()
            elif ots_bytes is None and ots_path.exists():
                ots_path.unlink()

            created_paths: list[Path] = []
            try:
                for target_path, data in pre_marker_writes:
                    _atomic_write_bytes(target_path, data)
                    created_paths.append(target_path)
                if before_record_commit is not None:
                    before_record_commit()
                # seq_NNNNNN.record.json is written LAST as the atomic commit marker
                _atomic_write_bytes(rec_path, canonical_json_bytes(envelope))
                created_paths.append(rec_path)
            except Exception:
                for created_path in reversed(created_paths):
                    if created_path.exists():
                        created_path.unlink()
                raise
            return envelope

    def rollback_sequence(self, seq: int) -> None:
        """Roll back all artifacts for seq when an outer transaction fails before commit completion."""
        for p in (self._record_path(seq), self._receipt_path(seq), self._ots_path(seq)):
            if p.exists():
                p.unlink()

    def verify_chain(self) -> tuple[bool, int | None, str | None]:
        """Verify canonical bytes, hashes, genesis, and gapless sequence links."""
        files = self._record_files()
        if not files:
            return True, None, None

        expected_seq = 0
        expected_prev = GENESIS_PREV_HASH
        chain_identity: tuple[object, ...] | None = None

        for file_seq, path in files:
            if file_seq != expected_seq:
                return False, expected_seq, f"sequence_gap: expected {expected_seq}, got {file_seq}"

            raw_bytes = path.read_bytes()
            try:
                raw_text = raw_bytes.decode("utf-8")
                envelope = parse_json_no_duplicates(raw_text)
            except Exception as exc:
                return False, expected_seq, f"corrupt_json_or_duplicate_key: {exc}"

            if not isinstance(envelope, dict) or set(envelope.keys()) != {"record_body", "record_sha256"}:
                return False, expected_seq, "invalid_envelope_structure"

            # Detect any byte-level mutation including formatting/whitespace mutation
            try:
                expected_file_bytes = canonical_json_bytes(envelope)
            except Exception as exc:
                return False, expected_seq, f"canonicalization_error: {exc}"

            if raw_bytes != expected_file_bytes:
                return False, expected_seq, "raw_record_bytes_mismatch_canonical_form"

            body = envelope["record_body"]
            try:
                validate_record_body(body)
            except CanonicalizationError as exc:
                return False, expected_seq, f"invalid_record_body: {exc}"

            if body["sequence"] != expected_seq:
                return False, expected_seq, f"body_sequence_mismatch: expected {expected_seq}, got {body['sequence']}"
            if body["prev_record_sha256"] != expected_prev:
                return False, expected_seq, "prev_record_sha256_mismatch"

            cur_identity = (
                body["chain_id"],
                body["mode"],
                body["ledger_mode"],
                body["policy_id"],
                body["policy_config_hash"],
                body["code_version_sha"],
                body["calendar_spec_digest"],
            )
            if chain_identity is None:
                chain_identity = cur_identity
            elif cur_identity != chain_identity:
                return False, expected_seq, "chain_identity_mismatch"

            computed_hash = sha256_canonical(body)
            if envelope["record_sha256"] != computed_hash:
                return False, expected_seq, "record_sha256_mismatch"

            expected_prev = computed_hash
            expected_seq += 1

        return True, None, None

    def verify_all_receipts(
        self,
        *,
        bitcoin_merkle_roots: Mapping[int, object] | None = None,
        require_bitcoin_verification: bool = False,
    ) -> tuple[bool, int | None, str | None]:
        """Verify detached timestamp receipts (and .ots sidecars) separately from chain hashes."""
        files = self._record_files()
        for seq, path in files:
            rpath = self._receipt_path(seq)
            if not rpath.is_file():
                return False, seq, f"missing_receipt_for_seq_{seq}"
            try:
                envelope = parse_json_no_duplicates(path.read_text(encoding="utf-8"))
                receipt = parse_json_no_duplicates(rpath.read_text(encoding="utf-8"))
            except Exception as exc:
                return False, seq, f"corrupt_receipt_json: {exc}"
            if not isinstance(envelope, dict) or not isinstance(receipt, dict):
                return False, seq, "invalid_receipt_type"
            ots_bytes: bytes | None = None
            if receipt.get("anchor") == "opentimestamps":
                opath = self._ots_path(seq)
                if not opath.is_file():
                    return False, seq, f"missing_ots_file_for_seq_{seq}"
                ots_bytes = opath.read_bytes()
            ok, err = verify_receipt(
                envelope,
                receipt,
                ots_bytes=ots_bytes,
                bitcoin_merkle_roots=bitcoin_merkle_roots,
                require_bitcoin_verification=require_bitcoin_verification,
            )
            if not ok:
                return False, seq, err
        return True, None, None

    def assess_receipt_creditability(
        self,
        *,
        bitcoin_merkle_roots: Mapping[int, object] | None = None,
        commitment_deadline_et: object | None = None,
        repo_root: str | Path | None = None,
        config_path: str | Path | None = None,
    ) -> tuple[bool, list[object]]:
        """Require valid chain + non-empty ledger with timely OTS receipts verified against trusted Bitcoin headers & block timestamps."""
        chain_ok, bad_seq, chain_err = self.verify_chain()
        if not chain_ok:
            return False, [{"sequence": bad_seq, "reason": f"chain_invalid:{chain_err}"}]
        files = self._record_files()
        if not files:
            return False, ["empty_ledger"]
        issues: list[object] = []
        for seq, path in files:
            rpath = self._receipt_path(seq)
            if not rpath.is_file():
                issues.append({"sequence": seq, "reason": "missing_receipt"})
                continue
            try:
                envelope = parse_json_no_duplicates(path.read_text(encoding="utf-8"))
                receipt = parse_json_no_duplicates(rpath.read_text(encoding="utf-8"))
            except Exception as exc:
                issues.append({"sequence": seq, "reason": f"corrupt_receipt:{exc}"})
                continue
            if not isinstance(envelope, dict) or not isinstance(receipt, dict):
                issues.append({"sequence": seq, "reason": "invalid_receipt_type"})
                continue
            if receipt.get("anchor") != "opentimestamps":
                issues.append({"sequence": seq, "reason": f"non_ots_anchor:{receipt.get('anchor')}"})
                continue
            ots_bytes = self._ots_path(seq).read_bytes() if self._ots_path(seq).is_file() else None
            ok_fmt, fmt_err = verify_receipt(
                envelope,
                receipt,
                ots_bytes=ots_bytes,
                require_bitcoin_verification=False,
            )
            if not ok_fmt:
                issues.append({"sequence": seq, "reason": str(fmt_err)})
                continue
            body = envelope.get("record_body", {})
            expected_deadline: str | None = None
            try:
                sess_date = body.get("session_date") if isinstance(body, dict) else None
                if not isinstance(sess_date, str):
                    raise ValueError("missing_session_date")
                expected_deadline = compute_session_commitment_deadline_utc(
                    sess_date,
                    commitment_deadline_et,
                    repo_root=repo_root,
                    config_path=config_path,
                )
            except Exception as exc:
                issues.append({"sequence": seq, "reason": f"invalid_commitment_deadline_config:{exc}"})
                continue
            timely, t_err = is_receipt_timely(
                receipt,
                expected_commitment_deadline_utc=expected_deadline,
                decision_timestamp_utc=body.get("decision_timestamp_utc") if isinstance(body, dict) else None,
                require_expected_deadline=True,
            )
            if not timely:
                issues.append({"sequence": seq, "reason": str(t_err)})
                continue
            ok_btc, btc_err = verify_receipt(
                envelope,
                receipt,
                ots_bytes=ots_bytes,
                bitcoin_merkle_roots=bitcoin_merkle_roots,
                require_bitcoin_verification=True,
                require_bitcoin_timestamp=True,
                expected_commitment_deadline_utc=expected_deadline,
            )
            if not ok_btc:
                issues.append({"sequence": seq, "reason": str(btc_err)})
        return len(issues) == 0, issues
