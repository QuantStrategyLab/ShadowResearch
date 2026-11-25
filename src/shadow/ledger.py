"""C-SH-LEDGER canonicalization (restricted RFC 8785), hash chain, and detached receipts."""

from __future__ import annotations

import fcntl
import hashlib
import hmac
import json
import os
import re
import threading
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

from shadow.calendar_consumer import DEFAULT_SPEC_DIR, load_spec, resolve_session
from shadow.evaluator import ALL_BREAKER_IDS, StubEvaluator, StubProductionForbiddenError
from shadow.fuse_adapter import (
    EngineIdentityError,
    PINNED_FUSE_ENGINE_HASH,
    PINNED_FUSE_SPEC_VERSION,
    build_fuse_validated_inputs,
    is_stub_evaluator,
    resolve_account_and_fund_control_states,
    resolve_aggregate_control_state,
    serialize_breakers_for_ledger,
)
from shadow.manifest import compute_code_manifest_hash, load_effective_runtime_config
from shadow.ots import OtsFormatError, parse_ots_proof, verify_ots_bytes
from shadow.sizing import (
    INSTRUMENT_ORDER,
    TIER_ORDER,
    canonical_decimal_str,
    compute_session_sizing,
    policy_config_hash,
)

GENESIS_PREV_HASH = "0" * 64
NO_PENDING_REJECTED_STATE_SHA256 = GENESIS_PREV_HASH
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


class ChainConcurrencyConflictError(RuntimeError):
    """Raised when another writer holds an active cross-instance chain lease."""


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
    if body["mode"] == "production" and body.get("observation_class") == "synthetic":
        raise CanonicalizationError("Synthetic observation_class is structurally forbidden in production mode")
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
    is_legacy_stub_or_sh1_fixture = (
        ev_obj["engine_hash"].startswith("stub-")
        or ev_obj["spec_version"] == "c-fuse-eval-stub-v1"
    )
    if "next_row_state_sha256" in ev_obj:
        nrs_sha = ev_obj.get("next_row_state_sha256")
        if not isinstance(nrs_sha, str) or not _HEX64_RE.fullmatch(nrs_sha):
            raise CanonicalizationError("evaluator.next_row_state_sha256 must be 64 lowercase hex chars")
    elif not is_legacy_stub_or_sh1_fixture:
        raise CanonicalizationError(
            "evaluator.next_row_state_sha256 is required for non-stub evaluator records"
        )
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
    if (
        not isinstance(tiers, list)
        or not all(isinstance(t, dict) for t in tiers)
        or [t.get("tier_id") for t in tiers] != list(TIER_ORDER)
    ):
        raise CanonicalizationError(f"tiers must appear once in stable ID order {TIER_ORDER}")
    for tier in tiers:
        for tk in ("D", "k", "x_cap", "x_joint", "w_max", "sigma_target", "c", "x_ML", "x_eff_joint", "alpha_joint"):
            if not is_canonical_decimal_str(tier.get(tk)):
                raise CanonicalizationError(f"tier {tier.get('tier_id')}.{tk} invalid decimal: {tier.get(tk)!r}")
        tier_orders = tier.get("orders_placed")
        if isinstance(tier_orders, bool) or not isinstance(tier_orders, int) or tier_orders != 0:
            raise CanonicalizationError("tier orders_placed must be integer 0 in Stage N")
        funds = tier.get("funds")
        if (
            not isinstance(funds, list)
            or not all(isinstance(f, dict) for f in funds)
            or [f.get("instrument") for f in funds] != list(INSTRUMENT_ORDER)
        ):
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


def o1_blocks_session_commit(o1_output: Mapping[str, Any]) -> bool:
    """Return True when O1 tripped on missing/stale/invalid input, allowing FUSE-1 B3 clearing_period."""
    return bool(o1_output.get("would_have_tripped")) and o1_output.get("reason") != "clearing_period"


def _sha256_canonical_state_dict(state_obj: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            state_obj,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


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
    evaluator: object | None = None,
    prior_row_state: dict | None = None,
    discipline_labels: dict | None = None,
    market_records_digest: str | None = None,
    runtime_config: dict[str, object] | None = None,
    calendar_spec: dict[str, object] | None = None,
) -> dict:
    """Run evaluator + sizing and assemble a canonical record_body (or O1/O2 fail-closed dict)."""
    ev = evaluator if evaluator is not None else StubEvaluator()
    if mode == "production" and is_stub_evaluator(ev):
        raise StubProductionForbiddenError(
            "Production mode structurally forbids StubEvaluator and requires vendored FUSE-1"
        )
    p_hash = policy_config_hash(
        runtime_config=runtime_config,
        calendar_spec=calendar_spec or calendar_session,
        code_manifest_hash=code_version_sha,
    )
    prior = prior_row_state if prior_row_state is not None else {}

    if is_stub_evaluator(ev):
        eval_inputs = dict(session_inputs)
        eval_inputs["calendar"] = calendar_session
        eval_mode = mode
        auth_events = None
    else:
        eval_inputs = build_fuse_validated_inputs(
            session_inputs,
            calendar_session,
            policy_hash=p_hash,
            session_date=session_date,
            as_of=decision_timestamp_utc,
        )
        eval_mode = "shadow"
        auth_events = session_inputs.get("authorized_events")

    rows_by_id, eff_scope, next_state = ev.evaluate(
        spec_version=ev.spec_version,
        engine_hash=ev.engine_hash,
        policy_hash=p_hash,
        session=session_date,
        as_of=decision_timestamp_utc,
        mode=eval_mode,
        validated_inputs=eval_inputs,
        prior_row_state=prior,
        authorized_events=auth_events,
    )

    if o1_blocks_session_commit(rows_by_id["O1"]) or rows_by_id["O2"]["would_have_tripped"]:
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

    account_state, fund_states, overall_state = resolve_account_and_fund_control_states(
        eff_scope, INSTRUMENT_ORDER
    )

    sizing_res = compute_session_sizing(
        session_inputs,
        active_control_state=account_state,
        fund_control_states=fund_states,
    )

    inputs_digest = compute_inputs_digest(
        session_inputs,
        w_ref=sizing_res["bankroll"]["W_ref"],
        active_control_state=overall_state,
        market_records_digest=market_records_digest,
        session_date=session_date,
    )

    if mode == "production":
        obs_class = "decision_time_market"
        ledger_mode = "stage_n_production"
    elif mode == "reconstruction":
        obs_class = "historical_reconstruction"
        ledger_mode = "stage_n_reconstruction"
    else:
        obs_class = "synthetic"
        ledger_mode = "stage_n_notional"

    forward_credit = bool(
        mode == "production"
        and getattr(ev, "production_clock_capable", False) is True
        and not (
            isinstance(discipline_labels, dict)
            and discipline_labels.get("forward_clock_credit_eligible") is False
        )
    )
    serialized_breakers = serialize_breakers_for_ledger(rows_by_id)

    evaluator_block: dict[str, object] = {
        "spec_version": ev.spec_version,
        "engine_hash": ev.engine_hash,
        "effective_state_by_scope": eff_scope,
        "breakers": serialized_breakers,
    }
    if not is_stub_evaluator(ev):
        evaluator_block["next_row_state_sha256"] = _sha256_canonical_state_dict(
            next_state if isinstance(next_state, dict) else {}
        )

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
        "evaluator": evaluator_block,
        "assertions": {
            "B0": "50000",
            "H": sizing_res["bankroll"]["H"],
            "forward_clock_credit_eligible": forward_credit,
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
    expected_engine_hash: str | None = None,
    expected_spec_version: str | None = None,
    production_chain_locked: bool | None = None,
) -> tuple[bool, list[str]]:
    """Determine whether a ledger record sequence is eligible for CLK-01..04 forward credit."""
    reasons: list[str] = []
    if not records:
        if production_chain_locked is True:
            reasons.append("production_chain_locked")
        reasons.append("empty_chain")
        return False, reasons

    eff_expected_engine_hash = expected_engine_hash
    eff_expected_spec_version = expected_spec_version
    eff_locked = production_chain_locked

    if config_path is not None:
        cfg_file = Path(config_path)
        if not cfg_file.is_file():
            reasons.append("missing_production_config")
            eff_locked = True
            if eff_expected_engine_hash is None:
                eff_expected_engine_hash = PINNED_FUSE_ENGINE_HASH
            if eff_expected_spec_version is None:
                eff_expected_spec_version = PINNED_FUSE_SPEC_VERSION
        else:
            try:
                raw_cfg = parse_json_no_duplicates(cfg_file.read_text(encoding="utf-8"))
            except Exception as exc:
                reasons.append(f"invalid_production_config:{exc}")
                raw_cfg = None
            if not isinstance(raw_cfg, dict) or raw_cfg.get("mode") != "production":
                bad_mode = raw_cfg.get("mode") if isinstance(raw_cfg, dict) else "invalid"
                reasons.append(f"non_production_config_rejected:{bad_mode}")
                eff_locked = True
                if eff_expected_engine_hash is None:
                    eff_expected_engine_hash = PINNED_FUSE_ENGINE_HASH
                if eff_expected_spec_version is None:
                    eff_expected_spec_version = PINNED_FUSE_SPEC_VERSION
            elif raw_cfg.get("production_mode_forbidden") is True or (
                "production_mode_forbidden" in raw_cfg
                and not isinstance(raw_cfg.get("production_mode_forbidden"), bool)
            ):
                reasons.append("non_production_config_rejected:production_mode_forbidden")
                eff_locked = True
                if eff_expected_engine_hash is None:
                    eff_expected_engine_hash = PINNED_FUSE_ENGINE_HASH
                if eff_expected_spec_version is None:
                    eff_expected_spec_version = PINNED_FUSE_SPEC_VERSION
            else:
                if eff_locked is None:
                    eff_locked = not (
                        raw_cfg.get("production_chain_locked") is False
                        and raw_cfg.get("clk01_active") is True
                    )
                if eff_expected_engine_hash is None:
                    eh = raw_cfg.get("fuse_engine_hash")
                    eff_expected_engine_hash = eh if isinstance(eh, str) and eh else "__MISSING__"
                if eff_expected_spec_version is None:
                    sv = raw_cfg.get("fuse_spec_version")
                    eff_expected_spec_version = sv if isinstance(sv, str) and sv else "__MISSING__"
    else:
        first_body = (
            records[0].get("record_body", records[0])
            if isinstance(records[0], dict)
            else {}
        )
        is_sh2_prod_chain = isinstance(first_body, dict) and (
            first_body.get("calendar_version") == "csa22-v2"
            or first_body.get("chain_id") == "prod-sh2-v1"
            or first_body.get("ledger_mode") == "stage_n_production"
        )
        if is_sh2_prod_chain:
            if eff_locked is None:
                base_root = Path(repo_root) if repo_root is not None else _SPECS_ROOT.parent
                default_prod_cfg = base_root / "config" / "production-scheduler.json"
                if not default_prod_cfg.is_file():
                    eff_locked = True
                else:
                    try:
                        raw_prod = parse_json_no_duplicates(default_prod_cfg.read_text(encoding="utf-8"))
                    except Exception:
                        raw_prod = None
                    if not isinstance(raw_prod, dict):
                        eff_locked = True
                    else:
                        eff_locked = not (
                            raw_prod.get("production_chain_locked") is False
                            and raw_prod.get("clk01_active") is True
                        )
            if eff_expected_engine_hash is None:
                eff_expected_engine_hash = PINNED_FUSE_ENGINE_HASH
            if eff_expected_spec_version is None:
                eff_expected_spec_version = PINNED_FUSE_SPEC_VERSION

    if eff_locked is True:
        reasons.append("production_chain_locked")

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
            if eff_expected_engine_hash is not None and top_engine_hash != eff_expected_engine_hash:
                reasons.append(f"seq_{seq}:unexpected_engine_hash")
            if eff_expected_spec_version is not None and ev_spec != eff_expected_spec_version:
                reasons.append(f"seq_{seq}:unexpected_spec_version")
            if (
                ev_spec == PINNED_FUSE_SPEC_VERSION
                or top_engine_hash == PINNED_FUSE_ENGINE_HASH
                or body.get("calendar_version") == "csa22-v2"
            ):
                nrs_sha = ev_obj.get("next_row_state_sha256")
                if not isinstance(nrs_sha, str) or not _HEX64_RE.fullmatch(nrs_sha):
                    reasons.append(f"seq_{seq}:missing_or_invalid_next_row_state_sha256")

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


def has_committed_detached_evidence(ledger: "ShadowLedger", seq: int) -> tuple[bool, str | None]:
    """Detect structured committed .receipt.json, valid OTS binary proof, or unreadable/corrupt detached evidence at seq."""
    rpath = ledger._receipt_path(seq)
    if rpath.exists():
        try:
            raw_txt = rpath.read_text(encoding="utf-8")
        except OSError as exc:
            return True, f"unreadable_detached_receipt_seq_{seq}:{type(exc).__name__}"
        try:
            rcp = parse_json_no_duplicates(raw_txt)
        except Exception:
            return True, f"corrupt_detached_receipt_seq_{seq}"
        if isinstance(rcp, dict):
            anchor = rcp.get("anchor")
            rec_seq = rcp.get("sequence")
            rec_sha = rcp.get("record_sha256")
            rec_ds = rcp.get("session_date")
            obtained = rcp.get("obtained_at_utc")
            date_ok = False
            if isinstance(rec_ds, str) and _DATE_YYYY_MM_DD_RE.fullmatch(rec_ds):
                try:
                    date.fromisoformat(rec_ds)
                    date_ok = True
                except ValueError:
                    date_ok = False
            if (
                anchor in ("opentimestamps", "synthetic-stub")
                and not isinstance(rec_seq, bool)
                and rec_seq == seq
                and isinstance(rec_sha, str)
                and _HEX64_RE.fullmatch(rec_sha) is not None
                and date_ok
                and is_utc_ms_timestamp(obtained)
            ):
                return True, f"committed_detached_receipt_without_record_seq_{seq}"

    opath = ledger._ots_path(seq)
    if opath.exists():
        try:
            raw_ots = opath.read_bytes()
        except OSError as exc:
            return True, f"unreadable_detached_ots_seq_{seq}:{type(exc).__name__}"
        try:
            parse_ots_proof(raw_ots)
            return True, f"committed_ots_proof_without_record_seq_{seq}"
        except OtsFormatError:
            pass

    return False, None


class ShadowLedger:
    """Append-only file-backed SHA-256 hash-chained ledger with detached receipts."""

    def __init__(self, root_dir: str | Path) -> None:
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _parse_iso_utc(raw: object) -> datetime | None:
        if not isinstance(raw, str) or not raw:
            return None
        iso = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
        try:
            return datetime.fromisoformat(iso).astimezone(timezone.utc)
        except ValueError:
            return None

    @staticmethod
    def _read_lease_snapshot(lease_path: Path) -> tuple[os.stat_result, bytes]:
        rfd = os.open(str(lease_path), os.O_RDONLY)
        with os.fdopen(rfd, "rb") as rf:
            st = os.fstat(rf.fileno())
            return st, rf.read()

    def _acquire_cross_instance_lease(
        self,
        lease_path: Path,
        *,
        lease_ttl_seconds: float,
    ) -> str:
        lease_id = uuid4().hex
        now_utc = datetime.now(timezone.utc)
        expires_utc = now_utc + timedelta(seconds=max(1.0, lease_ttl_seconds))
        lease_payload = {
            "schema_version": "c-sh-chain-lease-v1",
            "lease_id": lease_id,
            "pid": os.getpid(),
            "execution_id": os.environ.get("CLOUD_RUN_EXECUTION", ""),
            "acquired_at_utc": now_utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now_utc.microsecond // 1000:03d}Z",
            "expires_at_utc": expires_utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{expires_utc.microsecond // 1000:03d}Z",
        }
        lease_bytes = (
            json.dumps(lease_payload, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode("utf-8")

        for attempt in range(2):
            try:
                lfd = os.open(str(lease_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                try:
                    os.write(lfd, lease_bytes)
                    os.fsync(lfd)
                finally:
                    os.close(lfd)
                return lease_id
            except FileExistsError:
                existing: dict = {}
                try:
                    orig_st, existing_bytes = self._read_lease_snapshot(lease_path)
                    existing_raw = existing_bytes.decode("utf-8", errors="replace")
                    parsed = parse_json_no_duplicates(existing_raw) if existing_raw.strip() else {}
                    if isinstance(parsed, dict):
                        existing = parsed
                except FileNotFoundError:
                    continue
                except Exception:
                    raise ChainConcurrencyConflictError(
                        "Chain lease file unreadable during concurrent acquisition"
                    )

                exp_dt = self._parse_iso_utc(existing.get("expires_at_utc"))
                now_chk = datetime.now(timezone.utc)
                if exp_dt is not None and now_chk < exp_dt:
                    raise ChainConcurrencyConflictError(
                        "Chain lease held by active writer: "
                        f"lease_id={existing.get('lease_id')!r}, "
                        f"pid={existing.get('pid')!r}, "
                        f"execution_id={existing.get('execution_id')!r}"
                    )
                if attempt == 0:
                    stale_sha = hashlib.sha256(existing_bytes).hexdigest()
                    raw_lease_id = existing.get("lease_id")
                    if isinstance(raw_lease_id, str) and raw_lease_id:
                        claim_basis = f"id:{raw_lease_id}:{stale_sha}"
                    else:
                        claim_basis = (
                            f"raw:{stale_sha}:{int(orig_st.st_mtime)}:{orig_st.st_size}"
                        )
                    claim_key = hashlib.sha256(claim_basis.encode("utf-8")).hexdigest()[:32]
                    reclaim_path = lease_path.with_name(
                        f".chain.lease.reclaim.{claim_key}.lock"
                    )
                    reclaim_expires = now_chk + timedelta(
                        seconds=max(300.0, float(lease_ttl_seconds))
                    )
                    reclaim_bytes = (
                        json.dumps(
                            {
                                "schema_version": "c-sh-chain-lease-reclaim-v1",
                                "claim_key": claim_key,
                                "reclaimed_lease_id": raw_lease_id,
                                "claimed_by_lease_id": lease_id,
                                "claimed_at_utc": now_chk.strftime("%Y-%m-%dT%H:%M:%S.")
                                + f"{now_chk.microsecond // 1000:03d}Z",
                                "expires_at_utc": reclaim_expires.strftime("%Y-%m-%dT%H:%M:%S.")
                                + f"{reclaim_expires.microsecond // 1000:03d}Z",
                            },
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                        + "\n"
                    ).encode("utf-8")
                    try:
                        cfd = os.open(
                            str(reclaim_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600
                        )
                        try:
                            os.write(cfd, reclaim_bytes)
                            os.fsync(cfd)
                        finally:
                            os.close(cfd)
                    except FileExistsError as exc:
                        raise ChainConcurrencyConflictError(
                            "Stale chain lease reclamation already claimed by peer writer"
                        ) from exc

                    try:
                        cur_st, cur_bytes = self._read_lease_snapshot(lease_path)
                    except FileNotFoundError as exc:
                        raise ChainConcurrencyConflictError(
                            "Chain lease changed before stale CAS reclamation completed"
                        ) from exc
                    if (
                        cur_st.st_dev != orig_st.st_dev
                        or cur_st.st_ino != orig_st.st_ino
                        or cur_bytes != existing_bytes
                    ):
                        raise ChainConcurrencyConflictError(
                            "Chain lease identity changed during stale CAS reclamation"
                        )
                    try:
                        os.unlink(str(lease_path))
                    except FileNotFoundError:
                        pass
                    continue
                raise ChainConcurrencyConflictError(
                    "Chain lease creation conflicted after stale reclamation retry"
                )
        raise ChainConcurrencyConflictError("Failed to acquire cross-instance chain lease")

    @staticmethod
    def _release_cross_instance_lease(lease_path: Path, expected_lease_id: str) -> None:
        now_utc = datetime.now(timezone.utc)
        if lease_path.is_file():
            try:
                parsed = parse_json_no_duplicates(lease_path.read_text(encoding="utf-8"))
            except Exception:
                parsed = None
            if isinstance(parsed, dict) and parsed.get("lease_id") == expected_lease_id:
                exp_dt = ShadowLedger._parse_iso_utc(parsed.get("expires_at_utc"))
                if exp_dt is None or now_utc < exp_dt:
                    try:
                        lease_path.unlink()
                    except FileNotFoundError:
                        pass
        for rpath in lease_path.parent.glob(".chain.lease.reclaim.*.lock"):
            try:
                rparsed = parse_json_no_duplicates(rpath.read_text(encoding="utf-8"))
                rexp = (
                    ShadowLedger._parse_iso_utc(rparsed.get("expires_at_utc"))
                    if isinstance(rparsed, dict)
                    else None
                )
                if rexp is not None and now_utc >= rexp:
                    rpath.unlink()
            except Exception:
                pass

    @contextmanager
    def chain_lock(self, *, lease_ttl_seconds: float = 300.0) -> Iterator[None]:
        """Acquire exclusive flock + atomic .chain.lease.json cross-instance fencing lock."""
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
        lease_path = self.root_dir / ".chain.lease.json"
        fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            lease_id = self._acquire_cross_instance_lease(
                lease_path, lease_ttl_seconds=lease_ttl_seconds
            )
            locks[key] = {"fd": fd, "depth": 1, "lease_id": lease_id}
            try:
                yield
            finally:
                locks.pop(key, None)
                self._release_cross_instance_lease(lease_path, lease_id)
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
                record_body["engine_hash"],
                record_body["evaluator"]["spec_version"],
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
                    pb["engine_hash"],
                    pb["evaluator"]["spec_version"],
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
            # seq_NNNNNN.record.json commits BEFORE detached .ots/.receipt so valid detached evidence without .record.json unambiguously signals a missing committed record.
            if rec_path.exists():
                raise LedgerAppendError(f"target_sequence_already_exists: {seq}")
            has_ev, ev_reason = has_committed_detached_evidence(self, seq)
            if has_ev:
                raise LedgerAppendError(f"committed_evidence_exists_without_record:{ev_reason}")

            post_marker_writes: list[tuple[Path, bytes]] = []
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
                    post_marker_writes.append((ots_path, ots_bytes))
                post_marker_writes.append((rcp_path, canonical_json_bytes(receipt_obj)))

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
                if before_record_commit is not None:
                    before_record_commit()
                # Commit seq_NNNNNN.record.json BEFORE detached .ots/.receipt; rollback_sequence / except handler cleans up on Python exception.
                _atomic_write_bytes(rec_path, canonical_json_bytes(envelope))
                created_paths.append(rec_path)
                for target_path, data in post_marker_writes:
                    _atomic_write_bytes(target_path, data)
                    created_paths.append(target_path)
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

            try:
                raw_bytes = path.read_bytes()
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
            except Exception as exc:
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
        runtime_config: object | None = None,
    ) -> tuple[bool, list[object]]:
        """Require valid chain + non-empty ledger with timely OTS receipts verified against trusted Bitcoin headers & block timestamps."""
        eff_commitment_deadline_et = commitment_deadline_et
        if eff_commitment_deadline_et is None and runtime_config is not None:
            eff_commitment_deadline_et = (
                runtime_config.get("commitment_deadline_et")
                if isinstance(runtime_config, Mapping)
                else getattr(runtime_config, "commitment_deadline_et", None)
            )
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
                    eff_commitment_deadline_et,
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


def save_row_state_snapshot(
    snapshots_dir: str | Path,
    *,
    chain_id: str,
    sequence: int,
    session_date: str,
    record_sha256: str,
    engine_hash: str,
    spec_version: str,
    next_row_state: dict,
) -> Path:
    """Persist next_row_state atomically for restart continuity with identity pins."""
    if not isinstance(next_row_state, dict):
        raise EngineIdentityError("next_row_state must be a dict")
    s_dir = Path(snapshots_dir)
    s_dir.mkdir(parents=True, exist_ok=True)
    state_canonical = json.dumps(
        next_row_state,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    payload = {
        "schema_version": "c-sh-state-snapshot-v1",
        "chain_id": chain_id,
        "sequence": sequence,
        "session_date": session_date,
        "record_sha256": record_sha256,
        "engine_hash": engine_hash,
        "spec_version": spec_version,
        "state_sha256": hashlib.sha256(state_canonical).hexdigest(),
        "next_row_state": next_row_state,
    }
    raw_bytes = (
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n"
    ).encode("utf-8")
    seq_path = s_dir / f"seq_{sequence:06d}.state.json"
    latest_path = s_dir / "latest.state.json"
    _atomic_write_bytes(seq_path, raw_bytes)
    _atomic_write_bytes(latest_path, raw_bytes)
    return seq_path


def load_row_state_snapshot(
    snapshots_dir: str | Path,
    *,
    expected_chain_id: str,
    expected_engine_hash: str,
    expected_spec_version: str,
    expected_sequence: int | None = None,
    expected_record_sha256: str | None = None,
    expected_state_sha256: str | None = None,
    allow_missing_at_genesis: bool = True,
) -> tuple[dict, dict | None]:
    """Load and verify persisted row state before next evaluation (fail closed on drift/corruption)."""
    s_dir = Path(snapshots_dir)
    target = (
        s_dir / f"seq_{expected_sequence:06d}.state.json"
        if expected_sequence is not None
        else s_dir / "latest.state.json"
    )
    if not target.is_file():
        if allow_missing_at_genesis and expected_sequence is None and not (s_dir / "latest.state.json").exists():
            return {}, None
        raise EngineIdentityError(f"Missing persisted row state snapshot: {target}")

    try:
        payload = parse_json_no_duplicates(target.read_text(encoding="utf-8"))
    except Exception as exc:
        raise EngineIdentityError(f"Corrupt row state snapshot {target}: {exc}") from exc

    if not isinstance(payload, dict) or payload.get("schema_version") != "c-sh-state-snapshot-v1":
        raise EngineIdentityError(f"Invalid row state snapshot schema in {target}")
    if payload.get("chain_id") != expected_chain_id:
        raise EngineIdentityError(
            f"State snapshot chain_id mismatch: expected {expected_chain_id!r}, got {payload.get('chain_id')!r}"
        )
    if payload.get("engine_hash") != expected_engine_hash:
        raise EngineIdentityError(
            f"State snapshot engine_hash mismatch: expected {expected_engine_hash!r}, got {payload.get('engine_hash')!r}"
        )
    if payload.get("spec_version") != expected_spec_version:
        raise EngineIdentityError(
            f"State snapshot spec_version mismatch: expected {expected_spec_version!r}, got {payload.get('spec_version')!r}"
        )
    if expected_sequence is not None and payload.get("sequence") != expected_sequence:
        raise EngineIdentityError(
            f"State snapshot sequence mismatch: expected {expected_sequence}, got {payload.get('sequence')!r}"
        )
    if expected_record_sha256 is not None and payload.get("record_sha256") != expected_record_sha256:
        raise EngineIdentityError("State snapshot record_sha256 mismatch")

    state_obj = payload.get("next_row_state")
    if not isinstance(state_obj, dict):
        raise EngineIdentityError("State snapshot next_row_state must be a dict")
    recomputed_sha = _sha256_canonical_state_dict(state_obj)
    if payload.get("state_sha256") != recomputed_sha:
        raise EngineIdentityError("State snapshot checksum mismatch")
    if expected_state_sha256 is not None and (
        recomputed_sha != expected_state_sha256 or payload.get("state_sha256") != expected_state_sha256
    ):
        raise EngineIdentityError(
            f"State snapshot state_sha256 mismatch against committed ledger record: "
            f"expected {expected_state_sha256}, got {recomputed_sha}"
        )

    return state_obj, payload


def save_pending_rejected_state_snapshot(
    snapshots_dir: str | Path,
    *,
    chain_id: str,
    base_sequence: int,
    base_record_sha256: str,
    rejected_session_date: str,
    engine_hash: str,
    spec_version: str,
    next_row_state: dict,
    base_prior_row_state: dict | None = None,
) -> Path:
    """Persist next_row_state (plus session-start baseline) after an O1/O2-rejected evaluation."""
    if not isinstance(next_row_state, dict):
        raise EngineIdentityError("pending rejected next_row_state must be a dict")
    eff_base_prior = base_prior_row_state if base_prior_row_state is not None else {}
    if not isinstance(eff_base_prior, dict):
        raise EngineIdentityError("pending rejected base_prior_row_state must be a dict")
    s_dir = Path(snapshots_dir)
    s_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "c-sh-pending-rejected-state-v1",
        "chain_id": chain_id,
        "base_sequence": base_sequence,
        "base_record_sha256": base_record_sha256,
        "rejected_session_date": rejected_session_date,
        "engine_hash": engine_hash,
        "spec_version": spec_version,
        "state_sha256": _sha256_canonical_state_dict(next_row_state),
        "next_row_state": next_row_state,
        "base_prior_state_sha256": _sha256_canonical_state_dict(eff_base_prior),
        "base_prior_row_state": eff_base_prior,
    }
    raw_bytes = (
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n"
    ).encode("utf-8")
    target = s_dir / "pending_rejected_state.json"
    _atomic_write_bytes(target, raw_bytes)
    return target


def compute_pending_rejected_state_sha256(payload: Mapping[str, Any]) -> str:
    """Return canonical SHA-256 over a pending rejected state snapshot payload (allows FUSE floats)."""
    return _sha256_canonical_state_dict(payload)


def _max_elapsed_session_steps(
    ref_dates: tuple[object, ...],
    rej_ds: str,
    *,
    default_when_unknown: int = 1,
) -> int:
    """Return max calendar-day steps from the latest valid reference date to rej_ds."""
    rej_date = date.fromisoformat(rej_ds)
    parsed_refs: list[date] = []
    for raw in ref_dates:
        if isinstance(raw, str) and _DATE_YYYY_MM_DD_RE.fullmatch(raw):
            try:
                parsed_refs.append(date.fromisoformat(raw))
            except ValueError:
                continue
    if not parsed_refs:
        return default_when_unknown
    return max(0, (rej_date - max(parsed_refs)).days)


def _assert_pending_state_preserves_committed_breakers(
    committed_state: Mapping[str, Any],
    pending_state: Mapping[str, Any],
    *,
    rej_ds: str,
    expected_engine_hash: str,
    expected_spec_version: str,
    expected_base_session_date: str | None = None,
) -> None:
    """Enforce fail-closed monotonic latch/severity preservation from committed ledger state to pending state."""
    if "engine_hash" in pending_state and pending_state["engine_hash"] != expected_engine_hash:
        raise EngineIdentityError("Pending next_row_state engine_hash mismatch")
    if "spec_version" in pending_state and pending_state["spec_version"] != expected_spec_version:
        raise EngineIdentityError("Pending next_row_state spec_version mismatch")
    if "loss_breakers_armed" in pending_state and pending_state["loss_breakers_armed"] is not False:
        raise EngineIdentityError("Pending next_row_state loss_breakers_armed violation")
    if (
        "enforcement_mode" in pending_state
        and pending_state["enforcement_mode"] != "shadow_would_have_tripped"
    ):
        raise EngineIdentityError("Pending next_row_state enforcement_mode violation")

    for row_id in ("L2", "L3", "L4", "L6", "O3", "O6", "O7", "O8", "O9"):
        c_entry = committed_state.get(row_id)
        if isinstance(c_entry, Mapping) and c_entry.get("latched") is True:
            p_entry = pending_state.get(row_id)
            if not isinstance(p_entry, Mapping) or p_entry.get("latched") is not True:
                raise EngineIdentityError(
                    f"Pending state downgraded or cleared committed {row_id} latch"
                )
            if (
                c_entry.get("trip_session") is not None
                and p_entry.get("trip_session") != c_entry.get("trip_session")
            ):
                raise EngineIdentityError(
                    f"Pending state altered committed {row_id} trip_session"
                )
            if (
                row_id == "L3"
                and c_entry.get("cooling_off_until") is not None
                and p_entry.get("cooling_off_until") != c_entry.get("cooling_off_until")
            ):
                raise EngineIdentityError("Pending state altered committed L3 cooling_off_until")

    c_o5 = committed_state.get("O5")
    if isinstance(c_o5, Mapping):
        c_cnt = c_o5.get("rejection_count", 0)
        c_latched = c_o5.get("latched") is True
        c_last_sess = c_o5.get("last_rejection_session")
        p_o5 = pending_state.get("O5")
        if c_latched:
            if not isinstance(p_o5, Mapping) or p_o5.get("latched") is not True:
                raise EngineIdentityError("Pending state cleared committed O5 latch")
        same_o5_session = (
            c_last_sess is None
            or c_last_sess == rej_ds
            or (isinstance(p_o5, Mapping) and p_o5.get("last_rejection_session") == c_last_sess)
        )
        if (
            (c_latched or same_o5_session)
            and isinstance(c_cnt, int)
            and not isinstance(c_cnt, bool)
            and c_cnt > 0
        ):
            p_cnt = p_o5.get("rejection_count", 0) if isinstance(p_o5, Mapping) else -1
            if isinstance(p_cnt, bool) or not isinstance(p_cnt, int) or p_cnt < c_cnt:
                raise EngineIdentityError("Pending state reduced committed O5 rejection_count")
        elif isinstance(p_o5, Mapping) and "rejection_count" in p_o5:
            p_cnt = p_o5.get("rejection_count")
            if isinstance(p_cnt, bool) or not isinstance(p_cnt, int) or p_cnt < 0:
                raise EngineIdentityError("Pending state invalid O5 rejection_count")

    c_l5 = committed_state.get("L5")
    if isinstance(c_l5, Mapping):
        c_l5_fresh = c_l5.get("fresh_session_count", 0)
        c_l5_fresh_int = (
            c_l5_fresh
            if isinstance(c_l5_fresh, int) and not isinstance(c_l5_fresh, bool)
            else 0
        )
        p_l5 = pending_state.get("L5")
        if (c_l5.get("active") is True or c_l5.get("latched") is True) and (
            c_l5.get("trip_session") == rej_ds
            or (c_l5.get("trip_session") is None and c_l5_fresh_int < 1)
        ):
            if not isinstance(p_l5, Mapping) or not (p_l5.get("active") or p_l5.get("latched")):
                raise EngineIdentityError("Pending state cleared active L5 session interlock")
        if isinstance(p_l5, Mapping):
            p_l5_fresh = p_l5.get("fresh_session_count", 0)
            if isinstance(p_l5_fresh, bool) or not isinstance(p_l5_fresh, int) or p_l5_fresh < 0:
                raise EngineIdentityError("Pending state invalid L5 fresh_session_count")
            l5_steps = _max_elapsed_session_steps(
                (c_l5.get("last_session"), c_l5.get("trip_session"), expected_base_session_date),
                rej_ds,
                default_when_unknown=1,
            )
            if (c_l5.get("active") is True or c_l5.get("latched") is True) and p_l5_fresh > c_l5_fresh_int + l5_steps:
                raise EngineIdentityError("Pending state forged L5 fresh_session_count advancement")

    c_o1 = committed_state.get("O1")
    c_o1_fresh = (
        c_o1.get("fresh_session_count", 0)
        if isinstance(c_o1, Mapping)
        and isinstance(c_o1.get("fresh_session_count", 0), int)
        and not isinstance(c_o1.get("fresh_session_count", 0), bool)
        else 0
    )
    p_o1 = pending_state.get("O1")
    if isinstance(p_o1, Mapping):
        p_o1_fresh = p_o1.get("fresh_session_count", 0)
        if isinstance(p_o1_fresh, bool) or not isinstance(p_o1_fresh, int) or p_o1_fresh < 0:
            raise EngineIdentityError("Pending state invalid O1 fresh_session_count")
        if isinstance(c_o1, Mapping) and c_o1.get("last_tripped_session") == rej_ds:
            if p_o1.get("last_tripped_session") != rej_ds or p_o1_fresh != 0:
                raise EngineIdentityError("Pending state cleared same-session O1 trip")
        if p_o1.get("last_tripped_session") == rej_ds and p_o1_fresh != 0:
            raise EngineIdentityError("Pending state inconsistent same-session O1 fresh_session_count")
        if isinstance(c_o1, Mapping) or expected_base_session_date is not None:
            o1_steps = _max_elapsed_session_steps(
                (
                    c_o1.get("last_session") if isinstance(c_o1, Mapping) else None,
                    c_o1.get("last_tripped_session") if isinstance(c_o1, Mapping) else None,
                    expected_base_session_date,
                ),
                rej_ds,
                default_when_unknown=1,
            )
            if p_o1_fresh > c_o1_fresh + o1_steps:
                raise EngineIdentityError("Pending state forged O1 fresh_session_count advancement")

    for row_id in ("V1", "V3"):
        c_map = committed_state.get(row_id)
        if isinstance(c_map, Mapping):
            p_map = pending_state.get(row_id)
            for fund, cf in c_map.items():
                if isinstance(cf, Mapping) and cf.get("latched") is True:
                    pf = p_map.get(fund) if isinstance(p_map, Mapping) else None
                    if not isinstance(pf, Mapping) or pf.get("latched") is not True:
                        raise EngineIdentityError(
                            f"Pending state cleared committed {row_id}.{fund} latch"
                        )
                    if (
                        cf.get("trip_session") is not None
                        and pf.get("trip_session") != cf.get("trip_session")
                    ):
                        raise EngineIdentityError(
                            f"Pending state altered committed {row_id}.{fund} trip_session"
                        )

    c_v2 = committed_state.get("V2")
    if isinstance(c_v2, Mapping):
        p_v2 = pending_state.get("V2")
        if p_v2 is not None and not isinstance(p_v2, Mapping):
            raise EngineIdentityError("Pending state invalid V2 section")
        c_latched = c_v2.get("latched_mismatches")
        if isinstance(c_latched, Mapping):
            p_latched = p_v2.get("latched_mismatches") if isinstance(p_v2, Mapping) else None
            for fund, val in c_latched.items():
                if val and (not isinstance(p_latched, Mapping) or not p_latched.get(fund)):
                    raise EngineIdentityError(
                        f"Pending state cleared committed V2.latched_mismatches.{fund}"
                    )
        c_pending = c_v2.get("pending_adjustments")
        if isinstance(c_pending, Mapping):
            p_pending = p_v2.get("pending_adjustments") if isinstance(p_v2, Mapping) else None
            latched_map = c_latched if isinstance(c_latched, Mapping) else {}
            for fund, val in c_pending.items():
                if val and latched_map.get(fund) and (
                    not isinstance(p_pending, Mapping) or not p_pending.get(fund)
                ):
                    raise EngineIdentityError(
                        f"Pending state cleared committed V2.pending_adjustments.{fund}"
                    )

    c_v4 = committed_state.get("V4")
    if isinstance(c_v4, Mapping):
        p_v4 = pending_state.get("V4")
        for fund, cf in c_v4.items():
            if isinstance(cf, Mapping) and cf.get("clearing_counter") is not None:
                c_cnt = cf["clearing_counter"]
                c_last = cf.get("last_session_below_threshold")
                if not isinstance(c_cnt, int) or isinstance(c_cnt, bool):
                    raise EngineIdentityError(f"Invalid committed V4.{fund} clearing_counter")
                max_steps = _max_elapsed_session_steps(
                    (c_last, expected_base_session_date),
                    rej_ds,
                    default_when_unknown=1,
                )
                if not isinstance(p_v4, Mapping):
                    raise EngineIdentityError(
                        f"Pending state cleared active V4.{fund} clearing state"
                    )
                pf = p_v4.get(fund)
                if pf is not None and not isinstance(pf, Mapping):
                    raise EngineIdentityError(f"Invalid pending V4.{fund} clearing state")
                p_cnt = pf.get("clearing_counter") if isinstance(pf, Mapping) else None
                if pf is None or p_cnt is None:
                    # FUSE-1 clear_v4_state removes V4[fund] only once clearing reaches 10 calm sessions
                    if c_cnt + max_steps < 10:
                        raise EngineIdentityError(
                            f"Pending state cleared active V4.{fund} clearing state"
                        )
                    continue
                if isinstance(p_cnt, bool) or not isinstance(p_cnt, int):
                    raise EngineIdentityError(
                        f"Pending state cleared active V4.{fund} clearing state"
                    )
                max_allowed = c_cnt + max_steps
                if p_cnt < 0 or p_cnt >= 10 or p_cnt > max_allowed:
                    raise EngineIdentityError(
                        f"Pending state forged V4.{fund} clearing_counter jump ({c_cnt} -> {p_cnt})"
                    )
                if p_cnt > c_cnt:
                    p_last = pf.get("last_session_below_threshold") if isinstance(pf, Mapping) else None
                    if (
                        not isinstance(p_last, str)
                        or not _DATE_YYYY_MM_DD_RE.fullmatch(p_last)
                        or p_last > rej_ds
                    ):
                        raise EngineIdentityError(
                            f"Pending state V4.{fund} clearing_counter incremented without matching session"
                        )
                    try:
                        date.fromisoformat(p_last)
                    except ValueError as exc:
                        raise EngineIdentityError(
                            f"Pending state V4.{fund} clearing_counter invalid session date"
                        ) from exc
                    if (
                        isinstance(c_last, str)
                        and _DATE_YYYY_MM_DD_RE.fullmatch(c_last)
                        and p_last <= c_last
                    ) or (
                        expected_base_session_date is not None
                        and p_last <= expected_base_session_date
                    ):
                        raise EngineIdentityError(
                            f"Pending state V4.{fund} clearing_counter incremented without newer session"
                        )
                    steps_to_p_last = _max_elapsed_session_steps(
                        (c_last, expected_base_session_date),
                        p_last,
                        default_when_unknown=1,
                    )
                    if p_cnt > c_cnt + steps_to_p_last:
                        raise EngineIdentityError(
                            f"Pending state forged V4.{fund} clearing_counter jump ({c_cnt} -> {p_cnt})"
                        )

    c_v5 = committed_state.get("V5")
    if isinstance(c_v5, Mapping):
        p_v5 = pending_state.get("V5")
        for fund, cf in c_v5.items():
            if not isinstance(cf, Mapping):
                continue
            c_esc = bool(cf.get("escalated"))
            c_cnt = cf.get("consecutive_beyond_tolerance", 0)
            c_cnt_int = c_cnt if isinstance(c_cnt, int) and not isinstance(c_cnt, bool) else 0
            c_last = cf.get("last_session")
            pf = p_v5.get(fund) if isinstance(p_v5, Mapping) else None
            if c_esc or c_cnt_int >= 40:
                p_cnt_val = pf.get("consecutive_beyond_tolerance", 0) if isinstance(pf, Mapping) else -1
                if (
                    not isinstance(pf, Mapping)
                    or pf.get("escalated") is not True
                    or isinstance(p_cnt_val, bool)
                    or not isinstance(p_cnt_val, int)
                    or p_cnt_val < c_cnt_int
                ):
                    raise EngineIdentityError(f"Pending state downgraded escalated V5.{fund}")
            if isinstance(pf, Mapping):
                p_cnt = pf.get("consecutive_beyond_tolerance", 0)
                v5_steps = _max_elapsed_session_steps(
                    (c_last, expected_base_session_date),
                    rej_ds,
                    default_when_unknown=1,
                )
                max_allowed = c_cnt_int + v5_steps
                if (
                    isinstance(p_cnt, bool)
                    or not isinstance(p_cnt, int)
                    or p_cnt < 0
                    or p_cnt > max_allowed
                ):
                    raise EngineIdentityError(f"Pending state forged V5.{fund} counter")

    c_o4 = committed_state.get("O4")
    if isinstance(c_o4, Mapping):
        p_o4 = pending_state.get("O4")
        for sym, cs in c_o4.items():
            if isinstance(cs, Mapping) and "original_order_id" in cs:
                ps = p_o4.get(sym) if isinstance(p_o4, Mapping) else None
                if not isinstance(ps, Mapping) or ps.get("original_order_id") != cs["original_order_id"]:
                    raise EngineIdentityError(f"Pending state altered committed O4.{sym}")


def authenticate_pending_rejected_state_snapshot(
    snapshots_dir: str | Path,
    *,
    expected_pending_state_sha256: str | None = None,
    require_external_pending_auth: bool = False,
) -> dict | None:
    """Validate optional/required external digest authentication for pending_rejected_state.json."""
    if expected_pending_state_sha256 is not None:
        if not isinstance(expected_pending_state_sha256, str) or not _HEX64_RE.fullmatch(
            expected_pending_state_sha256
        ):
            raise EngineIdentityError(
                f"invalid_expected_pending_state_sha256:{expected_pending_state_sha256!r}"
            )
    if require_external_pending_auth and expected_pending_state_sha256 is None:
        raise EngineIdentityError(
            "production_pending_rejected_state_unauthenticated: require expected_pending_state_sha256 (or NO_PENDING_REJECTED_STATE_SHA256)"
        )
    s_dir = Path(snapshots_dir)
    target = s_dir / "pending_rejected_state.json"
    if not target.is_file():
        if expected_pending_state_sha256 is not None and not hmac.compare_digest(
            expected_pending_state_sha256,
            NO_PENDING_REJECTED_STATE_SHA256,
        ):
            raise EngineIdentityError("production_pending_rejected_state_missing")
        return None
    if expected_pending_state_sha256 is not None and hmac.compare_digest(
        expected_pending_state_sha256,
        NO_PENDING_REJECTED_STATE_SHA256,
    ):
        raise EngineIdentityError(
            "production_pending_rejected_state_unexpected: pending_rejected_state.json exists but NO_PENDING_REJECTED_STATE_SHA256 was provided"
        )
    try:
        payload = parse_json_no_duplicates(target.read_text(encoding="utf-8"))
    except Exception as exc:
        raise EngineIdentityError(f"Corrupt pending rejected state snapshot {target}: {exc}") from exc

    if not isinstance(payload, dict) or payload.get("schema_version") != "c-sh-pending-rejected-state-v1":
        raise EngineIdentityError(f"Invalid pending rejected state snapshot schema in {target}")
    if expected_pending_state_sha256 is not None and not hmac.compare_digest(
        compute_pending_rejected_state_sha256(payload),
        expected_pending_state_sha256,
    ):
        raise EngineIdentityError("production_pending_rejected_state_mismatch")
    return payload


def load_pending_rejected_state_snapshot(
    snapshots_dir: str | Path,
    *,
    expected_chain_id: str,
    expected_base_sequence: int,
    expected_base_record_sha256: str,
    current_session_date: str,
    expected_engine_hash: str,
    expected_spec_version: str,
    expected_base_prior_state_sha256: str | None = None,
    committed_base_prior_row_state: Mapping[str, Any] | None = None,
    expected_base_session_date: str | None = None,
    expected_pending_state_sha256: str | None = None,
    require_external_pending_auth: bool = False,
) -> tuple[dict, dict] | None:
    """Load and verify pending O1/O2 rejected-day row state if present (fail closed on tamper/drift)."""
    payload = authenticate_pending_rejected_state_snapshot(
        snapshots_dir,
        expected_pending_state_sha256=expected_pending_state_sha256,
        require_external_pending_auth=require_external_pending_auth,
    )
    if payload is None:
        return None
    if payload.get("chain_id") != expected_chain_id:
        raise EngineIdentityError(
            f"Pending state chain_id mismatch: expected {expected_chain_id!r}, got {payload.get('chain_id')!r}"
        )
    if payload.get("base_sequence") != expected_base_sequence:
        raise EngineIdentityError(
            f"Pending state base_sequence mismatch: expected {expected_base_sequence}, got {payload.get('base_sequence')!r}"
        )
    if payload.get("base_record_sha256") != expected_base_record_sha256:
        raise EngineIdentityError("Pending state base_record_sha256 mismatch")
    if payload.get("engine_hash") != expected_engine_hash:
        raise EngineIdentityError("Pending state engine_hash mismatch")
    if payload.get("spec_version") != expected_spec_version:
        raise EngineIdentityError("Pending state spec_version mismatch")
    rej_ds = payload.get("rejected_session_date")
    if (
        not isinstance(rej_ds, str)
        or not _DATE_YYYY_MM_DD_RE.fullmatch(rej_ds)
        or rej_ds > current_session_date
    ):
        raise EngineIdentityError(
            f"Pending state rejected_session_date invalid or after {current_session_date}: {rej_ds!r}"
        )
    try:
        date.fromisoformat(rej_ds)
    except ValueError as exc:
        raise EngineIdentityError(f"Pending state rejected_session_date invalid: {rej_ds!r}") from exc

    state_obj = payload.get("next_row_state")
    if not isinstance(state_obj, dict):
        raise EngineIdentityError("Pending state next_row_state must be a dict")
    if payload.get("state_sha256") != _sha256_canonical_state_dict(state_obj):
        raise EngineIdentityError("Pending state checksum mismatch")

    base_obj: dict | None = None
    if "base_prior_row_state" in payload:
        raw_base = payload.get("base_prior_row_state")
        if not isinstance(raw_base, dict):
            raise EngineIdentityError("Pending state base_prior_row_state must be a dict")
        base_sha = _sha256_canonical_state_dict(raw_base)
        if (
            "base_prior_state_sha256" in payload
            and payload.get("base_prior_state_sha256") != base_sha
        ):
            raise EngineIdentityError("Pending state base_prior_state checksum mismatch")
        if expected_base_prior_state_sha256 is not None and base_sha != expected_base_prior_state_sha256:
            raise EngineIdentityError(
                "Pending state base_prior_row_state does not match committed ledger state digest"
            )
        base_obj = raw_base
    elif expected_base_prior_state_sha256 is not None:
        raise EngineIdentityError("Pending state missing ledger-bound base_prior_row_state")

    eff_committed: Mapping[str, Any] | None = committed_base_prior_row_state
    if eff_committed is not None:
        if (
            expected_base_prior_state_sha256 is not None
            and _sha256_canonical_state_dict(eff_committed) != expected_base_prior_state_sha256
        ):
            raise EngineIdentityError(
                "Committed base row state digest mismatch while verifying pending rejected state"
            )
    elif expected_base_prior_state_sha256 is not None and base_obj is not None:
        eff_committed = base_obj

    if eff_committed is not None:
        _assert_pending_state_preserves_committed_breakers(
            eff_committed,
            state_obj,
            rej_ds=rej_ds,
            expected_engine_hash=expected_engine_hash,
            expected_spec_version=expected_spec_version,
            expected_base_session_date=expected_base_session_date,
        )

    return state_obj, payload
