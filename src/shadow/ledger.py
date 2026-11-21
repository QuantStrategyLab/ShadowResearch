"""C-SH-LEDGER canonicalization (restricted RFC 8785), hash chain, and detached receipts."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path

from shadow.evaluator import ALL_BREAKER_IDS, StubEvaluator
from shadow.sizing import (
    INSTRUMENT_ORDER,
    TIER_ORDER,
    canonical_decimal_str,
    compute_session_sizing,
    policy_config_hash,
)

GENESIS_PREV_HASH = "0" * 64
SCHEMA_VERSION = "c-sh-ledger-v1"

_DECIMAL_RE = re.compile(r"^(0|-?(0\.[0-9]*[1-9]|[1-9][0-9]*(\.[0-9]*[1-9])?))$")
_UTC_MS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")

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


def compute_code_manifest_hash(repo_root: str | Path | None = None) -> str:
    """Compute a deterministic content digest over src/shadow modules and csa22-v1 spec."""
    root = Path(repo_root) if repo_root is not None else Path(__file__).resolve().parents[2]
    targets = [
        root / "src" / "shadow" / "calendar_consumer.py",
        root / "src" / "shadow" / "sizing.py",
        root / "src" / "shadow" / "evaluator.py",
        root / "src" / "shadow" / "ledger.py",
        root / "specs" / "csa22-v1" / "manifest.json",
    ]
    h = hashlib.sha256()
    for path in targets:
        if path.is_file():
            h.update(path.name.encode("utf-8"))
            h.update(path.read_bytes())
    return h.hexdigest()


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
    if not isinstance(body["sequence"], int) or isinstance(body["sequence"], bool) or body["sequence"] < 0:
        raise CanonicalizationError(f"Invalid sequence: {body['sequence']!r}")
    if not isinstance(body["prev_record_sha256"], str) or not _HEX64_RE.fullmatch(body["prev_record_sha256"]):
        raise CanonicalizationError("prev_record_sha256 must be 64 lowercase hex chars")
    if not is_utc_ms_timestamp(body["decision_timestamp_utc"]):
        raise CanonicalizationError(
            f"decision_timestamp_utc must be UTC ms ISO-8601: {body['decision_timestamp_utc']!r}"
        )

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
        if tier.get("orders_placed") != 0:
            raise CanonicalizationError("orders_placed must be 0 in Stage N")
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

    ev = body["evaluator"]
    if not isinstance(ev, dict):
        raise CanonicalizationError("evaluator must be a dict")
    breakers = ev.get("breakers")
    if not isinstance(breakers, list) or [b.get("id") for b in breakers] != list(ALL_BREAKER_IDS):
        raise CanonicalizationError("evaluator.breakers must contain all 24 named IDs in sorted order")

    assertions = body["assertions"]
    if not isinstance(assertions, dict):
        raise CanonicalizationError("assertions must be a dict")
    if assertions.get("kelly_applied") is not False or assertions.get("orders_placed") != 0:
        raise CanonicalizationError("Standing assertions kelly_applied=false and orders_placed=0 violated")

    # Verify zero floats anywhere via canonicalization
    canonical_json_bytes(body)


def create_synthetic_receipt(
    record_envelope: dict,
    obtained_at_utc: str,
) -> dict:
    """Create a detached synthetic-stub timestamp receipt for an appended record."""
    if not is_utc_ms_timestamp(obtained_at_utc):
        raise CanonicalizationError(f"Invalid obtained_at_utc: {obtained_at_utc!r}")
    body = record_envelope["record_body"]
    return {
        "anchor": "synthetic-stub",
        "chain_id": body["chain_id"],
        "obtained_at_utc": obtained_at_utc,
        "record_sha256": record_envelope["record_sha256"],
        "sequence": body["sequence"],
        "session_date": body["session_date"],
    }


def verify_receipt(record_envelope: dict, receipt: dict) -> tuple[bool, str | None]:
    """Verify a detached timestamp receipt separately from the record byte hash."""
    if not isinstance(receipt, dict):
        return False, "receipt_not_dict"
    body = record_envelope.get("record_body", {})
    if receipt.get("anchor") != "synthetic-stub":
        return False, f"unexpected_anchor:{receipt.get('anchor')!r}"
    if receipt.get("record_sha256") != record_envelope.get("record_sha256"):
        return False, "receipt_record_hash_mismatch"
    if receipt.get("chain_id") != body.get("chain_id"):
        return False, "receipt_chain_id_mismatch"
    if receipt.get("sequence") != body.get("sequence"):
        return False, "receipt_sequence_mismatch"
    if receipt.get("session_date") != body.get("session_date"):
        return False, "receipt_session_date_mismatch"
    if not is_utc_ms_timestamp(receipt.get("obtained_at_utc")):
        return False, "receipt_invalid_obtained_at_utc"
    return True, None


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
) -> dict:
    """Run evaluator + sizing and assemble a canonical record_body (or O1 fail-closed dict)."""
    ev = evaluator if evaluator is not None else StubEvaluator()
    p_hash = policy_config_hash()
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

    if rows_by_id["O1"]["would_have_tripped"] and eff_scope["aggregate"] == "NO-NEW-RISK":
        return {
            "appended": False,
            "fail_closed": True,
            "o1_output": rows_by_id["O1"],
            "effective_state_by_scope": eff_scope,
            "next_row_state": next_state,
        }

    sizing_res = compute_session_sizing(
        session_inputs,
        active_control_state=eff_scope["aggregate"],
    )

    canonical_inputs_snapshot = {
        "E": canonical_decimal_str(session_inputs["E"]),
        "E_star": canonical_decimal_str(session_inputs["E_star"]),
        "H": canonical_decimal_str(session_inputs.get("H", "0")),
        "L": canonical_decimal_str(session_inputs["L"]),
        "funds": {
            inst: {
                "close_price": canonical_decimal_str(session_inputs["funds"][inst]["close_price"]),
                "q_pre": session_inputs["funds"][inst]["q_pre"],
                "s": canonical_decimal_str(session_inputs["funds"][inst]["s"]),
                "sigma_forecast": canonical_decimal_str(session_inputs["funds"][inst]["sigma_forecast"]),
            }
            for inst in INSTRUMENT_ORDER
        },
        "market_records_digest": market_records_digest,
        "session_date": session_date,
    }
    inputs_digest = sha256_canonical(canonical_inputs_snapshot)

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
        "bankroll": sizing_res["bankroll"],
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


def check_forward_clock_credit(
    records: list[dict],
    receipts: list[dict] | None = None,
) -> tuple[bool, list[str]]:
    """Determine whether a ledger record sequence is eligible for CLK-01..04 forward credit."""
    reasons: list[str] = []
    if not records:
        reasons.append("empty_chain")
        return False, reasons

    for idx, env in enumerate(records):
        body = env.get("record_body", env)
        seq = body.get("sequence", idx)
        if body.get("mode") != "production":
            reasons.append(f"seq_{seq}:non_production_mode:{body.get('mode')}")
        if body.get("observation_class") != "decision_time_market":
            reasons.append(f"seq_{seq}:non_decision_time_observation_class:{body.get('observation_class')}")
        if str(body.get("engine_hash", "")).startswith("stub-"):
            reasons.append(f"seq_{seq}:stub_engine_hash")
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

    if receipts is not None:
        for idx, rec in enumerate(receipts):
            if rec.get("anchor") == "synthetic-stub":
                reasons.append(f"receipt_{idx}:synthetic_stub_anchor")

    return len(reasons) == 0, reasons


class ShadowLedger:
    """Append-only file-backed SHA-256 hash-chained ledger with detached receipts."""

    def __init__(self, root_dir: str | Path) -> None:
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)

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
    ) -> dict:
        """Validate and append one record plus its detached receipt."""
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

        dedup_key = (
            record_body["mode"],
            record_body["policy_id"],
            record_body["policy_config_hash"],
            record_body["session_date"],
        )
        for prior in existing_records:
            pb = prior["record_body"]
            prior_key = (pb["mode"], pb["policy_id"], pb["policy_config_hash"], pb["session_date"])
            if prior_key == dedup_key:
                raise LedgerAppendError(
                    f"duplicate_session_append: {dedup_key}"
                )
            if pb["chain_id"] != record_body["chain_id"]:
                raise LedgerAppendError(
                    f"chain_id_mismatch: expected {pb['chain_id']}, got {record_body['chain_id']}"
                )

        record_sha256 = sha256_canonical(record_body)
        envelope = {
            "record_body": record_body,
            "record_sha256": record_sha256,
        }

        if receipt is None:
            obt = obtained_at_utc or record_body["decision_timestamp_utc"]
            receipt_obj = create_synthetic_receipt(envelope, obt)
        else:
            receipt_obj = receipt

        ok_rec, err_rec = verify_receipt(envelope, receipt_obj)
        if not ok_rec:
            raise LedgerAppendError(f"invalid_receipt: {err_rec}")

        rec_path = self._record_path(seq)
        rcp_path = self._receipt_path(seq)
        if rec_path.exists() or rcp_path.exists():
            raise LedgerAppendError(f"target_sequence_already_exists: {seq}")

        rec_path.write_bytes(canonical_json_bytes(envelope))
        rcp_path.write_bytes(canonical_json_bytes(receipt_obj))
        return envelope

    def verify_chain(self) -> tuple[bool, int | None, str | None]:
        """Verify canonical bytes, hashes, genesis, and gapless sequence links."""
        files = self._record_files()
        if not files:
            return True, None, None

        expected_seq = 0
        expected_prev = GENESIS_PREV_HASH

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

            computed_hash = sha256_canonical(body)
            if envelope["record_sha256"] != computed_hash:
                return False, expected_seq, "record_sha256_mismatch"

            expected_prev = computed_hash
            expected_seq += 1

        return True, None, None

    def verify_all_receipts(self) -> tuple[bool, int | None, str | None]:
        """Verify detached timestamp receipts separately from chain hashes."""
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
            ok, err = verify_receipt(envelope, receipt)
            if not ok:
                return False, seq, err
        return True, None, None
