"""C-FUSE-EVAL interface and SYNTHETIC stub adapter (24 named breaker IDs)."""

from __future__ import annotations

import hashlib
import json

from shadow.sizing import INSTRUMENT_ORDER, SizingInputError, compute_session_sizing

BREAKER_DEFINITIONS: dict[str, dict[str, str]] = {
    "L0": {"kind": "limit", "limit": "x_ML(c) every session"},
    "L1": {"kind": "interlock", "limit": "dd_B >= D/3"},
    "L2": {"kind": "breaker", "limit": "dd_B >= 2D/3"},
    "L3": {"kind": "breaker", "limit": "dd_B >= D"},
    "L4": {"kind": "breaker", "limit": "L_1d_B >= D/10"},
    "L5": {"kind": "interlock", "limit": "MWCB Level 1 or 2"},
    "L6": {"kind": "breaker", "limit": "MWCB Level 3 or -10% index day"},
    "V1": {"kind": "breaker", "limit": "tracking diff >0.75pp/1d or >2pp/5d"},
    "V2": {"kind": "breaker", "limit": "corporate action pending/detected"},
    "V3": {"kind": "breaker", "limit": "issuer fund event / delisting"},
    "V4": {"kind": "interlock", "limit": "sigma_forecast >= 2*sigma_long"},
    "V5": {"kind": "diagnostic", "limit": "60d decay monitor"},
    "V6": {"kind": "diagnostic", "limit": "carrying cost monitor"},
    "V7": {"kind": "interlock", "limit": "quote/liquidity/session check"},
    "O1": {"kind": "interlock", "limit": "required input fresh <1 session and present"},
    "O2": {"kind": "interlock", "limit": "calendar trading session & agreement"},
    "O3": {"kind": "breaker", "limit": "broker reconciliation match"},
    "O4": {"kind": "breaker", "limit": "order terminal status timeout"},
    "O5": {"kind": "breaker", "limit": "pre-trade limits / 3 rejects halt"},
    "O6": {"kind": "breaker", "limit": "policy/config hash match"},
    "O7": {"kind": "breaker", "limit": "dead-mans alarm <=1 interval"},
    "O8": {"kind": "owner", "limit": "owner kill switch"},
    "O9": {"kind": "breaker", "limit": "unauthorized tier/H/HWM change"},
    "J1": {"kind": "limit", "limit": "joint exposure <= x_joint"},
}

ALL_BREAKER_IDS: tuple[str, ...] = tuple(sorted(BREAKER_DEFINITIONS.keys()))

_STATE_PRECEDENCE = {
    "NORMAL": 0,
    "NO-NEW-RISK": 1,
    "DE-RISK": 2,
    "ORDER-UNKNOWN": 3,
    "HALT": 4,
}


REQUIRED_MARKET_SERIES: tuple[str, ...] = (
    "TQQQ",
    "SOXL",
    "QQQ",
    "XNDX",
    "SEMI",
    "SOFR",
    "TBILL_3M",
)


class StubProductionForbiddenError(RuntimeError):
    """Raised when the SH-1 stub evaluator is invoked for production forward-clock mode."""


def stub_engine_hash() -> str:
    """Return pinned synthetic stub evaluator hash."""
    payload = json.dumps(
        {"spec_version": "c-fuse-eval-stub-v1", "breakers": BREAKER_DEFINITIONS},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "stub-" + hashlib.sha256(payload).hexdigest()[:16]


class StubEvaluator:
    """Fixture-driven synthetic C-FUSE-EVAL stub with zero ledger handles."""

    production_clock_capable: bool = False

    def __init__(self, spec_version: str = "c-fuse-eval-stub-v1") -> None:
        self.spec_version = spec_version
        self.engine_hash = stub_engine_hash()

    def evaluate(
        self,
        spec_version: str,
        engine_hash: str,
        policy_hash: str,
        session: str,
        as_of: str,
        mode: str,
        validated_inputs: dict,
        prior_row_state: dict,
        authorized_events: dict | list | None = None,
    ) -> tuple[dict[str, dict], dict[str, str], dict]:
        """Evaluate all 24 named breaker rows synchronously with no ledger side effects."""
        if mode == "production":
            raise StubProductionForbiddenError(
                "StubEvaluator is structurally incapable of production forward-clock evaluation; "
                "SH-2 requires the accepted FUSE-1 engine"
            )

        rows_by_id: dict[str, dict] = {}
        for row_id in ALL_BREAKER_IDS:
            defn = BREAKER_DEFINITIONS[row_id]
            prior = prior_row_state.get(row_id, {}) if isinstance(prior_row_state, dict) else {}
            rows_by_id[row_id] = {
                "id": row_id,
                "kind": defn["kind"],
                "limit": defn["limit"],
                "state": prior.get("state", "NORMAL") if prior.get("latched") else "NORMAL",
                "would_have_tripped": bool(prior.get("latched", False)),
                "latched": bool(prior.get("latched", False)),
                "reason": prior.get("reason"),
                "diagnostics": {
                    "mode": mode,
                    "session": session,
                    "as_of": as_of,
                    "synthetic_stub": True,
                    "production_clock_capable": False,
                },
            }

        # Apply synthetic overrides before mandatory O1/O2 fail-closed checks
        overrides = (
            validated_inputs.get("stub_breaker_overrides")
            if isinstance(validated_inputs, dict)
            else None
        )
        if isinstance(overrides, dict):
            for row_id, ov in overrides.items():
                if row_id not in rows_by_id:
                    raise ValueError(f"Unknown breaker ID in override: {row_id}")
                if isinstance(ov, dict):
                    rows_by_id[row_id].update(ov)

        # Mandatory O1 & O2 fail-closed validation (cannot be cleared by stub overrides)
        missing_reasons: list[str] = []
        o2_tripped_reason: str | None = None
        if not isinstance(validated_inputs, dict):
            missing_reasons.append("validated_inputs_not_dict")
        else:
            try:
                compute_session_sizing(validated_inputs)
            except SizingInputError as exc:
                missing_reasons.append(f"missing_required_input:{exc.field}")

            if "calendar" in validated_inputs:
                cal = validated_inputs.get("calendar")
                if not isinstance(cal, dict):
                    o2_tripped_reason = "missing_or_invalid_calendar"
                else:
                    trade_date_mismatch = bool(cal.get("trade_date") and cal.get("trade_date") != session)
                    if (
                        cal.get("blocks_eligibility")
                        or cal.get("blocks_orders")
                        or cal.get("session_state") == "unknown"
                        or not cal.get("is_trading_day")
                        or cal.get("venue_disagreement")
                        or trade_date_mismatch
                    ):
                        o2_tripped_reason = (
                            "calendar_session_date_mismatch"
                            if trade_date_mismatch
                            else (cal.get("reason") or "calendar_closed_or_disagreement")
                        )

            if "market_records" in validated_inputs:
                mkt_recs = validated_inputs.get("market_records")
                req_series = validated_inputs.get("required_series", REQUIRED_MARKET_SERIES)
                cal_obj = validated_inputs.get("calendar") if isinstance(validated_inputs.get("calendar"), dict) else None
                if not isinstance(mkt_recs, list):
                    missing_reasons.append("market_records_not_list")
                else:
                    by_inst = {
                        r.get("instrument"): r
                        for r in mkt_recs
                        if isinstance(r, dict) and r.get("instrument")
                    }
                    for inst in req_series:
                        rec = by_inst.get(inst)
                        if rec is None:
                            missing_reasons.append(f"missing_required_series:{inst}")
                            continue
                        elig = rec.get("eligibility") if isinstance(rec.get("eligibility"), dict) else {}
                        if rec.get("normalized_value") is None or not elig.get("eligible"):
                            reason = elig.get("reason") or "ineligible_or_missing_value"
                            missing_reasons.append(f"market_input_ineligible:{inst}:{reason}")
                            continue
                        if cal_obj is not None and rec.get("calendar_spec_digest") != cal_obj.get("spec_digest"):
                            missing_reasons.append(f"calendar_spec_digest_mismatch:{inst}")

        if o2_tripped_reason is not None:
            if isinstance(overrides, dict) and isinstance(overrides.get("O2"), dict):
                ov_o2 = overrides["O2"]
                if ov_o2.get("state") == "NORMAL" or ov_o2.get("would_have_tripped") is False:
                    raise ValueError("stub_override_cannot_clear_mandatory_breaker:O2")
            rows_by_id["O2"]["state"] = "NO-NEW-RISK"
            rows_by_id["O2"]["would_have_tripped"] = True
            rows_by_id["O2"]["reason"] = o2_tripped_reason

        if missing_reasons:
            if isinstance(overrides, dict) and isinstance(overrides.get("O1"), dict):
                ov_o1 = overrides["O1"]
                if ov_o1.get("state") == "NORMAL" or ov_o1.get("would_have_tripped") is False:
                    raise ValueError("stub_override_cannot_clear_mandatory_breaker:O1")
            rows_by_id["O1"]["state"] = "NO-NEW-RISK"
            rows_by_id["O1"]["would_have_tripped"] = True
            rows_by_id["O1"]["reason"] = "; ".join(missing_reasons)

        agg_state = "NORMAL"
        for row_id in ALL_BREAKER_IDS:
            st = rows_by_id[row_id]["state"]
            if _STATE_PRECEDENCE.get(st, 0) > _STATE_PRECEDENCE.get(agg_state, 0):
                agg_state = st

        effective_state_by_scope = {
            "aggregate": agg_state,
            "conservative": agg_state,
            "balanced": agg_state,
            "aggressive": agg_state,
        }
        for inst in INSTRUMENT_ORDER:
            effective_state_by_scope[inst] = agg_state

        next_row_state = {
            row_id: {
                "state": rows_by_id[row_id]["state"],
                "latched": rows_by_id[row_id]["latched"],
                "reason": rows_by_id[row_id]["reason"],
            }
            for row_id in ALL_BREAKER_IDS
        }
        return rows_by_id, effective_state_by_scope, next_row_state
