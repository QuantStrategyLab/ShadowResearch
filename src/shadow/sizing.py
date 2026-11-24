"""Frozen policy_v0_pre 10-step notional sizing stack (U-01 + C-SA-Q21)."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_EVEN, localcontext
from pathlib import Path

from shadow.manifest import (
    compute_code_manifest_hash,
    load_calendar_manifest_identity,
    load_effective_runtime_config,
)

TIER_ORDER: tuple[str, ...] = ("aggressive", "balanced", "conservative")
INSTRUMENT_ORDER: tuple[str, ...] = ("SOXL", "TQQQ")
STAGE_N_NOTIONAL_B0 = Decimal("50000")
STAGE_N_DEFAULT_H = Decimal("0")

TIER_CONFIGS: dict[str, dict[str, str]] = {
    "conservative": {
        "contract_name": "CAPITAL_PRESERVATION",
        "D": "0.15",
        "k": "0.125",
        "x_cap": "0.20",
        "x_joint": "0.20",
        "w_max": "0.15",
        "sigma_target": "0.05",
    },
    "balanced": {
        "contract_name": "BALANCED_COMPOUNDING",
        "D": "0.30",
        "k": "0.25",
        "x_cap": "0.40",
        "x_joint": "0.40",
        "w_max": "0.35",
        "sigma_target": "0.10",
    },
    "aggressive": {
        "contract_name": "GROWTH_COMPOUNDING",
        "D": "0.50",
        "k": "0.50",
        "x_cap": "0.80",
        "x_joint": "0.80",
        "w_max": "0.60",
        "sigma_target": "0.19",
    },
}

CLOCK_START_DEFINITION: dict[str, object] = {
    "clk_id": "CLK-01",
    "requires_sh2_acceptance": True,
    "requires_fuse1_engine_hash": True,
    "requires_policy_digest": True,
    "requires_calendar_digest": True,
    "requires_detached_receipt": True,
    "requires_within_commitment_deadline": True,
    "drills_and_y2_credit_eligible": False,
    "reset_triggers": [
        "session_gap",
        "backfill",
        "missed_run_non_detection",
        "replay_mismatch",
        "unversioned_parameter_edit",
    ],
}

PINNED_POLICY_PARAMS: dict[str, object] = {
    "policy_id": "policy_v0_pre",
    "B0": "50000",
    "H_default": "0",
    "mu_shrunk": "0.0525",
    "sigma_long": "0.24",
    "beta_TQQQ": "1",
    "beta_SOXL": "1.5",
    "beta_fall_floor": "1.5",
    "soxl_joint_share_cap": "1/3",
    "no_trade_band": "0.20",
    "m0_crash_shock": "0.83",
    "vol_scalar_form": "linear",
    "share_rounding": "floor",
    "kelly_applied": False,
    "stop_loss_return_rule": "OFF",
    "loss_controls_armed": False,
    "unavailable_broker_inputs": "explicitly_unverified",
    "raw_vs_adjusted_separation": "7A_raw_vs_adjusted_separated",
    "monthly_diagnostic_rule": "diagnostic_only_unread_by_sizing",
    "cost_assumptions": {
        "stage": "stage_n_notional",
        "commission_model": "notional_zero_order_path",
        "spread_slippage_model": "unresolved_oi_cost_3_4_unverified",
        "accrual_day_count": "days/365",
    },
    "clock_start_definition": CLOCK_START_DEFINITION,
    "tiers": TIER_CONFIGS,
}


class SizingInputError(ValueError):
    """Raised when a required sizing input is missing or invalid."""

    def __init__(self, field: str, message: str) -> None:
        self.field = field
        super().__init__(message)


def canonical_decimal_str(value: Decimal | int | str) -> str:
    """Format a finite Decimal as an ordinary base-ten canonical string."""
    d = value if isinstance(value, Decimal) else Decimal(str(value))
    if not d.is_finite():
        raise ValueError(f"Non-finite decimal disallowed: {value!r}")
    if d == 0:
        return "0"
    s = format(d, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    if s in ("", "-0"):
        return "0"
    return s


def quantize_6dp(value: Decimal | str) -> Decimal:
    """Quantize a Decimal to 6 decimal places using ROUND_HALF_EVEN."""
    d = value if isinstance(value, Decimal) else Decimal(str(value))
    return d.quantize(Decimal("0.000001"), rounding=ROUND_HALF_EVEN)


def policy_config_hash(
    *,
    runtime_config: dict[str, object] | None = None,
    calendar_spec: dict[str, object] | None = None,
    code_manifest_hash: str | None = None,
    repo_root: str | Path | None = None,
) -> str:
    """Return deterministic SHA-256 of complete policy, runtime config, calendar, and code manifest."""
    eff_cfg = (
        runtime_config
        if runtime_config is not None
        else load_effective_runtime_config(repo_root)
    )
    cal_ver = str(eff_cfg.get("calendar_version", "csa22-v1"))
    if calendar_spec is not None:
        rules = calendar_spec.get("rules") if isinstance(calendar_spec.get("rules"), dict) else {}
        cal_ident = {
            "calendar_version": str(
                calendar_spec.get("calendar_version") or rules.get("calendar_version") or cal_ver
            ),
            "spec_digest": str(calendar_spec.get("spec_digest", "")),
        }
    else:
        cal_ident = load_calendar_manifest_identity(repo_root, calendar_version=cal_ver)
    exec_sha = (
        code_manifest_hash
        if code_manifest_hash is not None
        else compute_code_manifest_hash(repo_root, calendar_version=cal_ident["calendar_version"])
    )
    identity_payload = {
        "policy_params": PINNED_POLICY_PARAMS,
        "effective_runtime_config": eff_cfg,
        "calendar_identity": cal_ident,
        "executable_manifest_sha256": exec_sha,
    }
    blob = json.dumps(
        identity_payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _to_decimal(raw: object, field: str) -> Decimal:
    if raw is None or isinstance(raw, (bool, float)):
        raise SizingInputError(field, f"Missing or non-decimal input for {field}: {raw!r}")
    try:
        d = raw if isinstance(raw, Decimal) else Decimal(str(raw))
    except Exception as exc:
        raise SizingInputError(field, f"Invalid decimal for {field}: {raw!r}") from exc
    if not d.is_finite():
        raise SizingInputError(field, f"Non-finite decimal for {field}: {raw!r}")
    return d


def _resolve_q_pre(raw_q_pre: object, tier_id: str, field: str) -> int:
    if isinstance(raw_q_pre, dict):
        if tier_id not in raw_q_pre:
            raise SizingInputError(field, f"Missing q_pre for tier {tier_id}")
        raw_val = raw_q_pre[tier_id]
    else:
        raw_val = raw_q_pre
    if raw_val is None or isinstance(raw_val, (bool, float)):
        raise SizingInputError(field, f"Invalid q_pre for {field}: {raw_val!r}")
    try:
        val = int(raw_val)
    except Exception as exc:
        raise SizingInputError(field, f"Non-integer q_pre for {field}: {raw_val!r}") from exc
    if val < 0:
        raise SizingInputError(field, f"Negative q_pre disallowed for {field}: {val}")
    return val


def _m0_cap(c: Decimal, ln_one_minus_shock: Decimal) -> Decimal:
    if c <= 0:
        return Decimal("0")
    return (Decimal("1") - c).ln() / ln_one_minus_shock


def _binding_constraint(
    s_i: Decimal,
    v_i: Decimal,
    x_cap: Decimal,
    x_ML: Decimal,
    w_max_bound: bool,
    joint_cap_bound: bool,
    x_joint: Decimal,
    instrument: str,
    soxl_subcap_bound: bool,
) -> str:
    if s_i == 0:
        return "signal_zero"
    if x_ML == 0:
        return "x_ML"
    if w_max_bound:
        return "w_max"
    if joint_cap_bound:
        return "joint_cap(x_ML)" if x_ML <= x_joint else "joint_cap(x_joint)"
    if instrument == "SOXL" and soxl_subcap_bound:
        return "soxl_subcap"
    base = "x_ML" if x_ML < x_cap else "x_cap"
    suffixes: list[str] = []
    if v_i < Decimal("1"):
        suffixes.append("v")
    if s_i < Decimal("1"):
        suffixes.append("s")
    return base if not suffixes else f"{base}+{'+'.join(suffixes)}"


def compute_monthly_diagnostics(
    monthly_returns: Sequence[Decimal | int | str] | None,
) -> dict[str, object]:
    """Compute monthly win-rate, Bayesian CI, and payoff diagnostic (unread by sizing)."""
    if monthly_returns is None:
        return {
            "status": "unavailable",
            "reason": "insufficient_monthly_observations",
            "sample_months": 0,
            "win_count": 0,
            "loss_count": 0,
            "flat_count": 0,
            "win_rate": None,
            "win_rate_bayes_ci": None,
            "payoff_ratio": None,
            "payoff_reason": "insufficient_monthly_observations",
            "unread_by_sizing": True,
        }
    if isinstance(monthly_returns, (str, bytes, dict)):
        raise SizingInputError("monthly_returns", "monthly_returns must be a sequence")

    with localcontext() as ctx:
        ctx.prec = 28
        ctx.rounding = ROUND_HALF_EVEN
        parsed = [_to_decimal(item, "monthly_returns") for item in monthly_returns]
        if not parsed:
            return {
                "status": "unavailable",
                "reason": "insufficient_monthly_observations",
                "sample_months": 0,
                "win_count": 0,
                "loss_count": 0,
                "flat_count": 0,
                "win_rate": None,
                "win_rate_bayes_ci": None,
                "payoff_ratio": None,
                "payoff_reason": "insufficient_monthly_observations",
                "unread_by_sizing": True,
            }

        wins = [r for r in parsed if r > 0]
        losses = [r for r in parsed if r < 0]
        flats = [r for r in parsed if r == 0]
        n = len(parsed)
        win_count = len(wins)
        loss_count = len(losses)
        win_rate = Decimal(win_count) / Decimal(n)

        # Uniform Beta(1, 1) posterior mean and 95% normal-approximation credible interval
        alpha = Decimal(1 + win_count)
        beta = Decimal(1 + (n - win_count))
        total = alpha + beta
        post_mean = alpha / total
        post_var = (alpha * beta) / ((total ** 2) * (total + Decimal("1")))
        half_width = Decimal("1.959963984540054") * post_var.sqrt()
        ci_low = max(Decimal("0"), post_mean - half_width)
        ci_high = min(Decimal("1"), post_mean + half_width)

        if wins and losses:
            avg_win = sum(wins) / Decimal(win_count)
            avg_loss_abs = abs(sum(losses) / Decimal(loss_count))
            payoff_ratio: str | None = canonical_decimal_str(avg_win / avg_loss_abs)
            payoff_reason: str | None = None
        elif not losses:
            payoff_ratio = None
            payoff_reason = "no_losses_in_window"
        else:
            payoff_ratio = None
            payoff_reason = "no_wins_in_window"

        return {
            "status": "computed",
            "reason": None,
            "sample_months": n,
            "win_count": win_count,
            "loss_count": loss_count,
            "flat_count": len(flats),
            "win_rate": canonical_decimal_str(win_rate),
            "win_rate_bayes_ci": {
                "prior": "Beta(1,1)",
                "level": "0.95",
                "mean": canonical_decimal_str(post_mean),
                "lower": canonical_decimal_str(ci_low),
                "upper": canonical_decimal_str(ci_high),
            },
            "payoff_ratio": payoff_ratio,
            "payoff_reason": payoff_reason,
            "unread_by_sizing": True,
        }


def compute_raw_vs_adjusted_separation(
    e_val: Decimal,
    prior_e: Decimal | None,
    external_flow: Decimal = Decimal("0"),
) -> dict[str, object]:
    """Compute 7A separation between raw asset change and cash-flow-adjusted P&L/return."""
    labels = {
        "raw": "Asset change (includes cash flows)",
        "adjusted": "Cash-flow-adjusted P&L/return",
    }
    if prior_e is None:
        return {
            "status": "unavailable",
            "unavailable_reason": "prior_equity_unavailable",
            "external_flow_usd": canonical_decimal_str(external_flow),
            "raw_asset_change_usd": None,
            "raw_asset_change_pct": None,
            "adjusted_pnl_usd": None,
            "adjusted_return_pct": None,
            "labels": labels,
        }
    if prior_e <= 0:
        raise SizingInputError("prior_E", f"prior_E must be > 0, got {prior_e}")

    raw_change = e_val - prior_e
    raw_pct = raw_change / prior_e
    adj_pnl = e_val - prior_e - external_flow
    denom = prior_e + external_flow
    adj_pct = canonical_decimal_str(adj_pnl / denom) if denom > 0 else None
    return {
        "status": "computed",
        "unavailable_reason": None,
        "external_flow_usd": canonical_decimal_str(external_flow),
        "raw_asset_change_usd": canonical_decimal_str(raw_change),
        "raw_asset_change_pct": canonical_decimal_str(raw_pct),
        "adjusted_pnl_usd": canonical_decimal_str(adj_pnl),
        "adjusted_return_pct": adj_pct,
        "labels": labels,
    }


_CONTROL_STATE_RANK = {
    "NORMAL": 0,
    "NO-NEW-RISK": 1,
    "DE-RISK": 2,
    "ORDER-UNKNOWN": 3,
    "HALT": 4,
    "J1_TRIM": 1,
}


def _most_restrictive_sizing_state(base_state: str, fund_state: str | None) -> str:
    if fund_state is None or fund_state == base_state:
        return base_state
    if _CONTROL_STATE_RANK.get(fund_state, 0) > _CONTROL_STATE_RANK.get(base_state, 0):
        return fund_state
    return base_state


def compute_tier_max_loss_fraction(
    tier_id: str,
    *,
    e_val: Decimal,
    e_star: Decimal,
    l_val: Decimal,
    h_val: Decimal = Decimal("0"),
    w_ref: Decimal | None = None,
) -> tuple[Decimal, str, Decimal]:
    """Compute Step 3 (cushion, binding_subbound, x_ML) using prec=28 U-01 math."""
    if tier_id not in TIER_CONFIGS:
        raise SizingInputError("tier_id", f"Unknown tier {tier_id!r}")
    if e_val <= 0:
        raise SizingInputError("E", f"Equity E must be > 0, got {e_val}")
    if e_star <= 0 or e_val > e_star:
        raise SizingInputError("E_star", f"Must satisfy 0 < E <= E_star ({e_val}, {e_star})")
    if h_val < 0:
        raise SizingInputError("H", f"External capital H must be >= 0, got {h_val}")
    if l_val < 0:
        raise SizingInputError("L", f"Loss L must be >= 0, got {l_val}")

    with localcontext() as ctx:
        ctx.prec = 28
        ctx.rounding = ROUND_HALF_EVEN
        d_budget = Decimal(TIER_CONFIGS[tier_id]["D"])
        w_ref_eff = w_ref if w_ref is not None else (e_star + h_val)
        if w_ref_eff - l_val <= 0:
            raise SizingInputError("W_ref", "W_ref - L must be > 0")
        if l_val >= d_budget * w_ref_eff:
            cushion = Decimal("0")
        else:
            cushion = (d_budget * w_ref_eff - l_val) / (w_ref_eff - l_val)
        ln_one_minus_shock = (Decimal("1") - Decimal("0.83")).ln()
        bound_a = Decimal("3") * cushion
        bound_b = Decimal("2") * cushion
        bound_c = _m0_cap(cushion, ln_one_minus_shock)
        subbound, x_ml = min(
            [("a", bound_a), ("b", bound_b), ("c", bound_c)],
            key=lambda item: item[1],
        )
        return cushion, subbound, x_ml


def compute_session_sizing(
    inputs: dict,
    active_control_state: str = "NORMAL",
    fund_control_states: Mapping[str, str] | None = None,
) -> dict:
    """Compute the 10-step policy_v0_pre notional sizing stack across all 3 tiers."""
    with localcontext() as ctx:
        ctx.prec = 28
        ctx.rounding = ROUND_HALF_EVEN

        e_val = _to_decimal(inputs.get("E"), "E")
        e_star = _to_decimal(inputs.get("E_star"), "E_star")
        h_val = _to_decimal(inputs.get("H", "0"), "H")
        l_val = _to_decimal(inputs.get("L"), "L")

        if e_val <= 0:
            raise SizingInputError("E", f"Equity E must be > 0, got {e_val}")
        if e_star <= 0 or e_val > e_star:
            raise SizingInputError("E_star", f"Must satisfy 0 < E <= E_star ({e_val}, {e_star})")
        if h_val < 0:
            raise SizingInputError("H", f"External capital H must be >= 0, got {h_val}")
        if l_val < 0:
            raise SizingInputError("L", f"Loss L must be >= 0, got {l_val}")

        w_val = e_val + h_val
        w_star = e_star + h_val
        w_ref = _to_decimal(inputs["W_ref"], "W_ref") if inputs.get("W_ref") is not None else w_star
        if w_ref - l_val <= 0:
            raise SizingInputError("W_ref", "W_ref - L must be > 0")

        funds_in = inputs.get("funds")
        if not isinstance(funds_in, dict):
            raise SizingInputError("funds", "Missing funds dictionary")

        fund_parsed: dict[str, dict[str, object]] = {}
        for inst in INSTRUMENT_ORDER:
            f_in = funds_in.get(inst)
            if not isinstance(f_in, dict):
                raise SizingInputError(f"{inst}", f"Missing fund inputs for {inst}")
            s_val = _to_decimal(f_in.get("s"), f"{inst}_s")
            if s_val < 0 or s_val > 1:
                raise SizingInputError(f"{inst}_s", f"Signal s must be in [0, 1], got {s_val}")
            p_ref = _to_decimal(f_in.get("close_price"), f"{inst}_close")
            if p_ref <= 0:
                raise SizingInputError(f"{inst}_close", f"Close price must be > 0, got {p_ref}")
            sigma_f = _to_decimal(f_in.get("sigma_forecast"), f"{inst}_sigma_forecast")
            if sigma_f <= 0:
                raise SizingInputError(
                    f"{inst}_sigma_forecast",
                    f"Forecast volatility must be > 0, got {sigma_f}",
                )
            fund_parsed[inst] = {
                "s": s_val,
                "close_price": p_ref,
                "sigma_forecast": sigma_f,
                "raw_q_pre": f_in.get("q_pre"),
            }

        mu_shrunk = Decimal("0.0525")
        sigma_long = Decimal("0.24")
        beta_soxl = Decimal("1.5")
        b_band = Decimal("0.20")
        ln_one_minus_shock = (Decimal("1") - Decimal("0.83")).ln()
        conv_scale = Decimal("1") + (h_val / e_star)

        tiers_out: list[dict] = []
        for tier_id in TIER_ORDER:
            tc = TIER_CONFIGS[tier_id]
            d_budget = Decimal(tc["D"])
            k_frac = Decimal(tc["k"])
            x_cap = Decimal(tc["x_cap"])
            x_joint = Decimal(tc["x_joint"])
            w_max = Decimal(tc["w_max"])
            sigma_target = Decimal(tc["sigma_target"])

            # Cushion & Step 3 x_ML(c) = min(3c, 2c, M0(c)) per U-01 §3.4 & DEMO-BRIEF
            cushion, subbound, x_ml = compute_tier_max_loss_fraction(
                tier_id,
                e_val=e_val,
                e_star=e_star,
                l_val=l_val,
                h_val=h_val,
                w_ref=w_ref,
            )
            cap_23 = min(x_cap, x_ml)

            # Step 4: Kelly structural & conditional (recorded only)
            x_k_struct = k_frac * max(Decimal("0"), mu_shrunk / (sigma_long ** 2))
            kelly_cond: dict[str, Decimal] = {}
            v_scalars: dict[str, Decimal] = {}
            x_pre: dict[str, Decimal] = {}
            q_pre_map: dict[str, int] = {}

            for inst in INSTRUMENT_ORDER:
                fp = fund_parsed[inst]
                s_i: Decimal = fp["s"]  # type: ignore[assignment]
                sig_i: Decimal = fp["sigma_forecast"]  # type: ignore[assignment]
                q_pre_map[inst] = _resolve_q_pre(fp["raw_q_pre"], tier_id, f"{inst}_q_pre")
                ratio_sq = min(Decimal("1"), (sigma_long / sig_i) ** 2)
                kelly_cond[inst] = x_k_struct * ratio_sq
                v_i = min(Decimal("1"), sigma_target / sig_i)
                v_scalars[inst] = v_i
                denom = beta_soxl if inst == "SOXL" else Decimal("1")
                x_pre[inst] = (s_i * cap_23 * v_i) / denom

            eff_states: dict[str, str] = {
                inst: _most_restrictive_sizing_state(
                    active_control_state,
                    (fund_control_states or {}).get(inst),
                )
                for inst in INSTRUMENT_ORDER
            }
            frozen_insts = {
                inst
                for inst in INSTRUMENT_ORDER
                if eff_states[inst] in ("HALT", "ORDER-UNKNOWN")
            }

            # Current weights and exposures for Step 9 mandatory bypass & frozen-holding reservation
            w_cur_map = {
                inst: (Decimal(q_pre_map[inst]) * fund_parsed[inst]["close_price"]) / e_val  # type: ignore[operator]
                for inst in INSTRUMENT_ORDER
            }
            x_cur_map = {
                inst: (Decimal("3") * w_cur_map[inst]) / conv_scale
                for inst in INSTRUMENT_ORDER
            }
            w_cur_sum = w_cur_map["TQQQ"] + w_cur_map["SOXL"]

            # Step 7: Joint cap with OQ-1 resolution (and frozen HALT/ORDER-UNKNOWN reservation)
            x_eff_joint = min(x_joint, x_ml)
            soxl_subcap_limit = x_eff_joint / Decimal("3")
            w_ceil = min(w_max, Decimal("1.00"))
            joint_cur_exceeded = (x_cur_map["TQQQ"] + beta_soxl * x_cur_map["SOXL"]) > x_eff_joint

            if not frozen_insts:
                soxl_beta_pre = beta_soxl * x_pre["SOXL"]
                soxl_subcap_bound = soxl_beta_pre > soxl_subcap_limit
                x_soxl_mid = min(soxl_beta_pre, soxl_subcap_limit) / beta_soxl
                x_joint_beta = x_pre["TQQQ"] + beta_soxl * x_soxl_mid

                if x_joint_beta <= x_eff_joint:
                    alpha_joint = Decimal("1")
                    x_post = {"TQQQ": x_pre["TQQQ"], "SOXL": x_soxl_mid}
                    joint_cap_bound = False
                else:
                    alpha_joint = x_eff_joint / x_joint_beta
                    x_post = {
                        "TQQQ": alpha_joint * x_pre["TQQQ"],
                        "SOXL": alpha_joint * x_soxl_mid,
                    }
                    joint_cap_bound = True

                # Step 8: Account fund weights & combined hard ceiling min(w_max, 1.00)
                w_raw = {
                    inst: (x_post[inst] / Decimal("3")) * conv_scale
                    for inst in INSTRUMENT_ORDER
                }
                w_raw_sum = w_raw["TQQQ"] + w_raw["SOXL"]
                if w_raw_sum > w_ceil and w_raw_sum > 0:
                    w_scale = w_ceil / w_raw_sum
                    w_target = {inst: w_raw[inst] * w_scale for inst in INSTRUMENT_ORDER}
                    w_max_bound = True
                else:
                    w_target = w_raw
                    w_max_bound = False
            else:
                frozen_joint_beta = sum(
                    (beta_soxl if inst == "SOXL" else Decimal("1")) * x_cur_map[inst]
                    for inst in frozen_insts
                )
                rem_joint = max(Decimal("0"), x_eff_joint - frozen_joint_beta)
                active_insts = [
                    inst
                    for inst in INSTRUMENT_ORDER
                    if inst not in frozen_insts and eff_states[inst] != "DE-RISK"
                ]
                soxl_beta_pre = beta_soxl * x_pre["SOXL"]
                soxl_subcap_bound = (
                    "SOXL" in active_insts and soxl_beta_pre > soxl_subcap_limit
                )
                x_mid = {
                    inst: (
                        min(soxl_beta_pre, soxl_subcap_limit) / beta_soxl
                        if inst == "SOXL"
                        else x_pre[inst]
                    )
                    for inst in active_insts
                }
                active_joint_beta = sum(
                    (beta_soxl if inst == "SOXL" else Decimal("1")) * x_mid[inst]
                    for inst in active_insts
                )
                if active_joint_beta <= rem_joint:
                    alpha_joint = Decimal("1")
                    x_post_active = dict(x_mid)
                    joint_cap_bound = False
                elif rem_joint == 0 or active_joint_beta <= 0:
                    alpha_joint = Decimal("0")
                    x_post_active = {inst: Decimal("0") for inst in active_insts}
                    joint_cap_bound = True
                else:
                    alpha_joint = rem_joint / active_joint_beta
                    x_post_active = {inst: alpha_joint * x_mid[inst] for inst in active_insts}
                    joint_cap_bound = True
                x_post = {
                    inst: (
                        x_cur_map[inst]
                        if inst in frozen_insts
                        else x_post_active.get(inst, Decimal("0"))
                    )
                    for inst in INSTRUMENT_ORDER
                }

                frozen_w_sum = sum(w_cur_map[inst] for inst in frozen_insts)
                rem_w_ceil = max(Decimal("0"), w_ceil - frozen_w_sum)
                w_raw_active = {
                    inst: (x_post[inst] / Decimal("3")) * conv_scale
                    for inst in active_insts
                }
                w_raw_active_sum = sum(w_raw_active.values())
                if w_raw_active_sum > rem_w_ceil:
                    w_max_bound = True
                    if rem_w_ceil == 0 or w_raw_active_sum <= 0:
                        w_target_active = {inst: Decimal("0") for inst in active_insts}
                    else:
                        w_scale = rem_w_ceil / w_raw_active_sum
                        w_target_active = {
                            inst: w_raw_active[inst] * w_scale for inst in active_insts
                        }
                else:
                    w_max_bound = False
                    w_target_active = w_raw_active
                w_target = {
                    inst: (
                        w_cur_map[inst]
                        if inst in frozen_insts
                        else w_target_active.get(inst, Decimal("0"))
                    )
                    for inst in INSTRUMENT_ORDER
                }

            funds_out: list[dict] = []
            for inst in INSTRUMENT_ORDER:
                fp = fund_parsed[inst]
                s_i = fp["s"]  # type: ignore[assignment]
                p_ref = fp["close_price"]  # type: ignore[assignment]
                v_i = v_scalars[inst]
                x_k_c = kelly_cond[inst]
                x_p = x_pre[inst]
                denom = beta_soxl if inst == "SOXL" else Decimal("1")

                # Per demo/DEMO-CASES-CORRECTION-1.md (Astra interpretation-4 ruling):
                # compare counterfactual against x_pre at full precision before Step 7.
                kelly_cf_x = (s_i * min(x_cap, x_ml, x_k_c) * v_i) / denom
                kelly_would_bind = kelly_cf_x < x_p

                eff_state = eff_states[inst]
                w_tgt = w_target[inst]
                if eff_state == "DE-RISK":
                    w_tgt_eff = Decimal("0")
                else:
                    w_tgt_eff = w_tgt

                q_ideal = int((w_tgt_eff * e_val / p_ref).to_integral_value(rounding=ROUND_FLOOR))
                q_pre_i = q_pre_map[inst]
                no_new_risk_clamped = False
                if eff_state == "NO-NEW-RISK" and q_ideal > q_pre_i:
                    q_ideal = q_pre_i
                    no_new_risk_clamped = True
                w_cur_i = w_cur_map[inst]

                cap_exceeded = (
                    (x_cur_map[inst] > cap_23)
                    or (inst == "SOXL" and (beta_soxl * x_cur_map["SOXL"]) > soxl_subcap_limit)
                    or joint_cur_exceeded
                    or (w_cur_sum > w_ceil)
                )
                breaker_forced_reduce = eff_state == "DE-RISK" or active_control_state == "J1_TRIM"
                mandatory_bypass = (q_ideal < q_pre_i) and (cap_exceeded or breaker_forced_reduce)
                in_band = abs(w_cur_i - w_tgt_eff) <= (b_band * w_tgt_eff)

                if eff_state in ("HALT", "ORDER-UNKNOWN"):
                    delta_q_exec = 0
                    band_suppressed = False
                    emerg_bypass = False
                elif q_ideal == q_pre_i or no_new_risk_clamped:
                    delta_q_exec = 0
                    band_suppressed = False
                    emerg_bypass = False
                elif in_band and not mandatory_bypass:
                    delta_q_exec = 0
                    band_suppressed = True
                    emerg_bypass = False
                else:
                    delta_q_exec = q_ideal - q_pre_i
                    band_suppressed = False
                    emerg_bypass = bool(in_band and mandatory_bypass)

                binding = _binding_constraint(
                    s_i=s_i,
                    v_i=v_i,
                    x_cap=x_cap,
                    x_ML=x_ml,
                    w_max_bound=w_max_bound,
                    joint_cap_bound=joint_cap_bound,
                    x_joint=x_joint,
                    instrument=inst,
                    soxl_subcap_bound=soxl_subcap_bound,
                )

                funds_out.append(
                    {
                        "instrument": inst,
                        "s": canonical_decimal_str(s_i),
                        "x_cap": canonical_decimal_str(x_cap),
                        "x_ML": canonical_decimal_str(x_ml),
                        "x_ML_binding_subbound": subbound,
                        "x_K_struct": canonical_decimal_str(x_k_struct),
                        "x_K_cond": canonical_decimal_str(x_k_c),
                        "kelly_would_have_bound": kelly_would_bind,
                        "kelly_counterfactual_x": canonical_decimal_str(kelly_cf_x),
                        "kelly_applied": False,
                        "orders_placed": 0,
                        "v": canonical_decimal_str(v_i),
                        "x_pre": canonical_decimal_str(x_p),
                        "x_post_joint": canonical_decimal_str(x_post[inst]),
                        "w_target": canonical_decimal_str(w_tgt_eff),
                        "q_ideal": q_ideal,
                        "q_pre": q_pre_i,
                        "delta_q_exec": delta_q_exec,
                        "binding_constraint": binding,
                        "no_trade_band_suppressed": band_suppressed,
                        "emergency_bypass_triggered": emerg_bypass,
                    }
                )

            tiers_out.append(
                {
                    "tier_id": tier_id,
                    "contract_name": tc["contract_name"],
                    "D": canonical_decimal_str(d_budget),
                    "k": canonical_decimal_str(k_frac),
                    "x_cap": canonical_decimal_str(x_cap),
                    "x_joint": canonical_decimal_str(x_joint),
                    "w_max": canonical_decimal_str(w_max),
                    "sigma_target": canonical_decimal_str(sigma_target),
                    "c": canonical_decimal_str(cushion),
                    "x_ML": canonical_decimal_str(x_ml),
                    "x_ML_binding_subbound": subbound,
                    "x_eff_joint": canonical_decimal_str(x_eff_joint),
                    "alpha_joint": canonical_decimal_str(alpha_joint),
                    "w_max_bound": w_max_bound,
                    "orders_placed": 0,
                    "funds": funds_out,
                }
            )

        prior_e_raw = inputs.get("prior_E")
        prior_e = _to_decimal(prior_e_raw, "prior_E") if prior_e_raw is not None else None
        ext_flow_raw = inputs.get("external_flow", "0")
        ext_flow = _to_decimal(ext_flow_raw, "external_flow")
        raw_vs_adjusted = compute_raw_vs_adjusted_separation(e_val, prior_e, ext_flow)
        monthly_diagnostic = compute_monthly_diagnostics(inputs.get("monthly_returns"))

        return {
            "policy_id": "policy_v0_pre",
            "policy_config_hash": policy_config_hash(),
            "unavailable_broker_inputs": "explicitly_unverified",
            "bankroll": {
                "E": canonical_decimal_str(e_val),
                "E_star": canonical_decimal_str(e_star),
                "H": canonical_decimal_str(h_val),
                "W": canonical_decimal_str(w_val),
                "W_star": canonical_decimal_str(w_star),
                "W_ref": canonical_decimal_str(w_ref),
                "L": canonical_decimal_str(l_val),
            },
            "raw_vs_adjusted": raw_vs_adjusted,
            "monthly_diagnostic": monthly_diagnostic,
            "tiers": tiers_out,
        }
