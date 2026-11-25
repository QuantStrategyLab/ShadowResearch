"""Loss breaker rows L0-L6 per BRIEF-FUSE-1 §4.3 Table A.

All loss rows operate on bankroll basis, flow-adjusted and split-adjusted.
Thresholds are fractions of D (tier loss budget).
"""

from datetime import date
from typing import Any, Dict, Optional
from . import state as st
from . import canonical_state as cst
from .canonical import finite_float


# Tier loss budgets (D) per spec §4.3 and spec.json
TIER_D = {
    'conservative': 0.15,
    'balanced': 0.30,
    'aggressive': 0.50
}


def evaluate_L0(inputs: Dict[str, Any], prior_state: Optional[Dict[str, Any]],
                authorized_events: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """L0: Exposure limited to maximum-loss cap (standing limit).

    Kind: limit (symmetric, automatic)
    Scope: account
    State contribution: none (outputs only)
    Required inputs: joint_exposure, max_loss_cap, proposed_exposure
    """
    # Required finite non-negative inputs - fail closed on missing/NaN/inf/bool/negative (§4.4 rule 3, §5.2)
    joint_exposure = finite_float(inputs.get('joint_exposure'))
    max_loss_cap = finite_float(inputs.get('max_loss_cap'))
    proposed_exposure = finite_float(inputs.get('proposed_exposure'))

    if (
        joint_exposure is None or max_loss_cap is None or proposed_exposure is None or
        joint_exposure < 0.0 or max_loss_cap < 0.0 or proposed_exposure < 0.0
    ):
        return {
            'state': 'NORMAL',
            'would_have_tripped': True,
            'kind': 'limit',
            'latched': False,
            'reason': 'missing_input',
            'outputs': {
                'cap': None,
                'cap_breached': True,
                'increases_blocked': True
            }
        }

    # Evaluate limit
    cap_breached = joint_exposure > max_loss_cap
    increases_blocked = proposed_exposure > max_loss_cap

    # Fix 4: tier_change re-based evaluation - report limits but don't trip
    # O9 handles enforcement of unauthorized tier changes
    tier_change_event = inputs.get('tier_change_event')
    if tier_change_event:
        # Re-based: report new tier limits in outputs but don't trip
        return {
            'state': 'NORMAL',
            'would_have_tripped': False,
            'kind': 'limit',
            'latched': False,
            'reason': 'none',
            'outputs': {
                'cap': max_loss_cap,
                'cap_breached': cap_breached,
                'increases_blocked': increases_blocked
            }
        }

    return {
        'state': 'NORMAL',
        'would_have_tripped': cap_breached or increases_blocked,
        'kind': 'limit',
        'latched': False,
        'reason': 'breached' if (cap_breached or increases_blocked) else 'none',
        'outputs': {
            'cap': max_loss_cap,
            'cap_breached': cap_breached,
            'increases_blocked': increases_blocked
        }
    }


def evaluate_L1(inputs: Dict[str, Any], prior_state: Optional[Dict[str, Any]],
                authorized_events: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """L1: Drawdown reaches D/3 → notify and lock against loosening.

    Kind: interlock
    Scope: account
    State contribution: none (but locks tier/H/HWM)
    Clearing: Clears when drawdown is back under D/3
    """
    bankroll = finite_float(inputs.get('bankroll'))
    high_water_mark = finite_float(inputs.get('high_water_mark'))
    tier = inputs.get('tier')

    # Fail closed on missing, NaN, inf, bool, or out-of-range inputs
    if bankroll is None or high_water_mark is None or bankroll < 0.0 or high_water_mark <= 0.0 or tier is None:
        return {
            'state': 'NORMAL',
            'would_have_tripped': True,
            'kind': 'interlock',
            'latched': False,
            'reason': 'missing_input',
            'outputs': {
                'locked_against_loosening': True,
                'notify': True
            }
        }

    # Get tier D and threshold
    D = TIER_D.get(tier)
    if D is None:
        return {
            'state': 'NORMAL',
            'would_have_tripped': True,
            'kind': 'interlock',
            'latched': False,
            'reason': 'missing_input',
            'outputs': {
                'locked_against_loosening': True,
                'notify': True
            }
        }

    threshold = D / 3.0

    # Calculate drawdown
    drawdown = (high_water_mark - bankroll) / high_water_mark if high_water_mark > 0 else 0.0

    # Check trigger
    triggered = drawdown >= threshold

    return {
        'state': 'NORMAL',
        'would_have_tripped': triggered,
        'kind': 'interlock',
        'latched': False,
        'reason': 'drawdown_threshold' if triggered else 'none',
        'outputs': {
            'locked_against_loosening': triggered,
            'notify': triggered
        }
    }


def evaluate_L2(inputs: Dict[str, Any], prior_state: Optional[Dict[str, Any]],
                authorized_events: Optional[Dict[str, Any]],
                session: Optional[str] = None,
                as_of: Optional[str] = None) -> Dict[str, Any]:
    """L2: Drawdown reaches 2D/3 → NO-NEW-RISK.

    Kind: breaker
    Scope: account
    State contribution: NO-NEW-RISK
    Clearing: Owner re-arms via authorized_events (canonical format per Q6)
    """
    # Check for prior latch from canonical format (SUC-12)
    prior_latch = cst.get_l2_latch(prior_state)
    if prior_latch:
        rearm = cst.matches_account_rearm(
            authorized_events, 'L2', session=session, as_of=as_of, require_date=True,
        ) is not None

        if rearm:
            # Q4 short-circuit: re-arm wins, return authorized_rearm WITHOUT re-evaluating
            return {
                'state': 'NORMAL',
                'would_have_tripped': False,
                'kind': 'breaker',
                'latched': False,
                'reason': 'authorized_rearm'
            }

        # Stay latched without re-arm
        # Stay latched - check for specific latched conditions
        flow_event = inputs.get('flow_event')
        tier_change_event = inputs.get('tier_change_event')

        # Determine specific latched reason (SUC-12)
        if flow_event and flow_event.get('type') == 'deposit':
            reason = 'latched_survives_deposit'
        elif tier_change_event:
            reason = 'latched_survives_tier_change'
        else:
            reason = 'latched_prior_trip'

        return {
            'state': 'NO-NEW-RISK',
            'would_have_tripped': True,
            'kind': 'breaker',
            'latched': True,
            'reason': reason
        }

    # Evaluate trigger
    bankroll = finite_float(inputs.get('bankroll'))
    high_water_mark = finite_float(inputs.get('high_water_mark'))
    tier = inputs.get('tier')

    # Fail closed on missing, NaN, inf, bool, or out-of-range inputs
    if bankroll is None or high_water_mark is None or bankroll < 0.0 or high_water_mark <= 0.0 or tier is None:
        return {
            'state': 'NO-NEW-RISK',
            'would_have_tripped': True,
            'kind': 'breaker',
            'latched': True,
            'reason': 'missing_input'
        }

    # Get tier D and threshold
    D = TIER_D.get(tier)
    if D is None:
        return {
            'state': 'NO-NEW-RISK',
            'would_have_tripped': True,
            'kind': 'breaker',
            'latched': True,
            'reason': 'missing_input'
        }

    threshold = 2.0 * D / 3.0

    # Calculate drawdown
    drawdown = (high_water_mark - bankroll) / high_water_mark if high_water_mark > 0 else 0.0

    # Check trigger (inclusive per spec: "reaches 2D/3 (inclusive)")
    triggered = drawdown >= threshold

    if not triggered:
        return {
            'state': 'NORMAL',
            'would_have_tripped': False,
            'kind': 'breaker',
            'latched': False,
            'reason': 'none'
        }

    # Trip and latch (SUC-12 step 1: specific reason)
    return {
        'state': 'NO-NEW-RISK',
        'would_have_tripped': True,
        'kind': 'breaker',
        'latched': True,
        'reason': 'drawdown_at_2D_over_3'
    }


def evaluate_L3(inputs: Dict[str, Any], prior_state: Optional[Dict[str, Any]],
                authorized_events: Optional[Dict[str, Any]],
                session: Optional[str] = None,
                as_of: Optional[str] = None) -> Dict[str, Any]:
    """L3: Drawdown reaches D → DE-RISK to zero, then locked flat.

    Kind: breaker
    Scope: account
    State contribution: DE-RISK
    Clearing: Owner card with 30-day cooling-off; loss record not cleared (canonical per Q6)
    """
    # Check for prior latch from canonical format
    prior_latch = cst.get_l3_latch(prior_state)
    if prior_latch:
        rearm = False
        cooling_off_days_remaining = 30

        cur_date_str = cst.canonical_session_date(session, as_of)
        trip_date_str = prior_latch.get('trip_session') or prior_latch.get('trip_date')
        if cur_date_str and trip_date_str:
            try:
                days_elapsed = (date.fromisoformat(cur_date_str) - date.fromisoformat(trip_date_str)).days
                cooling_off_days_remaining = max(0, 30 - max(0, days_elapsed))
            except ValueError:
                cooling_off_days_remaining = 30

        matched_event = cst.matches_account_rearm(
            authorized_events, 'L3', session=session, as_of=as_of, require_date=True
        )
        if matched_event is not None:
            rearm = True

        if rearm and cooling_off_days_remaining == 0:
            # Q4 short-circuit after 30-day cooling-off: clear active lock, preserve loss record
            return {
                'state': 'NORMAL',
                'would_have_tripped': False,
                'kind': 'breaker',
                'latched': False,
                'reason': 'authorized_rearm',
                'outputs': {
                    'cooling_off_days_remaining': 0,
                    'locked_flat': False,
                    'loss_record_cleared': False
                }
            }

        # Stay latched
        return {
            'state': 'DE-RISK',
            'would_have_tripped': True,
            'kind': 'breaker',
            'latched': True,
            'reason': 'latched_prior_trip',
            'outputs': {
                'cooling_off_days_remaining': cooling_off_days_remaining,
                'locked_flat': True,
                'loss_record_cleared': False
            }
        }

    bankroll = finite_float(inputs.get('bankroll'))
    high_water_mark = finite_float(inputs.get('high_water_mark'))
    tier = inputs.get('tier')

    # Fail closed on missing or non-finite inputs
    if bankroll is None or high_water_mark is None or bankroll < 0.0 or high_water_mark <= 0.0 or tier is None:
        return {
            'state': 'DE-RISK',
            'would_have_tripped': True,
            'kind': 'breaker',
            'latched': True,
            'reason': 'missing_input',
            'outputs': {
                'cooling_off_days_remaining': 30,
                'locked_flat': True,
                'loss_record_cleared': False
            }
        }

    # Get tier D
    D = TIER_D.get(tier)
    if D is None:
        return {
            'state': 'DE-RISK',
            'would_have_tripped': True,
            'kind': 'breaker',
            'latched': True,
            'reason': 'missing_input',
            'outputs': {
                'cooling_off_days_remaining': 30,
                'locked_flat': True,
                'loss_record_cleared': False
            }
        }

    # Calculate drawdown
    drawdown = (high_water_mark - bankroll) / high_water_mark if high_water_mark > 0 else 0.0

    # Check trigger
    triggered = drawdown >= D

    if not triggered:
        return {
            'state': 'NORMAL',
            'would_have_tripped': False,
            'kind': 'breaker',
            'latched': False,
            'reason': 'none',
            'outputs': {
                'cooling_off_days_remaining': 0,
                'locked_flat': False,
                'loss_record_cleared': False
            }
        }

    # Trip and latch
    return {
        'state': 'DE-RISK',
        'would_have_tripped': True,
        'kind': 'breaker',
        'latched': True,
        'reason': 'drawdown_threshold',
        'outputs': {
            'cooling_off_days_remaining': 30,
            'locked_flat': True,
            'loss_record_cleared': False
        }
    }


def evaluate_L4(inputs: Dict[str, Any], prior_state: Optional[Dict[str, Any]],
                authorized_events: Optional[Dict[str, Any]],
                session: Optional[str] = None,
                as_of: Optional[str] = None,
                market_closed: bool = False) -> Dict[str, Any]:
    """L4: One-session loss reaches D/10 → NO-NEW-RISK.

    Kind: breaker
    Scope: account
    State contribution: NO-NEW-RISK
    Clearing: Owner acknowledges, not before next session (canonical per Q6)
    """
    # Check for prior latch from canonical format
    prior_latch = cst.get_account_latch(prior_state, 'L4')
    if prior_latch:
        rearm = False
        matched_event = cst.matches_account_rearm(
            authorized_events, 'L4', session=session, as_of=as_of, require_date=True,
        )
        if matched_event is not None and not market_closed:
            # Next session minimum computed strictly from persisted trading session (never as_of UTC midnight)
            cur_date = cst.canonical_session_date(session, as_of)
            trip_sess = prior_latch.get('trip_session')
            if cur_date and trip_sess and cur_date > trip_sess:
                rearm = True

        if rearm:
            return {
                'state': 'NORMAL',
                'would_have_tripped': False,
                'kind': 'breaker',
                'latched': False,
                'reason': 'authorized_rearm'
            }

        # Stay latched
        return {
            'state': 'NO-NEW-RISK',
            'would_have_tripped': True,
            'kind': 'breaker',
            'latched': True,
            'reason': 'latched_prior_trip'
        }

    # Evaluate trigger
    one_session_loss = finite_float(inputs.get('one_session_loss'))
    tier = inputs.get('tier')

    # Fail closed on missing, NaN, inf, bool, negative, or invalid tier
    if one_session_loss is None or one_session_loss < 0.0 or tier is None:
        return {
            'state': 'NO-NEW-RISK',
            'would_have_tripped': True,
            'kind': 'breaker',
            'latched': True,
            'reason': 'missing_input'
        }

    # Get tier D and threshold
    D = TIER_D.get(tier)
    if D is None:
        return {
            'state': 'NO-NEW-RISK',
            'would_have_tripped': True,
            'kind': 'breaker',
            'latched': True,
            'reason': 'missing_input'
        }

    threshold = D / 10.0

    # Check trigger
    triggered = one_session_loss >= threshold

    if not triggered:
        return {
            'state': 'NORMAL',
            'would_have_tripped': False,
            'kind': 'breaker',
            'latched': False,
            'reason': 'none'
        }

    # Trip and latch
    return {
        'state': 'NO-NEW-RISK',
        'would_have_tripped': True,
        'kind': 'breaker',
        'latched': True,
        'reason': 'one_session_loss_threshold'
    }


def _parse_market_halt_level(raw: Any) -> tuple:
    """Validate optional market_halt_level; only None or exact int in {0, 1, 2, 3} is valid."""
    if raw is None:
        return None, False
    if isinstance(raw, bool) or not isinstance(raw, int) or raw not in (0, 1, 2, 3):
        return None, True
    return raw, False


def evaluate_L5(inputs: Dict[str, Any], prior_state: Optional[Dict[str, Any]],
                authorized_events: Optional[Dict[str, Any]], session: str,
                as_of: Optional[str] = None) -> Dict[str, Any]:
    """L5: Market-wide trading halt Level 1 or 2 → NO-NEW-RISK for rest of session.

    Kind: interlock
    Scope: account
    State contribution: NO-NEW-RISK
    Clearing: Persists through remainder of trading session; clears on subsequent clean session (canonical per Q6)
    """
    prior_l5 = cst.get_l5_state(prior_state)
    cur_date = cst.canonical_session_date(session, as_of)

    halt_level, invalid_halt_level = _parse_market_halt_level(inputs.get('market_halt_level'))
    triggered = halt_level in (1, 2)

    if triggered:
        return {
            'state': 'NO-NEW-RISK',
            'would_have_tripped': True,
            'kind': 'interlock',
            'latched': False,
            'reason': 'market_halt_level_1_or_2'
        }

    if invalid_halt_level:
        return {
            'state': 'NO-NEW-RISK',
            'would_have_tripped': True,
            'kind': 'interlock',
            'latched': False,
            'reason': 'invalid_input'
        }

    if prior_l5['active']:
        # Same trading session as L5 trip -> remains NO-NEW-RISK for rest of session
        if prior_l5['trip_session'] is not None and prior_l5['trip_session'] == cur_date:
            return {
                'state': 'NO-NEW-RISK',
                'would_have_tripped': True,
                'kind': 'interlock',
                'latched': False,
                'reason': 'clearing_period'
            }
        # Legacy synthetic latched L5 without trip_session clears after one clean session
        if prior_l5['trip_session'] is None and prior_l5['fresh_session_count'] < 1:
            return {
                'state': 'NO-NEW-RISK',
                'would_have_tripped': True,
                'kind': 'interlock',
                'latched': False,
                'reason': 'clearing_period'
            }

    return {
        'state': 'NORMAL',
        'would_have_tripped': False,
        'kind': 'interlock',
        'latched': False,
        'reason': 'none'
    }


def evaluate_L6(inputs: Dict[str, Any], prior_state: Optional[Dict[str, Any]],
                authorized_events: Optional[Dict[str, Any]],
                session: Optional[str] = None,
                as_of: Optional[str] = None,
                o3_failed: bool = False,
                market_closed: bool = False) -> Dict[str, Any]:
    """L6: Market-wide Level 3 halt OR one-session fall ≥10% in either index → HALT.

    Kind: breaker
    Scope: account
    State contribution: HALT
    Clearing: Owner re-arms after reconciliation (canonical per Q6)
    """
    prior_latch = cst.get_account_latch(prior_state, 'L6')
    if prior_latch:
        rearm = (
            (not o3_failed) and
            cst.matches_account_rearm(
                authorized_events, 'L6', session=session, as_of=as_of, require_date=True,
            ) is not None
        )

        if rearm:
            return {
                'state': 'NORMAL',
                'would_have_tripped': False,
                'kind': 'breaker',
                'latched': False,
                'reason': 'authorized_rearm'
            }

        return {
            'state': 'HALT',
            'would_have_tripped': True,
            'kind': 'breaker',
            'latched': True,
            'reason': 'latched_prior_trip'
        }

    # Evaluate trigger
    halt_level, invalid_halt_level = _parse_market_halt_level(inputs.get('market_halt_level'))
    index_returns = inputs.get('index_one_session_returns')

    level_3_halt = halt_level == 3

    # Check ALL indices in index_one_session_returns; never abort past an invalid earlier entry.
    # Require >= 2 valid benchmark index returns on non-closed sessions when dict is supplied
    # (note: missing.L6 in frozen v2 golden-table.json requires None -> NORMAL until v3 refreeze).
    index_fall = False
    invalid_index_data = False
    missing_index_data = False
    if index_returns is not None:
        if not isinstance(index_returns, dict):
            invalid_index_data = True
        else:
            if not market_closed and len(index_returns) < 2:
                missing_index_data = True
            for index in sorted(index_returns.keys()):
                ret = index_returns[index]
                r_val = finite_float(ret)
                if r_val is None:
                    invalid_index_data = True
                elif r_val <= -0.10:
                    index_fall = True

    if level_3_halt:
        return {
            'state': 'HALT',
            'would_have_tripped': True,
            'kind': 'breaker',
            'latched': True,
            'reason': 'level_3_halt'
        }
    if index_fall:
        return {
            'state': 'HALT',
            'would_have_tripped': True,
            'kind': 'breaker',
            'latched': True,
            'reason': 'index_fall_10pct'
        }
    if invalid_halt_level or invalid_index_data or missing_index_data:
        return {
            'state': 'HALT',
            'would_have_tripped': True,
            'kind': 'breaker',
            'latched': True,
            'reason': 'invalid_input' if (invalid_halt_level or invalid_index_data) else 'missing_input'
        }

    return {
        'state': 'NORMAL',
        'would_have_tripped': False,
        'kind': 'breaker',
        'latched': False,
        'reason': 'none'
    }
