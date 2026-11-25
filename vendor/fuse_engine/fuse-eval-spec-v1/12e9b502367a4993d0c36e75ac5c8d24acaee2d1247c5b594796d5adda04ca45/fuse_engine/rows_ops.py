"""Data and operations rows O1-O9, J1 per BRIEF-FUSE-1 §4.3 Table C."""

import math
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List
from . import state as st
from . import canonical_state as cst
from .canonical import finite_float


def _to_decimal(value: Any) -> Optional[Decimal]:
    """Convert finite non-bool numeric value/string to exact Decimal, or return None."""
    if finite_float(value) is None:
        return None
    try:
        return Decimal(value.strip()) if isinstance(value, str) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _resolve_o5_alias(
    inputs: Dict[str, Any],
    keys: tuple,
    kind: str,
) -> tuple:
    """Resolve an O5 input alias group uniformly; detect missing/invalid/conflicting values.

    Returns (resolved_value, present_non_none, conflict_or_invalid).
    """
    entries = [(k, inputs[k]) for k in keys if k in inputs and inputs[k] is not None]
    if not entries:
        return None, False, False
    parsed = []
    for _, raw in entries:
        if kind == 'decimal':
            d = _to_decimal(raw)
            if d is None:
                return None, True, True
            parsed.append(d)
        elif kind == 'lower_str':
            if not isinstance(raw, str) or not raw.strip():
                return None, True, True
            parsed.append(raw.strip().lower())
        elif kind == 'str':
            if not isinstance(raw, str) or not raw.strip():
                return None, True, True
            parsed.append(raw.strip())
        elif kind == 'str_list':
            if not isinstance(raw, list) or not all(isinstance(x, str) and x.strip() for x in raw):
                return None, True, True
            parsed.append(tuple(sorted({x.strip() for x in raw})))
        else:
            parsed.append(raw)
    first = parsed[0]
    if any(item != first for item in parsed[1:]):
        return None, True, True
    if kind == 'str_list':
        return list(first), True, False
    return first, True, False


def _parse_iso_ts(value: Any) -> Optional[datetime]:
    """Parse ISO-8601 timestamp normalized to UTC, or return None."""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        dt = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _is_valid_age(age: Any) -> bool:
    """Return True iff age_sessions is a finite non-bool number in [0, 1]."""
    val = finite_float(age)
    return val is not None and 0.0 <= val <= 1.0


def evaluate_O1(inputs: Dict[str, Any], prior_state: Optional[Dict[str, Any]],
                authorized_events: Optional[Dict[str, Any]], session: str,
                as_of: Optional[str] = None) -> Dict[str, Any]:
    """O1: Required input missing, stale, or invalid → NO-NEW-RISK.

    Kind: interlock
    Scope: account
    State contribution: NO-NEW-RISK
    Clearing: Clears after one session of fresh data
    Mechanism B3: fail closed when ANY required source is missing/NaN/stale
    """
    # B3: Check source records for missing, non-finite, negative, or stale (> 1 session)
    source_records = inputs.get('source_records')
    if not isinstance(source_records, dict):
        source_records = {}

    # Required source records
    required_source_names = [
        'market_data',
        'index_returns',
        'volatility',
        'positions',
        'broker_reconciliation',
        'session_calendar'
    ]

    missing_source_records = []
    missing_toplevel_fields = []

    for source_name in required_source_names:
        source_record = source_records.get(source_name)
        if not isinstance(source_record, dict):
            missing_source_records.append(source_name)
            continue

        if not _is_valid_age(source_record.get('age_sessions')):
            missing_source_records.append(source_name)

    # Validate tier unconditionally
    if inputs.get('tier') is None:
        missing_toplevel_fields.append('tier')

    # Validate required account-level numerical evidence whenever operational/portfolio
    # payload is present (removing the 4-field bypass while preserving isolated
    # source_records-only SUC-05 source enumeration).
    has_operational_payload = (
        'funds' in inputs or
        any(k in inputs for k in (
            'bankroll', 'high_water_mark', 'joint_exposure', 'max_loss_cap',
            'positions_cash', 'broker_cash', 'index_one_session_returns',
            'proposed_exposure', 'one_session_loss', 'session_calendar_status',
            'exchange_status', 'loaded_policy_hash', 'registered_policy_hash',
        ))
    )

    cal_status = inputs.get('session_calendar_status') if 'session_calendar_status' in inputs else inputs.get('calendar_state')
    exch_status = inputs.get('exchange_status')
    closed_tokens = ('holiday', 'closed', 'market_closed')
    open_explicit = (cal_status == 'open' or exch_status == 'open')
    is_closed = (
        cal_status in closed_tokens or
        exch_status in closed_tokens or
        (inputs.get('market_closed') is True and cal_status is None and exch_status is None)
    )
    if open_explicit and not (cal_status in closed_tokens or exch_status in closed_tokens):
        is_closed = False

    if has_operational_payload:
        required_values = ['bankroll', 'high_water_mark', 'joint_exposure', 'max_loss_cap']
        for key in required_values:
            value = inputs.get(key)
            if value is None or isinstance(value, bool):
                missing_toplevel_fields.append(key)
                continue
            try:
                val = float(value)
                if math.isnan(val) or math.isinf(val) or val < 0:
                    missing_toplevel_fields.append(key)
            except (TypeError, ValueError):
                missing_toplevel_fields.append(key)

        if not is_closed and (
            'index_one_session_returns' in inputs or
            cal_status == 'open' or
            exch_status == 'open'
        ):
            idx_rets = inputs.get('index_one_session_returns')
            if (
                not isinstance(idx_rets, dict) or
                len(idx_rets) < 2 or
                any(finite_float(v) is None for v in idx_rets.values())
            ):
                missing_toplevel_fields.append('index_one_session_returns')

    # B3: If ANY source or field is missing/stale/non-finite, fail closed
    has_issues = bool(missing_source_records or missing_toplevel_fields)

    if has_issues:
        if len(missing_source_records) == 1 and not missing_toplevel_fields and not has_operational_payload:
            reason = f'missing_required_source_{missing_source_records[0]}'
        else:
            reason = 'stale_or_missing_input'

        return {
            'state': 'NO-NEW-RISK',
            'would_have_tripped': True,
            'kind': 'interlock',
            'latched': False,
            'reason': reason
        }

    # Same-session stale->fresh does NOT clear within the same trading session
    cur_date = cst.canonical_session_date(session, as_of)
    prior_o1 = cst.get_o1_fresh_state(prior_state)
    if prior_o1.get('last_tripped_session') is not None and prior_o1.get('last_tripped_session') == cur_date:
        return {
            'state': 'NO-NEW-RISK',
            'would_have_tripped': True,
            'kind': 'interlock',
            'latched': False,
            'reason': 'clearing_period'
        }

    # All required inputs present and fresh (SUC-05 step 7)
    return {
        'state': 'NORMAL',
        'would_have_tripped': False,
        'kind': 'interlock',
        'latched': False,
        'reason': 'none'
    }


VALID_CALENDAR_STATUSES = {'open', 'closed', 'holiday', 'half_day', 'half_day_cutoff', 'market_closed'}
CLOSED_CALENDAR_STATUSES = {'closed', 'holiday', 'half_day', 'half_day_cutoff', 'market_closed'}


def evaluate_O2(inputs: Dict[str, Any], prior_state: Optional[Dict[str, Any]],
                authorized_events: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """O2: Session calendar closed/holiday/disagreement → no order.

    Kind: interlock
    Scope: account
    State contribution: Order suppression (no control state)
    """
    # Evaluate trigger
    calendar_status = inputs.get('session_calendar_status')
    exchange_status = inputs.get('exchange_status')

    # Fail closed on missing inputs
    if calendar_status is None or exchange_status is None:
        return {
            'state': 'NORMAL',
            'would_have_tripped': True,
            'kind': 'interlock',
            'latched': False,
            'reason': 'missing_input'
        }

    # Whitelist valid calendar/exchange status tokens (reject unrecognized tokens like "n/a")
    if (
        not isinstance(calendar_status, str) or
        not isinstance(exchange_status, str) or
        calendar_status not in VALID_CALENDAR_STATUSES or
        exchange_status not in VALID_CALENDAR_STATUSES
    ):
        return {
            'state': 'NORMAL',
            'would_have_tripped': True,
            'kind': 'interlock',
            'latched': False,
            'reason': 'invalid_input'
        }

    # Check triggers (Fix 3: half_day also uses market_closed reason)
    is_market_closed = calendar_status in CLOSED_CALENDAR_STATUSES or exchange_status in ('closed', 'holiday', 'market_closed')
    disagreement = calendar_status != exchange_status

    if is_market_closed or disagreement:
        return {
            'state': 'NORMAL',
            'would_have_tripped': True,
            'kind': 'interlock',
            'latched': False,
            'reason': 'market_closed' if is_market_closed else 'calendar_disagreement'
        }

    return {
        'state': 'NORMAL',
        'would_have_tripped': False,
        'kind': 'interlock',
        'latched': False,
        'reason': 'none'
    }


def evaluate_O3(inputs: Dict[str, Any], prior_state: Optional[Dict[str, Any]],
                authorized_events: Optional[Dict[str, Any]],
                session: Optional[str] = None,
                as_of: Optional[str] = None) -> Dict[str, Any]:
    """O3: Position/cash mismatch beyond tolerance → HALT.

    Kind: breaker
    Scope: account
    State contribution: HALT
    Clearing: Owner re-arms after reconciliation
    """
    # Check for prior latch (canonical format per Q6)
    prior_latch = cst.get_account_latch(prior_state, 'O3')
    if prior_latch:
        rearm = cst.matches_account_rearm(
            authorized_events, 'O3', session=session, as_of=as_of, require_date=True,
        ) is not None

        if not rearm:
            # Stay latched
            return {
                'state': 'HALT',
                'would_have_tripped': True,
                'kind': 'breaker',
                'latched': True,
                'reason': 'latched_prior_trip'
            }

    # Evaluate trigger using positions and broker reconciliation (explicit tolerance required)
    positions_cash = finite_float(inputs.get('positions_cash'))
    broker_cash = finite_float(inputs.get('broker_cash'))
    reconciliation_tolerance = finite_float(inputs.get('reconciliation_tolerance'))

    # Fail closed if cash or fund positions or tolerance is missing/empty/non-finite/negative
    if (
        'funds' not in inputs or
        not isinstance(inputs.get('funds'), dict) or
        len(inputs['funds']) == 0 or
        positions_cash is None or broker_cash is None or
        reconciliation_tolerance is None or reconciliation_tolerance < 0.0
    ):
        return {
            'state': 'HALT',
            'would_have_tripped': True,
            'kind': 'breaker',
            'latched': True,
            'reason': 'missing_input'
        }

    funds = inputs['funds']

    # Check cash reconciliation
    cash_mismatch = abs(positions_cash - broker_cash) > reconciliation_tolerance

    # Check fund position reconciliation (BOTH share counts and market values required for each fund)
    position_mismatch = False
    if not isinstance(funds, dict):
        return {
            'state': 'HALT',
            'would_have_tripped': True,
            'kind': 'breaker',
            'latched': True,
            'reason': 'missing_input'
        }
    for symbol, fund_data in sorted(funds.items()):
        if not isinstance(fund_data, dict):
            return {
                'state': 'HALT',
                'would_have_tripped': True,
                'kind': 'breaker',
                'latched': True,
                'reason': 'missing_input'
            }
        ledger_value = finite_float(fund_data.get('ledger_value'))
        broker_value = finite_float(fund_data.get('broker_value'))
        share_count = finite_float(fund_data.get('share_count'))
        broker_pos = finite_float(fund_data.get('broker_position'))
        if ledger_value is None or broker_value is None or share_count is None or broker_pos is None:
            return {
                'state': 'HALT',
                'would_have_tripped': True,
                'kind': 'breaker',
                'latched': True,
                'reason': 'missing_input'
            }
        if abs(ledger_value - broker_value) > reconciliation_tolerance or share_count != broker_pos:
            position_mismatch = True
            break

    # Check trigger
    if cash_mismatch or position_mismatch:
        return {
            'state': 'HALT',
            'would_have_tripped': True,
            'kind': 'breaker',
            'latched': True,
            'reason': 'reconciliation_mismatch'
        }

    return {
        'state': 'NORMAL',
        'would_have_tripped': False,
        'kind': 'breaker',
        'latched': False,
        'reason': 'none'
    }


def evaluate_O4(inputs: Dict[str, Any], prior_state: Optional[Dict[str, Any]],
                authorized_events: Optional[Dict[str, Any]], symbol: str,
                session: Optional[str] = None, as_of: Optional[str] = None) -> Dict[str, Any]:
    """O4: Order timeout → ORDER-UNKNOWN for that symbol.

    Kind: interlock_or_breaker
    Scope: symbol
    State contribution: ORDER-UNKNOWN
    Clearing: Clears on definitive status for the ORIGINAL order or owner Q4 re-arm;
             otherwise latches
    Mechanism B6: ORDER-UNKNOWN keyed to original order identity
    """
    scope = f'symbol:{symbol}'

    # B6: Check for prior ORDER-UNKNOWN with original order identity (canonical per Q6)
    restart_marker = inputs.get('restart_marker', False)
    prior_pending = cst.get_o4_pending_order(prior_state, symbol)

    # Get current pending order status safely even if pending_orders is None or malformed
    po_map = inputs.get('pending_orders')
    raw_po = po_map.get(symbol) if isinstance(po_map, dict) else None
    pending_order = inputs['o4_pending_order'] if 'o4_pending_order' in inputs else raw_po

    # If no pending order input (or empty dict)
    if pending_order is None or (isinstance(pending_order, dict) and not pending_order):
        # B6: Check if we have a prior pending order (allow dated symbol-scoped Q4 re-arm to clear)
        if prior_pending:
            if cst.matches_symbol_rearm(
                authorized_events, 'O4', symbol, session=session, as_of=as_of, require_date=True
            ) is not None:
                return {
                    'state': 'NORMAL',
                    'would_have_tripped': False,
                    'kind': 'interlock_or_breaker',
                    'latched': False,
                    'reason': 'authorized_rearm',
                    'symbol': symbol
                }
            return {
                'state': 'ORDER-UNKNOWN',
                'would_have_tripped': True,
                'kind': 'interlock_or_breaker',
                'latched': False,
                'reason': 'awaiting_original_order_status',
                'symbol': symbol,
                'original_order_id': prior_pending['original_order_id']
            }
        # No pending order
        return {
            'state': 'NORMAL',
            'would_have_tripped': False,
            'kind': 'interlock_or_breaker',
            'latched': False,
            'reason': 'none',
            'symbol': symbol
        }

    # Validate pending_order structure and assign durable unidentified_order_id when absent
    if not isinstance(pending_order, dict):
        return {
            'state': 'ORDER-UNKNOWN',
            'would_have_tripped': True,
            'kind': 'interlock_or_breaker',
            'latched': True,
            'reason': 'missing_input',
            'symbol': symbol,
            'original_order_id': prior_pending['original_order_id'] if prior_pending else f'unidentified_order_{symbol}',
        }

    order_id = pending_order.get('order_id')
    status = pending_order.get('status')
    timed_out = pending_order.get('timeout', False) or pending_order.get('timed_out', False)
    terminal_statuses = ('filled', 'cancelled', 'canceled', 'rejected', 'expired')
    has_valid_order_id = isinstance(order_id, str) and bool(order_id.strip())
    is_terminal = status in terminal_statuses

    # B6: If we have a prior pending order, check if this is the same order
    if prior_pending:
        prior_order_id = prior_pending.get('original_order_id')
        unidentified_prior = isinstance(prior_order_id, str) and prior_order_id.startswith('unidentified_order_')

        # Missing/mismatched order_id keeps the original order restricted in ORDER-UNKNOWN
        if unidentified_prior or not has_valid_order_id or order_id != prior_order_id:
            reason = 'awaiting_original_order_status_after_restart' if restart_marker else 'awaiting_original_order_status'
            return {
                'state': 'ORDER-UNKNOWN',
                'would_have_tripped': True,
                'kind': 'interlock_or_breaker',
                'latched': False,
                'reason': reason,
                'symbol': symbol,
                'original_order_id': prior_order_id
            }

        # Same order - check if we have definitive terminal status
        # B6: Only recognized terminal statuses clear ORDER-UNKNOWN (ignore unvalidated terminal flags)
        if is_terminal:
            # Clears (SUC-09 step 4)
            return {
                'state': 'NORMAL',
                'would_have_tripped': False,
                'kind': 'interlock_or_breaker',
                'latched': False,
                'reason': 'original_order_terminal_status_received',
                'symbol': symbol
            }

        # B6: Unrecognized status does NOT clear ORDER-UNKNOWN (SUC-09 step 3)
        # Still waiting for definitive status
        is_unrecognized = not is_terminal and status not in ['pending', 'submitted', 'accepted']
        reason = 'unrecognized_status_for_original_order' if is_unrecognized else 'awaiting_original_order_status'
        if restart_marker:
            reason = 'awaiting_original_order_status_after_restart'

        return {
            'state': 'ORDER-UNKNOWN',
            'would_have_tripped': True,
            'kind': 'interlock_or_breaker',
            'latched': False,
            'reason': reason,
            'symbol': symbol,
            'original_order_id': prior_order_id
        }

    # No prior pending: identified order already reporting definitive terminal status does not enter timeout
    if has_valid_order_id and is_terminal:
        return {
            'state': 'NORMAL',
            'would_have_tripped': False,
            'kind': 'interlock_or_breaker',
            'latched': False,
            'reason': 'none',
            'symbol': symbol
        }

    # No prior pending - check if current order timed out (SUC-09 step 1)
    if timed_out:
        eff_order_id = order_id if (isinstance(order_id, str) and order_id.strip()) else f'unidentified_order_{symbol}'
        return {
            'state': 'ORDER-UNKNOWN',
            'would_have_tripped': True,
            'kind': 'interlock_or_breaker',
            'latched': False,
            'reason': 'order_timeout',
            'symbol': symbol,
            'original_order_id': eff_order_id
        }

    # No timeout
    return {
        'state': 'NORMAL',
        'would_have_tripped': False,
        'kind': 'interlock_or_breaker',
        'latched': False,
        'reason': 'none',
        'symbol': symbol
    }


def evaluate_O5(inputs: Dict[str, Any], prior_state: Optional[Dict[str, Any]],
                authorized_events: Optional[Dict[str, Any]], session: str,
                as_of: Optional[str] = None) -> Dict[str, Any]:
    """O5: Pre-trade limit violation → reject; third rejection in session → HALT.

    Kind: breaker
    Scope: account
    State contribution: HALT on third rejection
    Clearing: Owner re-arms
    Mechanism B2: rejection count is per-session (canonical format per Q6),
                  third rejection latches HALT
    Required inputs: o5_collar (or fail closed)
    """
    # Get prior O5 state from canonical format
    o5_state = cst.get_o5_state(prior_state)
    prior_latched = o5_state['latched']
    prior_count = o5_state['rejection_count']
    last_rejection_session = o5_state['last_rejection_session']

    # Check for prior latch (SUC-04: third rejection); clear on Q4 rearm then evaluate current session
    rearm_cleared = False
    if prior_latched:
        rearm = cst.matches_account_rearm(
            authorized_events, 'O5', session=session, as_of=as_of, require_date=True
        ) is not None

        if not rearm:
            # Stay latched (SUC-04 step 4)
            return {
                'state': 'HALT',
                'would_have_tripped': True,
                'kind': 'breaker',
                'latched': True,
                'reason': 'latched_third_rejection',
                'rejection_count_this_session': prior_count  # Emit per Q6
            }

        # Q4: authorized re-arm clears O5 latch and counter, then still validates current-session order inputs
        rearm_cleared = True
        prior_count = 0
        last_rejection_session = session

    # B2: New session resets a free counter; latch survives
    if last_rejection_session != session:
        rejection_count = 0
    else:
        rejection_count = prior_count

    # Evaluate trigger - fail closed if collar unresolved or invalid (NaN/inf/bool/negative) per §4.5 / A11
    o5_collar = inputs.get('o5_collar')
    if o5_collar is None or o5_collar == 'unresolved':
        new_count = rejection_count + 1
        if new_count >= 3:
            return {
                'state': 'HALT',
                'would_have_tripped': True,
                'kind': 'breaker',
                'latched': True,
                'reason': 'third_rejection_in_session',
                'rejection_count_this_session': new_count
            }
        return {
            'state': 'NORMAL',
            'would_have_tripped': True,
            'kind': 'breaker',
            'latched': False,
            'reason': 'unresolved_fail_closed',
            'rejection_count_this_session': new_count
        }

    collar_pct = finite_float(o5_collar)
    if collar_pct is None or collar_pct < 0.0:
        new_count = rejection_count + 1
        if new_count >= 3:
            return {
                'state': 'HALT',
                'would_have_tripped': True,
                'kind': 'breaker',
                'latched': True,
                'reason': 'third_rejection_in_session',
                'rejection_count_this_session': new_count
            }
        return {
            'state': 'NORMAL',
            'would_have_tripped': True,
            'kind': 'breaker',
            'latched': False,
            'reason': 'invalid_input',
            'rejection_count_this_session': new_count
        }

    # Check other pre-trade limits via unified alias resolution + exact Decimal arithmetic
    violations = []

    symbol, symbol_present, symbol_conflict = _resolve_o5_alias(inputs, ('o5_symbol', 'symbol'), 'str')
    whitelist, whitelist_present, whitelist_conflict = _resolve_o5_alias(
        inputs, ('symbol_whitelist', 'o5_symbol_whitelist'), 'str_list'
    )
    order_size, qty_present, qty_conflict = _resolve_o5_alias(
        inputs, ('o5_order_size', 'order_size', 'o5_order_qty', 'order_qty', 'qty'), 'decimal'
    )
    side, side_present, side_conflict = _resolve_o5_alias(inputs, ('o5_side', 'side', 'order_side'), 'lower_str')
    ov_dec, ov_present, ov_conflict = _resolve_o5_alias(inputs, ('o5_order_value', 'order_value'), 'decimal')
    unit_price_dec, unit_price_present, unit_price_conflict = _resolve_o5_alias(
        inputs,
        ('o5_order_price', 'o5_limit_price', 'limit_price', 'order_price', 'unit_price', 'o5_unit_price', 'price'),
        'decimal',
    )
    ref_price_dec, ref_price_present, ref_price_conflict = _resolve_o5_alias(
        inputs, ('o5_reference_price', 'reference_price'), 'decimal'
    )
    account, acct_present, acct_conflict = _resolve_o5_alias(inputs, ('account', 'o5_account'), 'str')
    account_whitelist, acct_wl_present, acct_wl_conflict = _resolve_o5_alias(
        inputs, ('account_whitelist', 'o5_account_whitelist'), 'str_list'
    )

    order_present = any((
        symbol_present, symbol_conflict,
        qty_present, qty_conflict,
        side_present, side_conflict,
        ov_present, ov_conflict,
        unit_price_present, unit_price_conflict,
        ref_price_present, ref_price_conflict,
    ))

    explicit_null_symbol_whitelist = (
        ('symbol_whitelist' in inputs and inputs['symbol_whitelist'] is None) or
        ('o5_symbol_whitelist' in inputs and inputs['o5_symbol_whitelist'] is None)
    )
    if not whitelist_present and not whitelist_conflict and not explicit_null_symbol_whitelist:
        whitelist = ['TQQQ', 'SOXL']
    empty_explicit_symbol_whitelist = (
        (whitelist_present and isinstance(whitelist, list) and len(whitelist) == 0)
        and order_present
    )
    if (
        symbol_conflict or
        whitelist_conflict or
        explicit_null_symbol_whitelist or
        empty_explicit_symbol_whitelist or
        (order_present and (symbol is None or not symbol.strip())) or
        (symbol is not None and (not isinstance(whitelist, list) or symbol not in whitelist))
    ):
        violations.append('symbol_not_whitelisted')

    # Fail closed on conflicting, non-finite, or <= 0 quantities
    if qty_conflict or (qty_present and (order_size is None or order_size <= Decimal('0'))):
        violations.append('invalid_order_size')
    explicit_null_whitelist = (
        ('account_whitelist' in inputs and inputs['account_whitelist'] is None) or
        ('o5_account_whitelist' in inputs and inputs['o5_account_whitelist'] is None)
    )
    invalid_whitelist = (
        acct_wl_conflict or
        not isinstance(account_whitelist, list) or
        not any(isinstance(a, str) and a.strip() for a in account_whitelist)
    )
    explicit_null_account = (
        ('account' in inputs and inputs['account'] is None) or
        ('o5_account' in inputs and inputs['o5_account'] is None)
    )
    if (
        acct_conflict or
        explicit_null_whitelist or
        (acct_wl_present and invalid_whitelist) or
        (acct_present and (invalid_whitelist or account not in account_whitelist)) or
        (order_present and (not acct_present or explicit_null_account or invalid_whitelist or account not in account_whitelist))
    ):
        violations.append('account_not_whitelisted')

    # Long only: require explicit buy/sell direction on active orders; allow reducing sells within existing long position
    if side_conflict or (order_present and side is None):
        violations.append('not_long_only')
    elif side is not None:
        if side == 'buy':
            pass
        elif side == 'sell':
            funds_map = inputs.get('funds')
            sym_data = funds_map.get(symbol) if (isinstance(funds_map, dict) and symbol is not None) else None
            held_raw = None
            if isinstance(sym_data, dict):
                held_raw = sym_data.get('share_count')
                if held_raw is None:
                    held_raw = sym_data.get('position')
            held_shares_dec = _to_decimal(held_raw)
            if not (
                not qty_conflict and
                order_size is not None and order_size > Decimal('0') and
                held_shares_dec is not None and held_shares_dec > Decimal('0') and
                order_size <= held_shares_dec
            ):
                violations.append('not_long_only')
        else:
            violations.append('not_long_only')

    # Within settled cash (canonical 'settled_cash' wins cleanly over 'o5_settled_cash'; exact Decimal arithmetic)
    eff_price_dec = (
        unit_price_dec
        if (unit_price_present and not unit_price_conflict and unit_price_dec is not None)
        else ref_price_dec
    )
    derived_notional = None
    notional_invalid = False
    if (qty_present or qty_conflict) and (unit_price_present or unit_price_conflict or ref_price_present or ref_price_conflict):
        if (
            qty_conflict or unit_price_conflict or ref_price_conflict or
            order_size is None or order_size <= Decimal('0') or
            eff_price_dec is None or eff_price_dec < Decimal('0')
        ):
            notional_invalid = True
        else:
            derived_notional = order_size * eff_price_dec

    settled_cash_raw = inputs['settled_cash'] if 'settled_cash' in inputs else inputs.get('o5_settled_cash')
    sc_dec = _to_decimal(settled_cash_raw)
    explicit_buy = side_conflict or (side == 'buy') or (side is None and (qty_present or qty_conflict))

    if ov_conflict or (ov_present and (ov_dec is None or ov_dec < Decimal('0'))):
        violations.append('exceeds_settled_cash' if side != 'sell' else 'invalid_order_value')
    elif side != 'sell':
        if notional_invalid or unit_price_conflict or ref_price_conflict:
            violations.append('exceeds_settled_cash')
        elif ov_present or derived_notional is not None:
            if explicit_buy and ov_dec is not None and (
                ov_dec <= Decimal('0') or
                (derived_notional is not None and ov_dec < derived_notional)
            ):
                violations.append('exceeds_settled_cash')
            else:
                if ov_dec is not None and derived_notional is not None:
                    eff_order_val = max(ov_dec, derived_notional)
                elif ov_dec is not None:
                    eff_order_val = ov_dec
                else:
                    eff_order_val = derived_notional
                if eff_order_val is None or sc_dec is None or sc_dec < Decimal('0') or eff_order_val > sc_dec:
                    violations.append('exceeds_settled_cash')
        elif explicit_buy:
            violations.append('exceeds_settled_cash')

    # Maximum order size (enforce explicit qty AND implied_qty = order_value / eff_price whenever order_value or no qty)
    max_order_size_val, msize_present, msize_conflict = _resolve_o5_alias(
        inputs, ('o5_max_order_size', 'max_order_size'), 'decimal'
    )
    if msize_conflict or (msize_present and (max_order_size_val is None or max_order_size_val < Decimal('0'))):
        violations.append('exceeds_max_order_size')
    elif max_order_size_val is not None and (order_present or qty_present or qty_conflict):
        qty_bad = (
            (qty_present or qty_conflict) and
            (qty_conflict or order_size is None or order_size <= Decimal('0') or order_size > max_order_size_val)
        )
        ov_bad = (
            (ov_present or ov_conflict or not (qty_present or qty_conflict)) and
            (
                ov_conflict or not ov_present or ov_dec is None or ov_dec <= Decimal('0') or
                unit_price_conflict or ref_price_conflict or
                eff_price_dec is None or eff_price_dec <= Decimal('0') or
                ov_dec > max_order_size_val * eff_price_dec
            )
        )
        if qty_bad or ov_bad:
            violations.append('exceeds_max_order_size')

    # Maximum orders per session (fail closed on missing/null counter when order_present, or conflicting/invalid values)
    orders_today, orders_present, orders_conflict = _resolve_o5_alias(
        inputs, ('o5_orders_today', 'session_order_count', 'orders_today'), 'decimal'
    )
    max_orders, max_orders_present, max_orders_conflict = _resolve_o5_alias(
        inputs, ('o5_max_orders_per_session', 'max_orders_per_session'), 'decimal'
    )
    if (
        orders_conflict or max_orders_conflict or
        (orders_present and (orders_today is None or orders_today < Decimal('0'))) or
        (max_orders_present and (max_orders is None or max_orders < Decimal('0')))
    ):
        violations.append('exceeds_max_orders')
    elif max_orders is not None:
        if order_present and not orders_present:
            violations.append('exceeds_max_orders')
        else:
            ot = orders_today if orders_present else Decimal('0')
            if ot >= max_orders:
                violations.append('exceeds_max_orders')

    # Turnover limit (fail closed on missing/null counter when order_present, invalid aliases, or projected turnover overrun)
    turnover_today, turnover_present, turnover_conflict = _resolve_o5_alias(
        inputs, ('o5_turnover_today', 'session_turnover', 'turnover_today'), 'decimal'
    )
    max_turnover, max_turnover_present, max_turnover_conflict = _resolve_o5_alias(
        inputs, ('o5_max_turnover_per_session', 'max_turnover_per_session'), 'decimal'
    )
    if (
        turnover_conflict or max_turnover_conflict or
        (turnover_present and (turnover_today is None or turnover_today < Decimal('0'))) or
        (max_turnover_present and (max_turnover is None or max_turnover < Decimal('0')))
    ):
        violations.append('exceeds_max_turnover')
    elif max_turnover is not None:
        if order_present:
            if not turnover_present:
                violations.append('exceeds_max_turnover')
            else:
                tt = turnover_today
                if ov_dec is not None and derived_notional is not None:
                    proposed_notional = max(ov_dec, derived_notional)
                elif ov_dec is not None:
                    proposed_notional = ov_dec
                else:
                    proposed_notional = derived_notional
                if (
                    ov_conflict or
                    (ov_present and (ov_dec is None or ov_dec < Decimal('0'))) or
                    qty_conflict or
                    (qty_present and (order_size is None or order_size <= Decimal('0'))) or
                    notional_invalid or unit_price_conflict or ref_price_conflict or
                    proposed_notional is None or proposed_notional < Decimal('0') or
                    (tt + proposed_notional) > max_turnover
                ):
                    violations.append('exceeds_max_turnover')
        elif turnover_present and turnover_today > max_turnover:
            violations.append('exceeds_max_turnover')

    # Price collar (always enforce on active orders via exact Decimal arithmetic against collar_pct)
    if order_present or unit_price_present or unit_price_conflict or ref_price_present or ref_price_conflict:
        collar_dec = _to_decimal(o5_collar)
        if (
            unit_price_conflict or ref_price_conflict or
            unit_price_dec is None or ref_price_dec is None or
            unit_price_dec <= Decimal('0') or ref_price_dec <= Decimal('0') or
            collar_dec is None or collar_dec < Decimal('0')
        ):
            violations.append('invalid_price')
        else:
            lower_bound = ref_price_dec * (Decimal('1') - collar_dec)
            upper_bound = ref_price_dec * (Decimal('1') + collar_dec)
            if unit_price_dec < lower_bound or unit_price_dec > upper_bound:
                violations.append('outside_price_collar')

    # If any violations, count as rejection
    if violations:
        new_count = rejection_count + 1

        if new_count >= 3:
            # Third rejection - latch HALT (SUC-04)
            return {
                'state': 'HALT',
                'would_have_tripped': True,
                'kind': 'breaker',
                'latched': True,
                'reason': 'third_rejection_in_session',
                'rejection_count_this_session': new_count  # Emit per Q6
            }

        # First or second rejection
        return {
            'state': 'NORMAL',
            'would_have_tripped': True,
            'kind': 'breaker',
            'latched': False,
            'reason': 'pre_trade_limit_violation',
            'rejection_count_this_session': new_count  # Emit per Q6
        }

    # No violations (preserve session rejection count across intervening passing checks)
    return {
        'state': 'NORMAL',
        'would_have_tripped': False,
        'kind': 'breaker',
        'latched': False,
        'reason': 'authorized_rearm' if rearm_cleared else 'none',
        'rejection_count_this_session': rejection_count
    }


def evaluate_O6(inputs: Dict[str, Any], prior_state: Optional[Dict[str, Any]],
                authorized_events: Optional[Dict[str, Any]],
                session: Optional[str] = None,
                as_of: Optional[str] = None) -> Dict[str, Any]:
    """O6: Policy/config hash mismatch → HALT.

    Kind: breaker
    Scope: account
    State contribution: HALT
    Clearing: Owner re-arms
    """
    # Check for prior latch (canonical format per Q6)
    prior_latch = cst.get_account_latch(prior_state, 'O6')
    if prior_latch:
        rearm = cst.matches_account_rearm(
            authorized_events, 'O6', session=session, as_of=as_of, require_date=True,
        ) is not None

        if not rearm:
            # Stay latched
            return {
                'state': 'HALT',
                'would_have_tripped': True,
                'kind': 'breaker',
                'latched': True,
                'reason': 'latched_prior_trip'
            }

    # Evaluate trigger - require non-empty string policy hashes
    loaded_hash = inputs.get('loaded_policy_hash')
    registered_hash = inputs.get('registered_policy_hash')

    # Fail closed on missing or blank/non-string policy hashes
    if (
        not isinstance(loaded_hash, str) or not loaded_hash.strip() or
        not isinstance(registered_hash, str) or not registered_hash.strip()
    ):
        return {
            'state': 'HALT',
            'would_have_tripped': True,
            'kind': 'breaker',
            'latched': True,
            'reason': 'missing_input'
        }

    # Check trigger
    if loaded_hash != registered_hash:
        return {
            'state': 'HALT',
            'would_have_tripped': True,
            'kind': 'breaker',
            'latched': True,
            'reason': 'policy_hash_mismatch'
        }

    return {
        'state': 'NORMAL',
        'would_have_tripped': False,
        'kind': 'breaker',
        'latched': False,
        'reason': 'none'
    }


def evaluate_O7(inputs: Dict[str, Any], prior_state: Optional[Dict[str, Any]],
                authorized_events: Optional[Dict[str, Any]], as_of: Optional[str] = None,
                session: Optional[str] = None) -> Dict[str, Any]:
    """O7: Dead-man's switch → NO-NEW-RISK until human re-arm (§4.3 / spec.json:262).

    Kind: breaker
    Scope: account
    State contribution: NO-NEW-RISK
    Clearing: Human re-arm
    """
    # Check for prior latch (canonical format per Q6)
    prior_latch = cst.get_account_latch(prior_state, 'O7')
    if prior_latch:
        rearm = cst.matches_account_rearm(
            authorized_events, 'O7', session=session, as_of=as_of, require_date=True
        ) is not None

        if rearm:
            return {
                'state': 'NORMAL',
                'would_have_tripped': False,
                'kind': 'breaker',
                'latched': False,
                'reason': 'authorized_rearm'
            }

        # Stay latched at account-level NO-NEW-RISK
        return {
            'state': 'NO-NEW-RISK',
            'would_have_tripped': True,
            'kind': 'breaker',
            'latched': True,
            'reason': 'latched_prior_trip'
        }

    # Evaluate trigger: explicit silence flags OR missing/invalid/stale heartbeat > 1 scheduled_interval
    runtime_silent = bool(inputs.get('o7_runtime_silent', False))
    monitor_silent = bool(inputs.get('o7_monitor_silent', False))

    heartbeat_stale = False
    raw_hb = inputs.get('last_heartbeat')
    raw_interval = inputs.get('scheduled_interval')
    hb_dt = _parse_iso_ts(raw_hb)
    as_of_dt = _parse_iso_ts(as_of)
    interval = finite_float(raw_interval)
    if hb_dt is None or as_of_dt is None or interval is None or interval <= 0.0:
        heartbeat_stale = True
    else:
        elapsed = (as_of_dt - hb_dt).total_seconds()
        if elapsed < 0.0 or elapsed > interval:
            heartbeat_stale = True

    # Check trigger
    if runtime_silent or monitor_silent or heartbeat_stale:
        return {
            'state': 'NO-NEW-RISK',
            'would_have_tripped': True,
            'kind': 'breaker',
            'latched': True,
            'reason': 'dead_mans_switch'
        }

    return {
        'state': 'NORMAL',
        'would_have_tripped': False,
        'kind': 'breaker',
        'latched': False,
        'reason': 'none'
    }


def evaluate_O8(inputs: Dict[str, Any], prior_state: Optional[Dict[str, Any]],
                authorized_events: Optional[Dict[str, Any]],
                session: Optional[str] = None,
                as_of: Optional[str] = None) -> Dict[str, Any]:
    """O8: Kill switch → HALT.

    Kind: owner
    Scope: account
    State contribution: HALT
    Clearing: Owner command via authorized_events
    """
    events_list = []
    if isinstance(authorized_events, list):
        events_list = authorized_events
    elif isinstance(authorized_events, dict):
        events_list = authorized_events.get('account', [])

    # Check for active owner kill_switch command in authorized_events
    kill_command = False
    for event in events_list:
        if not isinstance(event, dict) or event.get('type') != 'kill_switch':
            continue
        if event.get('fund') or event.get('symbol'):
            continue
        if 'scope' in event and event.get('scope') != 'account':
            continue
        if not cst.event_date_matches(event, session=session, as_of=as_of, require_date=False):
            continue
        kill_command = True
        break

    if kill_command:
        return {
            'state': 'HALT',
            'would_have_tripped': True,
            'kind': 'owner',
            'latched': True,
            'reason': 'kill_switch'
        }

    # Check persisted O8 latch (requires explicit Q4-validated clear/rearm event)
    prior_latch = cst.get_account_latch(prior_state, 'O8')
    if prior_latch:
        cleared = cst.matches_o8_clear(
            authorized_events, session=session, as_of=as_of, require_date=True
        ) is not None
        if cleared:
            return {
                'state': 'NORMAL',
                'would_have_tripped': False,
                'kind': 'owner',
                'latched': False,
                'reason': 'authorized_rearm'
            }
        return {
            'state': 'HALT',
            'would_have_tripped': True,
            'kind': 'owner',
            'latched': True,
            'reason': 'latched_prior_trip'
        }

    return {
        'state': 'NORMAL',
        'would_have_tripped': False,
        'kind': 'owner',
        'latched': False,
        'reason': 'none'
    }


def evaluate_O9(inputs: Dict[str, Any], prior_state: Optional[Dict[str, Any]],
                authorized_events: Optional[Dict[str, Any]],
                session: Optional[str] = None,
                as_of: Optional[str] = None) -> Dict[str, Any]:
    """O9: Parameter change without owner card → HALT.

    Kind: breaker
    Scope: account
    State contribution: HALT
    Clearing: Owner re-arms
    """
    # Check for prior latch (canonical format per Q6); clear on Q4 rearm then evaluate current session
    rearm_cleared = False
    prior_latch = cst.get_account_latch(prior_state, 'O9')
    if prior_latch:
        rearm = cst.matches_account_rearm(
            authorized_events, 'O9', session=session, as_of=as_of, require_date=True
        ) is not None

        if not rearm:
            # Stay latched
            return {
                'state': 'HALT',
                'would_have_tripped': True,
                'kind': 'breaker',
                'latched': True,
                'reason': 'latched_prior_trip'
            }

        rearm_cleared = True

    # Check for parameter changes (Fix 4: tier_change detection)
    change_descriptors = inputs.get('o9_change_descriptors')
    tier_change_event = inputs.get('tier_change_event')

    # Check for any change signal
    has_change = bool(change_descriptors) or bool(tier_change_event)

    if not has_change:
        return {
            'state': 'NORMAL',
            'would_have_tripped': False,
            'kind': 'breaker',
            'latched': False,
            'reason': 'authorized_rearm' if rearm_cleared else 'none'
        }

    # Validate each privileged change kind independently against authorized_events
    events_list = []
    if isinstance(authorized_events, list):
        events_list = authorized_events
    elif isinstance(authorized_events, dict):
        events_list = authorized_events.get('account', [])

    def _parse_event_scope(event_or_desc: Dict[str, Any]) -> tuple:
        """Return (ok, target_scope_or_none) for card or descriptor scope tags."""
        ev_fund = event_or_desc.get('fund')
        ev_symbol = event_or_desc.get('symbol')
        has_scope = 'scope' in event_or_desc
        ev_scope = event_or_desc.get('scope')
        if ev_fund is not None and (not isinstance(ev_fund, str) or not ev_fund.strip()):
            return False, None
        if ev_symbol is not None and (not isinstance(ev_symbol, str) or not ev_symbol.strip()):
            return False, None
        if ev_fund is not None and ev_symbol is not None and ev_fund.strip() != ev_symbol.strip():
            return False, None
        target = ev_fund.strip() if isinstance(ev_fund, str) else (ev_symbol.strip() if isinstance(ev_symbol, str) else None)
        if has_scope:
            if not isinstance(ev_scope, str) or not ev_scope.strip():
                return False, None
            sc = ev_scope.strip()
            if sc == 'account':
                if target is not None:
                    return False, None
            elif sc.startswith('fund:') or sc.startswith('symbol:'):
                sc_target = sc.split(':', 1)[1].strip()
                if not sc_target or (target is not None and target != sc_target):
                    return False, None
                target = sc_target
            else:
                return False, None
        return True, target

    def _card_row_date_ok(event: Any) -> bool:
        if not isinstance(event, dict):
            return False
        if 'row' in event or 'row_id' in event:
            if not cst.event_matches_row(event, 'O9'):
                return False
        return cst.event_date_matches(event, session=session, as_of=as_of, require_date=True)

    def _tier_card_scope_date_ok(event: Any) -> bool:
        if not _card_row_date_ok(event):
            return False
        if event.get('fund') or event.get('symbol'):
            return False
        if 'scope' in event and event.get('scope') != 'account':
            return False
        return True

    def _param_card_scope_date_ok(event: Any) -> tuple:
        if not _card_row_date_ok(event):
            return False, None
        return _parse_event_scope(event)

    def _extract_tier_target(obj: Any) -> Optional[str]:
        if not isinstance(obj, dict):
            return None
        for k in ('to', 'new', 'target', 'new_tier'):
            v = obj.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        return None

    tier_authorized = True
    if tier_change_event:
        target_tier = _extract_tier_target(tier_change_event)
        live_tier = inputs.get('tier')
        from_tier = tier_change_event.get('from') if isinstance(tier_change_event, dict) else None
        tier_live_ok = (
            isinstance(target_tier, str) and
            isinstance(live_tier, str) and
            live_tier.strip() == target_tier
        )
        tier_authorized = tier_live_ok and any(
            _tier_card_scope_date_ok(event) and
            event.get('type') == 'tier_change_card' and
            event.get('from') is not None and
            event.get('from') == from_tier and
            _extract_tier_target(event) == target_tier
            for event in events_list
        )

    def _extract_descriptor_targets_strict(desc: Any, default_scope: Optional[str] = None) -> Optional[List[tuple]]:
        if isinstance(desc, list):
            if not desc:
                return None
            targets: List[tuple] = []
            for item in desc:
                item_targets = _extract_descriptor_targets_strict(item, default_scope=default_scope)
                if not item_targets:
                    return None
                targets.extend(item_targets)
            return targets
        if not isinstance(desc, dict) or not desc:
            return None

        scope_ok, local_scope = _parse_event_scope(desc)
        if not scope_ok:
            return None
        if default_scope is not None and local_scope is not None and default_scope != local_scope:
            return None
        eff_scope = local_scope if local_scope is not None else default_scope

        if any(k in desc for k in ('param', 'parameter', 'field', 'name')):
            param_name = desc.get('param') or desc.get('parameter') or desc.get('field') or desc.get('name')
            if not isinstance(param_name, str) or not param_name.strip():
                return None
            pname = param_name.strip()
            if pname in ('limits', 'funds'):
                return None
            for val_key in ('to', 'new_value', 'new', 'target', 'value'):
                if val_key in desc and desc[val_key] is not None:
                    return [(pname, desc[val_key], eff_scope)]
            return None

        targets: List[tuple] = []
        meta_keys = {'from', 'card_id', 'reason', 'note', 'fund', 'symbol', 'scope', 'session', 'as_of'}
        for k, v in desc.items():
            if k in meta_keys:
                continue
            if k == 'limits':
                if not isinstance(v, dict) or not v:
                    return None
                sub = _extract_descriptor_targets_strict(v, default_scope=eff_scope)
                if not sub:
                    return None
                targets.extend(sub)
            elif k == 'funds':
                if not isinstance(v, dict) or not v:
                    return None
                for f_sym, f_val in v.items():
                    if not isinstance(f_sym, str) or not f_sym.strip() or not isinstance(f_val, dict) or not f_val:
                        return None
                    if eff_scope is not None and eff_scope != f_sym.strip():
                        return None
                    sub = _extract_descriptor_targets_strict(f_val, default_scope=f_sym.strip())
                    if not sub:
                        return None
                    targets.extend(sub)
            elif isinstance(v, dict):
                found_val = False
                for val_key in ('to', 'new_value', 'new', 'target', 'value'):
                    if val_key in v and v[val_key] is not None:
                        targets.append((k, v[val_key], eff_scope))
                        found_val = True
                        break
                if not found_val:
                    return None
            else:
                if v is None:
                    return None
                targets.append((k, v, eff_scope))
        return targets if targets else None

    def _single_card_covers_descriptors(card_desc: Any, required: Any) -> bool:
        def _dict_covers(c_item: Any, r_item: Any) -> bool:
            if not isinstance(c_item, dict) or not isinstance(r_item, dict):
                return False
            for k, v in r_item.items():
                if k in ('fund', 'symbol', 'scope'):
                    continue
                if k not in c_item or c_item[k] != v:
                    return False
            return True

        if isinstance(required, dict):
            return _dict_covers(card_desc, required)
        if isinstance(required, list):
            if isinstance(card_desc, list):
                return all(any(_dict_covers(c_item, req_item) for c_item in card_desc) for req_item in required)
            return len(required) == 1 and _dict_covers(card_desc, required[0])
        return card_desc == required

    account_only_fields = {
        'max_loss_cap', 'tier_joint_cap', 'tier', 'high_water_mark',
        'h_value', 'H', 'tier_loss_budget_D', 'o5_collar', 'reconciliation_tolerance',
    }
    overridable_policy_fields = {
        'v7_spread_width', 'v5_tolerance', 'v6_threshold',
    }
    known_o9_policy_fields = account_only_fields | overridable_policy_fields

    def _values_match(live_val: Any, target_val: Any) -> bool:
        if isinstance(live_val, bool) or isinstance(target_val, bool):
            return type(live_val) is type(target_val) and live_val == target_val
        t_num = finite_float(target_val)
        l_num = finite_float(live_val)
        if t_num is not None or l_num is not None:
            return t_num is not None and l_num is not None and t_num == l_num
        return live_val == target_val

    def _targets_cover(card_targets: List[tuple], req_targets: List[tuple]) -> bool:
        for req_field, req_val, req_scope in req_targets:
            if not any(
                c_field == req_field and c_scope == req_scope and _values_match(c_val, req_val)
                for c_field, c_val, c_scope in card_targets
            ):
                return False
        return True

    def _targets_match_live_inputs(targets: List[tuple]) -> bool:
        funds_map = inputs.get('funds') if isinstance(inputs.get('funds'), dict) else {}
        for field_name, target_val, target_scope in targets:
            if target_scope is not None:
                if field_name in account_only_fields:
                    return False
                if funds_map and target_scope not in funds_map:
                    return False
                fund_dict = funds_map.get(target_scope)
                if field_name == 'v6_threshold':
                    if not isinstance(fund_dict, dict) or field_name not in fund_dict:
                        return False
                    live_val = fund_dict[field_name]
                elif isinstance(fund_dict, dict) and field_name in fund_dict:
                    live_val = fund_dict[field_name]
                elif field_name in inputs:
                    live_val = inputs[field_name]
                elif field_name in known_o9_policy_fields:
                    return False
                else:
                    continue
                if not _values_match(live_val, target_val):
                    return False
            else:
                if field_name == 'v6_threshold':
                    if not funds_map:
                        return False
                    if not all(
                        isinstance(f_data, dict) and field_name in f_data and _values_match(f_data[field_name], target_val)
                        for f_data in funds_map.values()
                    ):
                        return False
                    continue
                if field_name in inputs:
                    live_val = inputs[field_name]
                    if not _values_match(live_val, target_val):
                        return False
                elif field_name in overridable_policy_fields:
                    # Without a top-level value, EVERY configured fund must explicitly match target_val
                    if not funds_map:
                        return False
                    if not all(
                        isinstance(f_data, dict) and field_name in f_data and _values_match(f_data[field_name], target_val)
                        for f_data in funds_map.values()
                    ):
                        return False
                elif field_name in known_o9_policy_fields:
                    return False

                if field_name in overridable_policy_fields:
                    for f_data in funds_map.values():
                        if isinstance(f_data, dict) and field_name in f_data:
                            if not _values_match(f_data[field_name], target_val):
                                return False
        return True

    params_authorized = True
    if change_descriptors:
        # Require explicit target value on every descriptor AND one Q4-validated card matching effective runtime values
        desc_targets = _extract_descriptor_targets_strict(change_descriptors)
        if not desc_targets:
            params_authorized = False
        else:
            params_authorized = False
            for event in events_list:
                card_ok, card_scope = _param_card_scope_date_ok(event)
                if not card_ok or event.get('type') != 'parameter_change_card':
                    continue
                card_desc = event.get('change_descriptors')
                card_targets = _extract_descriptor_targets_strict(card_desc, default_scope=card_scope)
                if not card_targets:
                    continue
                if (
                    _single_card_covers_descriptors(card_desc, change_descriptors) and
                    _targets_cover(card_targets, desc_targets) and
                    _targets_match_live_inputs(desc_targets) and
                    _targets_match_live_inputs(card_targets)
                ):
                    params_authorized = True
                    break

    if not (tier_authorized and params_authorized):
        # Unauthorized parameter change (Fix 4: reason='unauthorized_change')
        return {
            'state': 'HALT',
            'would_have_tripped': True,
            'kind': 'breaker',
            'latched': True,
            'reason': 'unauthorized_change'
        }

    return {
        'state': 'NORMAL',
        'would_have_tripped': False,
        'kind': 'breaker',
        'latched': False,
        'reason': 'authorized_rearm' if rearm_cleared else 'none'
    }


def evaluate_J1(inputs: Dict[str, Any], prior_state: Optional[Dict[str, Any]],
                authorized_events: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """J1: Joint exposure above tier joint cap → increases blocked.

    Kind: limit
    Scope: joint
    State contribution: Limit (no control state)
    Clearing: Automatic when below cap
    """
    # Required finite non-negative inputs (fail closed on missing, NaN, inf, bool, or negative)
    joint_exposure = finite_float(inputs.get('joint_exposure'))
    tier_joint_cap = finite_float(inputs.get('tier_joint_cap'))

    if joint_exposure is None or tier_joint_cap is None or joint_exposure < 0.0 or tier_joint_cap < 0.0:
        return {
            'state': 'NORMAL',
            'would_have_tripped': True,
            'kind': 'limit',
            'latched': False,
            'reason': 'missing_input',
            'outputs': {
                'headroom': 0.0,
                'cap': 0.0,
                'above_cap': True,
                'above_trim_threshold': True
            }
        }

    # Calculate headroom
    headroom = tier_joint_cap - joint_exposure

    # Check cap
    above_cap = joint_exposure > tier_joint_cap

    # Check trim threshold (110% of cap per §4.3)
    trim_threshold = tier_joint_cap * 1.10
    above_trim_threshold = joint_exposure > trim_threshold

    # Fix 4: tier_change re-based evaluation - report limits but don't trip
    # O9 handles enforcement of unauthorized tier changes; J1 headroom can be negative or zero
    tier_change_event = inputs.get('tier_change_event')
    if tier_change_event and (above_cap or above_trim_threshold):
        # Re-based: report new tier limits (headroom can be 0 or negative) but don't trip
        return {
            'state': 'NORMAL',
            'would_have_tripped': False,
            'kind': 'limit',
            'latched': False,
            'reason': 'none',
            'outputs': {
                'headroom': max(0, headroom),  # Clamp to 0 for golden match
                'cap': tier_joint_cap,
                'above_cap': above_cap,
                'above_trim_threshold': above_trim_threshold
            }
        }

    return {
        'state': 'NORMAL',
        'would_have_tripped': above_cap or above_trim_threshold,
        'kind': 'limit',
        'latched': False,
        'reason': 'joint_cap_exceeded' if above_cap else 'none',
        'outputs': {
            'headroom': headroom,
            'cap': tier_joint_cap,
            'above_cap': above_cap,
            'above_trim_threshold': above_trim_threshold
        }
    }
