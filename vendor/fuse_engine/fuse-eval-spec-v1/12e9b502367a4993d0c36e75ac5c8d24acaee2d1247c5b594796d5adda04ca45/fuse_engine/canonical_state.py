"""Canonical state management per Q6 adjudication ruling.

All state reads and writes use the golden nested row objects directly.
Retired private aliases: V2_pending, V4_counter, V4_last_increment_session,
V5_counter, V5_escalated, O4_pending, O5_counter, O5_last_rejection_session,
{row}_counter, {row}_latch.

Canonical state keys:
- V2.pending_adjustments[fund], V2.latched_mismatches[fund]
- V4[fund].clearing_counter, V4[fund].last_session_below_threshold
- V5[fund].consecutive_beyond_tolerance, V5[fund].escalated
- O5.rejection_count, O5.last_rejection_session, O5.latched
- O4[symbol].original_order_id, O4[symbol].status
- L2.latched, L2.trip_session
- V1[fund].latched, V1[fund].trip_session, V1[fund].deviation
- O1.fresh_session_count, O1.last_session
- L3.latched, L3.trip_session, L3.cooling_off_until, L3.loss_record
- L5.fresh_session_count
"""

from typing import Any, Dict, Optional
from .canonical import finite_float


# V2 canonical state accessors


_CA_NUMERIC_FIELDS = {'ratio', 'amount', 'dividend', 'cash_amount', 'distribution_amount', 'factor', 'price', 'split_ratio'}


def canonical_ca_identity(ca_obj: Any) -> Optional[Dict[str, Any]]:
    """Return normalized corporate-action identity keeping strings exact and excluding completion flags."""
    if not isinstance(ca_obj, dict) or not ca_obj.get('type'):
        return None
    volatile = {'adjustment_complete', 'exact_reconciliation', 'hwm_adjusted', 'awaiting_reconciliation'}
    ident: Dict[str, Any] = {}
    for k in sorted(ca_obj.keys()):
        if k in volatile:
            continue
        v = ca_obj[k]
        if isinstance(v, bool):
            ident[k] = v
        elif isinstance(v, (int, float)):
            num = finite_float(v)
            ident[k] = num if num is not None else v
        elif k in _CA_NUMERIC_FIELDS and isinstance(v, str):
            num = finite_float(v)
            ident[k] = num if num is not None else v.strip()
        elif isinstance(v, str):
            ident[k] = v.strip()
        else:
            ident[k] = v
    return ident


def ca_identity_matches(
    stored_ident: Optional[Dict[str, Any]],
    fallback_item: Optional[Dict[str, Any]],
    incoming_ca: Any,
) -> bool:
    """Match an incoming corporate action strictly against stored canonical identity (or fallback item)."""
    inc_ident = canonical_ca_identity(incoming_ca)
    if inc_ident is None:
        return False
    if isinstance(stored_ident, dict) and stored_ident.get('type'):
        return inc_ident == stored_ident
    if not isinstance(fallback_item, dict):
        return False
    if fallback_item.get('type') != inc_ident.get('type'):
        return False
    p_ratio = fallback_item.get('ratio')
    c_ratio = inc_ident.get('ratio')
    if p_ratio is None:
        return c_ratio is None
    p_num = finite_float(p_ratio)
    c_num = finite_float(c_ratio)
    return p_num is not None and c_num is not None and p_num == c_num

def get_v2_pending_adjustment(prior_state: Optional[Dict], fund: str) -> Optional[Dict]:
    """Get pending adjustment for a fund from V2.pending_adjustments."""
    if not prior_state or 'V2' not in prior_state:
        return None
    pending = prior_state['V2'].get('pending_adjustments', {})
    return pending.get(fund)


def set_v2_pending_adjustment(next_state: Dict, fund: str, adjustment: Dict) -> None:
    """Set pending adjustment in V2.pending_adjustments[fund]."""
    if 'V2' not in next_state:
        next_state['V2'] = {}
    if 'pending_adjustments' not in next_state['V2']:
        next_state['V2']['pending_adjustments'] = {}
    next_state['V2']['pending_adjustments'][fund] = adjustment


def clear_v2_pending_adjustment(next_state: Dict, fund: str) -> None:
    """Clear pending adjustment for a fund.

    Per golden target-state projections, V2 is serialized as {} when empty, not absent.
    """
    if 'V2' not in next_state:
        next_state['V2'] = {}
    if 'pending_adjustments' in next_state['V2'] and fund in next_state['V2']['pending_adjustments']:
        del next_state['V2']['pending_adjustments'][fund]
        if not next_state['V2']['pending_adjustments']:
            del next_state['V2']['pending_adjustments']
    if 'pending_action_identities' in next_state['V2'] and fund in next_state['V2']['pending_action_identities']:
        del next_state['V2']['pending_action_identities'][fund]
        if not next_state['V2']['pending_action_identities']:
            del next_state['V2']['pending_action_identities']
    # Keep empty V2:{} per golden projections


def get_v2_latched_mismatch(prior_state: Optional[Dict], fund: str) -> Optional[Dict]:
    """Get latched mismatch for a fund from V2.latched_mismatches."""
    if not prior_state or 'V2' not in prior_state:
        return None
    mismatches = prior_state['V2'].get('latched_mismatches', {})
    return mismatches.get(fund)


def get_v2_latched_mismatch_action(prior_state: Optional[Dict], fund: str) -> Optional[Dict]:
    """Get normalized corporate-action identity for a latched V2 mismatch."""
    if not prior_state or 'V2' not in prior_state:
        return None
    actions = prior_state['V2'].get('latched_mismatch_actions', {})
    val = actions.get(fund)
    return val if isinstance(val, dict) else None


def set_v2_latched_mismatch(next_state: Dict, fund: str, mismatch: Dict) -> None:
    """Set latched mismatch in V2.latched_mismatches[fund]."""
    if 'V2' not in next_state:
        next_state['V2'] = {}
    if 'latched_mismatches' not in next_state['V2']:
        next_state['V2']['latched_mismatches'] = {}
    next_state['V2']['latched_mismatches'][fund] = mismatch


def set_v2_latched_mismatch_action(next_state: Dict, fund: str, action_identity: Dict) -> None:
    """Persist normalized corporate-action identity alongside V2.latched_mismatches[fund]."""
    if 'V2' not in next_state:
        next_state['V2'] = {}
    if 'latched_mismatch_actions' not in next_state['V2']:
        next_state['V2']['latched_mismatch_actions'] = {}
    next_state['V2']['latched_mismatch_actions'][fund] = action_identity


def get_v2_pending_identities(prior_state: Optional[Dict], fund: str) -> Optional[Dict]:
    """Get normalized corporate-action identities for pending primary/queued V2 items."""
    if not prior_state or 'V2' not in prior_state:
        return None
    identities = prior_state['V2'].get('pending_action_identities', {})
    val = identities.get(fund)
    return val if isinstance(val, dict) else None


def set_v2_pending_identities(next_state: Dict, fund: str, identities: Dict) -> None:
    """Persist normalized corporate-action identities alongside V2.pending_adjustments[fund]."""
    if 'V2' not in next_state:
        next_state['V2'] = {}
    if 'pending_action_identities' not in next_state['V2']:
        next_state['V2']['pending_action_identities'] = {}
    next_state['V2']['pending_action_identities'][fund] = identities


def clear_v2_latched_mismatch(next_state: Dict, fund: str) -> None:
    """Clear latched mismatch for a fund.

    Per golden target-state projections, V2 is serialized as {} when empty, not absent.
    """
    if 'V2' not in next_state:
        next_state['V2'] = {}
    if 'latched_mismatches' in next_state['V2'] and fund in next_state['V2']['latched_mismatches']:
        del next_state['V2']['latched_mismatches'][fund]
        if not next_state['V2']['latched_mismatches']:
            del next_state['V2']['latched_mismatches']
    if 'latched_mismatch_actions' in next_state['V2'] and fund in next_state['V2']['latched_mismatch_actions']:
        del next_state['V2']['latched_mismatch_actions'][fund]
        if not next_state['V2']['latched_mismatch_actions']:
            del next_state['V2']['latched_mismatch_actions']
    # Keep empty V2:{} per golden projections


# V4 canonical state accessors

def get_v4_state(prior_state: Optional[Dict], fund: str) -> Dict:
    """Get V4 state for a fund. Returns dict with clearing_counter and last_session_below_threshold.

    Present entry with counter=0 means active after trip (distinct from absent=inactive).
    """
    if not prior_state or 'V4' not in prior_state:
        return {'clearing_counter': None, 'last_session_below_threshold': None}

    fund_state = prior_state['V4'].get(fund, {})
    return {
        'clearing_counter': fund_state.get('clearing_counter'),
        'last_session_below_threshold': fund_state.get('last_session_below_threshold')
    }


def set_v4_state(next_state: Dict, fund: str, counter: int, last_session: Optional[str]) -> None:
    """Set V4 state for a fund. Counter and last_session together.

    At count 10, caller should remove the fund entry instead of storing count 10.
    """
    if 'V4' not in next_state:
        next_state['V4'] = {}
    next_state['V4'][fund] = {
        'clearing_counter': counter,
        'last_session_below_threshold': last_session
    }


def clear_v4_state(next_state: Dict, fund: str) -> None:
    """Remove V4 fund entry (after clearing at count 10).

    Per golden target-state projections, V4 is serialized as {} when empty, not absent.
    """
    if 'V4' not in next_state:
        next_state['V4'] = {}
    if fund in next_state['V4']:
        del next_state['V4'][fund]


# V5 canonical state accessors

def get_v5_state(prior_state: Optional[Dict], fund: str) -> Dict:
    """Get V5 state for a fund. Returns dict with counter, escalated flag, and last_session."""
    if not prior_state or 'V5' not in prior_state:
        return {'consecutive_beyond_tolerance': 0, 'escalated': False, 'last_session': None}

    fund_state = prior_state['V5'].get(fund, {})
    return {
        'consecutive_beyond_tolerance': fund_state.get('consecutive_beyond_tolerance', 0),
        'escalated': fund_state.get('escalated', False),
        'last_session': fund_state.get('last_session')
    }


def set_v5_state(next_state: Dict, fund: str, count: int, escalated: bool, session: str) -> None:
    """Set V5 state for a fund.

    Q6 requires sufficient session identity for replay and duplicate evaluations.
    """
    if 'V5' not in next_state:
        next_state['V5'] = {}
    next_state['V5'][fund] = {
        'consecutive_beyond_tolerance': count,
        'escalated': escalated,
        'last_session': session
    }


def clear_v5_state(next_state: Dict, fund: str) -> None:
    """Clear V5 escalation state for a fund (Q4 authorized re-arm)."""
    if 'V5' not in next_state:
        next_state['V5'] = {}
    if fund in next_state['V5']:
        del next_state['V5'][fund]


# O5 canonical state accessors

def get_o5_state(prior_state: Optional[Dict]) -> Dict:
    """Get O5 account-level state. Returns dict with rejection_count, last_session, latched."""
    if not prior_state or 'O5' not in prior_state:
        return {'rejection_count': 0, 'last_rejection_session': None, 'latched': False}

    o5 = prior_state['O5']
    return {
        'rejection_count': o5.get('rejection_count', 0),
        'last_rejection_session': o5.get('last_rejection_session'),
        'latched': o5.get('latched', False)
    }


def set_o5_state(next_state: Dict, count: int, last_session: Optional[str], latched: bool) -> None:
    """Set O5 account-level state."""
    next_state['O5'] = {
        'rejection_count': count,
        'last_rejection_session': last_session,
        'latched': latched
    }


# O4 canonical state accessors

def get_o4_pending_order(prior_state: Optional[Dict], symbol: str) -> Optional[Dict]:
    """Get O4 pending order state for a symbol. Returns dict with original_order_id and status."""
    if not prior_state or 'O4' not in prior_state:
        return None

    symbol_state = prior_state['O4'].get(symbol, {})
    if 'original_order_id' not in symbol_state:
        return None

    return {
        'original_order_id': symbol_state['original_order_id'],
        'status': symbol_state.get('status', 'unknown')
    }


def set_o4_pending_order(next_state: Dict, symbol: str, order_id: str, status: str = 'unknown') -> None:
    """Set O4 pending order state for a symbol."""
    if 'O4' not in next_state:
        next_state['O4'] = {}
    next_state['O4'][symbol] = {
        'original_order_id': order_id,
        'status': status
    }


def clear_o4_pending_order(next_state: Dict, symbol: str) -> None:
    """Clear O4 pending order for a symbol (after resolution).

    Per golden target-state projections, O4 is serialized as {} when empty, not absent.
    """
    if 'O4' not in next_state:
        next_state['O4'] = {}
    if symbol in next_state['O4']:
        del next_state['O4'][symbol]


# L2 canonical state accessors

def get_l2_latch(prior_state: Optional[Dict]) -> Optional[Dict]:
    """Get L2 latch state. Returns dict with latched and trip_session."""
    if not prior_state or 'L2' not in prior_state:
        return None

    l2 = prior_state['L2']
    if not l2.get('latched'):
        return None

    return {
        'latched': True,
        'trip_session': l2.get('trip_session')
    }


def set_l2_latch(next_state: Dict, trip_session: str) -> None:
    """Set L2 latch state."""
    next_state['L2'] = {
        'latched': True,
        'trip_session': trip_session
    }


def clear_l2_latch(next_state: Dict) -> None:
    """Clear L2 latch state (Q4 authorized re-arm)."""
    next_state['L2'] = {}


# V1 canonical state accessors

def get_v1_history(prior_state: Optional[Dict], fund: str) -> list:
    """Get V1 rolling session return history for a fund."""
    if not prior_state or 'V1' not in prior_state:
        return []
    fund_state = prior_state['V1'].get(fund, {})
    history = fund_state.get('session_returns', [])
    return list(history) if isinstance(history, list) else []


def set_v1_history(next_state: Dict, fund: str, history: list) -> None:
    """Persist V1 rolling session return history for a fund."""
    if 'V1' not in next_state:
        next_state['V1'] = {}
    if fund not in next_state['V1']:
        next_state['V1'][fund] = {}
    next_state['V1'][fund]['session_returns'] = history


def get_v1_latch(prior_state: Optional[Dict], fund: str) -> Optional[Dict]:
    """Get V1 latch state for a fund. Returns dict with latched, trip_session, deviation."""
    if not prior_state or 'V1' not in prior_state:
        return None

    fund_state = prior_state['V1'].get(fund, {})
    if not fund_state.get('latched'):
        return None

    return {
        'latched': True,
        'trip_session': fund_state.get('trip_session'),
        'deviation': fund_state.get('deviation')
    }


def set_v1_latch(next_state: Dict, fund: str, trip_session: str, deviation: float) -> None:
    """Set V1 latch state for a fund."""
    if 'V1' not in next_state:
        next_state['V1'] = {}
    existing = next_state['V1'].get(fund, {})
    entry = {
        'latched': True,
        'trip_session': trip_session,
        'deviation': deviation
    }
    if 'session_returns' in existing:
        entry['session_returns'] = existing['session_returns']
    next_state['V1'][fund] = entry


def clear_v1_latch(next_state: Dict, fund: str) -> None:
    """Clear V1 latch state for a fund (Q4 authorized re-arm).

    Serializes as {"V1":{}} for one-target case per Q4 requirement.
    """
    if 'V1' not in next_state:
        next_state['V1'] = {}
    if fund in next_state['V1']:
        del next_state['V1'][fund]
    # Keep empty V1:{} for target-state projection


# O1 canonical state accessors

def get_o1_fresh_state(prior_state: Optional[Dict]) -> Dict:
    """Get O1 fresh-session state."""
    if not prior_state or 'O1' not in prior_state:
        return {'fresh_session_count': 0, 'last_session': None, 'last_tripped_session': None}

    o1 = prior_state['O1']
    return {
        'fresh_session_count': o1.get('fresh_session_count', 0),
        'last_session': o1.get('last_session'),
        'last_tripped_session': o1.get('last_tripped_session')
    }


def set_o1_fresh_state(
    next_state: Dict,
    count: int,
    session: str,
    last_tripped_session: Optional[str] = None
) -> None:
    """Set O1 fresh-session state."""
    entry = {
        'fresh_session_count': count,
        'last_session': session
    }
    if last_tripped_session is not None:
        entry['last_tripped_session'] = last_tripped_session
    next_state['O1'] = entry


# L3 canonical state accessors

def get_l3_latch(prior_state: Optional[Dict]) -> Optional[Dict]:
    """Get L3 latch and loss record."""
    if not prior_state or 'L3' not in prior_state:
        return None

    l3 = prior_state['L3']
    if not l3.get('latched'):
        return None

    return {
        'latched': True,
        'trip_session': l3.get('trip_session'),
        'cooling_off_until': l3.get('cooling_off_until'),
        'loss_record': l3.get('loss_record')
    }


def set_l3_latch(next_state: Dict, trip_session: str, cooling_off_until: str, loss_record: Dict) -> None:
    """Set L3 latch with cooling-off period and durable loss record."""
    next_state['L3'] = {
        'latched': True,
        'trip_session': trip_session,
        'cooling_off_until': cooling_off_until,
        'loss_record': loss_record
    }


def clear_l3_lock(next_state: Dict, loss_record: Dict) -> None:
    """Clear L3 active lock after cooling-off, preserve loss record."""
    next_state['L3'] = {
        'latched': False,
        'loss_record': loss_record
    }


# L5 canonical state accessors

def get_l5_state(prior_state: Optional[Dict]) -> Dict:
    """Get L5 interlock state (active session persistence + fresh counter)."""
    if not prior_state or 'L5' not in prior_state:
        return {'active': False, 'trip_session': None, 'fresh_session_count': 0}
    l5 = prior_state['L5']
    if not isinstance(l5, dict):
        return {'active': False, 'trip_session': None, 'fresh_session_count': 0}
    return {
        'active': bool(l5.get('active') or l5.get('latched')),
        'trip_session': l5.get('trip_session'),
        'fresh_session_count': l5.get('fresh_session_count', 0)
    }


def set_l5_active_trip(next_state: Dict, trip_session: str) -> None:
    """Persist L5 active halt state for remainder of trading session."""
    next_state['L5'] = {
        'active': True,
        'trip_session': trip_session,
        'fresh_session_count': 0
    }


def get_l5_fresh_count(prior_state: Optional[Dict]) -> int:
    """Get L5 fresh-session counter."""
    if not prior_state or 'L5' not in prior_state:
        return 0
    return prior_state['L5'].get('fresh_session_count', 0)


def set_l5_fresh_count(next_state: Dict, count: int) -> None:
    """Set L5 fresh-session counter."""
    if 'L5' not in next_state:
        next_state['L5'] = {}
    next_state['L5']['fresh_session_count'] = count


def canonical_session_date(session: Optional[str], as_of: Optional[str] = None) -> Optional[str]:
    """Return canonical trading-session identity: prefer session, fallback to as_of[:10]."""
    if isinstance(session, str) and session.strip():
        return session.strip()
    if isinstance(as_of, str) and len(as_of.strip()) >= 10:
        return as_of.strip()[:10]
    return None


def event_matches_row(event: Dict[str, Any], row_id: str) -> bool:
    """Match row/row_id on an event, rejecting conflicting dual tags (row != row_id)."""
    ev_row = event.get('row')
    ev_row_id = event.get('row_id')
    if ev_row is not None and ev_row_id is not None and ev_row != ev_row_id:
        return False
    target = ev_row if ev_row is not None else ev_row_id
    return target == row_id


def event_date_matches(
    event: Dict[str, Any],
    session: Optional[str] = None,
    as_of: Optional[str] = None,
    require_date: bool = True,
) -> bool:
    """Validate event session/as_of date against canonical session identity per Q4."""
    cur_date = canonical_session_date(session, as_of)
    ev_session = event.get('session')
    ev_as_of = event.get('as_of')
    has_session = isinstance(ev_session, str) and bool(ev_session.strip())
    has_as_of = isinstance(ev_as_of, str) and len(ev_as_of.strip()) >= 10
    if ev_session is not None and not has_session:
        return False
    if ev_as_of is not None and not has_as_of:
        return False
    if require_date and not (has_session or has_as_of):
        return False
    if has_session and cur_date is not None and ev_session.strip() != cur_date:
        return False
    if has_as_of and cur_date is not None and ev_as_of.strip()[:10] != cur_date:
        return False
    return True


def matches_account_rearm(
    authorized_events: Optional[Any],
    row_id: str,
    session: Optional[str] = None,
    as_of: Optional[str] = None,
    require_date: bool = True,
) -> Optional[Dict[str, Any]]:
    """Return matching account-scoped rearm event per Q4 (row, session/as_of, account scope)."""
    if not authorized_events:
        return None
    if isinstance(authorized_events, list):
        events_list = authorized_events
    elif isinstance(authorized_events, dict):
        events_list = authorized_events.get('account', [])
    else:
        return None

    for event in events_list:
        if not isinstance(event, dict):
            continue
        if event.get('type') != 'rearm':
            continue
        if not event_matches_row(event, row_id):
            continue
        # Reject fund/symbol/joint scoped cards on account-level latches
        if event.get('fund') or event.get('symbol'):
            continue
        if 'scope' in event and event.get('scope') != 'account':
            continue
        if not event_date_matches(event, session=session, as_of=as_of, require_date=require_date):
            continue
        return event
    return None


def _matches_scoped_rearm(
    authorized_events: Optional[Any],
    row_id: str,
    target_field: str,
    forbidden_field: str,
    target_val: str,
    session: Optional[str] = None,
    as_of: Optional[str] = None,
    require_date: bool = True,
) -> Optional[Dict[str, Any]]:
    """Return matching scoped (fund/symbol) rearm event per Q4."""
    if not authorized_events:
        return None
    scope = f'{target_field}:{target_val}'
    if isinstance(authorized_events, list):
        events_list = authorized_events
    elif isinstance(authorized_events, dict):
        events_list = authorized_events.get(scope, [])
    else:
        return None

    for event in events_list:
        if not isinstance(event, dict):
            continue
        if event.get('type') != 'rearm':
            continue
        if not event_matches_row(event, row_id):
            continue
        if event.get(forbidden_field):
            continue
        if event.get(target_field) != target_val:
            continue
        if 'scope' in event and event.get('scope') != scope:
            continue
        if not event_date_matches(event, session=session, as_of=as_of, require_date=require_date):
            continue
        return event
    return None


def matches_fund_rearm(
    authorized_events: Optional[Any],
    row_id: str,
    fund: str,
    session: Optional[str] = None,
    as_of: Optional[str] = None,
    require_date: bool = True,
) -> Optional[Dict[str, Any]]:
    """Return matching fund-scoped rearm event per Q4 (row, fund, fund scope, date)."""
    return _matches_scoped_rearm(
        authorized_events, row_id, 'fund', 'symbol', fund,
        session=session, as_of=as_of, require_date=require_date,
    )


def matches_symbol_rearm(
    authorized_events: Optional[Any],
    row_id: str,
    symbol: str,
    session: Optional[str] = None,
    as_of: Optional[str] = None,
    require_date: bool = True,
) -> Optional[Dict[str, Any]]:
    """Return matching symbol-scoped rearm event per Q4 (row, symbol, symbol scope, date)."""
    return _matches_scoped_rearm(
        authorized_events, row_id, 'symbol', 'fund', symbol,
        session=session, as_of=as_of, require_date=require_date,
    )


def matches_o8_clear(
    authorized_events: Optional[Any],
    session: Optional[str] = None,
    as_of: Optional[str] = None,
    require_date: bool = True,
) -> Optional[Dict[str, Any]]:
    """Return matching owner kill-switch clear/rearm event per Q4."""
    if not authorized_events:
        return None
    if isinstance(authorized_events, list):
        events_list = authorized_events
    elif isinstance(authorized_events, dict):
        events_list = authorized_events.get('account', [])
    else:
        return None

    for event in events_list:
        if not isinstance(event, dict):
            continue
        ev_type = event.get('type')
        if ev_type == 'rearm':
            if not event_matches_row(event, 'O8'):
                continue
        elif ev_type == 'kill_switch_clear':
            if event.get('row') is not None or event.get('row_id') is not None:
                if not event_matches_row(event, 'O8'):
                    continue
        else:
            continue
        if event.get('fund') or event.get('symbol'):
            continue
        if 'scope' in event and event.get('scope') != 'account':
            continue
        if not event_date_matches(event, session=session, as_of=as_of, require_date=require_date):
            continue
        return event
    return None


# General latch accessors for rows using old state.py pattern (L4, L6, V3)
# These can read from either old {row}_latch format or canonical format

def get_account_latch(prior_state: Optional[Dict], row_id: str) -> Optional[Dict]:
    """Get account-level latch for a row (L3/L4/L6).

    Reads from canonical {row}.latched format or falls back to old {row}_latch.
    """
    if not prior_state:
        return None

    # Try canonical format first
    if row_id in prior_state:
        row_state = prior_state[row_id]
        if row_state.get('latched'):
            return {
                'latched': True,
                'trip_session': row_state.get('trip_session'),
                'details': row_state.get('details', {})
            }

    return None


def set_account_latch(next_state: Dict, row_id: str, trip_session: str, details: Dict = None) -> None:
    """Set account-level latch in canonical format."""
    next_state[row_id] = {
        'latched': True,
        'trip_session': trip_session,
        'details': details or {}
    }


def get_fund_latch(prior_state: Optional[Dict], row_id: str, fund: str) -> Optional[Dict]:
    """Get fund-level latch for a row (V3).

    Reads from canonical {row}.{fund}.latched format or falls back to old {row}_latch.
    """
    if not prior_state:
        return None

    # Try canonical format first
    if row_id in prior_state:
        row_state = prior_state[row_id]
        fund_state = row_state.get(fund, {})
        if fund_state.get('latched'):
            return {
                'latched': True,
                'trip_session': fund_state.get('trip_session'),
                'details': fund_state.get('details', {})
            }

    return None


def set_fund_latch(next_state: Dict, row_id: str, fund: str, trip_session: str, details: Dict = None) -> None:
    """Set fund-level latch in canonical format."""
    if row_id not in next_state:
        next_state[row_id] = {}
    next_state[row_id][fund] = {
        'latched': True,
        'trip_session': trip_session,
        'details': details or {}
    }
