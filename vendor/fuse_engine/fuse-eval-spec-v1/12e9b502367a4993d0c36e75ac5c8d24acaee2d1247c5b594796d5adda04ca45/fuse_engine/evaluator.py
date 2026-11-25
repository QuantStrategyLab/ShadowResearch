"""FUSE-1 reference engine evaluator.

Implements the §5.1 signature with all 24 rows fully evaluated.
"""

import copy
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, Optional
from . import _EXPECTED_ENGINE_FILES, _verify_and_hash_engine_dir

# Re-verify directory & __pycache__ bytecode integrity BEFORE importing any row submodules
_CANONICAL_ENGINE_HASH = _verify_and_hash_engine_dir(Path(__file__).resolve().parent)

from . import rows_loss
from . import rows_vehicle
from . import rows_ops
from . import state as st
from . import canonical_state as cst
from .canonical import finite_float


_compute_canonical_engine_hash_from_dir = _verify_and_hash_engine_dir


def _compute_canonical_engine_hash() -> str:
    """Compute canonical SHA-256 of verified src/fuse_engine/*.py source files (§5.3)."""
    return _verify_and_hash_engine_dir(Path(__file__).resolve().parent)


def compute_engine_hash() -> str:
    """Return precomputed canonical engine bundle SHA-256 (zero runtime I/O in evaluate)."""
    return _CANONICAL_ENGINE_HASH


def _fund_sort_key(symbol: str) -> tuple[int, str]:
    """Deterministic canonical fund order preserving [TQQQ, SOXL] legacy order (§0.4)."""
    if symbol == 'TQQQ':
        return (0, symbol)
    if symbol == 'SOXL':
        return (1, symbol)
    return (2, symbol)


# The 24 named row IDs per §4.3
ROW_IDS = [
    "L0", "L1", "L2", "L3", "L4", "L5", "L6",
    "V1", "V2", "V3", "V4", "V5", "V6", "V7",
    "O1", "O2", "O3", "O4", "O5", "O6", "O7", "O8", "O9",
    "J1"
]


def evaluate(
    spec_version: str,
    engine_hash: str,
    policy_hash: str,
    session: str,
    as_of: str,
    mode: str,
    validated_inputs: Dict[str, Any],
    prior_row_state: Optional[Dict[str, Any]],
    authorized_events: Optional[Dict[str, Any]]
) -> tuple[Dict[str, Any], Dict[str, str], Dict[str, Any]]:
    """Evaluate all 24 breaker rows and return effective state.

    Args:
        spec_version: Version of the breaker specification
        engine_hash: SHA-256 of the canonical engine bundle
        policy_hash: Hash of the loaded policy configuration
        session: Session identifier (YYYY-MM-DD)
        as_of: Evaluation timestamp (ISO 8601)
        mode: Enforcement mode ('shadow' in FUSE-1)
        validated_inputs: All required inputs for breaker evaluation
        prior_row_state: State from previous evaluation (latches, counters)
        authorized_events: Human re-arms, tier changes, owner commands

    Returns:
        (rows_by_ID, effective_state_by_scope, next_row_state) tuple where:
        - rows_by_ID: dict with 24 row IDs, each with state/would_have_tripped/kind/latched/reason + outputs
        - effective_state_by_scope: precedence-resolved state per scope
        - next_row_state: complete successor state for next evaluation
    """
    # A10 / §5.1: Validate spec_version and FUSE-1 shadow-only mode contract
    if spec_version != 'fuse-eval-spec/v1':
        raise ValueError(f"Unsupported spec_version: {spec_version!r}; expected 'fuse-eval-spec/v1'")
    if mode != 'shadow':
        raise ValueError(f"FUSE-1 operates in shadow mode only; forbidden mode={mode!r}")

    # §5.2 / §5.3: Use precomputed canonical engine hash (zero file I/O); reject mismatched 64-hex pinned digests
    canonical_engine_hash = _CANONICAL_ENGINE_HASH
    if (
        isinstance(engine_hash, str) and
        len(engine_hash) == 64 and
        all(c in '0123456789abcdefABCDEF' for c in engine_hash) and
        engine_hash.lower() != canonical_engine_hash
    ):
        raise ValueError(
            f"engine_hash mismatch: caller={engine_hash}, actual={canonical_engine_hash}"
        )

    # Reject malformed non-None, non-dict prior_row_state rather than silently resetting latches to {}
    if prior_row_state is None:
        prior_row_state = {}
    elif not isinstance(prior_row_state, dict):
        raise ValueError(
            f"Invalid prior_row_state: expected dict or None, got {type(prior_row_state).__name__}"
        )
    if authorized_events is None:
        authorized_events = {}
    if validated_inputs is None or not isinstance(validated_inputs, dict):
        validated_inputs = {}

    # Initialize next_row_state
    next_row_state = st.init_next_state()

    # Get funds safely (null/non-dict guards prevent AttributeError on public evaluate())
    raw_funds = validated_inputs.get('funds')
    funds = raw_funds if isinstance(raw_funds, dict) else {}
    fund_symbols = sorted(funds.keys(), key=_fund_sort_key)

    # Build scopes: enumerate symbols from funds, proposed order symbols, pending_orders, AND durable O4 (Q6 wiring)
    all_symbols_set = set(fund_symbols)
    order_symbols_list = []
    invalid_order_symbol = False
    for sym_key in ('o5_symbol', 'symbol'):
        if sym_key in validated_inputs and validated_inputs[sym_key] is not None:
            raw_sym = validated_inputs[sym_key]
            if isinstance(raw_sym, str) and raw_sym.strip():
                cleaned_sym = raw_sym.strip()
                if cleaned_sym not in order_symbols_list:
                    order_symbols_list.append(cleaned_sym)
            else:
                invalid_order_symbol = True
    order_symbols = sorted(order_symbols_list)
    all_symbols_set.update(order_symbols)
    _CASH_ETF_SYMBOLS = frozenset({'BIL'})
    missing_vehicle_order_symbols = [
        sym for sym in order_symbols
        if sym not in funds and sym not in _CASH_ETF_SYMBOLS
    ]
    eval_fund_symbols = sorted(
        set(fund_symbols) | set(missing_vehicle_order_symbols),
        key=_fund_sort_key,
    )

    raw_pending_orders = validated_inputs.get('pending_orders')
    malformed_pending_orders = (
        'pending_orders' in validated_inputs and not isinstance(raw_pending_orders, dict)
    )
    pending_orders = raw_pending_orders if isinstance(raw_pending_orders, dict) else {}
    all_symbols_set.update(pending_orders.keys())
    # Add symbols from durable O4 state
    prior_o4 = prior_row_state.get('O4')
    if isinstance(prior_o4, dict):
        all_symbols_set.update(prior_o4.keys())
    if malformed_pending_orders and not all_symbols_set:
        all_symbols_set.add('__pending_orders__')
    all_symbols = sorted(all_symbols_set)

    # Also track any prior restricted funds (V1/V2/V3/V4/V5 escalated) omitted from current funds dict
    prior_latched_funds = set()
    if prior_row_state:
        for fn, fstate in (prior_row_state.get('V1') or {}).items():
            if isinstance(fstate, dict) and fstate.get('latched') and fn not in eval_fund_symbols:
                prior_latched_funds.add(fn)
        prior_v2 = prior_row_state.get('V2') or {}
        if isinstance(prior_v2, dict):
            for fn, mstate in (prior_v2.get('latched_mismatches') or {}).items():
                if mstate and fn not in eval_fund_symbols:
                    prior_latched_funds.add(fn)
            for fn, pstate in (prior_v2.get('pending_adjustments') or {}).items():
                if pstate and fn not in eval_fund_symbols:
                    prior_latched_funds.add(fn)
        for fn, v3state in (prior_row_state.get('V3') or {}).items():
            if isinstance(v3state, dict) and v3state.get('latched') and fn not in eval_fund_symbols:
                prior_latched_funds.add(fn)
        for fn, v4state in (prior_row_state.get('V4') or {}).items():
            if isinstance(v4state, dict) and v4state.get('clearing_counter') is not None and fn not in eval_fund_symbols:
                prior_latched_funds.add(fn)
        for fn, v5state in (prior_row_state.get('V5') or {}).items():
            if isinstance(v5state, dict) and fn not in eval_fund_symbols and (
                v5state.get('escalated') or (v5state.get('consecutive_beyond_tolerance') or 0) >= 40
            ):
                prior_latched_funds.add(fn)

    scopes = ['account']
    for symbol in eval_fund_symbols:
        scopes.append(f'fund:{symbol}')
    for symbol in sorted(prior_latched_funds):
        if symbol not in eval_fund_symbols:
            scopes.append(f'fund:{symbol}')
    for symbol in all_symbols:
        scopes.append(f'symbol:{symbol}')
    scopes.append('joint')

    # Initialize rows_by_ID
    rows_by_ID = {}

    # ==== LOSS ROWS (L0-L6) - Account scope ====

    # L0 - exposure limit
    l0_result = rows_loss.evaluate_L0(validated_inputs, prior_row_state, authorized_events)
    rows_by_ID['L0'] = l0_result

    # L1 - drawdown D/3 interlock
    l1_result = rows_loss.evaluate_L1(validated_inputs, prior_row_state, authorized_events)
    rows_by_ID['L1'] = l1_result

    # L2 - drawdown 2D/3 breaker (canonical format per Q6)
    l2_result = rows_loss.evaluate_L2(validated_inputs, prior_row_state, authorized_events, session=session, as_of=as_of)
    rows_by_ID['L2'] = l2_result
    if l2_result['latched']:
        # Preserve original trip_session if already latched (§4.4 rule 3: persistence)
        prior_latch = cst.get_l2_latch(prior_row_state)
        trip_session = prior_latch['trip_session'] if prior_latch else session
        cst.set_l2_latch(next_row_state, trip_session)
    elif l2_result['reason'] == 'authorized_rearm':
        # Q4: clear latch on authorized re-arm - serialize as {"L2":{}}
        cst.clear_l2_latch(next_row_state)

    # L3 - drawdown D breaker (canonical format per Q6; A07.3 durable loss_record & trip_session)
    l3_result = rows_loss.evaluate_L3(validated_inputs, prior_row_state, authorized_events, session=session, as_of=as_of)
    rows_by_ID['L3'] = l3_result
    if l3_result['latched']:
        # Preserve original trip_session, cooling_off_until, and loss_record if already latched
        prior_latch = cst.get_l3_latch(prior_row_state)
        if prior_latch:
            trip_session = prior_latch['trip_session']
            cooling_off_until = prior_latch.get('cooling_off_until', trip_session)
            loss_record = prior_latch.get('loss_record')
        else:
            trip_session = session
            try:
                cooling_off_until = (date.fromisoformat(session) + timedelta(days=30)).isoformat()
            except ValueError:
                cooling_off_until = session
            hwm_raw = validated_inputs.get('high_water_mark')
            bk_raw = validated_inputs.get('bankroll')
            hwm_val = finite_float(hwm_raw)
            bk_val = finite_float(bk_raw)
            dd = (hwm_val - bk_val) / hwm_val if (hwm_val is not None and bk_val is not None and hwm_val > 0.0) else None
            loss_record = {
                'bankroll': bk_val,
                'high_water_mark': hwm_val,
                'tier': validated_inputs.get('tier'),
                'drawdown': dd
            }
        cst.set_l3_latch(next_row_state, trip_session, cooling_off_until, loss_record)
    elif l3_result['reason'] == 'authorized_rearm':
        # Q4: clear lock after cooling-off, preserve loss_record
        prior_latch = cst.get_l3_latch(prior_row_state)
        loss_record = prior_latch['loss_record'] if prior_latch and 'loss_record' in prior_latch else {}
        cst.clear_l3_lock(next_row_state, loss_record)
    elif prior_row_state and isinstance(prior_row_state.get('L3'), dict) and prior_row_state['L3'].get('loss_record') is not None:
        # Loss record is never cleared (§4.3 L3)
        cst.clear_l3_lock(next_row_state, prior_row_state['L3']['loss_record'])

    market_closed = (
        validated_inputs.get('session_calendar_status') in ('holiday', 'closed', 'market_closed') or
        validated_inputs.get('exchange_status') in ('holiday', 'closed', 'market_closed')
    )

    # L4 - one-session loss D/10 breaker (canonical per Q6; A07.3 preserve original trip_session)
    l4_result = rows_loss.evaluate_L4(
        validated_inputs, prior_row_state, authorized_events,
        session=session, as_of=as_of, market_closed=market_closed,
    )
    rows_by_ID['L4'] = l4_result
    if l4_result['latched']:
        prior_latch = cst.get_account_latch(prior_row_state, 'L4')
        trip_session = prior_latch['trip_session'] if prior_latch and prior_latch.get('trip_session') else cst.canonical_session_date(session, as_of)
        cst.set_account_latch(next_row_state, 'L4', trip_session)
    elif l4_result['reason'] == 'authorized_rearm':
        next_row_state['L4'] = {}

    # L5 - market halt L1/L2 interlock (persists through remainder of trading session)
    l5_result = rows_loss.evaluate_L5(validated_inputs, prior_row_state, authorized_events, session, as_of=as_of)
    rows_by_ID['L5'] = l5_result
    prior_l5 = cst.get_l5_state(prior_row_state)
    l5_cur_date = cst.canonical_session_date(session, as_of)
    if l5_result['reason'] == 'market_halt_level_1_or_2':
        cst.set_l5_active_trip(next_row_state, l5_cur_date)
    elif l5_result['reason'] == 'clearing_period':
        cst.set_l5_active_trip(next_row_state, prior_l5['trip_session'] or l5_cur_date)
    elif l5_result['reason'] == 'invalid_input':
        if prior_l5['active'] and (
            (prior_l5['trip_session'] is not None and prior_l5['trip_session'] == l5_cur_date)
            or (prior_l5['trip_session'] is None and prior_l5['fresh_session_count'] < 1)
        ):
            cst.set_l5_active_trip(next_row_state, prior_l5['trip_session'] or l5_cur_date)
        else:
            cst.set_l5_fresh_count(next_row_state, 0)
    elif prior_l5['active']:
        next_row_state['L5'] = {}
    else:
        cst.set_l5_fresh_count(next_row_state, prior_l5['fresh_session_count'] + 1)

    # Pre-compute O3 reconciliation state so L6 re-arm refuses if O3 reconciliation fails
    o3_result = rows_ops.evaluate_O3(validated_inputs, prior_row_state, authorized_events, session=session, as_of=as_of)
    o3_failed = o3_result['would_have_tripped'] or o3_result['state'] != 'NORMAL'

    # L6 - market halt L3 or 10% index fall breaker (canonical per Q6; rearm requires reconciliation)
    l6_result = rows_loss.evaluate_L6(
        validated_inputs, prior_row_state, authorized_events,
        session=session, as_of=as_of, o3_failed=o3_failed, market_closed=market_closed,
    )
    rows_by_ID['L6'] = l6_result
    if l6_result['latched']:
        prior_latch = cst.get_account_latch(prior_row_state, 'L6')
        trip_session = prior_latch['trip_session'] if prior_latch and prior_latch.get('trip_session') else session
        cst.set_account_latch(next_row_state, 'L6', trip_session)
    elif l6_result['reason'] == 'authorized_rearm':
        next_row_state['L6'] = {}

    # ==== VEHICLE ROWS (V1-V7) - Fund/symbol scope ====

    # Initialize fund-level outputs
    v1_outputs = {}
    v2_outputs = {}
    v3_outputs = {}
    v4_outputs = {}
    v5_outputs = {}
    v6_outputs = {}
    v7_outputs = {}

    raw_quotes = validated_inputs.get('quotes')
    raw_symbols = validated_inputs.get('symbols')

    for symbol in eval_fund_symbols:
        fund_inputs = funds[symbol] if (symbol in funds and isinstance(funds[symbol], dict)) else {}

        # V1 - tracking deviation (canonical format per Q6; 1-session and 5-session per §4.3/§6)
        v1_result = rows_vehicle.evaluate_V1(
            fund_inputs, prior_row_state, authorized_events, symbol,
            session=session, market_closed=market_closed, as_of=as_of,
        )
        session_returns = v1_result.pop('_session_returns', None)
        v1_outputs[symbol] = v1_result
        if session_returns is not None:
            cst.set_v1_history(next_row_state, symbol, session_returns)
        if v1_result['latched']:
            # Preserve original trip_session and deviation if already latched (§4.4 rule 3)
            prior_latch = cst.get_v1_latch(prior_row_state, symbol)
            if prior_latch:
                trip_session = prior_latch['trip_session']
                deviation = prior_latch['deviation']
            else:
                trip_session = session
                deviation = v1_result.get('deviation', 0.0)
            cst.set_v1_latch(next_row_state, symbol, trip_session, deviation)
        elif v1_result['reason'] == 'authorized_rearm':
            # Q4: clear latch on authorized re-arm - serialize as {"V1":{}}
            cst.clear_v1_latch(next_row_state, symbol)

        # V2 - corporate action (validate top-level, fund-level, AND corporate_action hwm_adjusted independently)
        v2_inputs = dict(fund_inputs)
        if 'hwm_adjusted' in validated_inputs:
            v2_inputs['_top_hwm_adjusted'] = validated_inputs['hwm_adjusted']
        v2_result = rows_vehicle.evaluate_V2(
            v2_inputs, prior_row_state, authorized_events, symbol,
            session=session, as_of=as_of,
        )
        v2_reconcile_target = v2_result.pop('_v2_reconcile_target', None)
        v2_mismatch_target = v2_result.pop('_v2_mismatch_target', None)
        v2_clear_mismatch_only = v2_result.pop('_v2_clear_mismatch_only', False)
        v2_enqueue_pending = v2_result.pop('_v2_enqueue_pending', False)
        v2_outputs[symbol] = v2_result

        # B1: Store V2 latches and finite-sanitized mismatch details (SUC-02) - canonical format per Q6
        if v2_result['latched']:
            # For new reconciliation_mismatch, store finite mismatch details + action identity
            if v2_result['reason'] == 'reconciliation_mismatch':
                corporate_action = fund_inputs.get('corporate_action', {})
                lv_clean = finite_float(fund_inputs.get('ledger_value'))
                bv_clean = finite_float(fund_inputs.get('broker_value'))
                mismatch_amt = abs(lv_clean - bv_clean) if (lv_clean is not None and bv_clean is not None) else None
                mismatch_details = {
                    'type': corporate_action.get('type') if isinstance(corporate_action, dict) else 'unknown',
                    'ledger_value': lv_clean,
                    'broker_value': bv_clean,
                    'mismatch': mismatch_amt
                }
                cst.set_v2_latched_mismatch(next_row_state, symbol, mismatch_details)
                ca_ident = cst.canonical_ca_identity(corporate_action)
                if ca_ident is not None:
                    cst.set_v2_latched_mismatch_action(next_row_state, symbol, ca_ident)
            # For persisting latch (latched_prior_mismatch), carry forward details + action identity
            elif v2_result['reason'] == 'latched_prior_mismatch':
                prior_mismatch = cst.get_v2_latched_mismatch(prior_row_state, symbol)
                if prior_mismatch:
                    cst.set_v2_latched_mismatch(next_row_state, symbol, prior_mismatch)
                prior_mismatch_action = cst.get_v2_latched_mismatch_action(prior_row_state, symbol)
                if prior_mismatch_action:
                    cst.set_v2_latched_mismatch_action(next_row_state, symbol, prior_mismatch_action)
        else:
            # Clear mismatch latch on Q4 re-arm even if carried pending actions remain
            if v2_result['reason'] == 'authorized_rearm' or v2_clear_mismatch_only:
                cst.clear_v2_latched_mismatch(next_row_state, symbol)

        # Update pending state (B1 canonical format per Q6; never drop unrelated queued/primary items)
        prior_pending = cst.get_v2_pending_adjustment(prior_row_state, symbol)
        prior_identities = cst.get_v2_pending_identities(prior_row_state, symbol)

        def _drop_pending_target(pending_obj: Dict[str, Any], target: Any) -> Optional[Dict[str, Any]]:
            updated = copy.deepcopy(pending_obj)
            if target == 'primary':
                q_list = list(updated.get('queued_actions') or [])
                if not q_list:
                    return None
                promoted = q_list.pop(0)
                new_p = {
                    'type': promoted.get('type'),
                    'ratio': promoted.get('ratio'),
                    'awaiting_reconciliation': True
                }
                if q_list:
                    new_p['queued_actions'] = q_list
                return new_p
            if isinstance(target, tuple) and target[0] == 'queued':
                q_list = list(updated.get('queued_actions') or [])
                idx_to_pop = target[1]
                if 0 <= idx_to_pop < len(q_list):
                    q_list.pop(idx_to_pop)
                if q_list:
                    updated['queued_actions'] = q_list
                else:
                    updated.pop('queued_actions', None)
                return updated
            return updated

        def _drop_identity_target(ident_obj: Optional[Dict[str, Any]], target: Any) -> Optional[Dict[str, Any]]:
            if not isinstance(ident_obj, dict):
                return None
            updated_id = copy.deepcopy(ident_obj)
            q_ids = list(updated_id.get('queued') or [])
            if target == 'primary':
                if not q_ids:
                    return None
                promoted_id = q_ids.pop(0)
                res_id: Dict[str, Any] = {'primary': promoted_id}
                if q_ids:
                    res_id['queued'] = q_ids
                return res_id
            if isinstance(target, tuple) and target[0] == 'queued':
                idx_to_pop = target[1]
                if 0 <= idx_to_pop < len(q_ids):
                    q_ids.pop(idx_to_pop)
                if q_ids:
                    updated_id['queued'] = q_ids
                else:
                    updated_id.pop('queued', None)
                return updated_id
            return updated_id

        def _merge_pending_action(
            existing_pending: Optional[Dict[str, Any]],
            existing_idents: Optional[Dict[str, Any]],
            ca_obj: Any,
        ) -> tuple:
            inc_ident = cst.canonical_ca_identity(ca_obj)
            if existing_pending:
                merged = copy.deepcopy(existing_pending)
                merged_idents = copy.deepcopy(existing_idents) if isinstance(existing_idents, dict) else {}
                if isinstance(ca_obj, dict) and ca_obj.get('type'):
                    inc_type = ca_obj.get('type')
                    inc_ratio = ca_obj.get('ratio')
                    inc_ratio_num = finite_float(inc_ratio)
                    prim_ratio_num = finite_float(merged.get('ratio'))
                    ratio_matches_prim = (
                        inc_ratio_num == prim_ratio_num
                        if (inc_ratio_num is not None or prim_ratio_num is not None)
                        else inc_ratio == merged.get('ratio')
                    )
                    if isinstance(merged_idents.get('primary'), dict) and inc_ident is not None:
                        matches_primary = (inc_ident == merged_idents['primary'])
                    else:
                        matches_primary = (inc_type == merged.get('type') and ratio_matches_prim)
                    q_list = list(merged.get('queued_actions') or [])
                    q_ids = list(merged_idents.get('queued') or [])
                    matches_queued = False
                    for idx_q, q in enumerate(q_list):
                        if not isinstance(q, dict):
                            continue
                        if idx_q < len(q_ids) and isinstance(q_ids[idx_q], dict) and inc_ident is not None:
                            if inc_ident == q_ids[idx_q]:
                                matches_queued = True
                                break
                        else:
                            q_r_num = finite_float(q.get('ratio'))
                            r_eq = (
                                inc_ratio_num == q_r_num
                                if (inc_ratio_num is not None or q_r_num is not None)
                                else inc_ratio == q.get('ratio')
                            )
                            if q.get('type') == inc_type and r_eq:
                                matches_queued = True
                                break
                    if not matches_primary and not matches_queued:
                        q_list.append({'type': inc_type, 'ratio': inc_ratio})
                        merged['queued_actions'] = q_list
                        if inc_ident is not None:
                            while len(q_ids) < len(q_list) - 1:
                                q_ids.append(None)
                            q_ids.append(inc_ident)
                            merged_idents['queued'] = q_ids
                return merged, (merged_idents if merged_idents else None)
            if isinstance(ca_obj, dict) and ca_obj.get('type'):
                pending_obj = {
                    'type': ca_obj.get('type'),
                    'ratio': ca_obj.get('ratio'),
                    'awaiting_reconciliation': True
                }
                idents_obj = {'primary': inc_ident} if inc_ident is not None else None
                return pending_obj, idents_obj
            return None, None

        corporate_action = fund_inputs.get('corporate_action')
        if v2_result['reason'] == 'reconciliation_mismatch':
            if prior_pending:
                if v2_mismatch_target is not None:
                    remaining_pending = _drop_pending_target(prior_pending, v2_mismatch_target)
                    remaining_idents = _drop_identity_target(prior_identities, v2_mismatch_target)
                    if remaining_pending is not None:
                        cst.set_v2_pending_adjustment(next_row_state, symbol, remaining_pending)
                        if remaining_idents is not None:
                            cst.set_v2_pending_identities(next_row_state, symbol, remaining_idents)
                    else:
                        cst.clear_v2_pending_adjustment(next_row_state, symbol)
                else:
                    cst.set_v2_pending_adjustment(next_row_state, symbol, copy.deepcopy(prior_pending))
                    if prior_identities is not None:
                        cst.set_v2_pending_identities(next_row_state, symbol, copy.deepcopy(prior_identities))
        elif v2_result['reason'] in ['pending_corporate_action', 'pending_corporate_action_carried']:
            if v2_reconcile_target is not None and prior_pending:
                remaining_pending = _drop_pending_target(prior_pending, v2_reconcile_target)
                remaining_idents = _drop_identity_target(prior_identities, v2_reconcile_target)
                if remaining_pending is not None:
                    cst.set_v2_pending_adjustment(next_row_state, symbol, remaining_pending)
                    if remaining_idents is not None:
                        cst.set_v2_pending_identities(next_row_state, symbol, remaining_idents)
                else:
                    cst.clear_v2_pending_adjustment(next_row_state, symbol)
            else:
                merged_pending, merged_idents = _merge_pending_action(prior_pending, prior_identities, corporate_action)
                if merged_pending is not None:
                    cst.set_v2_pending_adjustment(next_row_state, symbol, merged_pending)
                    if merged_idents is not None:
                        cst.set_v2_pending_identities(next_row_state, symbol, merged_idents)
        elif v2_result['reason'] in ('exact_reconciliation_auto_clear', 'authorized_rearm'):
            # Clear pending on exact reconciliation or authorized re-arm (only when no pending remained)
            cst.clear_v2_pending_adjustment(next_row_state, symbol)
        elif v2_enqueue_pending:
            # Distinct/new corporate action announced while mismatch latch is active -> keep pending queue
            merged_pending, merged_idents = _merge_pending_action(prior_pending, prior_identities, corporate_action)
            if merged_pending is not None:
                cst.set_v2_pending_adjustment(next_row_state, symbol, merged_pending)
                if merged_idents is not None:
                    cst.set_v2_pending_identities(next_row_state, symbol, merged_idents)
        else:
            if prior_pending:
                cst.set_v2_pending_adjustment(next_row_state, symbol, copy.deepcopy(prior_pending))
                if prior_identities is not None:
                    cst.set_v2_pending_identities(next_row_state, symbol, copy.deepcopy(prior_identities))

        # V3 - fund event (canonical format per Q6)
        v3_result = rows_vehicle.evaluate_V3(
            fund_inputs, prior_row_state, authorized_events, symbol,
            session=session, as_of=as_of,
        )
        v3_outputs[symbol] = v3_result
        if v3_result['latched']:
            # Preserve original trip_session if already latched (§4.4 rule 3: persistence)
            prior_latch = cst.get_fund_latch(prior_row_state, 'V3', symbol)
            trip_session = prior_latch['trip_session'] if prior_latch else session
            details = {'reason': v3_result['reason'], 'fund_event': v3_result.get('fund_event')}
            cst.set_fund_latch(next_row_state, 'V3', symbol, trip_session, details)
        elif v3_result['reason'] == 'authorized_rearm':
            if 'V3' not in next_row_state:
                next_row_state['V3'] = {}

        # V4 - extreme volatility (B4 canonical format per Q6; closed/holiday skips increment)
        v4_result = rows_vehicle.evaluate_V4(
            fund_inputs, prior_row_state, authorized_events, symbol, session,
            market_closed=market_closed
        )
        preserve_v4_session = v4_result.pop('_preserve_prior_v4_session', False)
        v4_outputs[symbol] = v4_result
        # Update V4 state based on result reason - counter now in outputs
        if preserve_v4_session:
            prior_v4 = cst.get_v4_state(prior_row_state, symbol)
            if prior_v4['clearing_counter'] is not None:
                cst.set_v4_state(
                    next_row_state, symbol,
                    prior_v4['clearing_counter'],
                    prior_v4['last_session_below_threshold']
                )
        elif v4_result['reason'] == 'volatility_at_or_above_2x':
            # Initial trip: volatility HIGH, no "last session below threshold" yet
            counter = v4_result.get('outputs', {}).get('clearing_counter', 0)
            cst.set_v4_state(next_row_state, symbol, counter, None)
        elif v4_result['reason'] == 'volatility_below_2x_but_not_yet_cleared':
            # Clearing phase: record this session as below threshold
            counter = v4_result.get('outputs', {}).get('clearing_counter', 0)
            cst.set_v4_state(next_row_state, symbol, counter, session)
        elif v4_result['reason'] == 'cleared_after_ten_sessions':
            # At count 10, report 10 and remove the fund entry per Q1
            cst.clear_v4_state(next_row_state, symbol)
        elif v4_result['reason'] == 'missing_input':
            # A07.1: Missing forecast earns zero clearing credit but MUST preserve active V4 counter
            prior_v4 = cst.get_v4_state(prior_row_state, symbol)
            if prior_v4['clearing_counter'] is not None:
                cst.set_v4_state(
                    next_row_state, symbol,
                    prior_v4['clearing_counter'],
                    prior_v4['last_session_below_threshold']
                )

        # V5 - decay monitor (B5 canonical format per Q6; fallback to top-level policy v5_tolerance)
        v5_inputs = fund_inputs if 'v5_tolerance' in fund_inputs else {**fund_inputs, 'v5_tolerance': validated_inputs.get('v5_tolerance')}
        v5_result = rows_vehicle.evaluate_V5(
            v5_inputs, prior_row_state, authorized_events, symbol, session,
            as_of=as_of, market_closed=market_closed,
        )
        preserve_v5_session = v5_result.pop('_preserve_prior_v5_session', False)
        v5_outputs[symbol] = v5_result
        # Update V5 state - preserve prior state across omitted/missing/unresolved/invalid/closed checks without inventing last_session
        prior_v5_raw = (prior_row_state.get('V5') or {}).get(symbol) if isinstance(prior_row_state, dict) else None
        if v5_result['reason'] == 'authorized_rearm':
            cst.clear_v5_state(next_row_state, symbol)
        elif (
            preserve_v5_session
            or symbol not in funds
            or v5_result['reason'] in ('missing_input', 'unresolved_fail_closed', 'invalid_input')
            or (v5_result['outputs']['escalated_to_v1'] and v5_result['outputs']['excess_shortfall'] is None)
        ):
            if isinstance(prior_v5_raw, dict) and (prior_v5_raw or v5_result['outputs']['escalated_to_v1']):
                if 'V5' not in next_row_state:
                    next_row_state['V5'] = {}
                preserved_entry = copy.deepcopy(prior_v5_raw)
                if v5_result['outputs']['escalated_to_v1']:
                    preserved_entry.setdefault(
                        'consecutive_beyond_tolerance',
                        v5_result['outputs']['consecutive_sessions_beyond_tolerance'],
                    )
                    preserved_entry['escalated'] = True
                next_row_state['V5'][symbol] = preserved_entry
        else:
            new_count = v5_result['outputs']['consecutive_sessions_beyond_tolerance']
            escalated = v5_result['outputs']['escalated_to_v1']
            cst.set_v5_state(next_row_state, symbol, new_count, escalated, session)

        # V6 - carrying cost
        v6_result = rows_vehicle.evaluate_V6(fund_inputs, prior_row_state, authorized_events, symbol)
        v6_outputs[symbol] = v6_result

        # V7 - quote/liquidity (for fund missing from funds, inspect quotes/symbols lookup; inherit v7_spread_width)
        if symbol in funds:
            v7_base = fund_inputs
        elif isinstance(raw_quotes, dict) and isinstance(raw_quotes.get(symbol), dict):
            v7_base = raw_quotes[symbol]
        elif isinstance(raw_symbols, dict) and isinstance(raw_symbols.get(symbol), dict):
            v7_base = raw_symbols[symbol]
        else:
            v7_base = {}
        v7_inputs = v7_base if 'v7_spread_width' in v7_base else {**v7_base, 'v7_spread_width': validated_inputs.get('v7_spread_width')}
        v7_result = rows_vehicle.evaluate_V7(v7_inputs, prior_row_state, authorized_events, symbol)
        v7_outputs[symbol] = v7_result

    # V7 also evaluates remaining proposed order symbols outside eval_fund_symbols (e.g. cash ETFs like BIL)
    for sym in order_symbols:
        if sym in eval_fund_symbols:
            continue
        sym_quote = {}
        if isinstance(raw_quotes, dict) and isinstance(raw_quotes.get(sym), dict):
            sym_quote = raw_quotes[sym]
        elif isinstance(raw_symbols, dict) and isinstance(raw_symbols.get(sym), dict):
            sym_quote = raw_symbols[sym]
        v7_order_inputs = (
            sym_quote
            if 'v7_spread_width' in sym_quote
            else {**sym_quote, 'v7_spread_width': validated_inputs.get('v7_spread_width')}
        )
        v7_outputs[sym] = rows_vehicle.evaluate_V7(
            v7_order_inputs, prior_row_state, authorized_events, sym
        )
    if invalid_order_symbol:
        v7_outputs['__invalid_order_symbol__'] = {
            'state': 'NORMAL',
            'would_have_tripped': True,
            'kind': 'interlock',
            'latched': False,
            'reason': 'invalid_input',
            'fund': '__invalid_order_symbol__',
        }

    # Aggregate vehicle rows into single row outputs
    rows_by_ID['V1'] = aggregate_fund_row(v1_outputs)
    rows_by_ID['V2'] = aggregate_fund_row(v2_outputs)
    rows_by_ID['V3'] = aggregate_fund_row(v3_outputs)
    rows_by_ID['V4'] = aggregate_fund_row(v4_outputs)
    # V5, V6 are diagnostic with structured row-level outputs (Fix 2)
    # V7 is symbol-scoped interlock (not diagnostic)
    rows_by_ID['V5'] = aggregate_diagnostic_row(v5_outputs)
    rows_by_ID['V6'] = aggregate_diagnostic_row(v6_outputs)
    rows_by_ID['V7'] = aggregate_fund_row(v7_outputs, symbol_scoped=True, emit_scope_keys=False)

    # V2, V4, V5 state now written inline using canonical accessors per Q6

    # ==== OPERATIONS ROWS (O1-O9, J1) ====

    # O1 - required inputs stale/missing (B3; same-day stale->fresh cannot clear mid-session)
    o1_result = rows_ops.evaluate_O1(validated_inputs, prior_row_state, authorized_events, session, as_of=as_of)
    rows_by_ID['O1'] = o1_result
    o1_state = cst.get_o1_fresh_state(prior_row_state)
    o1_cur_date = cst.canonical_session_date(session, as_of)
    if o1_result['reason'] == 'clearing_period':
        cst.set_o1_fresh_state(
            next_row_state, 0, o1_cur_date,
            last_tripped_session=o1_state.get('last_tripped_session') or o1_cur_date
        )
    elif not o1_result['would_have_tripped']:
        if o1_state.get('last_session') == o1_cur_date and o1_state.get('fresh_session_count', 0) == 0:
            # Stale->fresh transition within the same session must NOT count as a full clean session
            cst.set_o1_fresh_state(next_row_state, 0, o1_cur_date)
        elif o1_state.get('last_session') == o1_cur_date:
            cst.set_o1_fresh_state(next_row_state, o1_state['fresh_session_count'], o1_cur_date)
        else:
            fresh_count = o1_state['fresh_session_count'] + 1
            cst.set_o1_fresh_state(next_row_state, fresh_count, o1_cur_date)
    else:
        # Operational stale/missing trip records last_tripped_session so same-day recovery cannot clear
        if o1_result['reason'] == 'stale_or_missing_input':
            cst.set_o1_fresh_state(next_row_state, 0, o1_cur_date, last_tripped_session=o1_cur_date)
        else:
            cst.set_o1_fresh_state(next_row_state, 0, o1_cur_date)

    # O2 - session calendar
    o2_result = rows_ops.evaluate_O2(validated_inputs, prior_row_state, authorized_events)
    rows_by_ID['O2'] = o2_result

    # O3 - reconciliation mismatch (already evaluated before L6)
    rows_by_ID['O3'] = o3_result
    if o3_result['latched']:
        # Preserve original trip_session if already latched (§4.4 rule 3: persistence)
        prior_latch = cst.get_account_latch(prior_row_state, 'O3')
        trip_session = prior_latch['trip_session'] if prior_latch else session
        cst.set_account_latch(next_row_state, 'O3', trip_session)
    elif o3_result['reason'] == 'authorized_rearm':
        next_row_state['O3'] = {}

    # O4 - order timeout (canonical format per Q6, enumerate from all sources)
    o4_outputs = {}
    restart_marker = validated_inputs.get('restart_marker')

    for symbol in all_symbols:
        # Build order input for this symbol safely even if funds[symbol] is missing/non-dict
        order_input = {}
        if symbol in funds and isinstance(funds[symbol], dict):
            order_input = funds[symbol].copy()

        # Add pending order if present
        if symbol in pending_orders:
            order_input['o4_pending_order'] = pending_orders[symbol]

        # Forward restart_marker into O4 evaluation (Q6)
        if restart_marker:
            order_input['restart_marker'] = restart_marker

        o4_result = rows_ops.evaluate_O4(
            order_input, prior_row_state, authorized_events, symbol,
            session=session, as_of=as_of,
        )

        if malformed_pending_orders:
            prior_pending = cst.get_o4_pending_order(prior_row_state, symbol)
            if prior_pending:
                # Never allow malformed pending_orders to clear an active prior O4 lock
                order_id = prior_pending['original_order_id']
                status = prior_pending.get('status', 'unknown')
                latched = True
            elif o4_result.get('original_order_id'):
                # Record newly reported timed-out/malformed fund o4_pending_order even when top-level pending_orders failed
                order_id = o4_result['original_order_id']
                status = 'unknown'
                latched = bool(o4_result.get('latched'))
            else:
                order_id = None
                status = 'unknown'
                latched = False

            o4_out = {
                'state': 'ORDER-UNKNOWN',
                'would_have_tripped': True,
                'kind': 'interlock_or_breaker',
                'latched': latched,
                'reason': 'malformed_pending_orders',
                'symbol': symbol,
            }
            if order_id:
                o4_out['original_order_id'] = order_id
                cst.set_o4_pending_order(next_row_state, symbol, order_id, status)
            o4_outputs[symbol] = o4_out
            continue

        o4_outputs[symbol] = o4_result

        # Store O4 state using canonical accessors (Q6)
        if o4_result.get('original_order_id'):
            order_id = o4_result['original_order_id']
            status = 'unknown'
            cst.set_o4_pending_order(next_row_state, symbol, order_id, status)
        elif o4_result['reason'] in ('original_order_terminal_status_received', 'order_filled', 'authorized_rearm'):
            # Clear resolved order
            cst.clear_o4_pending_order(next_row_state, symbol)

    rows_by_ID['O4'] = aggregate_fund_row(o4_outputs, symbol_scoped=True)

    # O5 - pre-trade limits (B2 canonical format per Q6)
    o5_result = rows_ops.evaluate_O5(validated_inputs, prior_row_state, authorized_events, session, as_of=as_of)
    rows_by_ID['O5'] = o5_result
    # O5 state: preserve session rejection count & latch across mixed PASS/REJECT checks in same session
    if o5_result['reason'] == 'authorized_rearm':
        next_row_state['O5'] = {}
    elif o5_result['would_have_tripped']:
        count = o5_result.get('rejection_count_this_session', 1)
        latched = o5_result['latched']
        cst.set_o5_state(next_row_state, count, session, latched)
    else:
        prior_o5 = cst.get_o5_state(prior_row_state)
        if prior_o5['last_rejection_session'] == session and (prior_o5['rejection_count'] > 0 or prior_o5['latched']):
            cst.set_o5_state(next_row_state, prior_o5['rejection_count'], session, prior_o5['latched'])

    # O6 - policy hash mismatch (canonical format per Q6)
    o6_result = rows_ops.evaluate_O6(validated_inputs, prior_row_state, authorized_events, session=session, as_of=as_of)
    rows_by_ID['O6'] = o6_result
    if o6_result['latched']:
        # Preserve original trip_session if already latched (§4.4 rule 3: persistence)
        prior_latch = cst.get_account_latch(prior_row_state, 'O6')
        trip_session = prior_latch['trip_session'] if prior_latch else session
        cst.set_account_latch(next_row_state, 'O6', trip_session)
    elif o6_result['reason'] == 'authorized_rearm':
        next_row_state['O6'] = {}

    # O7 - dead man's switch (canonical format per Q6)
    o7_result = rows_ops.evaluate_O7(validated_inputs, prior_row_state, authorized_events, as_of=as_of, session=session)
    rows_by_ID['O7'] = o7_result
    if o7_result['latched']:
        # Preserve original trip_session if already latched (§4.4 rule 3: persistence)
        prior_latch = cst.get_account_latch(prior_row_state, 'O7')
        trip_session = prior_latch['trip_session'] if prior_latch else session
        cst.set_account_latch(next_row_state, 'O7', trip_session)
    elif o7_result['reason'] == 'authorized_rearm':
        next_row_state['O7'] = {}

    # O8 - kill switch (persists latch ONLY when fired/active; omits O8 from clean next_row_state)
    o8_result = rows_ops.evaluate_O8(
        validated_inputs, prior_row_state, authorized_events, session=session, as_of=as_of
    )
    rows_by_ID['O8'] = o8_result
    if o8_result['latched']:
        prior_latch = cst.get_account_latch(prior_row_state, 'O8')
        trip_session = prior_latch['trip_session'] if prior_latch and prior_latch.get('trip_session') else cst.canonical_session_date(session, as_of)
        next_row_state['O8'] = {'latched': True, 'trip_session': trip_session}
    elif o8_result['reason'] == 'authorized_rearm':
        next_row_state['O8'] = {}

    # O9 - unauthorized parameter change (canonical format per Q6)
    o9_result = rows_ops.evaluate_O9(validated_inputs, prior_row_state, authorized_events, session=session, as_of=as_of)
    rows_by_ID['O9'] = o9_result
    if o9_result['latched']:
        # Preserve original trip_session if already latched (§4.4 rule 3: persistence)
        prior_latch = cst.get_account_latch(prior_row_state, 'O9')
        trip_session = prior_latch['trip_session'] if prior_latch else session
        cst.set_account_latch(next_row_state, 'O9', trip_session)
    elif o9_result['reason'] == 'authorized_rearm':
        next_row_state['O9'] = {}

    # J1 - joint exposure limit
    j1_result = rows_ops.evaluate_J1(validated_inputs, prior_row_state, authorized_events)
    rows_by_ID['J1'] = j1_result

    # Preserve durable scope state for funds/symbols not in current inputs (Q6 & A07.2)
    # V1, V2, V3, V4, V5, O4 state must survive across calls even when scope is absent
    if prior_row_state:
        # Preserve V1 state (latches and 5-session history) for absent funds, and reflect active latches
        for fund in sorted(prior_row_state.get('V1', {}).keys()):
            v1_fund_state = prior_row_state['V1'][fund]
            if fund not in eval_fund_symbols and isinstance(v1_fund_state, dict) and v1_fund_state:
                if 'V1' not in next_row_state:
                    next_row_state['V1'] = {}
                if fund not in next_row_state['V1']:
                    next_row_state['V1'][fund] = copy.deepcopy(v1_fund_state)
                if v1_fund_state.get('latched'):
                    v1_outputs[fund] = {
                        'state': 'NO-NEW-RISK',
                        'would_have_tripped': True,
                        'kind': 'breaker',
                        'latched': True,
                        'reason': 'latched_prior_trip',
                        'fund': fund
                    }

        # Preserve V2 latched mismatches, pending adjustments, and their action identities for absent funds
        prior_v2 = prior_row_state.get('V2', {})
        if isinstance(prior_v2, dict):
            prior_mism_actions = prior_v2.get('latched_mismatch_actions', {})
            prior_pend_idents = prior_v2.get('pending_action_identities', {})
            for fund in sorted(prior_v2.get('latched_mismatches', {}).keys()):
                mismatch = prior_v2['latched_mismatches'][fund]
                if fund not in eval_fund_symbols and mismatch:
                    cst.set_v2_latched_mismatch(next_row_state, fund, mismatch)
                    if isinstance(prior_mism_actions, dict) and isinstance(prior_mism_actions.get(fund), dict):
                        cst.set_v2_latched_mismatch_action(next_row_state, fund, copy.deepcopy(prior_mism_actions[fund]))
                    v2_outputs[fund] = {
                        'state': 'HALT',
                        'would_have_tripped': True,
                        'kind': 'interlock_or_breaker',
                        'latched': True,
                        'reason': 'latched_prior_mismatch',
                        'fund': fund
                    }
            for fund in sorted(prior_v2.get('pending_adjustments', {}).keys()):
                pending = prior_v2['pending_adjustments'][fund]
                if fund not in eval_fund_symbols and pending:
                    cst.set_v2_pending_adjustment(next_row_state, fund, pending)
                    if isinstance(prior_pend_idents, dict) and isinstance(prior_pend_idents.get(fund), dict):
                        cst.set_v2_pending_identities(next_row_state, fund, copy.deepcopy(prior_pend_idents[fund]))
                    if fund not in v2_outputs:
                        v2_outputs[fund] = {
                            'state': 'HALT',
                            'would_have_tripped': True,
                            'kind': 'interlock_or_breaker',
                            'latched': False,
                            'reason': 'pending_corporate_action_carried',
                            'fund': fund
                        }

        # Preserve V3 latches for absent funds
        for fund in sorted(prior_row_state.get('V3', {}).keys()):
            v3_fund_state = prior_row_state['V3'][fund]
            if fund not in eval_fund_symbols and isinstance(v3_fund_state, dict) and v3_fund_state.get('latched'):
                if 'V3' not in next_row_state:
                    next_row_state['V3'] = {}
                if fund not in next_row_state['V3']:
                    next_row_state['V3'][fund] = v3_fund_state
                v3_outputs[fund] = {
                    'state': 'DE-RISK',
                    'would_have_tripped': True,
                    'kind': 'breaker',
                    'latched': True,
                    'reason': 'latched_prior_trip',
                    'fund': fund
                }

        # Preserve V4 state and active restrictions for funds omitted from current inputs (A08)
        if 'V4' in prior_row_state:
            if 'V4' not in next_row_state:
                next_row_state['V4'] = {}
            for fund in sorted(prior_row_state['V4'].keys()):
                v4_fund_state = prior_row_state['V4'][fund]
                if fund not in eval_fund_symbols:
                    if fund not in next_row_state['V4']:
                        next_row_state['V4'][fund] = copy.deepcopy(v4_fund_state)
                    if isinstance(v4_fund_state, dict) and v4_fund_state.get('clearing_counter') is not None:
                        v4_outputs[fund] = {
                            'state': 'NO-NEW-RISK',
                            'would_have_tripped': True,
                            'kind': 'interlock',
                            'latched': False,
                            'reason': 'volatility_below_2x_but_not_yet_cleared',
                            'fund': fund,
                            'outputs': {'clearing_counter': v4_fund_state['clearing_counter']}
                        }

        # Preserve V5 state and escalated restrictions (or clear on Q4 re-arm) for funds omitted from current inputs
        if 'V5' in prior_row_state and isinstance(prior_row_state['V5'], dict):
            if 'V5' not in next_row_state:
                next_row_state['V5'] = {}
            for fund in sorted(prior_row_state['V5'].keys()):
                if fund in eval_fund_symbols:
                    continue
                v5_fund_state = prior_row_state['V5'][fund]
                is_esc_omitted = isinstance(v5_fund_state, dict) and (
                    bool(v5_fund_state.get('escalated')) or (
                        isinstance(v5_fund_state.get('consecutive_beyond_tolerance'), (int, float))
                        and not isinstance(v5_fund_state.get('consecutive_beyond_tolerance'), bool)
                        and v5_fund_state.get('consecutive_beyond_tolerance') >= 40
                    )
                )
                if is_esc_omitted and cst.matches_fund_rearm(
                    authorized_events, 'V5', fund, session=session, as_of=as_of, require_date=True
                ) is not None:
                    cst.clear_v5_state(next_row_state, fund)
                    v5_outputs[fund] = {
                        'state': 'NORMAL',
                        'would_have_tripped': False,
                        'kind': 'diagnostic',
                        'latched': False,
                        'reason': 'authorized_rearm',
                        'fund': fund,
                        'outputs': {
                            'excess_shortfall': None,
                            'consecutive_sessions_beyond_tolerance': 0,
                            'escalated_to_v1': False,
                            'disposition': 'authorized_rearm',
                        },
                    }
                    continue
                if fund not in next_row_state['V5']:
                    next_row_state['V5'][fund] = copy.deepcopy(v5_fund_state)
                if is_esc_omitted:
                    cnt = v5_fund_state.get('consecutive_beyond_tolerance')
                    preserved_cnt = cnt if isinstance(cnt, int) and not isinstance(cnt, bool) and cnt >= 40 else 40
                    v5_outputs[fund] = {
                        'state': 'NO-NEW-RISK',
                        'would_have_tripped': True,
                        'kind': 'diagnostic',
                        'latched': False,
                        'reason': 'escalated_to_v1_treatment',
                        'fund': fund,
                        'outputs': {
                            'excess_shortfall': None,
                            'consecutive_sessions_beyond_tolerance': preserved_cnt,
                            'escalated_to_v1': True,
                            'disposition': 'escalated_to_v1',
                        },
                    }

        # Re-aggregate V1/V2/V3/V4/V5 if absent restricted/escalated funds were carried into outputs
        if prior_latched_funds:
            rows_by_ID['V1'] = aggregate_fund_row(v1_outputs)
            rows_by_ID['V2'] = aggregate_fund_row(v2_outputs)
            rows_by_ID['V3'] = aggregate_fund_row(v3_outputs)
            rows_by_ID['V4'] = aggregate_fund_row(v4_outputs)
            rows_by_ID['V5'] = aggregate_diagnostic_row(v5_outputs)

        # O4 already preserved via symbol enumeration (all_symbols includes prior O4 keys)

    # ==== PRECEDENCE RESOLUTION ====
    # Per §4.1: HALT > ORDER-UNKNOWN > DE-RISK > NO-NEW-RISK > NORMAL
    # Account HALT dominates all narrower scopes

    effective_state_by_scope = {}

    # Precedence order
    precedence = ['HALT', 'ORDER-UNKNOWN', 'DE-RISK', 'NO-NEW-RISK', 'NORMAL']

    # First, resolve account scope to check for dominance (Q5)
    account_states = []
    for row_id in ['L0', 'L1', 'L2', 'L3', 'L4', 'L5', 'L6', 'O1', 'O2', 'O3', 'O5', 'O6', 'O7', 'O8', 'O9']:
        row_result = rows_by_ID[row_id]
        account_states.append(row_result['state'])

    account_state = 'NORMAL'
    for state in precedence:
        if state in account_states:
            account_state = state
            break

    effective_state_by_scope['account'] = account_state

    # A08 / Q5: Account HALT dominates every narrower scope (§4.1), including joint
    if account_state == 'HALT':
        for scope in scopes:
            effective_state_by_scope[scope] = 'HALT'
    else:
        # Account not HALT - resolve each scope independently
        for scope in scopes:
            if scope == 'account':
                continue  # Already resolved above

            states_for_scope = []

            if scope.startswith('fund:'):
                symbol = scope.split(':')[1]
                # Fund-scoped rows
                if symbol in v1_outputs:
                    states_for_scope.append(v1_outputs[symbol]['state'])
                if symbol in v2_outputs:
                    states_for_scope.append(v2_outputs[symbol]['state'])
                if symbol in v3_outputs:
                    states_for_scope.append(v3_outputs[symbol]['state'])
                if symbol in v4_outputs:
                    states_for_scope.append(v4_outputs[symbol]['state'])
                if symbol in v5_outputs:
                    states_for_scope.append(v5_outputs[symbol]['state'])
            elif scope.startswith('symbol:'):
                symbol = scope.split(':')[1]
                # Symbol-scoped rows (O4, V7)
                if symbol in o4_outputs:
                    states_for_scope.append(o4_outputs[symbol]['state'])
                if symbol in v7_outputs:
                    states_for_scope.append(v7_outputs[symbol]['state'])
            elif scope == 'joint':
                # Joint-scoped rows
                states_for_scope.append(j1_result['state'])

            # Resolve by precedence
            effective_state = 'NORMAL'
            for state in precedence:
                if state in states_for_scope:
                    effective_state = state
                    break

            effective_state_by_scope[scope] = effective_state

    # G6 (F1-A10 / §5.3): Bind spec_version, canonical engine_hash, and shadow safety assertions
    next_row_state['spec_version'] = spec_version
    next_row_state['engine_hash'] = canonical_engine_hash
    next_row_state['enforcement_mode'] = 'shadow_would_have_tripped'
    next_row_state['loss_breakers_armed'] = False

    return (rows_by_ID, effective_state_by_scope, next_row_state)


def aggregate_fund_row(
    fund_outputs: Dict[str, Dict[str, Any]],
    symbol_scoped: bool = False,
    emit_scope_keys: bool = True
) -> Dict[str, Any]:
    """Aggregate per-fund row outputs into a single row output.

    For fund-scoped rows, returns the most restrictive state across all funds.
    """
    if not fund_outputs:
        return {
            'state': 'NORMAL',
            'would_have_tripped': False,
            'kind': 'breaker',
            'latched': False,
            'reason': 'none'
        }

    # Order-independent deterministic key traversal (canonical order across dict permutations)
    ordered_keys = sorted(fund_outputs.keys()) if symbol_scoped else sorted(fund_outputs.keys(), key=_fund_sort_key)
    sample = fund_outputs[ordered_keys[0]]
    kind = sample['kind']

    # Aggregate: take most restrictive state (Fix 2: single-token reason)
    precedence = ['HALT', 'ORDER-UNKNOWN', 'DE-RISK', 'NO-NEW-RISK', 'NORMAL']
    most_restrictive = 'NORMAL'
    any_tripped = False
    any_latched = False
    reasons = []
    affected_funds = []

    for symbol in ordered_keys:
        result = fund_outputs[symbol]
        state = result['state']
        if precedence.index(state) < precedence.index(most_restrictive):
            most_restrictive = state
        if result['would_have_tripped']:
            any_tripped = True
            affected_funds.append(symbol)
        if result.get('latched'):
            any_latched = True
        if result['reason'] != 'none':
            reasons.append((symbol, result['reason']))

    # Determine single reason token (Fix 2 + Fix 3: golden expects single token with fund suffix where applicable)
    if not reasons:
        reason = 'none'
    elif len(reasons) == 1:
        symbol, base_reason = reasons[0]
        needs_suffix = (len(fund_outputs) > 1 and
                       base_reason in ['pending_corporate_action', 'pending_corporate_action_carried'])
        reason = f'{base_reason}_{symbol}' if needs_suffix else base_reason
    else:
        unique_reasons = set(r for _, r in reasons)
        if len(unique_reasons) == 1:
            reason = next(iter(unique_reasons))
            for symbol in ordered_keys:
                if reason.endswith(f'_{symbol}'):
                    reason = reason[:-len(f'_{symbol}')]
                    break
        else:
            reason = ', '.join(f"{sym}:{r}" for sym, r in reasons)

    # Build aggregate output
    aggregate = {
        'state': most_restrictive,
        'would_have_tripped': any_tripped,
        'kind': kind,
        'latched': any_latched,
        'reason': reason
    }

    # Include per-fund outputs in deterministic order whenever any fund emits outputs
    if any('outputs' in fund_outputs[symbol] for symbol in ordered_keys):
        if len(ordered_keys) == 1:
            single_fund = ordered_keys[0]
            aggregate['outputs'] = copy.deepcopy(fund_outputs[single_fund].get('outputs', {}))
        else:
            aggregate['outputs'] = {
                symbol: copy.deepcopy(fund_outputs[symbol].get('outputs', {}))
                for symbol in ordered_keys
            }

    # For fund/symbol-scoped rows, include affected funds/symbols
    if emit_scope_keys:
        if len(ordered_keys) == 1:
            single_key = ordered_keys[0]
            single_result = fund_outputs[single_key]
            if symbol_scoped:
                aggregate['symbol'] = single_result.get('symbol', single_key)
                if affected_funds:
                    aggregate['symbols'] = affected_funds
            else:
                aggregate['fund'] = single_result.get('fund', single_key)
        elif affected_funds:
            if symbol_scoped:
                aggregate['symbols'] = affected_funds
            else:
                already_suffixed = any(reason.endswith(f'_{s}') for s in affected_funds)
                if len(affected_funds) == 1 and not already_suffixed:
                    aggregate['fund'] = affected_funds[0]
                elif len(affected_funds) > 1:
                    aggregate['funds'] = affected_funds

    # G1.3: Preserve declared successor evidence fields (deviation, original_order_id)
    if len(ordered_keys) == 1:
        single_fund = ordered_keys[0]
        single_result = fund_outputs[single_fund]

        if 'deviation' in single_result:
            aggregate['deviation'] = single_result['deviation']

        if 'original_order_id' in single_result:
            aggregate['original_order_id'] = single_result['original_order_id']

    return aggregate


def aggregate_diagnostic_row(fund_outputs: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate diagnostic rows (V5, V6) in a deterministic, dict-order-independent manner."""
    if not fund_outputs:
        return {
            'state': 'NORMAL',
            'would_have_tripped': False,
            'kind': 'diagnostic',
            'latched': False,
            'reason': 'none'
        }

    ordered_keys = sorted(fund_outputs.keys(), key=_fund_sort_key)
    precedence = ['HALT', 'ORDER-UNKNOWN', 'DE-RISK', 'NO-NEW-RISK', 'NORMAL']
    disposition_rank = {
        'UNRESOLVED-FAIL-CLOSED': 0,
        'beyond_tolerance': 1,
        'missing_data': 2,
        'invalid_data': 2,
        'authorized_rearm': 3,
        'benign': 4,
    }

    def _diagnostic_severity(sym: str) -> tuple:
        res = fund_outputs[sym]
        outs = res.get('outputs', {})
        disp = outs.get('disposition', 'benign')
        return (
            precedence.index(res['state']),
            0 if res.get('would_have_tripped') else 1,
            0 if outs.get('escalated_to_v1') else 1,
            0 if outs.get('notify') else 1,
            0 if outs.get('exceeded') else 1,
            disposition_rank.get(disp, 4),
            0 if res.get('reason', 'none') != 'none' else 1,
            -(outs.get('consecutive_sessions_beyond_tolerance') or 0),
            _fund_sort_key(sym),
        )

    worst_sym = min(ordered_keys, key=_diagnostic_severity)
    worst_result = fund_outputs[worst_sym]
    kind = worst_result['kind']

    most_restrictive = worst_result['state']
    any_tripped = any(fund_outputs[s]['would_have_tripped'] for s in ordered_keys)

    raw_reason = worst_result.get('reason', 'none')
    reason = raw_reason[:-len(f'_{worst_sym}')] if raw_reason.endswith(f'_{worst_sym}') else raw_reason

    aggregate = {
        'state': most_restrictive,
        'would_have_tripped': any_tripped,
        'kind': kind,
        'latched': False,  # Diagnostics don't latch
        'reason': reason
    }

    if 'outputs' in worst_result:
        agg_outputs = copy.deepcopy(worst_result['outputs'])
        if 'notify' in agg_outputs:
            agg_outputs['notify'] = any(
                bool(fund_outputs[s].get('outputs', {}).get('notify'))
                for s in ordered_keys
            )
        if 'exceeded' in agg_outputs:
            agg_outputs['exceeded'] = any(
                bool(fund_outputs[s].get('outputs', {}).get('exceeded'))
                for s in ordered_keys
            )
        aggregate['outputs'] = agg_outputs

    if len(ordered_keys) == 1:
        single_fund = ordered_keys[0]
        aggregate['fund'] = fund_outputs[single_fund].get('fund', single_fund)

    return aggregate
