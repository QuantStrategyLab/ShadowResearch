"""State management for FUSE-1 reference engine.

Handles prior_row_state → next_row_state persistence per §5.1 and §4.4 rule 3.
State includes latches, counters, and session-keyed values that survive
restart, deposit, withdrawal, split, reverse split, and tier change.
"""

from typing import Any, Dict, Optional


def get_prior_state(prior_row_state: Optional[Dict[str, Any]], key: str, default: Any = None) -> Any:
    """Safely retrieve a value from prior_row_state."""
    if prior_row_state is None:
        return default
    return prior_row_state.get(key, default)


def init_next_state() -> Dict[str, Any]:
    """Initialize empty next_row_state dict."""
    return {}


def set_latch(next_state: Dict[str, Any], row_id: str, scope: str, latched: bool,
              reason: str = None, details: Dict[str, Any] = None) -> None:
    """Set a latch in next_row_state.

    Args:
        next_state: The next_row_state dict being built
        row_id: Row identifier (e.g., 'L2', 'V2', 'O3')
        scope: Scope of the latch ('account', 'fund:SYMBOL', 'symbol:SYMBOL')
        latched: Whether the breaker is latched
        reason: Latch reason (optional)
        details: Additional latch details (optional)
    """
    latch_key = f'{row_id}_latch'
    if latch_key not in next_state:
        next_state[latch_key] = {}

    next_state[latch_key][scope] = {
        'latched': latched,
        'reason': reason,
        'details': details or {}
    }


def get_latch(prior_row_state: Optional[Dict[str, Any]], row_id: str, scope: str) -> Optional[Dict[str, Any]]:
    """Retrieve a latch from prior_row_state.

    Returns None if not latched, or dict with {latched, reason, details} if present.
    """
    if prior_row_state is None:
        return None

    latch_key = f'{row_id}_latch'
    latches = prior_row_state.get(latch_key, {})
    return latches.get(scope)


def clear_latch(next_state: Dict[str, Any], row_id: str, scope: str) -> None:
    """Clear a latch in next_row_state."""
    latch_key = f'{row_id}_latch'
    if latch_key in next_state and scope in next_state[latch_key]:
        del next_state[latch_key][scope]
        if not next_state[latch_key]:
            del next_state[latch_key]


def set_counter(next_state: Dict[str, Any], row_id: str, scope: str,
                counter_name: str, value: int, session: str = None) -> None:
    """Set a counter in next_row_state.

    Args:
        next_state: The next_row_state dict being built
        row_id: Row identifier
        scope: Counter scope
        counter_name: Name of the counter (e.g., 'clearing_sessions', 'persistence_sessions')
        value: Counter value
        session: Session identifier (for per-session counters like V4, O5)
    """
    counter_key = f'{row_id}_counter'
    if counter_key not in next_state:
        next_state[counter_key] = {}
    if scope not in next_state[counter_key]:
        next_state[counter_key][scope] = {}

    if session is not None:
        # Per-session counter
        if counter_name not in next_state[counter_key][scope]:
            next_state[counter_key][scope][counter_name] = {}
        next_state[counter_key][scope][counter_name][session] = value
    else:
        # Simple counter
        next_state[counter_key][scope][counter_name] = value


def get_counter(prior_row_state: Optional[Dict[str, Any]], row_id: str, scope: str,
                counter_name: str, session: str = None, default: int = 0) -> int:
    """Retrieve a counter from prior_row_state."""
    if prior_row_state is None:
        return default

    counter_key = f'{row_id}_counter'
    counters = prior_row_state.get(counter_key, {})
    scope_counters = counters.get(scope, {})

    if session is not None:
        # Per-session counter
        session_counters = scope_counters.get(counter_name, {})
        return session_counters.get(session, default)
    else:
        # Simple counter
        return scope_counters.get(counter_name, default)


def copy_state_field(next_state: Dict[str, Any], prior_row_state: Optional[Dict[str, Any]], key: str) -> None:
    """Copy a state field from prior to next (for fields that persist unchanged)."""
    if prior_row_state is not None and key in prior_row_state:
        next_state[key] = prior_row_state[key]
