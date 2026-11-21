"""C-FUSE-EVAL interface and synthetic stub adapter (skeleton)."""

from __future__ import annotations


class StubEvaluator:
    """Synthetic stub evaluator with zero ledger handles."""

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
    ) -> tuple[dict, dict, dict]:
        """Return (rows_by_ID, effective_state_by_scope, next_row_state)."""
        raise NotImplementedError("evaluator not implemented")
