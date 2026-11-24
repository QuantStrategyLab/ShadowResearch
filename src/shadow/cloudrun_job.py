"""Secret-free GCP Cloud Run Jobs (us-east1) entrypoint with stateless idempotency."""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import sys
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from pathlib import Path

from shadow.calendar_consumer import load_spec, resolve_prior_close_session
from shadow.fuse_adapter import (
    EngineIdentityError,
    PINNED_CSA22_V2_DIGEST,
    PINNED_FUSE_ENGINE_HASH,
    PINNED_FUSE_SPEC_VERSION,
    ProductionChainLockedError,
    VendoredFuseEvaluator,
    verify_f1_entry_gates,
)
from shadow.feeder_live import Transport
from shadow.ots import OpenTimestampsClient, OtsTransport
from shadow.scheduler import (
    REPO_ROOT,
    DrillSchedulerConfig,
    load_production_scheduler_config,
    load_scheduler_config,
    run_daily_drill_cycle,
    run_production_cycle,
)

DEFAULT_CLOUDRUN_DATA_DIR = Path("data") / "cloudrun-sh2"


def _default_session_date(
    *,
    cfg: DrillSchedulerConfig,
    spec_dir: str | Path | None = None,
    now_utc: datetime | None = None,
) -> str:
    spec_path = Path(spec_dir) if spec_dir is not None else (REPO_ROOT / "specs" / cfg.calendar_version)
    spec = load_spec(spec_path)
    ts = (now_utc or datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    resolved = resolve_prior_close_session(ts, spec=spec, venue=cfg.venue)
    return str(resolved["trade_date"])


def run_cloudrun_job(
    *,
    data_dir: str | Path | None = None,
    config_path: str | Path | None = None,
    mode: str | None = None,
    session_date: str | None = None,
    fixtures_dir: str | Path | None = None,
    spec_dir: str | Path | None = None,
    transport: Transport | None = None,
    ots_client: OpenTimestampsClient | None = None,
    ots_transport: OtsTransport | None = None,
    wall_clock: Callable[[], datetime] | None = None,
    clock: Callable[[], float] | None = None,
    sleeper: Callable[[float], None] | None = None,
    environ: Mapping[str, str] | None = None,
    allow_unanchored_on_ots_failure: bool | None = None,
    unlock_production_override: bool = False,
) -> dict[str, object]:
    """Execute one idempotent Cloud Run Job task against vendored FUSE-1 + csa22-v2."""
    env = dict(os.environ) if environ is None else dict(environ)
    eff_data_dir = Path(
        data_dir
        if data_dir is not None
        else env.get("SHADOW_DATA_DIR") or str(DEFAULT_CLOUDRUN_DATA_DIR)
    )
    eff_config_path = (
        Path(config_path)
        if config_path is not None
        else (
            Path(env["SHADOW_CONFIG_PATH"])
            if env.get("SHADOW_CONFIG_PATH")
            else (
                REPO_ROOT / "config" / "production-scheduler.json"
                if (mode or env.get("SHADOW_CHAIN_MODE")) == "production"
                else REPO_ROOT / "config" / "drill-scheduler.json"
            )
        )
    )

    gate_report = verify_f1_entry_gates(require_read_only=False)
    ev = VendoredFuseEvaluator(require_read_only=False)

    raw_cfg = json.loads(eff_config_path.read_text(encoding="utf-8"))
    target_mode = mode or env.get("SHADOW_CHAIN_MODE") or str(raw_cfg.get("mode", "drill"))

    if target_mode == "production":
        cfg = load_production_scheduler_config(eff_config_path)
        if unlock_production_override:
            cfg = dataclasses.replace(cfg, production_chain_locked=False, clk01_active=True)
        if cfg.production_chain_locked is not False or not cfg.clk01_active:
            raise ProductionChainLockedError(
                "Production chain is locked (OA-SH2-2); 0 appends permitted in Cloud Run job"
            )
    else:
        cfg = load_scheduler_config(eff_config_path)
        if cfg.fuse_engine_hash is None:
            cfg = dataclasses.replace(
                cfg,
                calendar_version="csa22-v2",
                calendar_spec_digest=PINNED_CSA22_V2_DIGEST,
                fuse_spec_version=PINNED_FUSE_SPEC_VERSION,
                fuse_engine_hash=PINNED_FUSE_ENGINE_HASH,
            )

    eff_session = (
        session_date
        or env.get("SHADOW_SESSION_DATE")
        or _default_session_date(
            cfg=cfg,
            spec_dir=spec_dir,
            now_utc=wall_clock() if wall_clock is not None else None,
        )
    )

    if target_mode == "production":
        result = run_production_cycle(
            session_date=eff_session,
            data_dir=eff_data_dir,
            config=cfg,
            spec_dir=spec_dir,
            transport=transport,
            environ=env,
            fixtures_dir=fixtures_dir,
            ots_client=ots_client,
            ots_transport=ots_transport,
            wall_clock=wall_clock,
            clock=clock,
            sleeper=sleeper,
            allow_unanchored_on_ots_failure=False,
            evaluator=ev,
        )
    else:
        allow_unanchored = (
            True if allow_unanchored_on_ots_failure is None else allow_unanchored_on_ots_failure
        )
        result = run_daily_drill_cycle(
            session_date=eff_session,
            data_dir=eff_data_dir,
            config=cfg,
            spec_dir=spec_dir,
            transport=transport,
            environ=env,
            fixtures_dir=fixtures_dir,
            ots_client=ots_client,
            ots_transport=ots_transport,
            wall_clock=wall_clock,
            clock=clock,
            sleeper=sleeper,
            allow_unanchored_on_ots_failure=allow_unanchored,
            evaluator=ev,
        )

    if result.get("reason") == "already_committed_session_preserved":
        status = "already_committed_no_op"
        idempotent_noop = True
    elif result.get("appended") and not result.get("fail_closed"):
        status = "committed"
        idempotent_noop = False
    elif result.get("fail_closed"):
        status = "fail_closed"
        idempotent_noop = False
    else:
        status = "completed"
        idempotent_noop = False

    return {
        "schema_version": "sh2-cloudrun-job-result-v1",
        "status": status,
        "idempotent_noop": idempotent_noop,
        "mode": cfg.mode,
        "chain_id": cfg.chain_id,
        "session_date": eff_session,
        "deployment_target": cfg.deployment_target or "cloudrun-job",
        "cloudrun_region": cfg.cloudrun_region or "us-east1",
        "cloudrun_execution": env.get("CLOUD_RUN_EXECUTION", ""),
        "cloudrun_task_index": env.get("CLOUD_RUN_TASK_INDEX", "0"),
        "engine_hash": ev.engine_hash,
        "spec_version": ev.spec_version,
        "entry_gates_ok": gate_report["ok"],
        "appended": bool(result.get("appended")),
        "fail_closed": bool(result.get("fail_closed")),
        "reason": result.get("reason"),
        "sequence": result.get("sequence"),
        "record_sha256": result.get("record_sha256"),
        "standing_safety_locks": {
            "live": "OFF",
            "kelly": "OFF",
            "kelly_applied": False,
            "orders_placed": 0,
            "loss_breakers_armed": False,
            "enforcement_mode": "shadow_would_have_tripped",
            "H": "0",
            "B0": "50000",
            "stop_loss_return_rule": "OFF",
            "production_chain_locked": bool(cfg.production_chain_locked is not False),
        },
    }


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for Cloud Run Jobs: python3 -m shadow.cloudrun_job."""
    parser = argparse.ArgumentParser(
        description="SHADOW-N SH-2 Cloud Run Jobs (us-east1) idempotent cycle runner"
    )
    parser.add_argument("--data-dir", help="Persistent state/ledger root directory")
    parser.add_argument("--config", help="Scheduler configuration JSON path")
    parser.add_argument("--mode", choices=("drill", "production"), help="Execution mode")
    parser.add_argument("--session-date", help="Explicit ISO session date YYYY-MM-DD")
    parser.add_argument("--fixtures-dir", help="Optional offline recorded fixture directory")
    parser.add_argument("--spec-dir", help="Optional C-SA-22 spec directory override")
    args = parser.parse_args(argv)

    try:
        out = run_cloudrun_job(
            data_dir=args.data_dir,
            config_path=args.config,
            mode=args.mode,
            session_date=args.session_date,
            fixtures_dir=args.fixtures_dir,
            spec_dir=args.spec_dir,
        )
    except ProductionChainLockedError as exc:
        err_payload = {
            "schema_version": "sh2-cloudrun-job-result-v1",
            "status": "production_chain_locked",
            "appended": False,
            "fail_closed": True,
            "reason": str(exc),
            "orders_placed": 0,
            "production_chain_locked": True,
        }
        print(json.dumps(err_payload, indent=2, sort_keys=True))
        return 2
    except EngineIdentityError as exc:
        err_payload = {
            "schema_version": "sh2-cloudrun-job-result-v1",
            "status": "engine_identity_error",
            "appended": False,
            "fail_closed": True,
            "reason": str(exc),
        }
        print(json.dumps(err_payload, indent=2, sort_keys=True))
        return 3

    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
