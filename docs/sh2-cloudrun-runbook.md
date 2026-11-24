# SH-2 Cloud Run Jobs Deployment Runbook (`us-east1`, Item 18)

> **Supersession Notice (Item 18):** Mac mini runtime and clock templates in
> [`deploy/scheduler/`](../deploy/scheduler/) and [`docs/sh1-scheduler-runbook.md`](sh1-scheduler-runbook.md)
> are **deprecated / superseded** for SH-2 runtime operations (archived only to preserve SH-1 unit tests).
> SH-2 runs on secret-free GCP Cloud Run Jobs in `us-east1`, triggered by Cloud Scheduler.

## 1. Architecture & Secret Hygiene

- **Image:** [`Dockerfile`](../Dockerfile) (`python:3.13-slim-bookworm`, unprivileged user `shadow`, zero baked credentials).
- **Vendored Engine Pin:** Copies `vendor/fuse_engine/fuse-eval-spec-v1/12e9b502367a4993d0c36e75ac5c8d24acaee2d1247c5b594796d5adda04ca45/` with `0444` file permissions and zero pre-generated `__pycache__` mutation.
- **Credentials:** `TIINGO_API_KEY` is bound via GCP Secret Manager reference in [`deploy/cloudrun/job-spec.example.yaml`](../deploy/cloudrun/job-spec.example.yaml), never stored in container images or logs.
- **Persistent State:** Persistent volume (`ledger/`, `archive/`, `replay/`, `reports/`, `state_snapshots/`) mounted at `SHADOW_DATA_DIR` (`/var/lib/shadow/data`).

## 2. Standing Safety Locks & OA-SH2-2 Gate

The default [`config/production-scheduler.json`](../config/production-scheduler.json) enforces:

- `production_chain_locked: true` (`OA-SH2-2` locked)
- `clk01_active: false`
- `live=OFF`, `kelly=OFF`, `kelly_applied=false`, `orders_placed=0`, `loss_breakers_armed=false`, `enforcement_mode="shadow_would_have_tripped"`, `H="0"`, `B0="50000"`, `stop_loss_return_rule="OFF"`

While `production_chain_locked: true`, `python3 -m shadow.cloudrun_job --mode production` fails closed with `0` ledger appends (`exit 2`, `status="production_chain_locked"`).

## 3. Stateless Retry Idempotency & Continuity

1. Both `run_daily_drill_cycle` and `run_production_cycle` serialize access with `ShadowLedger.chain_lock()`.
2. Re-invoking `python3 -m shadow.cloudrun_job` on an already committed `(chain_id, session_date)` detects the committed record and returns `status: "already_committed_no_op"` (`idempotent_noop: true`, `appended: false`, `exit 0`) without re-evaluating or duplicating entries.
3. Every committed session writes an identity-pinned atomic checkpoint under `state_snapshots/` (`seq_NNNNNN.state.json` + `latest.state.json`) verified against `engine_hash`, `spec_version`, `chain_id`, `sequence`, and `record_sha256` before the next session runs.
