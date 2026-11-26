# SH-2 Cloud Run Jobs Deployment Runbook (`us-east1`, Item 18)

> **Supersession Notice (Item 18):** Mac mini runtime and clock templates in
> [`deploy/scheduler/`](../deploy/scheduler/) and [`docs/sh1-scheduler-runbook.md`](sh1-scheduler-runbook.md)
> are **deprecated / superseded** for SH-2 runtime operations (archived only to preserve SH-1 unit tests).
> SH-2 runs on secret-free GCP Cloud Run Jobs in `us-east1`, triggered by Cloud Scheduler.

## 1. Architecture & Secret Hygiene

- **Image:** [`Dockerfile`](../Dockerfile) (`python:3.13-slim-bookworm`, unprivileged user `shadow`, zero baked credentials).
- **Vendored Engine Pin:** Copies `vendor/fuse_engine/fuse-eval-spec-v1/12e9b502367a4993d0c36e75ac5c8d24acaee2d1247c5b594796d5adda04ca45/` with `0444` file permissions and zero pre-generated `__pycache__` mutation.
- **Credentials:** `TIINGO_API_KEY` is bound via GCP Secret Manager reference in [`deploy/cloudrun/job-spec.example.yaml`](../deploy/cloudrun/job-spec.example.yaml), never stored in container images or logs.
- **Persistent State:** Persistent volume (`ledger/`, `archive/`, `replay/`, `reports/`, `state_snapshots/`) mounted at `SHADOW_DATA_DIR` (`/var/lib/shadow/data`) via the Cloud Run Managed Cloud Storage FUSE CSI driver (`gcsfuse.run.googleapis.com`).

## 2. Standing Safety Locks & OA-SH2-2 Gate

The default [`config/production-scheduler.json`](../config/production-scheduler.json) enforces:

- `production_chain_locked: true` (`OA-SH2-2` locked)
- `clk01_active: false`
- `live=OFF`, `kelly=OFF`, `kelly_applied=false`, `orders_placed=0`, `loss_breakers_armed=false`, `enforcement_mode="shadow_would_have_tripped"`, `H="0"`, `B0="50000"`, `stop_loss_return_rule="OFF"`

While `production_chain_locked: true`, `python3 -m shadow.cloudrun_job --mode production` fails closed with `0` ledger appends (`exit 2`, `status="production_chain_locked"`).

## 3. Stateless Retry Idempotency, Lease Fencing & Continuity

1. **Single-Writer Serialization & Lease Fencing:** [`ShadowLedger.chain_lock()`](../src/shadow/ledger.py) combines local `fcntl.flock` with an exclusive `.chain.lease.json` lease file (`O_CREAT | O_EXCL`). On GCS FUSE, `O_EXCL` alone does not enforce distributed cross-instance locking before `close`/flush, so [`deploy/cloudrun/job-spec.example.yaml`](../deploy/cloudrun/job-spec.example.yaml) requires `parallelism: 1`, `taskCount: 1`, `maxRetries: 0`, serialized Cloud Scheduler triggers, and matching `mountOptions: "uid=10001,gid=10001,implicit-dirs"` ownership for the non-root `shadow` (`10001:10001`) container user (or a POSIX Filestore / GCS `ifGenerationMatch=0` native lease if cross-instance launches could overlap). Active local writers fail closed with `ChainConcurrencyConflictError` (`exit 4`).
2. **Idempotent Retry (`already_committed_no_op`):** Re-invoking `python3 -m shadow.cloudrun_job` after completion on an already committed `(chain_id, session_date)` returns `status: "already_committed_no_op"` (`idempotent_noop: true`, `appended: false`, `exit 0`), verifies or deterministically recovers `state_snapshots/`, and retires any superseded `pending_rejected_state.json` left by an interrupted unlink.
3. **Atomic State Checkpoints:** Every committed session stages `state_snapshots/seq_NNNNNN.state.json` and `latest.state.json` atomically inside `before_record_commit` (rolling back if commit fails). Rejected `O1`/`O2` evaluations persist `state_snapshots/pending_rejected_state.json` so breaker latches survive into the next session without fabricating ledger entries.
4. **External Genesis, History, Monotonic Anchor & Pending-State Authentication (`seq=0`, `seq >= 1`, Retry, & Rejection Continuity):** In `mode="production"`, every run requires `SHADOW_TRUSTED_HEAD_ANCHOR_PATH` (`--trusted-head-anchor-path`) pointing outside `SHADOW_DATA_DIR` (e.g., on a separate dedicated GCS anchor mount `/var/run/shadow/anchor/trusted_head.json`) so committed ledger heads (`sequence`, `head_sha256`) and persisted rejection state digests (`pending_rejected_state_sha256`) atomically advance a monotonic external anchor (`c-sh-trusted-head-anchor-v1`) on each commit and post-evaluation rejection, rejecting state-bucket rollbacks even before secret rotation. Empty-bucket genesis (`seq=0`) additionally fails closed (`production_genesis_unauthenticated`, `exit 3`, `0` writes) unless `SHADOW_TRUSTED_HEAD_SHA256` explicitly carries `GENESIS_PREV_HASH` (`"0" * 64`). On every `seq >= 1` run AND same-session retry, `SHADOW_TRUSTED_HEAD_SHA256` MUST match the externally authorized current committed head `record_sha256` AND the external anchor (WITHOUT retaining the zero genesis marker; `SHADOW_BITCOIN_MERKLE_ROOTS_PATH` corroborates OTS receipts but never substitutes for an authorized head). Additionally, `SHADOW_TRUSTED_PENDING_STATE_SHA256` is mandatory on every advancing production run (`"0" * 64` `NO_PENDING_REJECTED_STATE_SHA256` when no unconsumed rejection exists, failing closed if a pending snapshot or post-head rejected completeness report exists; or the emitted `pending_rejected_state_sha256` after an `O1`/`O2`/deadline/OTS failure until a committed session retires it, failing closed with `exit 3` if unauthenticated, missing, deleted, rolled back, or tampered).
