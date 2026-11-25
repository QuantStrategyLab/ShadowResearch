# SH-2 Acceptance Pack (`SH2-A01`–`SH2-A14`) & Runtime Architecture

- **Branch:** `feat/sh2-live-engine`
- **Standing Safety Locks:** `live=OFF`, `kelly=OFF`, `kelly_applied=false`, `orders_placed=0`, `loss_breakers_armed=false`, `enforcement_mode="shadow_would_have_tripped"`, `H="0"`, `B0="50000"`, `stop_loss_return_rule="OFF"`, `production_chain_locked=true` (`OA-SH2-2` locked; `clk01_active=false`).

## 1. Pinned Upstream & Production Identities

| Artifact | Value |
| --- | --- |
| Upstream `FuseControl` commit | `46a86eedb0d33f6133a066edeb8c3660cda9097d` (`main`) |
| `fuse_spec_version` | `fuse-eval-spec/v1` |
| `fuse_engine_hash` | `12e9b502367a4993d0c36e75ac5c8d24acaee2d1247c5b594796d5adda04ca45` |
| `fuse_bundle_hash` | `78fed9dfc9b0b0a618fce6bb5a956063135f34845545e6a5154a32e119354cc9` |
| `fuse_freeze_hash` | `1da74ca508161ba6d9a424101321381a9f5156d8cb2c84c03be2fd2fe3f22a74` |
| `csa22-v2` `spec_digest` | `2ba8cd7684c361209df68bfa2e10004c1f9922a645c7ec55d421fda520561798` |
| `production_code_version_sha` | `300ca9ea3e191b537a0b9b098e35391d2adac3976ff4cb9a43d83546f87c4780` |
| `production_policy_config_hash` | `ec11d06b8011a0463291a311c545d8480c09fc9bb36f42c91168ec3bd1152766` |
| Production config | [`config/production-scheduler.json`](../config/production-scheduler.json) (`chain_id="prod-sh2-v1"`, `mode="production"`, `production_chain_locked=true`, `clk01_active=false`) |
| Deployment target (Item 18) | Secret-free GCP Cloud Run Jobs (`us-east1`); legacy Mac mini units marked deprecated |

## 2. Offline Acceptance Matrix (`SH2-A01`–`SH2-A14`)

Reproduced offline via `PYTHONPATH=src python3 -m shadow.sh2_acceptance` and `demo/run_sh2_demo.py`:

| Row | Status | Verified Scope |
| --- | --- | --- |
| `SH2-A01` | `PASS_OFFLINE` | Entry gates `F1-A01` (`24/24` IDs), `F1-A03` (`264/264`), `F1-A04` (`132/132`), `F1-A10` (`13/13`), `F1-A11` (`['O5', 'V5', 'V7']` `UNRESOLVED-FAIL-CLOSED`), and `SH1-ACCEPTANCE.json` (`20be23f1ce4f4f23bd2f863aef43213894947bf3`) verified. |
| `SH2-A02` | `PASS_OFFLINE` | Vendored 8-file package (`0444` read-only permissions, zero committed `__pycache__/`) verified byte-for-byte against `provenance.json` and framed SHA-256 `12e9b502...`, including `__pycache__/*.pyc` verification. |
| `SH2-A03` | `PASS_OFFLINE` | Structural `StubEvaluator` and `stub-*` hash quarantine verified on production `build_record_body`, `replay_window`, and `run_production_cycle`. |
| `SH2-A04` | `PASS_OFFLINE` | Zero conformance shims or fabricated sources/returns: omitted `positions`/`broker_reconciliation`/`index_one_session_returns` fail `O1` closed, feeder `corporate_action` (`split_ratio`/`cash_per_share_usd`) wires into `V2`, `NO-NEW-RISK` clamps + per-fund sizing isolation hold, `U-01` unresolved parameters stay fail-closed (`['O5', 'V5', 'V7']`), `live` mode fails verbatim, and 1-byte source/bytecode tamper fails pre-`evaluate()`. |
| `SH2-A05` | `PASS_OFFLINE` | Upstream caller schema enforces explicit boolean `hwm_adjusted` whenever `corporate_action.adjustment_complete=true` (`V2PayloadSchemaError` before `evaluate()`); explicit `false` triggers FUSE-1 `V2` `HALT`. |
| `SH2-A06` | `PASS_OFFLINE` | `csa22-v2` verified by digest (`2ba8cd76...`) and pinned `calendar_spec_digest` mismatch rejection, covering 2026 holidays (`2026-07-03` closed) and early closes (`2026-11-27`, `2026-12-24` at `13:00` ET). |
| `SH2-A07` | `PASS_OFFLINE` | US `23/5` `16:00` ET Official Close isolation & post-`21:00` ET `T+1` clearing boundary enforced across calendar, Tiingo (`row["timestamp"]` midnight rejection), Yahoo (`dataGranularity` non-daily rejection), and market record validation (`Item 13`). |
| `SH2-A08` | `PASS_OFFLINE` | Atomic commit-staged `state_snapshots/` bound to committed `evaluator.next_row_state_sha256` (plus `pending_rejected_state.json` carrying ledger-bound `base_prior_row_state` with elapsed-day monotonic latch/counter preservation including 10th-session `V4` clearing, external `SHADOW_TRUSTED_PENDING_STATE_SHA256` authentication on production consumption, retirement of superseded pending files on committed-session retry, uncommitted pre-marker snapshot pruning/recovery of `latest.state.json` from committed ledger head, and deterministic recovery on `already_committed_session_preserved`) prevents forged state downgrades across restarts. |
| `SH2-A09` | `PASS_OFFLINE` | `python3 -m shadow.cloudrun_job` (`us-east1`, `Item 18`) enforces strict boolean config validation, forbids `fixtures_dir` in production, removes `unlock_production_override`, requires external monotonic `SHADOW_TRUSTED_HEAD_ANCHOR_PATH` outside `SHADOW_DATA_DIR` (atomically advancing on each commit and persisted pending state to reject state-bucket rollback before secret rotation), explicit `GENESIS_PREV_HASH` for one-time genesis, mandatory `SHADOW_TRUSTED_HEAD_SHA256` matching both the anchor and current committed head across advancing runs and same-session retries (with `SHADOW_BITCOIN_MERKLE_ROOTS_PATH` as corroborating OTS check), and mandatory `SHADOW_TRUSTED_PENDING_STATE_SHA256` on every advancing production run (`0*64` `NO_PENDING_REJECTED_STATE_SHA256` when clean or `pending_rejected_state_sha256` after a rejection) with post-head rejected report cross-check against tamper, substitution, deletion, rollback, or wiped buckets (`exit 3` on failure), guarantees `already_committed_no_op` idempotency (`<= 1` append/session), and fences concurrent writers via CAS-reclaimed `.chain.lease.json` (`ChainConcurrencyConflictError`, exit `4`). |
| `SH2-A10` | `PASS_OFFLINE` | `replay_window()` deterministically re-derives vendored `FUSE-1` ledger records (including `fuse_context` immediate-prior trading session closes and `prior_row_state_used`) bit-for-bit without `StubEvaluator`. |
| `SH2-A11` | `PASS_OFFLINE` | Default `config/production-scheduler.json` remains structurally locked (`production_chain_locked=true`, `clk01_active=false`, `OA-SH2-2` blocks appends with `0` writes). |
| `SH2-A12` | `PASS_OFFLINE` | Forward-clock creditability verifies OTS BTC proof, `engine_hash`/`spec_version`, and `observation_class="decision_time_market"` when unlocked via `mode="production"` `config_path`, and fails closed by default when locked or given non-production configs. |
| `SH2-A13` | `PASS_OFFLINE` | Secret-free `Dockerfile` (`chmod 0444` vendored artifacts, non-root `shadow` user, `-B` bytecode guard) and [`deploy/cloudrun/job-spec.example.yaml`](../deploy/cloudrun/job-spec.example.yaml) (`us-east1`, `maxRetries: 0`, `gcsfuse.run.googleapis.com`, Secret Manager ref) verified cleanly. |
| `SH2-A14` | `PASS_OFFLINE` | Standing locks enforced across every emitted tier/fund/assertion and repository scan confirms zero secrets and zero workstation paths (`0` violations). |

## 3. Design Demonstration Summary (`D1`–`D10`)

`python3 demo/run_sh2_demo.py` passes `10/10` covering entry gates (`D1`), core swap + tamper + `hwm_adjusted` negative controls (`D2`), FUSE-1 breaker transitions `NO-NEW-RISK`/`DE-RISK`/`HALT` (`D3`), fresh-subprocess `state_snapshots/` restart (`D4`), `OA-SH2-2` locked production guard (`D5`), zero conformance shims / zero fabricated sources (`D6`), bit-for-bit replay (`D7`), `csa22-v2` + `23/5` `16:00 ET` isolation (`D8`), Cloud Run concurrent/sequential retry idempotency (`D9`), and default-locked vs config-unlocked mock-HTTP OTS creditability (`D10`).

## 4. Verification Scope Notes

- Local Docker daemon binary is not installed in this development workspace; container runtime semantics are verified via [`Dockerfile`](../Dockerfile) static inspection and direct execution of the identical `python3 -B -m shadow.cloudrun_job` entrypoint under stripped environment variables.
