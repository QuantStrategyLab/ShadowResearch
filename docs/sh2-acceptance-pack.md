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
| `production_code_version_sha` | `17a5a63c49a3a4083c9380db8167e1efb58f2a3611b10126c078a3e73ed74530` |
| `production_policy_config_hash` | `700e8e6e21e15049fddfa12b93bdaa625568ce56368885da245d83749295180f` |
| Production config | [`config/production-scheduler.json`](../config/production-scheduler.json) (`chain_id="prod-sh2-v1"`, `mode="production"`, `production_chain_locked=true`, `clk01_active=false`) |
| Deployment target (Item 18) | Secret-free GCP Cloud Run Jobs (`us-east1`); legacy Mac mini units marked deprecated |

## 2. Offline Acceptance Matrix (`SH2-A01`–`SH2-A14`)

Reproduced offline via `PYTHONPATH=src python3 -m shadow.sh2_acceptance` and `demo/run_sh2_demo.py`:

| Row | Status | Verified Scope |
| --- | --- | --- |
| `SH2-A01` | `PASS_OFFLINE` | Entry gates `F1-A01` (`24/24` IDs), `F1-A03` (`264/264`), `F1-A04` (`132/132`), `F1-A10` (`13/13`), `F1-A11` (`['O5', 'V5', 'V7']` `UNRESOLVED-FAIL-CLOSED`), and `SH1-ACCEPTANCE.json` (`20be23f1ce4f4f23bd2f863aef43213894947bf3`) verified. |
| `SH2-A02` | `PASS_OFFLINE` | Vendored 8-file package (`0444` read-only permissions, zero committed `__pycache__/`) verified byte-for-byte against `provenance.json` and framed SHA-256 `12e9b502...`, including `__pycache__/*.pyc` verification. |
| `SH2-A03` | `PASS_OFFLINE` | `StubEvaluator` and `stub-*` hashes structurally refused in `production` mode (`build_record_body`, `replay_window`, `run_production_cycle`). |
| `SH2-A04` | `PASS_OFFLINE` | Zero conformance shims: `U-01` unresolved parameters stay fail-closed (`unresolved_fail_closed`), `live` evaluation mode fails verbatim, and 1-byte source/bytecode tamper fails closed via `EngineIdentityError` before `evaluate()`. |
| `SH2-A05` | `PASS_OFFLINE` | Upstream caller schema enforces explicit boolean `hwm_adjusted` whenever `corporate_action.adjustment_complete=true` (`V2PayloadSchemaError` before `evaluate()`); explicit `false` triggers FUSE-1 `V2` `HALT`. |
| `SH2-A06` | `PASS_OFFLINE` | `specs/csa22-v2` verified by digest (`2ba8cd76...`), covering 2026 holidays (`2026-07-03` closed) and early closes (`2026-11-27`, `2026-12-24` at `13:00` ET). |
| `SH2-A07` | `PASS_OFFLINE` | US `23/5` `16:00` ET Official Close isolation & post-`21:00` ET `T+1` clearing boundary enforced across calendar, Tiingo, Yahoo, and market record validation (`Item 13`). |
| `SH2-A08` | `PASS_OFFLINE` | Atomic `state_snapshots/` persistence preserves latches and clearing counters (`V4` / `L2` / `L3` / `O5`) across separate subprocess restarts and rejects engine/checksum drift. |
| `SH2-A09` | `PASS_OFFLINE` | `python3 -m shadow.cloudrun_job` guarantees stateless idempotency (`already_committed_no_op` on sequential and concurrent retries, `<= 1` ledger append per session date). |
| `SH2-A10` | `PASS_OFFLINE` | `replay_window()` deterministically re-derives vendored `FUSE-1` ledger records bit-for-bit without `StubEvaluator`. |
| `SH2-A11` | `PASS_OFFLINE` | Default `config/production-scheduler.json` remains structurally locked (`production_chain_locked=true`, `clk01_active=false`), yielding `0` production appends until `OA-SH2-2` sign-off. |
| `SH2-A12` | `PASS_OFFLINE` | `check_forward_clock_credit()` verifies pinned `engine_hash`, `spec_version`, `observation_class="decision_time_market"`, and timely BTC OTS proof when unlocked, and rejects when locked. |
| `SH2-A13` | `PASS_OFFLINE` | Secret-free `Dockerfile` (`chmod 0444` vendored artifacts, non-root `shadow` user, `-B` bytecode guard) and [`deploy/cloudrun/job-spec.example.yaml`](../deploy/cloudrun/job-spec.example.yaml) (`us-east1`, Secret Manager ref) verified cleanly. |
| `SH2-A14` | `PASS_OFFLINE` | Standing locks enforced across every emitted tier/fund/assertion and repository scan confirms zero secrets and zero workstation paths (`0` violations). |

## 3. Design Demonstration Summary (`D1`–`D10`)

`python3 demo/run_sh2_demo.py` passes `10/10` covering entry gates (`D1`), core swap + tamper + `hwm_adjusted` negative controls (`D2`), FUSE-1 breaker transitions `NO-NEW-RISK`/`DE-RISK`/`HALT` (`D3`), fresh-subprocess `state_snapshots/` restart (`D4`), `OA-SH2-2` locked production guard (`D5`), zero conformance shims (`D6`), bit-for-bit replay (`D7`), `csa22-v2` + `23/5` `16:00 ET` isolation (`D8`), Cloud Run concurrent/sequential retry idempotency (`D9`), and OTS creditability boundary (`D10`).

## 4. Verification Scope Notes

- Local Docker daemon binary is not installed in this development workspace; container runtime semantics are verified via [`Dockerfile`](../Dockerfile) static inspection and direct execution of the identical `python3 -B -m shadow.cloudrun_job` entrypoint under stripped environment variables.
