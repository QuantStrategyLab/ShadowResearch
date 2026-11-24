# SH-1 §7 Design Demonstration — Final Results (D1–D8)

- **Branch:** `demo/sh1-design-demo` (created from `main` @ `dff3ecc`)
- **Pass / Epoch:** Correction pass 1 (`SHADOW-N-DESIGN-2026-11-21-v1`) under Astra re-verification **FAIL** on head `fe3f656` (`/home/hatch/workspace/quant-s2/review-sh1-demo/s2-review-sh1-demo-REPORT.md`)
- **Frozen Case List:** [`demo/DEMO-CASES.md`](DEMO-CASES.md) (frozen at `b140c2a`, blob `fc5d7f1d186096da293fa6b4dedbf4e3ee05c5e3`, untouched)
- **Versioned Correction 1:** [`demo/DEMO-CASES-CORRECTION-1.md`](DEMO-CASES-CORRECTION-1.md) (supersedes Interpretation 4 for the gate)
- **Failing Baseline:** [`demo/BASELINE.md`](BASELINE.md) (`d5433be`, untouched)
- **Language / Dependencies:** Python 3.12 (`stdlib` only; zero network calls; zero Mac mini access)

---

## Correction Pass 1 Summary (vs `fe3f656`)

Issued under `/home/hatch/workspace/quant-s2/review-sh1-demo/s2-review-sh1-demo-REPORT.md`:

1. **FIX 1 — `D1` `kelly_would_have_bound` (Astra Interpretation 4 ruling):** Updated `src/shadow/sizing.py` to compute `kelly_cf_x` first and set `kelly_would_bind = kelly_cf_x < x_p` at full precision before Step 7, recorded the ruling in [`demo/DEMO-CASES-CORRECTION-1.md`](DEMO-CASES-CORRECTION-1.md), and flipped the three Session `S4` expectations in `demo/run_demo.py` (`conservative/TQQQ`, `conservative/SOXL`, `balanced/SOXL`) from `False` to `True` so all six `S4` flags are `true`. Every other D1 expected value (`cf`, `x_pre`, `x_post_joint`, `w_target`, `q_ideal`, `delta_q_exec`, `no_trade_band_suppressed`) remains byte-identical.
2. **FIX 2 — `D3` per-fund `orders_placed: 0`:** Added `"orders_placed": 0` (integer) to every fund row emitted by `src/shadow/sizing.py` (`funds_out`) alongside existing tier-level and record-level `orders_placed: 0`, asserted integer `fund["orders_placed"] == 0` in `demo/run_demo.py` (`_check_standing_assertions`) across all 96 fund rows (`36` D1 drill + `60` D6 reconstruction), and added per-fund integer-`0` validation in `src/shadow/ledger.py` (`validate_record_body`).
3. **Frozen Evidence Untouched:** [`demo/DEMO-CASES.md`](DEMO-CASES.md) (`fc5d7f1d186096da293fa6b4dedbf4e3ee05c5e3`) and [`demo/BASELINE.md`](BASELINE.md) were not modified; the Interpretation 4 supersession is documented in [`demo/DEMO-CASES-CORRECTION-1.md`](DEMO-CASES-CORRECTION-1.md).

---

## 1. D1–D8 Results Table

| Case | Result | Summary |
| --- | --- | --- |
| D1 | PASS | 6 sessions x 3 tiers x 2 funds matched independent 6dp + positive controls (all 6 `S4` `kelly_would_have_bound` flags `true` per [`demo/DEMO-CASES-CORRECTION-1.md`](DEMO-CASES-CORRECTION-1.md)) |
| D2 | PASS | Split 2:1 invariance (`x`, `w_target`, decision outputs) and half-day invariance verified |
| D3 | PASS | `kelly_applied=false`, integer `orders_placed=0` (record, tier, and all 96 fund rows), 24 named breakers across all 96 output rows (`D1` + `D6`) |
| D4 | PASS | 6/6 D1 records replayed bit-for-bit; chain integrity and detached receipts verified separately |
| D5 | PASS | 1-byte mutation (`first_bad_sequence=2`), forked `prev_record_sha256`, dropped record gap (`first_bad_sequence=1`), duplicate append all rejected |
| D6 | PASS | 10-session Y2-overlap (`2025-11-14`..`2025-11-28`) `reconstruction` records carry Y2 discipline labels; forward clock credit rejected |
| D7 | PASS | Withheld `SOXL` close emits `O1` (`NO-NEW-RISK`, `missing_required_input:SOXL_close`) and appends 0 decision records |
| D8 | PASS | 365/365 2025 dates match `specs/csa22-v1/expected-2025.json`; unsupported venue (`XHKG`) and out-of-range dates return `unknown` and block orders |

---

## 2. Mandatory Positive Controls

| Control | Expected (Contract / REPORT §4.2) | Observed (`sizing.py`) | Status |
| --- | --- | --- | --- |
| Full-cushion `M_0(D)` (2dp) | `0.09 / 0.20 / 0.39` | `0.09 / 0.20 / 0.39` (`0.091717 / 0.201289 / 0.391176`) | PASS |
| Balanced cushion table `0%` DD | `c = 0.300`, `x_ML = 0.20` | `c = 0.300`, `x_ML = 0.20` (`0.201289`) | PASS |
| Balanced cushion table `10%` DD | `c = 0.222`, `x_ML = 0.14` | `c = 0.222`, `x_ML = 0.14` (`0.141829`) | PASS |
| Balanced cushion table `20%` DD | `c = 0.125`, `x_ML = 0.08` | `c = 0.125`, `x_ML = 0.08` (`0.075358`) | PASS |
| Balanced cushion table `25%` DD | `c = 0.067`, `x_ML = 0.04` | `c = 0.067`, `x_ML = 0.04` (`0.038936`) | PASS |
| Kelly structural illustrative (2dp) | `0.11 / 0.23 / 0.46` | `0.11 / 0.23 / 0.46` (`0.113932 / 0.227865 / 0.455729`) | PASS |

---

## 3. Command Outputs

### Demo Runner (`python3 demo/run_demo.py`)

```text
D1 PASS: 6 sessions x 3 tiers x 2 funds matched independent 6dp + positive controls
D2 PASS: split 2:1 invariance (x, w_target, decisions) and half-day invariance verified
D3 PASS: kelly_applied=false, orders_placed=0, 24 breakers across 96 rows
D4 PASS: 6/6 D1 records replayed bit-for-bit; chain + receipts verified separately
D5 PASS: mutation(seq=2), fork, gap(seq=1), duplicate append all rejected
D6 PASS: 10-session Y2 reconstruction labelled; forward clock credit rejected
D7 PASS: O1 fail-closed (missing_required_input:SOXL_close); 0 records appended
D8 PASS: 365/365 2025 dates matched expected-2025.json; unknown venue/range blocked
```

Exit code: `0`.

### Unit & Integration Test Suite (`python3 -m unittest discover -s tests -v`)

- **Total tests:** `25` (across `test_calendar.py`: `4`, `test_demo.py`: `2`, `test_evaluator.py`: `3`, `test_feeder.py`: `3`, `test_ledger.py`: `3`, `test_replay.py`: `1`, `test_sizing.py`: `9`)
- **Result:** `Ran 25 tests ... OK` (exit code `0`, `0` skipped, `0` failures)

---

## 4. Recorded Interpretations & Scoping Notes

Interpretations were frozen in [`demo/DEMO-CASES.md`](DEMO-CASES.md) §1 prior to implementation, with item 4 superseded for the gate by [`demo/DEMO-CASES-CORRECTION-1.md`](DEMO-CASES-CORRECTION-1.md):

1. **Authority & U-01 sizing stack (`definitions-contract.md` > `DEMO-BRIEF.md` > `REPORT.md`):** Implemented signed U-01 Steps 6–9 strictly (`SOXL` divided by `beta_SOXL = 1.5` in Step 6; Step 7 `x_eff_joint = min(x_joint, x_ML(c))`, `1/3` SOXL clamp before proportional scaling; Step 9 `q_ideal = floor(w_target * E / P_ref)` and weight-drift band `|w_cur - w_target| <= 0.20 * w_target`).
2. **Cushion `W_ref` vs `W*`:** Under `H = 0` and no external flows, `W_ref = W* = E*`, so `(D * W* - L) / (W* - L)` matches U-01 §3.4 identically (`W_ref` override parameter is also accepted for flow-adjusted paths).
3. **Step 3 `x_ML(c)` tie-break:** For `c in (0, 0.50]`, `M_0(c) < 2c < 3c` strictly (`"c"` binds). At `c = 0`, all three equal `0`; deterministic check order `("a", "b", "c")` records `"a"`.
4. **Step 4 Kelly counterfactual & would-have-bound (authority: [`demo/DEMO-CASES-CORRECTION-1.md`](DEMO-CASES-CORRECTION-1.md)):** Superseding the frozen `DEMO-CASES.md` §1 item 4 comparison under the Astra interpretation-4 ruling (`/home/hatch/workspace/quant-s2/review-sh1-demo/s2-review-sh1-demo-REPORT.md`): with `A = min(x_cap, x_ML)` and `g = s * v / beta`, `x_pre = g * A` and `kelly_counterfactual_x = cf = g * min(A, x_K_cond)`. `kelly_would_have_bound = (cf < g * A) = (kelly_counterfactual_x < x_pre)`, evaluated at full precision before Step 7 (equivalently `g > 0 and x_K_cond < A`; equality and zero exposure produce `false`). The two fields describe one hypothetical intervention and must agree; in D1 `S4`, all six rows report `kelly_would_have_bound = true` (`conservative/TQQQ`, `conservative/SOXL`, and `balanced/SOXL` flipped from `false` to `true`). `kelly_applied` stays `false` and Kelly stays diagnostic-only throughout actual Steps 6–10.
5. **Step 5 Volatility scalar (`v`):** Pinned linear `v_i = min(1, sigma_target / sigma_forecast_i)` with tier `sigma_target = 0.05 / 0.10 / 0.19`, applied in Step 6. Missing-sigma `O1` fail-closed override is covered via `O1` missing-input checks; D1–D6 fixture sigmas are always present.
6. **Step 6 `beta_SOXL`:** Pinned to `1.5` placeholder floor (`beta_TQQQ = 1`).
7. **Step 8 Combined ceiling (`w_max`):** Enforced `sum_i w_target_i <= min(w_max, 1.00)` by proportional scaling when exceeded; inactive at `H = 0` given Step 7, verified via direct `H > 0` unit test (`H = 250000 = 5 * E*`, since under `M_0` even `H = 3 * E*` keeps `sum w_target <= 0.5216 < 0.60`).
8. **Step 9 No-trade band, mandatory bypass, and out-of-scope venue machinery:** Suppression (`no_trade_band_suppressed: true`, `delta_q_exec = 0`) triggers iff `q_ideal != q_pre`, `|w_cur - w_target| <= 0.20 * w_target`, and mandatory bypass does not apply. Mandatory bypass triggers on reductions (`q_ideal < q_pre`) where current holdings exceed a Step 2/3/7/8 cap ceiling or an active reduction control state (`DE-RISK`, `J1_TRIM`) requires reduction; `emergency_bypass_triggered` is `true` iff mandatory bypass overrides an otherwise-suppressible band trade (`false` on D1–D8). Fee/collar/`C_avail` buy-quantity machinery (`SUPPLY-A/B` scope) is out of demo scope (Stage N is notional).
9. **D6 Y2-overlap window & `is_modelled` handling:** Selected `2025-11-14`..`2025-11-28` (10 `XNYS` sessions across all 5 Y2 CSVs, inside `specs/csa22-v1` 2025 coverage, spanning Thanksgiving closure + early close + TQQQ `2025-11-20` split). Uses synthetic reconstruction `sigma_forecast = "0.24"` (since 10 sessions have no 20-day lookback history) and `B0 = 50000, H = 0, s = 1`. Reads `is_modelled` per row directly from the Y2 CSVs (`false` on 2025 native rows; `true` separately unit-tested on `Path C` rows).
10. **Provisional C-SA-22 spec & stub anchors:** `specs/csa22-v1/` is explicitly labelled **PROVISIONAL** (coordinator owns R-3 publication). `engine_hash` is a pinned stub digest (`StubEvaluator`), `code_version_sha` is a deterministic module+spec content digest invariant across doc commits, and detached receipts carry `anchor: "synthetic-stub"`.
11. **24-breaker ID set:** Exact adjudicated 24-row set `L0`–`L6`, `V1`–`V7`, `O1`–`O9`, `J1`, sorted lexicographically by ID.
