# SH-1 §7 Design Demonstration — Frozen Case List (D1–D8)

**Status:** FROZEN before implementation (branch `demo/sh1-design-demo` from `main` @ `dff3ecc`).
**Authority Order:** `docs/BRIEF-SH-1.md` > owner-signed U-01 `definitions-contract.md` > `DEMO-BRIEF.md` > `REPORT.md` (background only).

---

## 1. Recorded Interpretations & Scoping Choices

Every place where the texts leave a detail open, where `DEMO-BRIEF.md` notes a demo interpretation, or where `U-01` governs over background text is explicitly pinned below:

1. **Sizing math authority (`U-01` vs background summaries):** Follow `definitions-contract.md` §§3–4 strictly:
   - Step 6 divides SOXL pre-exposure by `beta_SOXL` (`x_SOXL_pre = (s_SOXL * min(x_cap, x_ML) * v_SOXL) / beta_SOXL`).
   - Step 7 enforces `x_eff_joint(c) = min(x_joint, x_ML(c))`, applies the `1/3` SOXL sub-cap first (`beta_SOXL * x_SOXL_mid = min(beta_SOXL * x_SOXL_pre, x_eff_joint / 3)`), and scales both funds proportionally by `alpha = x_eff_joint / x_joint_beta` only when `x_joint_beta > x_eff_joint`.
   - Step 9 uses `q_ideal = floor(w_target * E / P_ref)` (`C-SA-Q21`, integer floor, never `round-half-even`) and weight-drift no-trade band `|w_cur - w_target| <= 0.20 * w_target` (inclusive).
2. **Cushion reference (`W_ref` vs `W*`):** Under `H = 0` and no external flows (`L = E* - E`), `W_ref = W* = E*` and `W* - L = E = W`, so `c = (D * W* - L) / (W* - L)` (and `0` once `L >= D * W*`) matches both `U-01` §3.4 and `DEMO-BRIEF.md`.
3. **Step 3 `x_ML(c)` tie-breaking:** `x_ML(c) = min(3c, 2c, M_0(c))` with `M_0(c) = ln(1 - c) / ln(1 - 0.83)`. For every `c in (0, 0.50]`, `M_0(c) < 2c < 3c` strictly so sub-bound `"c"` binds. At `c = 0`, all three equal `0`; tie-breaking checks `("a", 3c)`, `("b", 2c)`, `("c", M_0)` in order and records `"a"`.
4. **Step 4 Kelly diagnostic (`kelly_would_have_bound`, `kelly_counterfactual_x`):**
   - `kelly_applied: false` always.
   - `x_K_struct = k * max(0, 0.0525 / 0.24^2)` and `x_K_cond = x_K_struct * min(1, (0.24 / sigma_forecast)^2)`.
   - Per `DEMO-BRIEF.md` demo interpretation: `kelly_would_have_bound` is evaluated against the conditional ceiling strictly as `x_K_cond < x_pre` (Step 6 result before Step 7), and `kelly_counterfactual_x` is the Step 6 formula with `x_K_cond` included inside `min(x_cap, x_ML, x_K_cond)` (so for SOXL, `(s_SOXL * min(x_cap, x_ML, x_K_cond) * v_SOXL) / beta_SOXL`).
   - Mathematical note recorded for reviewer transparency: because `s`, `v`, and `1/beta_SOXL` scale `min(...)` at Step 6, `x_K_cond < min(x_cap, x_ML)` can reduce `kelly_counterfactual_x` below `x_pre` even when `x_K_cond >= x_pre` (`kelly_would_have_bound = false`), as occurs in D1 Session 4 for `conservative` and `balanced` SOXL.
5. **Step 5 Volatility scalar (`v`):** `DEMO-BRIEF.md` pins the **linear** scalar `v_i = min(1, sigma_target / sigma_forecast_i)` with tier `sigma_target = 0.05 / 0.10 / 0.19`, applied in Step 6 (the squared form is out of demo scope and deferred to `PILOT`). Missing-sigma fail-closed override (`O1` + `max(last_valid_sigma, sigma_stress_default)`) is tested via `O1` missing-input checks; D1–D6 fixture sigmas are always present.
6. **Step 6 `beta_SOXL`:** Pinned to the `1.5` placeholder floor (`beta_fall = 1.5`, `beta_TQQQ = 1`).
7. **Step 8 Combined ceiling (`w_max`):** `w_target_i = (x_post_joint_i / 3) * (1 + H / E*)`. Hard constraint `sum_i w_target_i <= min(w_max, 1.00)` scales both fund weights proportionally when exceeded (cannot trigger when `H = 0` given Step 7; verified in a dedicated `H > 0` unit test).
8. **Step 9 No-trade band, mandatory bypass, and out-of-scope fee/collar machinery:**
   - Discretionary rebalance is suppressed (`delta_q_exec = 0`, `no_trade_band_suppressed: true`) iff `q_ideal != q_pre`, `|w_cur - w_target| <= 0.20 * w_target` (inclusive), and mandatory bypass does not apply. When `q_ideal == q_pre`, no trade exists to suppress (`no_trade_band_suppressed: false`, `delta_q_exec = 0`).
   - Mandatory bypass applies to any reduction (`q_ideal < q_pre`) where current holdings exceed a Step 2/3/7/8 cap ceiling (`x_cur_i > min(x_cap, x_ML)`, `beta_SOXL * x_cur_SOXL > x_eff_joint / 3`, `x_cur_TQQQ + beta_SOXL * x_cur_SOXL > x_eff_joint`, or `sum w_cur > min(w_max, 1.00)`) or an active reduction breaker/interlock requires reducing exposure. `emergency_bypass_triggered` is `true` iff a mandatory bypass overrides what the band would otherwise suppress (`false` on all D1–D8 runs).
   - Fee/collar/`C_avail` buy-quantity machinery (`SUPPLY-A/B` scope) is **out of demo scope**; Stage N is notional.
9. **Canonical decimal & `binding_constraint` formatting:** `decimal.Decimal` (`prec=28`) throughout; demo comparisons use 6dp `ROUND_HALF_EVEN` quantization. Ledger records emit exact canonical ordinary base-ten strings (`"0"` for zero, fixed-point without scientific notation or leading/trailing zeros). Stable array sorting is lexicographical by ID (`tiers`: `aggressive`, `balanced`, `conservative`; `funds`: `SOXL`, `TQQQ`; `breakers`: sorted by ID across all 24 named IDs `L0`–`L6`, `V1`–`V7`, `O1`–`O9`, `J1`).
10. **D6 Y2-overlap window & labels:** Window `2025-11-14` through `2025-11-28` (10 `XNYS` sessions) extracted across all five named CSVs (`tqqq_daily.csv`, `soxl_daily.csv`, `ndx_index_daily.csv`, `semi_index_daily.csv`, `risk_free_rates_daily.csv`) into `tests/fixtures/y2overlap/`, with source SHA-256s recorded in `tests/fixtures/y2overlap/README.md`. This 2025 window falls completely inside `specs/csa22-v1` (2025 `XNYS` coverage), spans Thanksgiving (`2025-11-27` closed) + the `2025-11-28` early close + TQQQ's `2025-11-20` split date, carries `tr_regime = native_xsox_sub_for_icesemit` and `quality_flag = market_close_basis_no_public_daily_nav`, and reads per-series `is_modelled` directly from the source CSV `is_modelled` column (`false` on native post-2009 rows; `true` verified separately on pre-TR `Path C` rows in unit tests).
11. **Provisional C-SA-22 spec package (`specs/csa22-v1/`):** Labelled **PROVISIONAL** (formal R-3 publication belongs to coordinator). Covers `2025-01-01`..`2025-12-31` (`XNYS`, `America/New_York`), standard 2025 holidays, early closes (`2025-07-03`, `2025-11-28`, `2025-12-24` at `13:00`), and unscheduled closure (`2025-01-09`, President Jimmy Carter National Day of Mourning). Cutoff defaults to session close; order window `[open, cutoff)` capped by `close`. Unsupported venue / out-of-range returns `session_state: "unknown"`, null times, `blocks_orders: true`.
12. **Ledger stub anchors & manifest hashing:** Detached receipts carry `anchor: "synthetic-stub"`. `engine_hash` uses a pinned stub identifier (`"stub-c-fuse-eval-v1"`). `code_version_sha` is a deterministic content digest of the `src/shadow/` implementation modules + `specs/csa22-v1/` spec package so replay stays invariant across documentation commits.

---

## 2. Frozen Tier Constants (`policy_v0_pre`)

| Tier ID | Contract Name | `D` | `k` | `x_cap` | `x_joint` | `w_max` | `sigma_target` |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `conservative` | `CAPITAL_PRESERVATION` | `0.15` | `1/8` (`0.125`) | `0.20` | `0.20` | `0.15` | `0.05` |
| `balanced` | `BALANCED_COMPOUNDING` | `0.30` | `1/4` (`0.25`) | `0.40` | `0.40` | `0.35` | `0.10` |
| `aggressive` | `GROWTH_COMPOUNDING` | `0.50` | `1/2` (`0.50`) | `0.80` | `0.80` | `0.60` | `0.19` |

Pinned Kelly illustrative constants: `mu_shrunk = 0.0525`, `sigma_long = 0.24`, `beta_SOXL = 1.5`, `beta_TQQQ = 1`, `H = 0`, `B0 = 50000`.

---

## 3. Mandatory Positive Controls

1. **Full-cushion `M_0(D)`** (`M_0(c) = ln(1 - c) / ln(0.17)`):
   - `conservative` (`c = 0.15`): `ln(0.85) / ln(0.17) = 0.0917172053...` -> `0.09` (2dp)
   - `balanced` (`c = 0.30`): `ln(0.70) / ln(0.17) = 0.2012887309...` -> `0.20` (2dp)
   - `aggressive` (`c = 0.50`): `ln(0.50) / ln(0.17) = 0.3911761078...` -> `0.39` (2dp)
2. **Balanced cushion table** (`D = 0.30`, `H = 0`, `W* = 50000`):
   - `0%` drawdown (`L = 0`): `c = (15000 - 0)/50000 = 0.300000` (`0.300` at 3dp), `x_ML = 0.201289` (`0.20` at 2dp)
   - `10%` drawdown (`L = 5000`): `c = (15000 - 5000)/45000 = 2/9 = 0.222222` (`0.222` at 3dp), `x_ML = ln(7/9)/ln(0.17) = 0.141829` (`0.14` at 2dp)
   - `20%` drawdown (`L = 10000`): `c = (15000 - 10000)/40000 = 0.125000` (`0.125` at 3dp), `x_ML = ln(0.875)/ln(0.17) = 0.075358` (`0.08` at 2dp)
   - `25%` drawdown (`L = 12500`): `c = (15000 - 12500)/37500 = 1/15 = 0.066667` (`0.067` at 3dp), `x_ML = ln(14/15)/ln(0.17) = 0.038936` (`0.04` at 2dp)
3. **Kelly structural illustrative** (`x_K_struct = k * 0.0525 / 0.24^2 = k * 0.9114583333...`):
   - `conservative` (`k = 1/8`): `0.113932` -> `0.11` (2dp)
   - `balanced` (`k = 1/4`): `0.227865` -> `0.23` (2dp)
   - `aggressive` (`k = 1/2`): `0.455729` -> `0.46` (2dp)

---

## 4. Demo Cases (D1–D8)

### D1 — Synthetic 6-Session Fixture (Exact 3-Tier Sizing Outputs)

#### Fixture Inputs

All sessions have `H = 0`, `E* = 50000`, `W* = 50000`.

| Session ID | Date (`XNYS`) | Scenario | `E` | `L` | `s_TQQQ` | `s_SOXL` | `P_TQQQ` | `P_SOXL` | `sigma_TQQQ` | `sigma_SOXL` | `q_pre_TQQQ` (C/B/A) | `q_pre_SOXL` (C/B/A) |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| `S1` | `2025-11-21` | Full cushion, `s=1`, regular | `50000` | `0` | `1` | `1` | `50` | `40` | `0.05` | `0.05` | `0 / 0 / 0` | `0 / 0 / 0` |
| `S2` | `2025-11-24` | 10% drawdown state | `45000` | `5000` | `1` | `1` | `50` | `40` | `0.10` | `0.10` | `0 / 0 / 0` | `0 / 0 / 0` |
| `S3` | `2025-11-25` | `s < 1` with joint cap binding (`x_joint_beta > x_eff_joint`) | `50000` | `0` | `0.85` | `0.90` | `50` | `40` | `0.05` | `0.05` | `0 / 0 / 0` | `0 / 0 / 0` |
| `S4` | `2025-11-26` | High forecast-vol (conditional Kelly binds) | `50000` | `0` | `1` | `1` | `50` | `40` | `0.72` | `0.80` | `0 / 0 / 0` | `0 / 0 / 0` |
| `S5` | `2025-11-28` | Half-day (`13:00 EST`), same state as `S1` | `50000` | `0` | `1` | `1` | `50` | `40` | `0.05` | `0.05` | `0 / 0 / 0` | `0 / 0 / 0` |
| `S6` | `2025-12-01` | Post-2:1 TQQQ split (`P_TQQQ: 50->25`, `q_pre_TQQQ` 2x of `S5` `q_ideal`) | `50000` | `0` | `1` | `1` | `25` | `40` | `0.05` | `0.05` | `44 / 100 / 194` | `6 / 13 / 27` |

#### Independent Arithmetic & Expected 3-Tier Outputs (6dp `ROUND_HALF_EVEN`)

Formulas used in hand derivation:
- `c = (D * 50000 - L) / (50000 - L)`
- `x_ML = M_0(c) = ln(1 - c) / ln(0.17)` (sub-bound `"c"` binds since `M_0(c) < 2c < 3c` for all `c in (0, 0.50]`)
- `x_K_struct = k * 0.0525 / 0.0576 = k * 0.9114583333...`
- `x_K_cond_i = x_K_struct * min(1, (0.24 / sigma_i)^2)`
- `v_i = min(1, sigma_target / sigma_i)`
- `x_pre_TQQQ = s_TQQQ * min(x_cap, x_ML) * v_TQQQ`; `x_pre_SOXL = (s_SOXL * min(x_cap, x_ML) * v_SOXL) / 1.5`
- `kelly_would_have_bound = (x_K_cond < x_pre)`
- `kelly_cf_TQQQ = s_TQQQ * min(x_cap, x_ML, x_K_cond_TQQQ) * v_TQQQ`; `kelly_cf_SOXL = (s_SOXL * min(x_cap, x_ML, x_K_cond_SOXL) * v_SOXL) / 1.5`
- `x_eff_joint = min(x_joint, x_ML)`; `1.5 * x_SOXL_mid = min(1.5 * x_pre_SOXL, x_eff_joint / 3)`; `x_joint_beta = x_pre_TQQQ + 1.5 * x_SOXL_mid`
- `alpha = 1` if `x_joint_beta <= x_eff_joint` else `x_eff_joint / x_joint_beta`; `x_post_TQQQ = alpha * x_pre_TQQQ`; `x_post_SOXL = alpha * x_SOXL_mid`
- `w_target_i = x_post_i / 3` (`H = 0`); `q_ideal_i = floor(w_target_i * E / P_i)`

**Session `S1` (`2025-11-21`) and Session `S5` (`2025-11-28`, identical closes & state):**
- `x_ML = 0.091717 / 0.201289 / 0.391176 < x_cap`; `v = 1` across all tiers and both funds; `x_K_cond = x_K_struct` (`0.113932 / 0.227865 / 0.455729`).
- For each tier: `x_pre_TQQQ = x_ML`, `1.5 * x_pre_SOXL = x_ML` -> SOXL clamped by `1/3` sub-cap to `1.5 * x_SOXL_mid = x_ML / 3` (`x_SOXL_mid = x_ML / 4.5`). `x_joint_beta = (4/3) * x_ML > x_ML = x_eff_joint`, so `alpha = 3/4 = 0.75`.
  - `conservative`: `c = 0.150000`, `x_ML = 0.091717` (`c`), `x_K_struct = 0.113932`
    - `TQQQ`: `x_K_cond = 0.113932`, `kelly_would_have_bound = false`, `kelly_counterfactual_x = 0.091717`, `v = 1`, `x_pre = 0.091717`, `x_post_joint = 0.068788`, `w_target = 0.022929`, `q_ideal = floor(0.02292930*50000/50) = 22`, `delta_q_exec = 22`, `no_trade_band_suppressed = false`
    - `SOXL`: `x_K_cond = 0.113932`, `kelly_would_have_bound = false`, `kelly_counterfactual_x = 0.061145`, `v = 1`, `x_pre = 0.061145`, `x_post_joint = 0.015286` (`0.091717 / 6`), `w_target = 0.005095`, `q_ideal = floor(0.00509540*50000/40) = 6`, `delta_q_exec = 6`, `no_trade_band_suppressed = false`
  - `balanced`: `c = 0.300000`, `x_ML = 0.201289` (`c`), `x_K_struct = 0.227865`
    - `TQQQ`: `x_K_cond = 0.227865`, `kelly_would_have_bound = false`, `kelly_counterfactual_x = 0.201289`, `v = 1`, `x_pre = 0.201289`, `x_post_joint = 0.150967`, `w_target = 0.050322`, `q_ideal = floor(0.05032218*50000/50) = 50`, `delta_q_exec = 50`, `no_trade_band_suppressed = false`
    - `SOXL`: `x_K_cond = 0.227865`, `kelly_would_have_bound = false`, `kelly_counterfactual_x = 0.134192`, `v = 1`, `x_pre = 0.134192`, `x_post_joint = 0.033548`, `w_target = 0.011183`, `q_ideal = floor(0.01118271*50000/40) = 13`, `delta_q_exec = 13`, `no_trade_band_suppressed = false`
  - `aggressive`: `c = 0.500000`, `x_ML = 0.391176` (`c`), `x_K_struct = 0.455729`
    - `TQQQ`: `x_K_cond = 0.455729`, `kelly_would_have_bound = false`, `kelly_counterfactual_x = 0.391176`, `v = 1`, `x_pre = 0.391176`, `x_post_joint = 0.293382`, `w_target = 0.097794`, `q_ideal = floor(0.09779403*50000/50) = 97`, `delta_q_exec = 97`, `no_trade_band_suppressed = false`
    - `SOXL`: `x_K_cond = 0.455729`, `kelly_would_have_bound = false`, `kelly_counterfactual_x = 0.260784`, `v = 1`, `x_pre = 0.260784`, `x_post_joint = 0.065196`, `w_target = 0.021732`, `q_ideal = floor(0.02173201*50000/40) = 27`, `delta_q_exec = 27`, `no_trade_band_suppressed = false`

**Session `S2` (`2025-11-24`, 10% drawdown `E = 45000, L = 5000`, `sigma = 0.10`):**
- `conservative`: `c = (7500 - 5000)/45000 = 1/18 = 0.055556`, `x_ML = ln(17/18)/ln(0.17) = 0.032257` (`c`), `v = 0.05/0.10 = 0.500000`.
  - `x_pre_TQQQ = 0.032257 * 0.5 = 0.016129`; `x_pre_SOXL = (0.032257 * 0.5) / 1.5 = 0.010752` (`1.5 * x_pre_SOXL = 0.016129 > 0.032257 / 3 = 0.010752`, so SOXL clamped to `x_SOXL_mid = 0.010752 / 1.5 = 0.007168`).
  - `x_joint_beta = 0.016129 + 0.010752 = 0.026881 <= 0.032257 = x_eff_joint`, so `alpha = 1`.
  - `TQQQ`: `x_K_cond = 0.113932`, `bind = false`, `cf = 0.016129`, `x_post = 0.016129`, `w = 0.005376`, `q_ideal = floor(0.00537622*45000/50) = 4`, `delta_q_exec = 4`.
  - `SOXL`: `x_K_cond = 0.113932`, `bind = false`, `cf = 0.010752`, `x_post = 0.007168`, `w = 0.002389`, `q_ideal = floor(0.00238943*45000/40) = 2`, `delta_q_exec = 2`.
- `balanced`: `c = (15000 - 5000)/45000 = 2/9 = 0.222222`, `x_ML = ln(7/9)/ln(0.17) = 0.141829` (`c`), `v = 0.10/0.10 = 1`.
  - `x_joint_beta = (4/3)*0.141829 = 0.189105 > 0.141829`, `alpha = 0.75`.
  - `TQQQ`: `x_K_cond = 0.227865`, `bind = false`, `cf = 0.141829`, `x_pre = 0.141829`, `x_post = 0.106372`, `w = 0.035457`, `q_ideal = floor(0.03545721*45000/50) = 31`, `delta_q_exec = 31`.
  - `SOXL`: `x_K_cond = 0.227865`, `bind = false`, `cf = 0.094553`, `x_pre = 0.094553`, `x_post = 0.023638`, `w = 0.007879`, `q_ideal = floor(0.00787938*45000/40) = 8`, `delta_q_exec = 8`.
- `aggressive`: `c = (25000 - 5000)/45000 = 4/9 = 0.444444`, `x_ML = ln(5/9)/ln(0.17) = 0.331716` (`c`), `v = 1`.
  - `x_joint_beta = (4/3)*0.331716 = 0.442288 > 0.331716`, `alpha = 0.75`.
  - `TQQQ`: `x_K_cond = 0.455729`, `bind = false`, `cf = 0.331716`, `x_pre = 0.331716`, `x_post = 0.248787`, `w = 0.082929`, `q_ideal = floor(0.08292898*45000/50) = 74`, `delta_q_exec = 74`.
  - `SOXL`: `x_K_cond = 0.455729`, `bind = false`, `cf = 0.221144`, `x_pre = 0.221144`, `x_post = 0.055286`, `w = 0.018429`, `q_ideal = floor(0.01842866*45000/40) = 20`, `delta_q_exec = 20`.

**Session `S3` (`2025-11-25`, `s_TQQQ = 0.85, s_SOXL = 0.90`, `sigma = 0.05`, full cushion):**
- Across all three tiers (`v = 1`): `x_pre_TQQQ = 0.85 * x_ML`, `1.5 * x_pre_SOXL = 0.90 * x_ML > x_ML / 3` -> `1.5 * x_SOXL_mid = x_ML / 3`.
- `x_joint_beta = (0.85 + 1/3) * x_ML = (71/60) * x_ML > x_ML = x_eff_joint` -> `alpha = 60/71 = 0.8450704225...`!
- `conservative`: `TQQQ`: `x_pre = 0.077960`, `x_post = 0.065881`, `w = 0.021960`, `q_ideal = 21`; `SOXL`: `x_pre = 0.055030`, `x_post = 0.017224`, `w = 0.005741`, `q_ideal = 7`. (`kelly_would_have_bound = false` for both.)
- `balanced`: `TQQQ`: `x_pre = 0.171095`, `x_post = 0.144588`, `w = 0.048196`, `q_ideal = 48`; `SOXL`: `x_pre = 0.120773`, `x_post = 0.037801`, `w = 0.012600`, `q_ideal = 15`. (`kelly_would_have_bound = false` for both.)
- `aggressive`: `TQQQ`: `x_pre = 0.332500`, `x_post = 0.280986`, `w = 0.093662`, `q_ideal = 93`; `SOXL`: `x_pre = 0.234706`, `x_post = 0.073460`, `w = 0.024487`, `q_ideal = 30`. (`kelly_would_have_bound = false` for both.)

**Session `S4` (`2025-11-26`, high vol `sigma_TQQQ = 0.72, sigma_SOXL = 0.80`, full cushion):**
- Volatility scalars: `(0.24/0.72)^2 = 1/9`, `(0.24/0.80)^2 = 0.09`.
- `conservative`:
  - `TQQQ`: `v = 0.05/0.72 = 0.069444`, `x_K_cond = 0.113932/9 = 0.012659`, `x_pre = 0.091717*0.069444 = 0.006369` -> `bind = false` (`0.012659 >= 0.006369`), `cf = 0.012659*0.069444 = 0.000879`, `x_post = 0.006369`, `w = 0.002123`, `q_ideal = 2`.
  - `SOXL`: `v = 0.05/0.80 = 0.062500`, `x_K_cond = 0.113932*0.09 = 0.010254`, `x_pre = (0.091717*0.0625)/1.5 = 0.003822` -> `bind = false`, `cf = (0.010254*0.0625)/1.5 = 0.000427`, `x_post = 0.003822`, `w = 0.001274`, `q_ideal = 1`.
- `balanced`:
  - `TQQQ`: `v = 0.10/0.72 = 0.138889`, `x_K_cond = 0.227865/9 = 0.025318`, `x_pre = 0.201289*0.138889 = 0.027957` -> **`bind = true`** (`0.025318 < 0.027957`), `cf = 0.025318*0.138889 = 0.003516`, `x_post = 0.027957`, `w = 0.009319`, `q_ideal = 9`.
  - `SOXL`: `v = 0.10/0.80 = 0.125000`, `x_K_cond = 0.227865*0.09 = 0.020508`, `x_pre = (0.201289*0.125)/1.5 = 0.016774` -> `bind = false`, `cf = (0.020508*0.125)/1.5 = 0.001709`, `x_post = 0.016774`, `w = 0.005591`, `q_ideal = 6`.
- `aggressive`:
  - `TQQQ`: `v = 0.19/0.72 = 0.263889`, `x_K_cond = 0.455729/9 = 0.050637`, `x_pre = 0.391176*0.263889 = 0.103227` -> **`bind = true`**, `cf = 0.050637*0.263889 = 0.013362`, `x_post = 0.103227`, `w = 0.034409`, `q_ideal = 34`.
  - `SOXL`: `v = 0.19/0.80 = 0.237500`, `x_K_cond = 0.455729*0.09 = 0.041016`, `x_pre = (0.391176*0.2375)/1.5 = 0.061936` -> **`bind = true`**, `cf = (0.041016*0.2375)/1.5 = 0.006494`, `x_post = 0.061936`, `w = 0.020645`, `q_ideal = 25`.
- Note for all 3 tiers in `S4`: `x_joint_beta <= x_eff_joint` (`alpha = 1`) and `1.5 * x_pre_SOXL <= x_eff_joint / 3`.

**Session `S6` (`2025-12-01`, post-2:1 TQQQ split: `P_TQQQ = 25`, `P_SOXL = 40`, `q_pre_TQQQ = 44/100/194`, `q_pre_SOXL = 6/13/27`):**
- `x_pre`, `x_post_joint`, `w_target` are identical to `S1`/`S5`.
- `TQQQ`:
  - `conservative`: `q_ideal = floor(0.02292930*50000/25) = 45`; `w_cur = 44*25/50000 = 0.022000`; `|0.022 - 0.02292930| = 0.00092930 <= 0.20 * 0.02292930 = 0.004586` -> `no_trade_band_suppressed = true`, `delta_q_exec = 0`.
  - `balanced`: `q_ideal = floor(0.05032218*50000/25) = 100`; `q_pre = 100` (`q_ideal == q_pre`) -> `no_trade_band_suppressed = false`, `delta_q_exec = 0`.
  - `aggressive`: `q_ideal = floor(0.09779403*50000/25) = 195`; `w_cur = 194*25/50000 = 0.097000`; `|0.097 - 0.09779403| = 0.00079403 <= 0.20 * 0.09779403 = 0.019559` -> `no_trade_band_suppressed = true`, `delta_q_exec = 0`.
- `SOXL`: `q_ideal == q_pre` (`6 / 13 / 27`), `delta_q_exec = 0`, `no_trade_band_suppressed = false`.

---

### D2 — Split Invariance & Half-Day Invariance
- **Split invariance:** Compare economic state pre-2:1 split (`P_TQQQ = 50, P_SOXL = 40, q_pre = 0` or `q_pre` scaled cleanly at integer boundary e.g. `q_pre_TQQQ = 10`) vs post-2:1 split (`P_TQQQ = 25, P_SOXL = 40, q_pre_TQQQ = 20`) at identical `(E, E*, H, L, s, sigma_forecast)` -> assert identical `x_pre`, `x_post_joint`, `w_target`, `no_trade_band_suppressed`, `binding_constraint`, and decision/breaker outputs (`q_ideal_post` preserves dollar position within 1 post-split share, and equals exact `2 * q_ideal_pre` when pre/post share ratios are even, e.g. `S5` vs `S6` balanced `50 -> 100`).
- **Half-day invariance:** Compare regular session (`2025-11-21`, `16:00 EST`) vs early-close half-day session (`2025-11-28`, `13:00 EST`) with identical `(E, E*, H, L, s, P, q_pre, sigma_forecast)` (specifically `S1` vs `S5`) -> assert identical sizing outputs across all 3 tiers and both funds.

### D3 — Standing Stage N Assertions
- Across **every** output row of **every** demo run (`D1`, `D2`, `D4`, `D6`, etc.): assert `kelly_applied is False`, `orders_placed == 0`, all 24 named breaker IDs (`L0`–`L6`, `V1`–`V7`, `O1`–`O9`, `J1`) appear exactly once, `H == "0"`, and `B0 == "50000"`.

### D4 — Bit-for-Bit Replay of D1 Fixture Window
- Persist D1's 6-session `drill` ledger chain, detached synthetic-stub receipts, and retained inputs/policy/calendar manifest.
- Run `replay` from retained inputs alone -> assert canonical JSON bytes and `record_sha256` match stored records **bit-for-bit** for all 6 sequences (`0..5`), and assert separate chain integrity + detached receipt verification pass.

### D5 — Ledger Tamper & Append-Rule Rejection
1. **One-byte mutation:** Mutate 1 byte in sequence `2` of a cloned D1 chain -> `verify_chain` MUST fail and return `first_bad_sequence == 2`.
2. **Forked previous hash:** Attempt to append a record with `prev_record_sha256 != head.record_sha256` -> append MUST be rejected.
3. **Dropped record (gap):** Remove sequence `1` from a cloned D1 chain -> `verify_chain` MUST fail and detect the gap at `first_bad_sequence == 1`.
4. **Duplicate append:** Attempt to append a second record for the same `(mode, policy_id, session_date)` -> append MUST be rejected.

### D6 — Y2-Overlap Segment (`reconstruction` Mode & Credit Rejection)
- Copy 10-session window `2025-11-14`..`2025-11-28` across all five Y2 CSVs (`tqqq_daily.csv`, `soxl_daily.csv`, `ndx_index_daily.csv`, `semi_index_daily.csv`, `risk_free_rates_daily.csv`) into `tests/fixtures/y2overlap/`, recording source SHA-256s in `tests/fixtures/y2overlap/README.md`.
- Run recorder in `reconstruction` mode over the 10 sessions -> assert 10 ledger records appended, every record carries:
  - `observation_class == "historical_reconstruction"`
  - Y2 standing qualification `"as-retrieved reconstruction; not point-in-time inputs"`
  - `SOXL` close-proxy label `"market-close proxy; NAV criterion unverified"`
  - `XSOX`-for-`ICESEMIT` substitution disclosure on semiconductor rows
  - Per-series `is_modelled` flag derived from source CSV rows (`false` in 2025 native window; separate unit test verifies `is_modelled: true` on pre-TR `Path C` fixture rows)
  - `forward_clock_credit_eligible == False`, and `check_forward_clock_credit(...)` explicitly **rejects** every record and the chain for any forward clock.

### D7 — `O1` Fail-Closed Output on Withheld Required Input
- Run D1 fixture on a session with `SOXL` close withheld (`None` / missing) -> assert evaluator emits `O1` (`NO-NEW-RISK`, `would_have_tripped: true`, reason naming withheld `SOXL` input), and **no decision record is appended** to the ledger (chain length unchanged).

### D8 — C-SA-22 Calendar Vectors (Full 2025 Coverage + Boundaries)
- Verify `calendar_consumer` validates `specs/csa22-v1/` SHA-256 digests before use.
- Compare `calendar_consumer` output across all 365 dates of 2025 field-by-field against independently generated `specs/csa22-v1/expected-2025.json` (including weekends, all standard 2025 holidays, the three early closes `2025-07-03` EDT, `2025-11-28` EST, `2025-12-24` EST, both DST transition weeks `2025-03-07`/`2025-03-10` and `2025-10-31`/`2025-11-03`, and the `2025-01-09` unscheduled closure).
- Verify unsupported venue (e.g. `"XHKG"`) and out-of-range dates (`"2024-12-31"`, `"2026-01-01"`) return `session_state: "unknown"`, null open/close/cutoff times, and `blocks_orders: true`.
