# SH-1 §7 Design Demonstration — Versioned Correction 1 (`DEMO-CASES-CORRECTION-1`)

- **Status:** Versioned correction 1 to frozen [`demo/DEMO-CASES.md`](DEMO-CASES.md), issued under the Astra re-verification **FAIL** on head `fe3f656` (`fe3f656e65e725abafbf093c6bb15095790886e2`), branch `demo/sh1-design-demo`, epoch `SHADOW-N-DESIGN-2026-11-21-v1`.
- **Authority Report:** `/home/hatch/workspace/quant-s2/review-sh1-demo/s2-review-sh1-demo-REPORT.md` (and receipt `/home/hatch/workspace/quant-s2/review-sh1-demo/s2-review-sh1-demo-RECEIPT.md`).
- **Frozen Evidence Preservation:** [`demo/DEMO-CASES.md`](DEMO-CASES.md) itself is preserved **byte-identical** as frozen evidence (git blob `fc5d7f1d186096da293fa6b4dedbf4e3ee05c5e3`) and MUST NOT be modified.
- **Supersession Scope:** For the SH-1 §7 gate, this document supersedes **Interpretation 4** of [`demo/DEMO-CASES.md`](DEMO-CASES.md) — specifically Section 1 item 4 and the corresponding `kelly_would_have_bound` formula line and Session `S4` flag expectations in the D1 arithmetic derivation (referenced in the correction pass 1 directive as section 1 item 4 and the section 3 formula line).

---

## 1. Adjudicated Interpretation 4 Semantics (Astra Ruling)

Under U-01 §4.1 Step 4 and §4.2 Step 6, the inside-min placement for `kelly_counterfactual_x` is accepted, and the frozen mixed-stage comparison (`x_K_cond < x_pre`) is rejected. For sleeve `i`:

- Let `A = min(x_cap, x_ML)` (where `x_ML = x_ML(c)`).
- Let `g = s * v / beta` (more explicitly `g = s_i * v_i / beta_i`, where `beta_TQQQ = 1` and `beta_SOXL = 1.5`).
- `x_pre = g * A`.
- `kelly_counterfactual_x = cf = g * min(A, x_K_cond)` (unchanged).
- `kelly_would_have_bound = (cf < g * A) = (kelly_counterfactual_x < x_pre)`, evaluated at full precision before Step 7 (and before display rounding). Equivalently: `g > 0 and x_K_cond < A` on the unrounded inputs.
- **Boundary rules:** Equality (`cf == x_pre`) and zero exposure (`g == 0` or `A == 0`) produce `false`.
- **Coherence invariant:** `kelly_counterfactual_x` and `kelly_would_have_bound` describe one hypothetical intervention and must agree on whether it reduced exposure.
- **Diagnostic-only rule:** `kelly_applied` remains `false` always; Kelly stays diagnostic-only throughout actual Steps 6–10.

---

## 2. Corrected D1 Expectations (`S4` Would-Have-Bound Flags)

Under the ruled semantics above, **all six** `kelly_would_have_bound` flags in Session `S4` (`2025-11-26`, `sigma_TQQQ = 0.72`, `sigma_SOXL = 0.80`) are `true`. Exactly three expectations flip from `false` to `true` relative to frozen `DEMO-CASES.md`:

| Session | Tier | Fund | `cf` (`kelly_counterfactual_x`) | `x_pre` | Frozen Flag | Corrected Flag | Change |
| --- | --- | --- | ---: | ---: | --- | --- | --- |
| `S4` | `conservative` | `TQQQ` | `0.000879` | `0.006369` | `false` | **`true`** | **Flipped** (`0.000879 < 0.006369`) |
| `S4` | `conservative` | `SOXL` | `0.000427` | `0.003822` | `false` | **`true`** | **Flipped** (`0.000427 < 0.003822`) |
| `S4` | `balanced` | `TQQQ` | `0.003516` | `0.027957` | `true` | `true` | Unchanged (`0.003516 < 0.027957`) |
| `S4` | `balanced` | `SOXL` | `0.001709` | `0.016774` | `false` | **`true`** | **Flipped** (`0.001709 < 0.016774`) |
| `S4` | `aggressive` | `TQQQ` | `0.013362` | `0.103227` | `true` | `true` | Unchanged (`0.013362 < 0.103227`) |
| `S4` | `aggressive` | `SOXL` | `0.006494` | `0.061936` | `true` | `true` | Unchanged (`0.006494 < 0.061936`) |

All other values across `S1`–`S6` — every `kelly_counterfactual_x`, `x_pre`, `x_post_joint`, `w_target`, `q_ideal`, `delta_q_exec`, and `no_trade_band_suppressed` value, as well as all `kelly_would_have_bound = false` flags in `S1`, `S2`, `S3`, `S5`, and `S6` — remain unchanged.

---

## 3. Note on D3 Standing Per-Fund Order Count

The second blocking finding in the same Astra FAIL report (D3 missing per-fund `orders_placed: 0` across all 96 fund rows) does not alter [`demo/DEMO-CASES.md`](DEMO-CASES.md), whose D3 case definition already specifies `orders_placed == 0` across every output row of every demo run.
