# Brief — SH-1: SHADOW-N Minimum Valid Bring-Up (Feeder, Recorder, Ledger, Scheduler)

- **Batch:** SH-1 (Roadmap Phase 4 SHADOW, Stage N — research lane R1, Slot 1)
- **Identity:** First batch of the SHADOW-N standing service. Builds the decision-time
  market-data feeder, the recorder, the append-only hash-chained ledger scaffolding,
  the frozen sizing stack, and the AI-independent scheduler on the approved research
  host, so that the forward evidence clocks (CLK-01 20 sessions, CLK-02 60 sessions,
  CLK-03 ~6 months, CLK-04 250 sessions / 12 months) **can** start the moment SH-2 is
  accepted. SH-1 itself starts no forward clock (see §0.3).
- **Authority:** Roadmap parallel plan `~/workspace/quant-s2/roadmap-parallel/PLAN.md`
  (§1.2 contract registry, §2.2 Slot 1, §3.2 SHADOW-N) as adjudicated in
  `~/workspace/quant-s2/roadmap-parallel/ADJUDICATION.md` and the binding review
  `~/workspace/quant-s2/review-roadmap-parallel/s2-review-roadmap-parallel-REPORT.md`.
  Owner approvals: host decision 2026-11-21 (§0.4, OA-SH1-1); the full OA
  package OA-SH1-2…OA-SH1-7 was APPROVED by the owner on 2026-11-21 (with the
  §10 conditions) — registrations in §8.1.
- **Base:** New repository (see §3.1); no dependency on any QuantRuntimeSettings
  branch, head, or runtime artifact.
- **Shape:** High track (sizing math; immutable evidence publication authority;
  `kelly_applied: false` / `orders_placed: 0` / O1–O2 fail-closed invariants).
  High-track shape applies: a runnable **design demonstration first** (§7, rows
  D1–D8) over a synthetic + Y2-overlap multi-session fixture, independently
  re-verified, then single-writer bulk with a checkpoint commit per slice, then
  one acceptance review round.
- **Writer:** One writer lane for the whole batch (single writer per batch;
  Antigravity Hydrogen XHigh or Sonnet per the fidelity rules — sizing-stack and
  ledger slices are dictated-text fidelity work and default to Sonnet; feeder
  adapters may be Hydrogen XHigh). No second writer in the same worktree or repo
  branch at any time.
- **Review tier:** High — Astra adjudication/review (GPT-6.2 Astra) on the
  demonstration and on the final head, plus the repository bot gate
  (`codex-review-receipt:v1`, zero unresolved findings, `agent-approved` bound to
  the head) on every PR, plus coordinator host battery including hosted CI on the
  exact head before the final review opens.
- **Epoch budget:** Design epoch `SHADOW-N-DESIGN-2026-11-21-v1`. SH-1 is allocated
  the demonstration plus **two** implementation/review rounds. A FAIL with
  `round_progress = WORSENING`, or two consecutive rounds carrying a `NEW_LAYER`
  blocking finding, stops the batch and returns it for a new epoch disposition
  under the standing anti-loop rules.

## 0. Governing facts

### 0.1 Two-lane framing (binding correction)

SHADOW is two lanes, not one. Stage N (this batch) is the **research lane**: it
consumes market data only, runs on a notional bankroll outside the production
runtime, and places no order of any kind. It consumes **zero** D2 outputs and
**zero** SUPPLY-A implementation outputs. It does consume shared contract
*definitions*: the pinned C-SA-22 calendar specification, C-SA-CA market-side
event semantics, and the U-01 sizing math. No account adapter, no broker
connection, no dependency on B5's collector, the historical flow bindings,
C-SB-PROOF, or any SUPPLY-A runtime artifact exists in this batch.

### 0.2 Frozen policy facts (Stage N)

- Notional bankroll **B₀ = USD 50,000** (a record, not real money). **H = 0.**
- Three tiers (conservative / balanced / aggressive) computed side by side in
  every record, all notional.
- Stop-loss as a return rule is **OFF**; loss controls are unarmed in Stage N.
- Kelly is structural/conditional, **recorded, never applied**: every shadow row
  asserts `kelly_applied: false` and `orders_placed: 0` (§5.5).
- The sizing policy is the frozen `policy_v0_pre`: the 10-step sizing stack of
  plan §4.2 at B₀ (M₀(c) starting constants, raw-vs-adjusted separation per 7A,
  monthly win-rate/payoff diagnostic). Its complete effective parameter set —
  including cost assumptions — is pinned by canonical digest before Day 1 of
  any clock (policy identity rule, §5.2).

### 0.3 Clock reconciliation (read carefully)

Per adjudicated contradiction C9 and plan §3.2: CLK-01 Day 1 requires SH-2's
accepted FUSE-1 engine evaluating the full 24-breaker set. A stub run cannot
start or repair a forward clock. Therefore:

- SH-1 builds the feeder, recorder, ledger, sizing stack, scheduler, timestamp
  anchoring, replay tool, and missed-run detector, and proves them on a
  **separate drill chain** and synthetic fixtures.
- SH-1 pins the **clock-start definition** (§6.4) in the frozen policy/config
  digest.
- The forward clock itself starts at **SH-2 acceptance**, on the production
  chain, with the accepted FUSE-1 engine hash pinned. Nothing SH-1 records —
  drills, synthetic runs, Y2-overlap replays — earns forward-clock credit, and
  modelled history and Y2 overlap can never count as forward days.

### 0.4 Host decision (owner, 2026-11-21)

The SHADOW scheduler host **and** the research workspace are the **Mac mini dev
environment**: the locked-down `dev` user (uid 1001) on the owner's Mac mini
(Ubuntu 24.04, 4 CPU / 7.6 GiB RAM / 228G disk, 167G free at inventory), whose
Phase B acceptance passed **8/8** (`~/workspace/mac-mini-pilot/PHASE-B-ACCEPTANCE.md`):

- Fail-closed egress for the dev user: the only outbound paths are loopback and
  the SSH tunnel to qVPS over tailscale0; proxied egress is
  **<egress-ip>, loc=SG**; direct internet, LAN, direct DNS, own-address and
  IPv6 paths are all dropped and were acceptance-tested as dropped.
- Per-process environment pinned by `~/dev-env-mini/env.sh`:
  `TZ=Asia/Singapore`, `LANG/LC_ALL=en_SG.UTF-8`, HTTP proxy
  `http://127.0.0.1:18080` (gost bridge → SOCKS → qVPS tunnel).
- The tunnel and bridge already run under a proven watchdog + `@reboot` crontab
  pattern (`~/dev-env-mini/start.sh`); SH-1 services mirror that discipline (§3.3).
- **Runner-first constraint (owner, binding):** the mini's primary job is the
  three MedicalClaimAssistant GitHub runners. SH-1 services are guests: light,
  capped, and must never contend with runner jobs (§3.4).

## 1. Scope (in)

1. **Decision-time market-data feeder (C-SH-MKT)** for the required series set
   (§4.1): TQQQ and SOXL closes (price basis, proxy-labelled until NAV-verified),
   QQQ close, XNDX gross total-return close, semiconductor total-return close
   (XSOX substitute, disclosed), SOFR and the 3M T-bill accrual rate. Raw bytes
   archived immutably with SHA-256; normalized inputs derived from the archived
   bytes only; every record carries the full C-SH-MKT field set (§5.1).
2. **Shared sessions calendar consumer (C-SA-22)**: consumes the pinned,
   coordinator-published calendar specification + expected-output fixture **by
   digest** (§5.3); passes the full published vector set locally. SH-1 does not
   author the specification and does not wait for SA-3's runtime.
3. **Frozen sizing stack**: the `policy_v0_pre` 10-step stack (§4.2 of the plan)
   implemented as a pure deterministic function over validated inputs, with the
   C-FUSE-EVAL **stub adapter** behind the pinned evaluator interface (§5.2).
   The stub returns synthetic breaker states for drills only and is structurally
   incapable of starting a forward clock (it cannot produce a production-chain
   append; see §6.4).
4. **Append-only ledger (C-SH-LEDGER)**: RFC 8785 canonical record bodies,
   SHA-256 hash chain with genesis of 64 zero hex digits, one append per
   (mode, policy identity, session) containing all tiers, detached external
   timestamp receipt per append (§5.2, anchor in §5.6), duplicate/fork/gap
   rejection, and a **deterministic bit-for-bit replay CLI** that verifies
   record bytes and the original receipt separately.
5. **Recorder + scheduler**: an AI-independent scheduler on the mini (§3.3) that
   runs the daily cycle — fetch → validate → record inputs → append ledger
   record (drill chain in SH-1) → obtain timestamp receipt → write the daily
   completeness report — with a **missed-run detector** and a same-day
   commitment deadline pinned in configuration before any activation.
6. **Daily completeness report**: per session, which required series arrived,
   which are missing/late/revised with reasons, chain head digest, receipt
   status, and the standing assertions (§5.5). Reports are research artifacts
   in the repo workspace, never notifications (no notification channel is in
   scope or authorized).
7. **Design demonstration** (§7) executed and independently re-verified before
   bulk implementation opens.

## 2. Hard out-of-scope

- SH-2 in full: swapping the stub for the accepted FUSE-1 engine, production
  chain activation, CLK-01 start. SH-1 must not preclude SH-2 (pinned evaluator
  interface, engine-hash field present from the first record schema version),
  but must not implement it.
- Any broker connection, account adapter, order path, paper or live trading,
  margin, shorting, or options. Any production runtime, console, digest, KV,
  DO, or GCS write. Any read of production account facts.
- SUPPLY-A implementation of any part; SA-3's calendar runtime; authoring the
  C-SA-22 specification (the coordinator publishes it; §5.3).
- Account creation, API-key requests, signups, or purchases of any kind. A
  source that needs a free key is an OA item (§10); adapters are built and
  demonstrated against recorded response fixtures so no key is needed to
  complete SH-1's demonstration.
- Notifications to the owner or anyone else; changes to the mini outside the
  dev user's own home and user-level scheduling (§3.3); any change to the MCA
  runners, their users, or their workspaces; any change to the devlock
  firewall, the tunnel, or the bridge.
- Backfill of any kind: no synthetic backfill, no historical reconstruction
  presented as decision-time data, no repair of a missed day after the fact.

## 3. Batch manifest

### 3.1 Code location — decision

**A new private repository, `QuantStrategyLab/ShadowResearch`, created under
OA-SH1-4.** Reasoning: (a) SHADOW-N is a separate research lane with its own
evidence chain and a standing service lifetime measured in years; mixing it
into QuantRuntimeSettings would couple a research ledger to the production
configuration governance gate and invite exactly the cross-lane dependency the
adjudication removed; (b) the alternative — a directory inside
QuantRuntimeSettings — was rejected for the same reason the plan separates
Lane R1 from Lane E1 (disjoint writer surfaces are a precondition of the
parallel graph); (c) a directory on the mini without a repo was rejected
because the ledger's replay guarantee requires versioned, content-addressed
code and manifests, and the repo is also the transport that gets code onto the
mini (clone/pull over the SG tunnel) without touching this VM's clones.
The mini holds a checkout under the dev user's home; this VM holds a
coordinator clone under `~/workspace/quant-s2/shadow-n/` for review only.
Ledger data (records, receipts, raw market bytes) lives in the mini checkout's
data directory and is committed to a dedicated append-only data branch per
§5.6; code lives on `main` under normal PR governance.

### 3.2 Work items (single writer, checkpoint commit per slice)

| ID | Slice | Contents |
| --- | --- | --- |
| SH1-W1 | Repo + skeleton | Repo bootstrap, layout, CI (hosted, public-repo rules do not apply — private repo CI minutes are the owner's existing arrangement; keep CI light), governance enrolment check (registration R-2), `env.sh`-pattern runtime config, licence/attribution file for data sources. |
| SH1-W2 | Calendar consumer | C-SA-22 spec package vendored by digest; session resolution; full published vector suite green locally. |
| SH1-W3 | Feeder adapters | One adapter per approved source (§4); raw-byte archive + SHA-256; normalization; C-SH-MKT record emission; revision-append behaviour; recorded-fixture test mode (no network, no key). |
| SH1-W4 | Sizing stack | `policy_v0_pre` 10-step stack as a pure function; evaluator interface with stub adapter; per-row assertions (§5.5); unit fixtures incl. split and half-day invariance. |
| SH1-W5 | Ledger + replay | Canonicalization, decimal form, hash chain, genesis, append rules, fork/gap/duplicate rejection, detached receipt handling, replay CLI with byte + receipt verification. |
| SH1-W6 | Scheduler + recorder | User-level scheduler units (§3.3), daily cycle, completeness report, missed-run detector, drill-chain operation, resource caps (§3.4). |
| SH1-W7 | Demonstration | The §7 demonstration suite, frozen case list first, failing baseline recorded, contraction to zero on the gate set. |
| SH1-W8 | Acceptance pack | Full test suite, hosted CI on the exact head, acceptance rows §6 executed with evidence archived in the repo. |

### 3.3 Runtime layout on the mini (dev user)

- Home layout: `~/shadow-research/` (git checkout), `~/shadow-research/data/`
  (ledger chains, raw bytes, receipts, reports — append-only by code discipline
  and file permissions where the OS supports it), `~/dev-env-mini/` unchanged.
- Every process runs with the `~/dev-env-mini/env.sh` environment sourced
  (TZ, locale, proxy). No process may rely on the mini's system timezone (CST)
  or direct network.
- Scheduling: **systemd user units + timers** for the dev user (linger state
  verified as a registration item, R-4; if linger is unavailable, the proven
  watchdog + `@reboot` crontab pattern from `start.sh` is the fallback and the
  substitution is recorded). The scheduler must not depend on any AI session,
  SSH session, tmux, or this VM being alive — the independence drill (§6,
  SH1-A11) is the proof.
- Restart discipline mirrors the tunnel services: a watchdog loop that restarts
  a dead service process, `@reboot`/unit restart on boot, and a missed-run
  detector that treats a silent scheduler as a failed day, never as a day to
  be repaired later.
- Activation of any timer on the mini is **OA-SH1-6**; SH-1 implementation and
  drills run the cycle manually or on the drill chain only.

### 3.4 Runner-first resource discipline

- The daily cycle is a batch job measured in seconds-to-minutes of light CPU
  (a handful of HTTPS fetches, hashing, JSON canonicalization). It runs under
  explicit caps: `nice` level and, where systemd user slices are available,
  `CPUQuota`/`MemoryMax` ceilings recorded in the unit files; peak RSS and
  wall time are measured in the acceptance run (SH1-A13).
- The scheduled instant is chosen to avoid the MCA runners' known peaks where
  observable, and the job never retries in a tight loop against a data source
  (bounded retries with backoff inside the commitment deadline only).
- Disk: raw market bytes are small (daily JSON/CSV per series). Retention is
  append-forever for ledger records and receipts (they are the evidence);
  feeder raw bytes retained for the life of the chain; logs rotated at 30 days
  with a size cap; total SH-1 footprint budget **≤ 2 GiB** on the mini's disk,
  checked in the acceptance pack. Nothing is written outside the dev home.

## 4. Data-source proposal (free sources first)

### 4.1 Required series and what "decision-time" demands

The decision cycle runs **after** the XNYS session close, against the pinned
calendar's registered cutoff (default: session close for regular-session
eligibility). SHADOW-N needs end-of-day values demonstrably available by that
cutoff — not intraday data. Required series: TQQQ close, SOXL close, QQQ close,
XNDX gross total-return close, semiconductor total-return close (XSOX
substitute for ICESEMIT, disclosed), SOFR (accrual leg), 3M T-bill rate
(accrual leg). Corporate-action facts (splits, distributions) for TQQQ/SOXL/QQQ
on their ex-dates, on the C-SA-CA market-side semantics (§5.4).

### 4.2 Per-source assessment

**Tiingo — free Starter tier (RECOMMENDED PRIMARY for fund prices + actions).**
Provides: daily OHLCV raw **and** CRSP-method adjusted prices for US ETFs, with
`divCash` and `splitFactor` carried on every daily bar — corporate actions
arrive on the price rows at no extra request, which is exactly the C-SA-CA
capture shape. Limits (published): 50 requests/hour, 1,000/day, 500 unique
symbols/month, 1 GB/month bandwidth; the SH-1 universe is ~6 series and one
daily batch is ~6–10 requests. Licence: self-serve plans are internal-use
only, no display/redistribution — compatible with a private research ledger
that publishes nothing. Reliability: operating since 2014, IEX-sourced
cleansing, public status page, dated changelog. Constraint: **requires a free
API key (signup) → OA-SH1-2.** No index total-return series (no XNDX/XSOX) —
see the index note below.

**Stooq (RECOMMENDED FALLBACK for fund prices; cross-check).** Provides: daily
OHLCV CSV per symbol (`tqqq.us`, `soxl.us`, `qqq.us`), no adjusted prices, no
corporate actions, no indices TR. Historically keyless with no published
limits; recent third-party reports (unverified) say the per-symbol endpoint
began requiring a free key obtained through a CAPTCHA in early 2026, and the
bulk database download is CAPTCHA-gated manual only (not automatable, out of
scope). Licence/terms for programmatic use are not formally published — a
standing caveat. Reliability: long-running, widely used, but no SLA and no
status page. Role: outage cover and an independent second print for the daily
close cross-check; if the pre-implementation probe (§6, SH1-A02) finds the
endpoint key-gated, the key becomes contingent OA-SH1-7 and Yahoo (below)
covers the interim.

**Yahoo Finance chart API (index series source; interim price fallback).**
Provides: daily closes for ETFs **and** the index symbols (^NDX price, XNDX
total return, ^SOX price, XSOX total return where listed), plus dividend and
split events in the chart payload. This is the **only free programmatic source
identified for the XNDX and semiconductor total-return closes**; the
programme already relies on Yahoo downloads in a production benchmark sync,
which is operating evidence, not a licence. Constraints, stated plainly:
unofficial API, Yahoo's terms do not grant programmatic use, no published rate
limits (HTTP 429 throttling is observed in practice), symbol availability for
the TR indices must be proven by the SH1-A02 probe. If a TR index symbol
proves unavailable, the fallback is a disclosed reconstruction (price index +
distribution model) labelled per the U-02 discipline — never a silent
substitution. Role in the recommendation: index TR closes in both stacks,
interim fund-price fallback, never the sole source for fund closes once the
primary is keyed.

**New York Fed Markets Data API — SOFR (PRIMARY for the rate leg).** Keyless,
no registration, official. SOFR is published ~08:00 ET on the business day
after its effective date; the Fed may revise a published rate the same day
(~14:30 ET) if the change exceeds 1 bp, flagged in the payload. Point-in-time
consequence, recorded as design: at the decision cutoff for session T the
latest published SOFR has effective date T−1 (or the prior business day); the
feeder records value, effective date, publication time and retrieval time, the
accrual leg uses the last-published rate, and a later same-day revision
appends as a revision record and never rewrites a decision input (§5.1).
Terms: New York Fed Terms of Use (attribution/republication notice) —
compatible with private research use.

**U.S. Treasury Daily Par Yield Curve Rates (PRIMARY for the T-bill leg).**
Keyless official CSV/XML from the Treasury resource center; the 3-month par
yield is the T-bill accrual input. FRED's `fredgraph` CSV for DTB3 (keyless)
is the cross-check print. No signup for either in CSV form; the FRED *API*
key is **not** requested (CSV suffices).

**Issuer NAV publications (validation only — not a feeder).** ProShares
(TQQQ) and Direxion (SOXL) publish NAV on product pages but offer no stable
free machine-readable NAV feed. NAV verification is therefore a best-effort
secondary check (issuer page or the Nasdaq website's unofficial API), and
until a verified NAV record exists for a session, TQQQ/SOXL closes carry the
C-SH-MKT label `price_basis: close (proxy, NAV unverified)`. The forward clock
does not depend on NAV verification succeeding; it depends on the label being
honest. SOXL's close remains a proxy on the same rule, and the post-2021-08-25
XSOX substitution for ICESEMIT is disclosed on every semiconductor record.

**Alpha Vantage free tier (considered, not recommended).** 25 requests/day is
technically sufficient for ~6 series, but adjusted series are paywalled and
the free tier's reliability headroom is the thinnest of the candidates; kept
as a named emergency substitute only.

### 4.3 Recommendation (for one-tap approval, OA-SH1-3)

- **Primary stack:** Tiingo free Starter (fund closes + corporate actions) +
  Yahoo chart API (XNDX / XSOX total-return closes) + NY Fed SOFR API +
  U.S. Treasury par yield curve (3M). One free key total (Tiingo, OA-SH1-2).
- **Fallback stack:** Stooq per-symbol CSV (fund closes, cross-check + outage
  cover) + Yahoo chart API (indices; interim fund closes) + FRED DTB3 CSV
  (rate cross-check). Keyless today, subject to the SH1-A02 probe.

## 5. Binding contracts carried in

The texts below carry the adjudicated contract amendments **verbatim in
substance**; where the plan or adjudication is more specific than this brief,
the plan/adjudication governs.

### 5.0 Common evidence rule (applies to every contract below)

Every result is bound to source class, account and environment where
applicable, effective observation time, retrieval and evaluation cutoffs,
native precision, coverage window, and an immutable evidence reference; only
safe identifiers are published. Observation classes are distinguished:
synthetic / historical reconstruction / decision-time market /
operator-declared / authorized operational / paper-broker / authorized
live-broker. Missing or unsupported values remain **null with a reason**; raw
precision and mixed-currency unavailability are preserved; a USD amount
requires a qualified native USD basis. **No inferred FX, no empty-as-zero, no
synthetic backfill, and no secrets** in fields, values, evidence references,
logs, or test artifacts. Production account claims are earned only by
authorized, source-qualified contemporaneous observations with the required
completeness, reconciliation and window — a digest, synthetic trip, anchored
shadow decision or later snapshot cannot earn that credit.

### 5.1 C-SH-MKT (CONFIRM WITH AMENDMENT) — decision-time market inputs

For every required series and action, the feeder records instrument, provider,
series ID, price/NAV/TR/rate basis, units, effective session, source
publication time if known, actual retrieval time, decision cutoff, vintage,
and an immutable raw-byte reference with SHA-256. Raw bytes **and** normalized
inputs are both archived. Eligibility requires demonstrable availability by
the cutoff: retrieval by that time, correct prior-close session, calendar
version, and per-field validation. Late, missing, or revised values are
recorded as such; **revisions append with a new digest and never replace a
decision input**. SOXL close stays labelled a proxy until the fund's NAV is
verified; the post-2021-08-25 XSOX substitution is disclosed; Y2
reconstruction/modelled labels and validation limits are preserved. The rate
series and the days/365 accrual basis are specified in the record. Modelled
history and Y2 overlap cannot count as forward days. Corporate-action content
follows C-SA-CA's ex-date/ratio basis for announcements and distributions,
with no broker-reconciliation claim. Missing required inputs produce a reason
plus the O1 fail-closed output; a timely recorded failure can document an
outage but cannot certify fresh data. **No broker connection.**

### 5.2 C-SH-LEDGER (CONFIRM WITH AMENDMENT) — append-only shadow ledger

`record_body` is canonicalized as UTF-8 **RFC 8785** JSON — duplicate keys,
non-finite numbers, and missing required fields are rejected. Financial
decimals are canonical ordinary base-ten strings (zero is `"0"`; no extra
leading or fractional trailing zeros). Times are UTC with fixed millisecond
precision. Tier, instrument, and row arrays are ordered by stable ID. SHA-256
is computed over those bytes, lowercase hex. Chain ID, sequence, and previous
hash are **in** the body; the genesis previous hash is **64 zero hex digits**.
The record's own hash and any external receipt are **excluded from the
preimage**; the external anchor is a detached timestamp receipt (§5.6). There
is **one append per (mode, policy identity, session) containing all tiers**.
Stage N, Stage A, and materially changed policies use **distinct chains**.
Immutable content-addressed inputs, parameters, code, calendar, prior state,
and engine are retained sufficiently for exact replay. A scheduled decision
instant and a same-day commitment deadline are pinned **before Day 1**.
Duplicates, forks, gaps, wrong authority/hash, and late or missing receipts
are rejected **for clock credit**; replay verifies record bytes and the
original receipt separately; **late recovery cannot repair a missed day**.
The evaluator interface is pinned as:
`evaluate(spec_version, engine_hash, policy_hash, session, as_of, mode, validated_inputs, prior_row_state, authorized_events) → (rows_by_ID, effective_state_by_scope, next_row_state)`.
Latches and prior state persist per tier/fund plus the aggregate precedence
result. The breaker set is **exactly the signed named ID set — L0–L6, V1–V7,
O1–O9, J1 (24 IDs), each once** — with per-row kinds, limits, and diagnostics
preserved. SH-1 stubs are synthetic only; SH-2 binds the verified engine hash
and the full expected-state matrix. Every effective policy parameter is
pinned, including cost assumptions; unavailable broker inputs are explicitly
unverified. Policy identity is the canonical digest of the complete effective
policy/config plus executable manifests: identical manifests earn an
append-only alias attestation and the clock continues; any effective change
starts a new version and a new series (C7 continuity rule).

### 5.3 C-SA-22 (CONFIRM WITH AMENDMENT) + amended C5 — sessions calendar

Before either implementation consumes it, **one** immutable calendar
specification + expected-output fixture is published with version, content
digest, authoritative dated XNYS rule/exception inputs, effective coverage,
and pinned timezone rules, by **one named publisher** for that version. The
publisher for the first version is the **coordinator** (published as a
versioned spec package in the ShadowResearch repo's `specs/` area and mirrored
to the SUPPLY-A lane by digest); both lanes consume **read-only copies by
digest**. SH-1 owns C-SH-MKT on the approved research surface; SH-1 and SA-3
each pass the same vectors **without waiting for the other's runtime** — a
fixture produced late by SA-3 would recreate the dependency this split
removes. Vectors include full-year regular sessions, weekends, holidays,
named unscheduled closures, early closes in standard and daylight time, both
DST transitions, unsupported venues, and out-of-range dates; every returned
field is compared to independent expected values. Session facts: 09:30 local
open; scheduled 16:00 or 13:00 local close; registered cutoff (default =
session close for regular-session eligibility); order window [open, cutoff)
capped by close. Closed and unscheduled-closure days project as closed with a
named reason and null open/close/cutoff. Unknown coverage or disagreement
with observed venue status blocks orders (in Stage N: blocks the record's
eligibility). A new exception creates a new version; old versions are
retained for replay. Calendar and event revisions are separately versioned
and never change a past decision.

### 5.4 C-SA-CA (as consumed) — corporate-action semantics

SH-1 consumes C-SA-CA's market-side definitions only: announcement and
distribution facts keyed by **ex-date**, with ratio semantics for splits as
defined in the shared contract. SH-1 records these as market observations
under C-SH-MKT (§5.1) and makes **no broker-reconciliation claim**. Account-
ledger adjustments and reconciliation belong to SA-1 alone and are out of
scope here.

### 5.5 Standing assertions (every shadow row, no exceptions)

- `kelly_applied: false`
- `orders_placed: 0`
- Observation class labelled per §5.0; Stage N rows are decision-time market
  or synthetic (drills) — never presented as anything else.
- B₀ = USD 50,000 notional; H = 0; stop-loss return rule OFF; loss controls
  unarmed.

### 5.6 External timestamp anchor (concrete proposal)

**OpenTimestamps** (Bitcoin-anchored, free, no account, no key): after each
append, the scheduler submits the chain's terminal digest to OpenTimestamps
calendar servers and stores the detached `.ots` receipt beside the record.
The receipt is the independently timed, append-only/signed timestamp evidence
the contract requires: it carries the calendar attestation and, once upgraded,
the Bitcoin block header proof, verifiable offline against the receipt +
digest alone. Verification material (receipt bytes, calendar identities,
upgrade state) is stored with the chain data and committed to the repo's data
branch — the repo commit is transport and storage, **not** a second anchor.
A receipt not obtained within the same-day commitment deadline is a late
receipt: the day is documented as failed and earns no clock credit (§5.2);
there is no next-day repair. Anchor-service approval is OA-SH1-5 (the service
is free and keyless; approval is recorded because the contract requires the
anchor to be a named, owner-visible choice).

## 6. Acceptance property — SH1-ACCEPT-v1 (finite rows)

All rows are machine-checkable on the named evidence surface. "Drill chain"
means the non-production chain used for SH-1 proofs; no drill output earns
clock credit (§0.3).

### 6.1 Feeder rows

| Row | Property | Evidence surface |
| --- | --- | --- |
| SH1-A01 | For a recorded multi-session fixture window, every required series (§4.1) yields exactly one C-SH-MKT record per session, or one explicit missing-with-reason record; no silent gaps. | Feeder run log + record store dump |
| SH1-A02 | Pre-implementation source probe: from the mini dev environment, each approved source's exact endpoints are fetched once; availability, key-gating (Stooq), TR index symbol availability (Yahoo), and EOD finalization timing are recorded with timestamps. A source failing the probe is replaced per §4.3 fallback before W3 opens. | Probe report in repo `docs/` |
| SH1-A03 | Freshness: every accepted record's retrieval time ≤ decision cutoff, effective session = the calendar's prior-close session for the decision instant, calendar version digest matches the pinned spec package. | Record fields vs calendar fixture |
| SH1-A04 | Raw-byte integrity: for every record, SHA-256 of the archived raw bytes equals the recorded digest, and the normalized inputs re-derive from the archived bytes byte-for-byte. | Verification script output |
| SH1-A05 | Revision behaviour: a fixture with a revised value (incl. a SOFR same-day revision) appends a new record with a new digest; the original decision input is unchanged. | Fixture run diff |
| SH1-A06 | Missing-input behaviour: with any required series withheld, the cycle emits the reason record plus the O1 fail-closed output and appends **no** decision record for that session. | Fixture run log |
| SH1-A07 | Basis honesty: TQQQ/SOXL records carry the proxy label until a verified NAV record exists; semiconductor records disclose the XSOX substitution; rate records state the days/365 basis and the SOFR effective-date lag. | Record inspection |

### 6.2 Ledger rows

| Row | Property | Evidence surface |
| --- | --- | --- |
| SH1-A08 | Canonicalization: the published RFC 8785 + decimal-form vector set passes, including rejection of duplicate keys, non-finite numbers, and non-canonical decimals. | Vector suite output |
| SH1-A09 | Chain integrity: genesis previous hash is 64 zero hex digits; sequence is gapless; every link's previous hash equals the prior record's hash; a one-byte mutation anywhere makes verification fail and identifies the first bad sequence. | Verify + tamper fixture |
| SH1-A10 | Append rules: a second append for the same (mode, policy identity, session) is rejected; a forked previous hash is rejected; one append contains all three tiers exactly once. | Negative fixture runs |
| SH1-A11 | Receipt discipline: every drill-chain append has a detached OpenTimestamps receipt stored beside it; replay verifies bytes and receipt separately; a record with a missing/late receipt is flagged non-creditable. | Receipt store + replay log |

### 6.3 Scheduler and replay rows

| Row | Property | Evidence surface |
| --- | --- | --- |
| SH1-A12 | **Scheduler independence drill:** with no AI session, no SSH session, and no process owned by any agent tooling alive on the mini, the scheduler fires the daily cycle on the drill chain at the pinned instant and completes it (fetch → append → receipt → completeness report). | Drill log with process listing captured at fire time |
| SH1-A13 | Runner-first: during the drill, peak RSS and wall time of the cycle stay within the caps recorded in the unit files, and the three MCA runners remain online throughout. | Resource log + runner status check |
| SH1-A14 | Missed-run detection: with one drill firing suppressed, the detector flags the gap within its window and the day is recorded as failed; no backfilled record appears. | Detector log + chain dump |
| SH1-A15 | Replay: the replay CLI reproduces every drill-chain and fixture record bit-for-bit from the retained inputs, parameters, code, calendar, and prior state; any mismatch fails the row. | Replay report |
| SH1-A16 | Standing assertions: every row in every produced record carries `kelly_applied: false` and `orders_placed: 0`; B₀, H, and the OFF states match §5.5. | Record scan |

### 6.4 Clock-start definition (pinned by SH-1, executed by SH-2)

Session 1 of CLK-01 is the **first XNYS session strictly after SH-2
acceptance** on which the production chain — accepted FUSE-1 engine hash
pinned, `policy_v0_pre` digest pinned, C-SA-22 spec digest pinned, scheduler
and anchor live on the approved host — completes a full cycle within the
pinned decision instant and same-day commitment deadline, with a valid
detached receipt. Sessions count consecutively from there; any gap, backfill,
missed-run non-detection, replay mismatch, or unversioned parameter edit
resets CLK-01 per the plan's reset rules. SH-1's deliverable is this
definition frozen in the policy/config digest plus drill evidence that the
machinery satisfies it; SH-1 starts no count.

## 7. Design demonstration (opens the batch; gate for bulk)

Frozen case list written **before** any implementation, failing baseline
recorded on the skeleton head, contraction to zero failures required on the
gate set with positive controls held, independently re-verified by Astra
re-running the demonstration scripts on the pinned demonstration head:

- D1 Exact 3-tier sizing outputs for a synthetic multi-session fixture
  (expected values computed independently of the implementation).
- D2 Split invariance and half-day invariance of the sizing stack.
- D3 `kelly_applied: false` / `orders_placed: 0` on every output row.
- D4 Bit-for-bit replay of the fixture window.
- D5 Hash-chain tamper rejection (mutation, fork, gap, duplicate).
- D6 Y2-overlap segment replay labelled reconstruction/modelled, asserted
  non-creditable for any forward clock.
- D7 O1 fail-closed output on a withheld required input.
- D8 Calendar vector set (full year, holidays, early closes, both DST
  transitions, unscheduled closure) against the pinned spec fixture.

## 8. Registrations checklist

- R-1 Repo `QuantStrategyLab/ShadowResearch` created (OA-SH1-4) with branch
  protection mirroring the org's standard; coordinator clone registered.
- R-2 Bot enrolment: the Codex review connector app installed on the new repo;
  if enrolment needs an owner action it is folded into OA-SH1-4 — the bot
  gate is **not** waived for this repo.
- R-3 C-SA-22 spec package v1 published by the coordinator with version +
  digest recorded here and in the plan's registry before W2/W3 consume it.
- R-4 Mini runtime facts verified: systemd user linger state (or the crontab
  fallback recorded), dev-user disk headroom, runner status before activation.
- R-5 `policy_v0_pre` parameter freeze: canonical digest of the complete
  effective parameter set (incl. cost assumptions) recorded; any later change
  is a new policy version and a new chain (§5.2).
- R-6 Data-source probe report (SH1-A02) filed; fallback substitutions, if
  any, recorded against §4.3.
- R-7 Anchor configuration: OpenTimestamps calendar endpoints + commitment
  deadline pinned in config before activation (OA-SH1-5).

### 8.1 Owner approval registrations (2026-11-21)

The owner approved the full OA package on 2026-11-21 (the same approval
covers the FUSE-1 parallel start, Lane R2 — a separate dispatch). Conditions
are recorded verbatim in substance from §10:

- **OA-SH1-2 — APPROVED.** Tiingo free API key. Condition: the owner signs
  up for the free Starter tier herself and provides the key through the
  secure channel when W3 is ready for live probing — **not now**; no paid
  tier. The §7 demonstration must not need the key.
- **OA-SH1-3 — APPROVED.** The §4.3 data-source pairing (primary: Tiingo +
  Yahoo indices + NY Fed SOFR + U.S. Treasury curve; fallback: Stooq +
  Yahoo + FRED DTB3 CSV), with Yahoo's unofficial-API status and Stooq's
  unpublished terms accepted as recorded caveats for private research use,
  and the disclosed-reconstruction fallback if a TR index symbol is
  unavailable.
- **OA-SH1-4 — APPROVED.** Create private repo
  `QuantStrategyLab/ShadowResearch` (incl. bot app enrolment if an owner
  click is required).
- **OA-SH1-5 — APPROVED.** OpenTimestamps as the named external anchor
  service per §5.6.
- **OA-SH1-6 — APPROVED, conditional.** Scheduler activation **only after
  SH-1 acceptance**, and on the **drill chain** only; production chain
  activation and CLK-01 start remain SH-2's, separately approved.
- **OA-SH1-7 — APPROVED, contingent.** Only if the SH1-A02 probe finds the
  Stooq endpoint key-gated: the owner obtains the free key via its CAPTCHA
  flow (it cannot be automated) or directs the fallback to Yahoo-only cover.

## 9. Executor discipline

- Single writer; checkpoint commit after each slice (SH1-W1…W8) passes its
  local checks; conventional commit subjects ≤ 72 chars; no row of trailing
  whitespace (`git diff --check` clean).
- All model-calling tools run through the guarded Singapore environment
  (`dev-run.sh` discipline on this VM; on the mini, the dev user's `env.sh`).
  Ordinary git/test/build work follows the standing egress scope rules.
- Scratch discipline: one reused scratch tree, deleted after use; evidence
  copied into the repo or the coordinator workspace immediately — never left
  only in `/tmp`.
- Every new test suite declares and installs its test-environment
  dependencies; hosted CI on the exact candidate head is part of the
  coordinator battery and batch-owned red blocks the final review.
- The writer stops and reports — does not improvise — on: any source whose
  observed behaviour contradicts §4.2 (key-gating, symbol absence, licence
  change); any need to touch the mini outside the dev home; any pressure to
  backfill, substitute silently, or relax a §5 assertion.

## 10. Owner approvals (OA list — one tap covers the batch)

- **OA-SH1-1 — Host and workspace: DECIDED 2026-11-21.** Mac mini dev
  environment (scheduler host + research workspace). Recorded for
  completeness; no further action.
- **OA-SH1-2 — Tiingo free API key.** Owner signs up for the free Starter
  tier and provides the key through the secure channel when W3 is ready for
  live probing. No paid tier.
- **OA-SH1-3 — Data-source pairing.** Approve the §4.3 recommendation:
  primary = Tiingo + Yahoo (indices) + NY Fed SOFR + U.S. Treasury curve;
  fallback = Stooq + Yahoo + FRED DTB3 CSV; Yahoo's unofficial-API status and
  Stooq's unpublished terms accepted as recorded caveats for private research
  use, with the disclosed-reconstruction fallback if a TR index symbol is
  unavailable.
- **OA-SH1-4 — New repo.** Create private repo `QuantStrategyLab/ShadowResearch`
  (incl. bot app enrolment if an owner click is required).
- **OA-SH1-5 — Timestamp anchor.** OpenTimestamps as the named external
  anchor service per §5.6.
- **OA-SH1-6 — Scheduler activation.** After SH-1 acceptance: enable the
  user-level scheduler on the mini on the **drill chain** (production chain
  activation and CLK-01 start remain SH-2's, separately approved).
- **OA-SH1-7 — Contingent: Stooq key.** Only if the SH1-A02 probe finds the
  Stooq endpoint key-gated: owner obtains the free key via its CAPTCHA flow
  (it cannot be automated) or directs the fallback to Yahoo-only cover.

## 11. Risks and unknowns (stated plainly)

- **EOD finalization timing vs the decision instant.** The decision needs
  closes demonstrably available by the pinned cutoff. Free EOD sources
  finalize at varying times after 16:00 ET; if the primary's finalization
  regularly lands after the chosen decision instant, the instant must move
  later in the evening (still same-day) — the SH1-A02 probe measures this
  before the instant is pinned. A day whose data arrives after the
  commitment deadline is a failed day, not a backfilled one.
- **Yahoo is unofficial.** Its terms do not grant programmatic use and its
  throttling is undocumented; it is nevertheless the only free source for
  the total-return index closes. The exposure is availability, not
  correctness of the ledger: an outage day fails closed and is documented.
- **Stooq key uncertainty.** The reported early-2026 move to a
  CAPTCHA-obtained key is unverified third-party reporting; the probe
  settles it. Worst case is OA-SH1-7 or Yahoo-only fallback cover.
- **SOFR publication lag is structural.** The accrual leg always uses the
  last-published rate (effective T−1 at decision time). This is the correct
  point-in-time behaviour and is recorded on every rate record; it is not a
  defect to be "fixed" by reaching for a later print.
- **NAV verification may never fully land.** No stable free machine-readable
  issuer NAV feed exists; TQQQ/SOXL may carry the proxy label indefinitely.
  The sizing policy runs on closes by design; the label keeps the basis
  honest. This does not block the clock.
- **The mini is a home machine.** Power or home-network loss on a session
  day means a missed day that, by the clock rules, cannot be repaired. The
  missed-run detector + timely failure record document the outage; they do
  not earn credit. This is the accepted cost of the owner-chosen host.
- **Runner contention is bounded, not zero.** The cycle is light and capped
  (§3.4), but the runners' peak times are not fully predictable; SH1-A13
  measures the drill under observation and the caps are set from that
  measurement, not assumed.
- **Private-repo CI.** Hosted CI on a private repo follows the owner's
  existing GitHub arrangement (the MCA repos already run on the mini's
  self-hosted runners for the same reason); if ShadowResearch CI needs the
  mini's runners, that registration is part of R-1/R-4 and adds no new
  machine load beyond light test runs.

## 12. Gate and completion

SH-1 is complete when, on one final head: the §7 demonstration gate has
passed with Astra's independent re-verification; acceptance rows
SH1-A01…A16 all pass with evidence archived; the full test suite and hosted
CI are green on that head; the bot receipt shows zero unresolved findings
with `agent-approved` bound to that head; Astra's final review returns PASS
or premise-preserving PASS WITH RESIDUALS; and the OA items needed for the
next step (OA-SH1-6 drill-chain activation) are recorded as granted or
explicitly deferred. SH-2's brief is a separate document, opened only after
FUSE-1 acceptance, and owns production-chain activation and CLK-01 Day 1.
