# ShadowResearch — SHADOW-N research lane

Standalone research lane for the SHADOW programme (Roadmap Phase 4, Stage N).
This repository holds the code for the notional shadow decision service:
a decision-time market-data feeder, the frozen `policy_v0_pre` sizing stack,
an append-only SHA-256 hash-chained ledger with detached external timestamp
receipts, and a deterministic replay tool.

Stage N is **notional only**. It consumes market data only — no broker
connection, no account adapter, no order path of any kind. Every record
asserts `kelly_applied: false` and `orders_placed: 0`. Notional bankroll
B0 = USD 50,000 (a record, not real money); H = 0.

## Authority

- Batch brief: [`docs/BRIEF-SH-1.md`](docs/BRIEF-SH-1.md) (SH-1: minimum
  valid bring-up — feeder, recorder, ledger scaffolding, scheduler).
- Roadmap parallel plan + adjudication (coordinator workspace):
  `~/workspace/quant-s2/roadmap-parallel/PLAN.md` and `ADJUDICATION.md`.
- Owner approvals: host decision 2026-11-21 (Mac mini dev environment);
  OA-SH1-2…OA-SH1-7 approved 2026-11-21 (registered in brief §8.1).

## Layout

- `docs/` — batch briefs and probe reports.
- `specs/` — pinned specification packages consumed by digest
  (C-SA-22 calendar spec package is published here by the coordinator).
- `src/` — feeder, sizing stack, ledger, replay, scheduler.
- `tests/` — unit and fixture suites.
- Ledger data (records, receipts, raw market bytes) is committed to a
  dedicated append-only data branch, never to `main`.

## Operating constraints

- Single writer per batch; checkpoint commit per slice; the repository bot
  gate (`codex-review-receipt:v1`, zero unresolved findings,
  `agent-approved` bound to the head) applies to every PR.
- No backfill: a missed day is a failed day, recorded as such.
- The runtime host is a guest on a machine whose primary job is another
  project's CI runners; all services are light and capped.
