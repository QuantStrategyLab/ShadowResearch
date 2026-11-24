# AGENTS.md

Rules for AI assistants and maintainers in `QuantStrategyLab/ShadowResearch`. Reusable PR governance actions live in `Pigbibi/PrGovernance`.

## What this repository is

Standalone research lane for the SHADOW programme (`SHADOW-N`): decision-time market-data feeder, frozen `policy_v0_pre` sizing stack, append-only SHA-256 shadow ledger, and replay tool.

## Layout

- `docs/` — batch briefs and probe reports.
- `specs/` — pinned specification packages consumed by digest.
- `src/` — feeder, sizing stack, ledger, replay, scheduler.
- `tests/` — unit and fixture suites.

## Local checks

```sh
PYTHONPATH=src python3 -m pytest -q tests
```

## Naming and documentation

- English is default; new document filenames use lowercase kebab-case (`README.md`, `AGENTS.md`, `CLAUDE.md` keep standard casing).
- Update `README.md` only when user-visible behavior, CLI, or configuration changes; never append PR narration or sections after `## License`.

<!-- repository-files:begin -->
- Public repositories (with `LICENSE`) include `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`, and issue/PR templates (`pull_request_template.md`).
- Public `SECURITY.md` covers disclosure only; never mix private rotation procedures into public files.
<!-- repository-files:end -->

## Do not

- Do not add broker connections, account adapters, or live order paths (`kelly_applied: false`, `orders_placed: 0`).
- Do not commit ledger records, receipts, or raw market data to `main`.
- Do not commit `docs/plans/`, `docs/audits/`, `docs/changes/`, or `docs/acceptance-policy.md`.

<!-- content-boundary:begin -->
- Public material carries no real resource names or accounts: use synthetic placeholders (`<bucket>`, `<account-id>`).
- Judge whether a change is clean by `git diff` and `git ls-files`; `.gitignore` affects untracked files only.
<!-- content-boundary:end -->

## Commits and pull requests

- Use English Conventional Commits (`feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `perf`, `ci`, `build`) with headers ≤ 72 characters.
- Fill in PR template sections (`AI assistance`, `What changed`, `How verified`, `Risk`). R2 paths require green CI, `ai-review` on the exact head SHA, and one clearing label (`agent-approved` or `owner-approved`). Never push to `main` or use `--admin`.
