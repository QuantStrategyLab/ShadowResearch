# Security Policy

`ShadowResearch` is a private research repository for the SHADOW-N notional
decision service.

## Reporting a Vulnerability

- Report security issues privately to the repository owner via GitHub's
  private vulnerability reporting or a private issue marked `security`.
- Do not open public disclosures or post findings in external channels; this
  repository is private.
- Include the affected commit or branch, reproduction steps, and minimal
  redacted logs without secrets or identifiers.

## Credentials and Data Discipline

- Never commit API keys, credentials, tokens, or account identifiers to any
  branch, pull request, test fixture, log, or artifact.
- The append-only ledger data branch and archived market-data evidence follow
  the same rule: no secrets, credentials, or account identifiers may appear
  in raw bytes, records, receipts, or reports.
- If a credential is exposed, rotate it at the provider immediately and
  coordinate cleanup with the repository owner.
