# SH1-A02 Data-Source Probe Record

- **Acceptance Row:** SH1-A02 (`docs/BRIEF-SH-1.md` §6.1, Registration R-6)
- **Probe Date (UTC / ET):** Saturday `2026-11-21` (`05:55:05Z`–`06:06:26Z` / `00:55`–`01:06` EST)
- **Latest Completed XNYS Session:** Friday `2026-11-20`
- **Probe Environment:** `probe_host=mac-mini`, `user=dev`, egress `ip=43.156.238.238`, `loc=SG`
- **Evidence Source:** `/home/hatch/workspace/quant-s2/shadow-n/a02-evidence/` (`probe-log.txt`, `probe-log-2.txt`, saved `.body`/`.err` captures)

## 1. Per-Source Probe Matrix

| Source / Attempt | Exact Endpoint | Fetch Window (UTC) | HTTP / `curl_rc` | Bytes | Body SHA-256 | Payload Classification | Latest Session Present | Verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Stooq (`stooq_tqqq`) | `https://stooq.com/q/d/l/?s=tqqq.us&i=d` | `2026-11-21T05:55:05.871Z..05:55:09.382Z` | `200` / `0` | `796` | `5321b8936e24f797df812cdec5f6ab0f6c53cd258617024d223e1f7555650d6a` | HTML JS proof-of-work browser-verification challenge | None | **GATED** |
| Stooq (`stooq_soxl`) | `https://stooq.com/q/d/l/?s=soxl.us&i=d` | `2026-11-21T05:55:09.389Z..05:55:12.283Z` | `200` / `0` | `796` | `81d8e1320c803d47ee728110b19e261b98ca9f8e51a9162304a4a80fc0d451d4` | HTML JS proof-of-work browser-verification challenge | None | **GATED** |
| Stooq (`stooq_qqq`) | `https://stooq.com/q/d/l/?s=qqq.us&i=d` | `2026-11-21T05:55:12.290Z..05:55:16.972Z` | `200` / `0` | `796` | `d78cbb61bccb414e12a675a05781c21e8916a85b6f81f80ad3d87f0b0d1b1f3d` | HTML JS proof-of-work browser-verification challenge | None | **GATED** |
| Yahoo (`yahoo_xndx`) | `https://query1.finance.yahoo.com/v8/finance/chart/XNDX?range=5d&interval=1d` | `2026-11-21T05:55:16.978Z..05:55:23.576Z` | `200` / `0` | `1784` | `d218f8f03c5fb3c5c488ddcded7ce3074a768eda274f0a32308685afe304ec10` | JSON chart (`instrumentType: "ETF"`, `longName: "Tradr 2X Long XNDU Daily ETF"`, `regularMarketPrice: 0.1479`) | `2026-11-20` (wrong instrument) | **WRONG-INSTRUMENT** |
| Yahoo (`yahoo_xsox`) | `https://query1.finance.yahoo.com/v8/finance/chart/XSOX?range=5d&interval=1d` | `2026-11-21T05:55:23.583Z..05:55:26.246Z` | `429` / `0` | `23` | `0d24c98db98d3b4a87b9626a026e7d72c99754ab6bd88211a17bc372350b7d89` | Plain text `Edge: Too Many Requests` | None | **UNRESOLVED** |
| Yahoo (`yahoo_tqqq`) | `https://query1.finance.yahoo.com/v8/finance/chart/TQQQ?range=5d&interval=1d` | `2026-11-21T05:55:26.253Z..05:55:29.954Z` | `429` / `0` | `23` | `0d24c98db98d3b4a87b9626a026e7d72c99754ab6bd88211a17bc372350b7d89` | Plain text `Edge: Too Many Requests` | None | **UNRESOLVED** |
| Yahoo (`yahoo_caret_xndx`) | `https://query1.finance.yahoo.com/v8/finance/chart/%5EXNDX?range=5d&interval=1d` | `2026-11-21T06:02:35.115Z..06:03:10.071Z` | `000` / `56` | `0` | _(empty)_ | Transport error (`curl_rc=56`, connection reset) | None | **UNRESOLVED** |
| Yahoo (`yahoo_xsox_spaced`) | `https://query1.finance.yahoo.com/v8/finance/chart/XSOX?range=5d&interval=1d` | `2026-11-21T06:03:55.080Z..06:04:41.463Z` | `429` / `0` | `23` | `0d24c98db98d3b4a87b9626a026e7d72c99754ab6bd88211a17bc372350b7d89` | Plain text `Edge: Too Many Requests` (45 s after prior request) | None | **UNRESOLVED** |
| Yahoo (`yahoo_caret_sox`) | `https://query1.finance.yahoo.com/v8/finance/chart/%5ESOX?range=5d&interval=1d` | `2026-11-21T06:05:26.473Z..06:06:26.493Z` | `000` / `28` | `0` | _(empty)_ | Transport error (`curl_rc=28`, operation timeout) | None | **UNRESOLVED** |
| NY Fed SOFR (`nyfed_sofr_iso`) | `https://markets.newyorkfed.org/api/rates/secured/sofr/search.json?startDate=2026-11-09&endDate=2026-11-21&typeCode=fr&sort=postDt:1` | `2026-11-21T05:55:29.961Z..05:55:32.846Z` | `200` / `0` | `1922` | `c069d756334eac79fc0554d37d5af757e216bbcc0b8715e6896f2d9c76a0c600` | JSON (`8 refRates`; `percentRate=3.91`, `revisionIndicator=""`) | `2026-11-19` (T−1 lag) | **WORKS** |
| U.S. Treasury (`treasury_yield2026`) | `https://home.treasury.gov/sites/default/files/interest-rates/yield2026.csv` | `2026-11-21T05:55:32.853Z..05:56:02.873Z` | `000` / `28` | `0` | _(empty)_ | Timeout (`curl: (28) Operation timed out after 30002 milliseconds`) | None | **UNRESOLVED** |
| U.S. Treasury (`treasury_yield2026_retry`) | `https://home.treasury.gov/sites/default/files/interest-rates/yield2026.csv` | _(not in `probe-log.txt`; retry captured separately)_ | `404` (per body/dictation) | `95934` | `fe9de27201fb0d84ae76e184160dfb5c8c75c922eef6dfd6138a715c044ff21c` | HTML 404 failover page (`Page Not Found \| U.S. Department of the Treasury`) | None | **UNRESOLVED** |
| U.S. Treasury (`treasury_yield_xml`) | `https://home.treasury.gov/sites/default/files/interest-rates/yield.xml` | `2026-11-21T06:01:17.341Z..06:01:50.106Z` | `000` / `35` | `0` | _(empty)_ | SSL connect error through tunnel (`curl_rc=35`) | None | **UNRESOLVED** |
| FRED DTB3 (`fred_dtb3`) | `https://fred.stlouisfed.org/graph/fredgraph.csv?id=DTB3` | `2026-11-21T05:56:02.880Z..05:56:06.465Z` | `200` / `0` | `301757` | `f161d9aa7335792298ba356743ce588e9b3015a1f5851674e8eeac5478b6e3ed` | CSV (`observation_date,DTB3`; 19,015 lines, `1954-01-04`..`2026-11-19`) | `2026-11-19` (`4.16`) | **WORKS** |
| Tiingo (`tiingo`) | `https://api.tiingo.com/tiingo/daily/{ticker}/prices?startDate=...&endDate=...&format=json` | _(not probed)_ | N/A | N/A | N/A | Not probed; requires `TIINGO_API_KEY` (`Authorization: Token` header) | N/A | **PENDING-OWNER** |

## 2. Findings Summary

1. **Stooq — GATED (`OA-SH1-7` contingency triggered):** All three endpoints (`tqqq.us`, `soxl.us`, `qqq.us`) returned HTTP 200 with a 796-byte JavaScript SHA-256 proof-of-work verification page (`/__verify`), differing only in `<script nonce="...">` and challenge token `c`. No CSV is served to plain HTTP clients from this egress. Under `BRIEF-SH-1.md` §4.2 and §10 (`OA-SH1-7`), this triggers the contingent owner action (owner-obtained free CAPTCHA key or Yahoo-only interim cover). `StooqDailyAdapter` detects the HTML/JS verification response and raises `SourceChallengeGatedError` (`source_challenge_gated`), never treating the page as data.
2. **Yahoo Finance — WRONG-INSTRUMENT / THROTTLED / UNRESOLVED:**
   - Bare `XNDX` returned HTTP 200 for `"Tradr 2X Long XNDU Daily ETF"` (`instrumentType: "ETF"`, `regularMarketPrice: 0.1479`), **not** the Nasdaq-100 Total Return Index.
   - `^XNDX` (`%5EXNDX`) failed with connection reset (`curl_rc=56`); `^SOX` (`%5ESOX`) timed out (`curl_rc=28`); `XSOX` and `TQQQ` returned HTTP 429 `"Edge: Too Many Requests"`, including a 45 s spaced retry on `XSOX`. The `43.156.238.238` egress is edge-throttled by Yahoo prior to symbol resolution.
   - `YahooChartAdapter` therefore defaults to `{"XNDX": "^XNDX", "XSOX": "XSOX"}` marked `YAHOO_SYMBOL_VERIFICATION = "unverified"`, rejects any payload unless `meta.symbol == configured_symbol` and `meta.instrumentType == "INDEX"` (`InstrumentMismatchError`), enforces ≥ 20 s spacing across fetches on an instance, and performs a single 30 s backoff retry on HTTP 429 before raising `SourceThrottledError`.
3. **NY Fed SOFR — WORKS:** Keyless `search.json` with ISO dates (`typeCode=fr&sort=postDt:1`) returned HTTP 200 with 8 `refRates` entries. At probe time (`2026-11-21 00:55` ET), the latest entry was `effectiveDate: "2026-11-19"` (`3.91%`, `revisionIndicator: ""`), confirming the structural T−1 publication lag assumed by `BRIEF-SH-1.md` §4.2 (at a Friday decision instant, the latest published SOFR is effective Thursday).
4. **U.S. Treasury — UNRESOLVED:** `yield2026.csv` timed out at 30 s on the logged attempt (`curl_rc=28`) and returned a 95,934-byte HTML 404 failover page (`/page-not-found`) on retry; `yield.xml` failed with `curl_rc=35` (SSL error through tunnel). Observed responses from `home.treasury.gov` via this egress are slow (~30–40 s). `TreasuryYieldAdapter` ships with full multi-column and 2-column CSV parsing and a configurable endpoint, but marks the default yearly-CSV URL unverified and raises `SourceUnavailableError("treasury_endpoint_unverified")` without fetching unless an override endpoint is supplied.
5. **FRED DTB3 — WORKS:** Keyless `fredgraph.csv?id=DTB3` returned HTTP 200 (301,757 bytes), latest observation `2026-11-19` = `4.16` (Friday's observation publishes the next business day). Operative as the T-bill cross-check.
6. **Tiingo — PENDING-OWNER:** No API key has been provided (`OA-SH1-2`). `TiingoDailyAdapter` reads `TIINGO_API_KEY` from the environment into an `Authorization: Token` header (never in the URL) and emits `credential_absent` without fetching when absent.

## 3. Finalization-Timing Caveat

- The probe ran on Saturday `2026-11-21` (`05:55`–`06:06` UTC / `00:55`–`01:06` EST).
- In the captured payloads, Yahoo `XNDX` (wrong ETF instrument) contained the `2026-11-20` daily bar (`timestamp=1795185000`, `regularMarketTime=1795208400` = `2026-11-20T16:00:00-05:00`) by `2026-11-21T05:55:23.576Z`, while NY Fed SOFR and FRED DTB3 latest available observations were `2026-11-19` (consistent with T−1 / next-business-day rate publication).
- Probing on a Saturday cannot measure weekday post-close EOD finalization lag after `16:00` ET. That measurement remains open for `SH1-W6` before the daily decision instant is pinned.

## 4. W6 Carry-Overs

1. **Yahoo index-symbol verification:** Re-probe `^XNDX`, `XSOX`, and `^SOX` on a weekday under spaced/throttled timing (`>= 20 s` spacing, 429 backoff), verifying `meta.instrumentType == "INDEX"` and `meta.symbol` match; invoke the disclosed-reconstruction fallback (`BRIEF-SH-1.md` §4.2, `OA-SH1-3`) if index TR symbols remain unreachable or throttled.
2. **U.S. Treasury endpoint resolution:** Verify the active Treasury daily par-yield CSV/XML URL (and ~30–40 s egress timeout budget) from the mini dev environment, or escalate primary-vs-FRED fallback disposition if `home.treasury.gov` remains unreachable through the tunnel.
3. **Weekday post-close finalization measurement:** Measure actual post-`16:00` ET availability lag across verified primary and fallback feeds to pin the scheduler decision instant and commitment deadline (`BRIEF-SH-1.md` §11).
4. **Tiingo live probe:** Run the first live Tiingo probe for `TQQQ`, `SOXL`, and `QQQ` once the owner provides the free Starter `TIINGO_API_KEY` (`OA-SH1-2`).
