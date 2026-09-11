# ACME Insurance category funnel

## Category pass coverage

| category | passing | unique passes | shared passes |
|---|---|---|---|
| db-only | 11/11 | — | q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11 |
| db+schema | 11/11 | — | q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11 |
| ingest | 11/11 | — | q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11 |
| enriched | 11/11 | — | q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11 |

## Metrics

| category | pass rate | total tokens | cache-adj cost | agg used | p50 cand ms |
|---|---|---|---|---|---|
| db-only | 1.00 | 337,566 | 89,080 | 0/11 | 4 |
| db+schema | 1.00 | 318,318 | 83,425 | 0/11 | 4 |
| ingest | 1.00 | 1,310,282 | 317,604 | 0/11 | 4 |
| enriched | 1.00 | 627,040 | 161,402 | 0/11 | 4 |

## Per-query matrix

| query | db-only | db+schema | ingest | enriched |
|---|---|---|---|---|
| q01 | ✅ | ✅ | ✅ | ✅ |
| q02 | ✅ | ✅ | ✅ | ✅ |
| q03 | ✅ | ✅ | ✅ | ✅ |
| q04 | ✅ | ✅ | ✅ | ✅ |
| q05 | ✅ | ✅ | ✅ | ✅ |
| q06 | ✅ | ✅ | ✅ | ✅ |
| q07 | ✅ | ✅ | ✅ | ✅ |
| q08 | ✅ | ✅ | ✅ | ✅ |
| q09 | ✅ | ✅ | ✅ | ✅ |
| q10 | ✅ | ✅ | ✅ | ✅ |
| q11 | ✅ | ✅ | ✅ | ✅ |
