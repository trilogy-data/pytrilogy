# ACME Insurance category funnel

## Category pass coverage

| category | passing | unique passes | shared passes |
|---|---|---|---|
| db-only | 9/11 | — | q01, q03, q04, q05, q06, q07, q09, q10, q11 |
| db+schema | 11/11 | — | q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11 |
| ingest | 11/11 | — | q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11 |
| enriched | 11/11 | — | q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11 |

## Metrics

| category | pass rate | total tokens | cache-adj cost | agg used | p50 cand ms |
|---|---|---|---|---|---|
| db-only | 0.82 | 292,444 | 75,522 | 0/11 | 5 |
| db+schema | 1.00 | 278,322 | 77,068 | 0/11 | 5 |
| ingest | 1.00 | 883,691 | 227,973 | 0/11 | 5 |
| enriched | 1.00 | 424,663 | 128,138 | 0/11 | 5 |

## Per-query matrix

| query | db-only | db+schema | ingest | enriched |
|---|---|---|---|---|
| q01 | ✅ | ✅ | ✅ | ✅ |
| q02 | ❌ fail | ✅ | ✅ | ✅ |
| q03 | ✅ | ✅ | ✅ | ✅ |
| q04 | ✅ | ✅ | ✅ | ✅ |
| q05 | ✅ | ✅ | ✅ | ✅ |
| q06 | ✅ | ✅ | ✅ | ✅ |
| q07 | ✅ | ✅ | ✅ | ✅ |
| q08 | ❌ fail | ✅ | ✅ | ✅ |
| q09 | ✅ | ✅ | ✅ | ✅ |
| q10 | ✅ | ✅ | ✅ | ✅ |
| q11 | ✅ | ✅ | ✅ | ✅ |
