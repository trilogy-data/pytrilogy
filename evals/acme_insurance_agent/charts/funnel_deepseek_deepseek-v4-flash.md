# ACME Insurance category funnel

## Category pass coverage

| category | passing | unique passes | shared passes |
|---|---|---|---|
| db-only | 10/11 | — | q01, q02, q04, q05, q06, q07, q08, q09, q10, q11 |
| db+schema | 10/11 | — | q01, q02, q04, q05, q06, q07, q08, q09, q10, q11 |
| ingest | 9/11 | — | q01, q02, q04, q05, q06, q07, q08, q10, q11 |
| enriched | 11/11 | q03 | q01, q02, q04, q05, q06, q07, q08, q09, q10, q11 |

## Metrics

| category | pass rate | total tokens | cache-adj cost | agg used | p50 cand ms |
|---|---|---|---|---|---|
| db-only | 0.91 | 696,972 | 162,905 | 0/11 | 4 |
| db+schema | 0.91 | 382,347 | 100,222 | 0/11 | 4 |
| ingest | 0.82 | 1,574,419 | 361,017 | 0/11 | 4 |
| enriched | 1.00 | 648,574 | 166,347 | 0/11 | 4 |

## Per-query matrix

| query | db-only | db+schema | ingest | enriched |
|---|---|---|---|---|
| q01 | ✅ | ✅ | ✅ | ✅ |
| q02 | ✅ | ✅ | ✅ | ✅ |
| q03 | ❌ fail | ❌ fail | ❌ fail | ✅ |
| q04 | ✅ | ✅ | ✅ | ✅ |
| q05 | ✅ | ✅ | ✅ | ✅ |
| q06 | ✅ | ✅ | ✅ | ✅ |
| q07 | ✅ | ✅ | ✅ | ✅ |
| q08 | ✅ | ✅ | ✅ | ✅ |
| q09 | ✅ | ✅ | ❌ fail | ✅ |
| q10 | ✅ | ✅ | ✅ | ✅ |
| q11 | ✅ | ✅ | ✅ | ✅ |
