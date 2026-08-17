# TPC-DS category funnel

## Category pass coverage

| category | passing | unique passes | shared passes |
|---|---|---|---|
| db-only | 18/20 | — | q01, q02, q03, q04, q06, q07, q08, q09, q10, q11, q12, q13, q15, q16, q17, q18, q19, q20 |
| db+schema | 18/20 | — | q01, q02, q03, q04, q06, q07, q08, q09, q10, q11, q12, q13, q15, q16, q17, q18, q19, q20 |

## Metrics

| category | pass rate | total tokens | cache-adj cost | agg used | p50 cand ms |
|---|---|---|---|---|---|
| db-only | 0.90 | 6,274,353 | 1,359,836 | 0/20 | 56 |
| db+schema | 0.90 | 2,805,145 | 679,801 | 0/20 | 68 |

## Per-query matrix

| query | db-only | db+schema |
|---|---|---|
| q01 | ✅ | ✅ |
| q02 | ✅ | ✅ |
| q03 | ✅ | ✅ |
| q04 | ✅ | ✅ |
| q05 | ❌ fail | ❌ fail |
| q06 | ✅ | ✅ |
| q07 | ✅ | ✅ |
| q08 | ✅ | ✅ |
| q09 | ✅ | ✅ |
| q10 | ✅ | ✅ |
| q11 | ✅ | ✅ |
| q12 | ✅ | ✅ |
| q13 | ✅ | ✅ |
| q14 | ❌ fail | ❌ fail |
| q15 | ✅ | ✅ |
| q16 | ✅ | ✅ |
| q17 | ✅ | ✅ |
| q18 | ✅ | ✅ |
| q19 | ✅ | ✅ |
| q20 | ✅ | ✅ |
