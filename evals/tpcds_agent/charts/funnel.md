# TPC-DS category funnel

## Category pass coverage

| category | passing | unique passes | shared passes |
|---|---|---|---|
| db+schema+aggregates | 17/20 | q14 | q01, q02, q03, q04, q05, q07, q08, q09, q10, q11, q13, q15, q16, q17, q18, q19 |
| db+schema+aggregates+noise | 16/20 | — | q01, q03, q04, q05, q07, q08, q09, q10, q11, q12, q13, q15, q16, q17, q18, q19 |
| enriched+aggregates | 19/20 | — | q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11, q12, q13, q15, q16, q17, q18, q19, q20 |
| enriched+aggregates+noise | 19/20 | — | q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11, q12, q13, q15, q16, q17, q18, q19, q20 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db+schema+aggregates | 0.85 | 4,053,992 |
| db+schema+aggregates+noise | 0.80 | 6,361,839 |
| enriched+aggregates | 0.95 | 10,814,205 |
| enriched+aggregates+noise | 0.95 | 10,376,374 |

## Per-query matrix

| query | db+schema+aggregates | db+schema+aggregates+noise | enriched+aggregates | enriched+aggregates+noise |
|---|---|---|---|---|
| q01 | ✅ | ✅ | ✅ | ✅ |
| q02 | ✅ | ❌ fail | ✅ | ✅ |
| q03 | ✅ | ✅ | ✅ | ✅ |
| q04 | ✅ | ✅ | ✅ | ✅ |
| q05 | ✅ | ✅ | ✅ | ✅ |
| q06 | ❌ fail | ❌ fail | ✅ | ✅ |
| q07 | ✅ | ✅ | ✅ | ✅ |
| q08 | ✅ | ✅ | ✅ | ✅ |
| q09 | ✅ | ✅ | ✅ | ✅ |
| q10 | ✅ | ✅ | ✅ | ✅ |
| q11 | ✅ | ✅ | ✅ | ✅ |
| q12 | ❌ fail | ✅ | ✅ | ✅ |
| q13 | ✅ | ✅ | ✅ | ✅ |
| q14 | ✅ | ❌ fail | ❌ fail | ❌ fail |
| q15 | ✅ | ✅ | ✅ | ✅ |
| q16 | ✅ | ✅ | ✅ | ✅ |
| q17 | ✅ | ✅ | ✅ | ✅ |
| q18 | ✅ | ✅ | ✅ | ✅ |
| q19 | ✅ | ✅ | ✅ | ✅ |
| q20 | ❌ fail | ❌ fail | ✅ | ✅ |
