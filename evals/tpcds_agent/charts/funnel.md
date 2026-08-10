# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| db+schema+aggregates | 19/20 | q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11, q12, q13, q14, q16, q17, q18, q19, q20 | — |
| db+schema+noise | 17/20 | q15 | q01, q18, q20 |
| enriched+aggregates | 17/20 | — | q06, q11, q14 |
| enriched+noise | 19/20 | — | q20 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db+schema+aggregates | 0.95 | 1,796,144 |
| db+schema+noise | 0.85 | 1,829,424 |
| enriched+aggregates | 0.85 | 5,540,578 |
| enriched+noise | 0.95 | 4,622,135 |

## Per-query matrix

| query | db+schema+aggregates | db+schema+noise | enriched+aggregates | enriched+noise |
|---|---|---|---|---|
| q01 | ✅ | ❌ fail | ✅ | ✅ |
| q02 | ✅ | ✅ | ✅ | ✅ |
| q03 | ✅ | ✅ | ✅ | ✅ |
| q04 | ✅ | ✅ | ✅ | ✅ |
| q05 | ✅ | ✅ | ✅ | ✅ |
| q06 | ✅ | ✅ | ❌ fail | ✅ |
| q07 | ✅ | ✅ | ✅ | ✅ |
| q08 | ✅ | ✅ | ✅ | ✅ |
| q09 | ✅ | ✅ | ✅ | ✅ |
| q10 | ✅ | ✅ | ✅ | ✅ |
| q11 | ✅ | ✅ | ❌ fail | ✅ |
| q12 | ✅ | ✅ | ✅ | ✅ |
| q13 | ✅ | ✅ | ✅ | ✅ |
| q14 | ✅ | ✅ | ❌ fail | ✅ |
| q15 | ❌ fail | ✅ | ✅ | ✅ |
| q16 | ✅ | ✅ | ✅ | ✅ |
| q17 | ✅ | ✅ | ✅ | ✅ |
| q18 | ✅ | ❌ fail | ✅ | ✅ |
| q19 | ✅ | ✅ | ✅ | ✅ |
| q20 | ✅ | ❌ fail | ✅ | ❌ fail |
