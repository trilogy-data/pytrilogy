# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| db+schema+aggregates | 15/20 | q01, q02, q03, q04, q06, q07, q09, q10, q11, q12, q13, q14, q16, q17, q19 | — |
| db+schema+aggregates+noise | 18/20 | q08, q15, q18, q20 | q02 |
| enriched+aggregates | 17/20 | q05 | q11, q16, q20 |
| enriched+aggregates+noise | 16/20 | — | q06, q11, q14, q20 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db+schema+aggregates | 0.75 | 2,003,768 |
| db+schema+aggregates+noise | 0.90 | 1,936,625 |
| enriched+aggregates | 0.85 | 7,643,576 |
| enriched+aggregates+noise | 0.80 | 6,223,720 |

## Per-query matrix

| query | db+schema+aggregates | db+schema+aggregates+noise | enriched+aggregates | enriched+aggregates+noise |
|---|---|---|---|---|
| q01 | ✅ | ✅ | ✅ | ✅ |
| q02 | ✅ | ❌ fail | ✅ | ✅ |
| q03 | ✅ | ✅ | ✅ | ✅ |
| q04 | ✅ | ✅ | ✅ | ✅ |
| q05 | ❌ fail | ❌ fail | ✅ | ✅ |
| q06 | ✅ | ✅ | ✅ | ❌ fail |
| q07 | ✅ | ✅ | ✅ | ✅ |
| q08 | ❌ error | ✅ | ✅ | ✅ |
| q09 | ✅ | ✅ | ✅ | ✅ |
| q10 | ✅ | ✅ | ✅ | ✅ |
| q11 | ✅ | ✅ | ❌ fail | ❌ fail |
| q12 | ✅ | ✅ | ✅ | ✅ |
| q13 | ✅ | ✅ | ✅ | ✅ |
| q14 | ✅ | ✅ | ✅ | ❌ fail |
| q15 | ❌ fail | ✅ | ✅ | ✅ |
| q16 | ✅ | ✅ | ❌ fail | ✅ |
| q17 | ✅ | ✅ | ✅ | ✅ |
| q18 | ❌ fail | ✅ | ✅ | ✅ |
| q19 | ✅ | ✅ | ✅ | ✅ |
| q20 | ❌ fail | ✅ | ❌ fail | ❌ fail |
