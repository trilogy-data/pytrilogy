# TPC-DS category funnel

## Category pass coverage

| category | passing | unique passes | shared passes |
|---|---|---|---|
| db+schema+aggregates | 17/20 | — | q01, q02, q03, q04, q06, q07, q08, q09, q10, q11, q12, q13, q14, q15, q16, q17, q19 |
| db+schema+aggregates+noise | 18/20 | — | q01, q02, q03, q04, q05, q07, q08, q09, q10, q11, q12, q13, q14, q15, q16, q17, q18, q19 |
| enriched+aggregates | 16/20 | — | q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11, q13, q14, q16, q18, q19 |
| enriched+aggregates+noise | 17/20 | q20 | q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11, q12, q13, q16, q18, q19 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db+schema+aggregates | 0.85 | 3,729,773 |
| db+schema+aggregates+noise | 0.90 | 3,410,857 |
| enriched+aggregates | 0.80 | 10,300,910 |
| enriched+aggregates+noise | 0.85 | 9,928,497 |

## Per-query matrix

| query | db+schema+aggregates | db+schema+aggregates+noise | enriched+aggregates | enriched+aggregates+noise |
|---|---|---|---|---|
| q01 | ✅ | ✅ | ✅ | ✅ |
| q02 | ✅ | ✅ | ✅ | ✅ |
| q03 | ✅ | ✅ | ✅ | ✅ |
| q04 | ✅ | ✅ | ✅ | ✅ |
| q05 | ❌ fail | ✅ | ✅ | ✅ |
| q06 | ✅ | ❌ fail | ✅ | ✅ |
| q07 | ✅ | ✅ | ✅ | ✅ |
| q08 | ✅ | ✅ | ✅ | ✅ |
| q09 | ✅ | ✅ | ✅ | ✅ |
| q10 | ✅ | ✅ | ✅ | ✅ |
| q11 | ✅ | ✅ | ✅ | ✅ |
| q12 | ✅ | ✅ | ❌ fail | ✅ |
| q13 | ✅ | ✅ | ✅ | ✅ |
| q14 | ✅ | ✅ | ✅ | ❌ timeout |
| q15 | ✅ | ✅ | ❌ timeout | ❌ timeout |
| q16 | ✅ | ✅ | ✅ | ✅ |
| q17 | ✅ | ✅ | ❌ timeout | ❌ timeout |
| q18 | ❌ fail | ✅ | ✅ | ✅ |
| q19 | ✅ | ✅ | ✅ | ✅ |
| q20 | ❌ fail | ❌ fail | ❌ fail | ✅ |
