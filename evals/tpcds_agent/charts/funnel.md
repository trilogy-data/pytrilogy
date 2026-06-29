# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| db-only | 62/99 | q01, q03, q04, q05, q07, q08, q09, q10, q13, q14, q19, q20, q21, q22, q25, q26, q28, q29, q30, q32, q33, q34, q37, q39, q40, q41, q42, q43, q44, q46, q48, q49, q50, q51, q52, q53, q55, q59, q60, q61, q62, q64, q68, q70, q72, q73, q74, q76, q78, q82, q84, q85, q87, q88, q90, q91, q92, q94, q95, q96, q97, q99 | — |
| db+schema | 67/99 | q02, q06, q12, q16, q24, q36, q45, q47, q57, q58, q63, q65, q71, q81, q98 | q07, q20, q30, q41, q49, q50, q51, q62, q76, q91 |
| enriched | 58/99 | q11, q15, q27, q89 | q05, q14, q24, q25, q28, q29, q30, q32, q50, q51, q59, q64, q73, q78, q81, q82, q84, q87, q88, q90, q91, q95, q97 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db-only | 0.63 | 13,944,657 |
| db+schema | 0.68 | 15,514,315 |
| enriched | 0.59 | 26,318,655 |

## Per-query matrix

| query | db-only | db+schema | enriched |
|---|---|---|---|
| q01 | ✅ | ✅ | ✅ |
| q02 | ❌ fail | ✅ | ✅ |
| q03 | ✅ | ✅ | ✅ |
| q04 | ✅ | ✅ | ✅ |
| q05 | ✅ | ✅ | ❌ fail |
| q06 | ❌ fail | ✅ | ✅ |
| q07 | ✅ | ❌ fail | ✅ |
| q08 | ✅ | ✅ | ✅ |
| q09 | ✅ | ✅ | ✅ |
| q10 | ✅ | ✅ | ✅ |
| q11 | ❌ fail | ❌ fail | ✅ |
| q12 | ❌ fail | ✅ | ✅ |
| q13 | ✅ | ✅ | ✅ |
| q14 | ✅ | ✅ | ❌ fail |
| q15 | ❌ fail | ❌ fail | ✅ |
| q16 | ❌ fail | ✅ | ✅ |
| q17 | ❌ fail | ❌ fail | ❌ fail |
| q18 | ❌ fail | ❌ fail | ❌ fail |
| q19 | ✅ | ✅ | ✅ |
| q20 | ✅ | ❌ fail | ✅ |
| q21 | ✅ | ✅ | ✅ |
| q22 | ✅ | ✅ | ✅ |
| q23 | ❌ fail | ❌ fail | ❌ fail |
| q24 | ❌ fail | ✅ | ❌ fail |
| q25 | ✅ | ✅ | ❌ fail |
| q26 | ✅ | ✅ | ✅ |
| q27 | ❌ fail | ❌ fail | ✅ |
| q28 | ✅ | ✅ | ❌ fail |
| q29 | ✅ | ✅ | ❌ fail |
| q30 | ✅ | ❌ fail | ❌ fail |
| q31 | ❌ fail | ❌ fail | ❌ fail |
| q32 | ✅ | ✅ | ❌ fail |
| q33 | ✅ | ✅ | ✅ |
| q34 | ✅ | ✅ | ✅ |
| q35 | ❌ fail | ❌ fail | ❌ fail |
| q36 | ❌ fail | ✅ | ✅ |
| q37 | ✅ | ✅ | ✅ |
| q38 | ❌ fail | ❌ fail | ❌ fail |
| q39 | ✅ | ✅ | ✅ |
| q40 | ✅ | ✅ | ✅ |
| q41 | ✅ | ❌ fail | ✅ |
| q42 | ✅ | ✅ | ✅ |
| q43 | ✅ | ✅ | ✅ |
| q44 | ✅ | ✅ | ✅ |
| q45 | ❌ fail | ✅ | ✅ |
| q46 | ✅ | ✅ | ✅ |
| q47 | ❌ fail | ✅ | ✅ |
| q48 | ✅ | ✅ | ✅ |
| q49 | ✅ | ❌ fail | ✅ |
| q50 | ✅ | ❌ fail | ❌ fail |
| q51 | ✅ | ❌ fail | ❌ fail |
| q52 | ✅ | ✅ | ✅ |
| q53 | ✅ | ✅ | ✅ |
| q54 | ❌ fail | ❌ fail | ❌ fail |
| q55 | ✅ | ✅ | ✅ |
| q56 | ❌ fail | ❌ fail | ❌ fail |
| q57 | ❌ fail | ✅ | ✅ |
| q58 | ❌ fail | ✅ | ✅ |
| q59 | ✅ | ✅ | ❌ fail |
| q60 | ✅ | ✅ | ✅ |
| q61 | ✅ | ✅ | ✅ |
| q62 | ✅ | ❌ fail | ✅ |
| q63 | ❌ fail | ✅ | ✅ |
| q64 | ✅ | ✅ | ❌ fail |
| q65 | ❌ fail | ✅ | ✅ |
| q66 | ❌ fail | ❌ timeout | ❌ fail |
| q67 | ❌ fail | ❌ fail | ❌ fail |
| q68 | ✅ | ✅ | ✅ |
| q69 | ❌ fail | ❌ fail | ❌ fail |
| q70 | ✅ | ✅ | ✅ |
| q71 | ❌ fail | ✅ | ✅ |
| q72 | ✅ | ✅ | ✅ |
| q73 | ✅ | ✅ | ❌ fail |
| q74 | ✅ | ✅ | ✅ |
| q75 | ❌ fail | ❌ fail | ❌ fail |
| q76 | ✅ | ❌ fail | ✅ |
| q77 | ❌ fail | ❌ fail | ❌ fail |
| q78 | ✅ | ✅ | ❌ fail |
| q79 | ❌ fail | ❌ fail | ❌ fail |
| q80 | ❌ fail | ❌ fail | ❌ fail |
| q81 | ❌ fail | ✅ | ❌ fail |
| q82 | ✅ | ✅ | ❌ fail |
| q83 | ❌ fail | ❌ fail | ❌ fail |
| q84 | ✅ | ✅ | ❌ fail |
| q85 | ✅ | ✅ | ✅ |
| q86 | ❌ fail | ❌ fail | ❌ fail |
| q87 | ✅ | ✅ | ❌ fail |
| q88 | ✅ | ✅ | ❌ fail |
| q89 | ❌ fail | ❌ fail | ✅ |
| q90 | ✅ | ✅ | ❌ fail |
| q91 | ✅ | ❌ fail | ❌ fail |
| q92 | ✅ | ✅ | ✅ |
| q93 | ❌ fail | ❌ fail | ❌ fail |
| q94 | ✅ | ✅ | ✅ |
| q95 | ✅ | ✅ | ❌ fail |
| q96 | ✅ | ✅ | ✅ |
| q97 | ✅ | ✅ | ❌ fail |
| q98 | ❌ fail | ✅ | ✅ |
| q99 | ✅ | ✅ | ✅ |
