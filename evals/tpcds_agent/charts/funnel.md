# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| db-only | 72/99 | q01, q02, q03, q04, q05, q07, q09, q10, q11, q12, q13, q14, q16, q17, q19, q21, q22, q25, q26, q28, q29, q30, q32, q33, q34, q35, q37, q38, q39, q40, q42, q43, q44, q46, q47, q48, q50, q52, q53, q54, q55, q59, q60, q61, q62, q64, q65, q67, q68, q69, q70, q71, q72, q73, q74, q76, q81, q82, q84, q85, q87, q88, q89, q90, q91, q92, q94, q95, q96, q97, q98, q99 | — |
| enriched | 61/99 | q06, q08, q15, q20, q36, q45, q51, q58, q63, q83, q86 | q05, q11, q14, q16, q17, q29, q30, q39, q47, q50, q65, q67, q72, q73, q74, q81, q82, q87, q89, q94, q97, q99 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db-only | 0.73 | 13,625,110 |
| enriched | 0.62 | 26,698,522 |

## Per-query matrix

| query | db-only | enriched |
|---|---|---|
| q01 | ✅ | ✅ |
| q02 | ✅ | ✅ |
| q03 | ✅ | ✅ |
| q04 | ✅ | ✅ |
| q05 | ✅ | ❌ fail |
| q06 | ❌ fail | ✅ |
| q07 | ✅ | ✅ |
| q08 | ❌ fail | ✅ |
| q09 | ✅ | ✅ |
| q10 | ✅ | ✅ |
| q11 | ✅ | ❌ fail |
| q12 | ✅ | ✅ |
| q13 | ✅ | ✅ |
| q14 | ✅ | ❌ fail |
| q15 | ❌ fail | ✅ |
| q16 | ✅ | ❌ fail |
| q17 | ✅ | ❌ fail |
| q18 | ❌ fail | ❌ fail |
| q19 | ✅ | ✅ |
| q20 | ❌ fail | ✅ |
| q21 | ✅ | ✅ |
| q22 | ✅ | ✅ |
| q23 | ❌ fail | ❌ fail |
| q24 | ❌ fail | ❌ fail |
| q25 | ✅ | ✅ |
| q26 | ✅ | ✅ |
| q27 | ❌ fail | ❌ fail |
| q28 | ✅ | ✅ |
| q29 | ✅ | ❌ fail |
| q30 | ✅ | ❌ fail |
| q31 | ❌ fail | ❌ fail |
| q32 | ✅ | ✅ |
| q33 | ✅ | ✅ |
| q34 | ✅ | ✅ |
| q35 | ✅ | ✅ |
| q36 | ❌ fail | ✅ |
| q37 | ✅ | ✅ |
| q38 | ✅ | ✅ |
| q39 | ✅ | ❌ fail |
| q40 | ✅ | ✅ |
| q41 | ❌ fail | ❌ fail |
| q42 | ✅ | ✅ |
| q43 | ✅ | ✅ |
| q44 | ✅ | ✅ |
| q45 | ❌ fail | ✅ |
| q46 | ✅ | ✅ |
| q47 | ✅ | ❌ fail |
| q48 | ✅ | ✅ |
| q49 | ❌ fail | ❌ fail |
| q50 | ✅ | ❌ fail |
| q51 | ❌ fail | ✅ |
| q52 | ✅ | ✅ |
| q53 | ✅ | ✅ |
| q54 | ✅ | ✅ |
| q55 | ✅ | ✅ |
| q56 | ❌ fail | ❌ fail |
| q57 | ❌ fail | ❌ fail |
| q58 | ❌ fail | ✅ |
| q59 | ✅ | ✅ |
| q60 | ✅ | ✅ |
| q61 | ✅ | ✅ |
| q62 | ✅ | ✅ |
| q63 | ❌ fail | ✅ |
| q64 | ✅ | ✅ |
| q65 | ✅ | ❌ fail |
| q66 | ❌ fail | ❌ fail |
| q67 | ✅ | ❌ fail |
| q68 | ✅ | ✅ |
| q69 | ✅ | ✅ |
| q70 | ✅ | ✅ |
| q71 | ✅ | ✅ |
| q72 | ✅ | ❌ fail |
| q73 | ✅ | ❌ fail |
| q74 | ✅ | ❌ fail |
| q75 | ❌ fail | ❌ fail |
| q76 | ✅ | ✅ |
| q77 | ❌ fail | ❌ fail |
| q78 | ❌ fail | ❌ fail |
| q79 | ❌ fail | ❌ fail |
| q80 | ❌ fail | ❌ fail |
| q81 | ✅ | ❌ fail |
| q82 | ✅ | ❌ fail |
| q83 | ❌ fail | ✅ |
| q84 | ✅ | ✅ |
| q85 | ✅ | ✅ |
| q86 | ❌ fail | ✅ |
| q87 | ✅ | ❌ fail |
| q88 | ✅ | ✅ |
| q89 | ✅ | ❌ fail |
| q90 | ✅ | ✅ |
| q91 | ✅ | ✅ |
| q92 | ✅ | ✅ |
| q93 | ❌ fail | ❌ fail |
| q94 | ✅ | ❌ fail |
| q95 | ✅ | ✅ |
| q96 | ✅ | ✅ |
| q97 | ✅ | ❌ fail |
| q98 | ✅ | ✅ |
| q99 | ✅ | ❌ fail |
