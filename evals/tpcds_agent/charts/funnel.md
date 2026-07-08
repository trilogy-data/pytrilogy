# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| db-only | 72/99 | q01, q03, q04, q06, q07, q08, q09, q10, q11, q13, q14, q15, q19, q21, q22, q25, q26, q28, q30, q32, q33, q34, q35, q36, q37, q38, q39, q40, q42, q43, q44, q46, q47, q48, q49, q51, q52, q53, q54, q55, q59, q60, q61, q62, q63, q64, q65, q68, q69, q70, q71, q72, q73, q74, q76, q78, q81, q82, q83, q84, q85, q86, q87, q88, q89, q90, q91, q92, q93, q94, q96, q99 | — |
| enriched | 60/99 | q02, q12, q16, q29, q31, q45, q57, q58, q67, q97, q98 | q10, q11, q14, q25, q30, q35, q36, q37, q38, q46, q47, q49, q51, q62, q64, q65, q73, q74, q76, q78, q81, q85, q88 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db-only | 0.73 | 12,727,125 |
| enriched | 0.61 | 28,069,199 |

## Per-query matrix

| query | db-only | enriched |
|---|---|---|
| q01 | ✅ | ✅ |
| q02 | ❌ fail | ✅ |
| q03 | ✅ | ✅ |
| q04 | ✅ | ✅ |
| q05 | ❌ fail | ❌ fail |
| q06 | ✅ | ✅ |
| q07 | ✅ | ✅ |
| q08 | ✅ | ✅ |
| q09 | ✅ | ✅ |
| q10 | ✅ | ❌ fail |
| q11 | ✅ | ❌ fail |
| q12 | ❌ fail | ✅ |
| q13 | ✅ | ✅ |
| q14 | ✅ | ❌ fail |
| q15 | ✅ | ✅ |
| q16 | ❌ fail | ✅ |
| q17 | ❌ fail | ❌ fail |
| q18 | ❌ fail | ❌ fail |
| q19 | ✅ | ✅ |
| q20 | ❌ fail | ❌ fail |
| q21 | ✅ | ✅ |
| q22 | ✅ | ✅ |
| q23 | ❌ fail | ❌ exhausted |
| q24 | ❌ fail | ❌ fail |
| q25 | ✅ | ❌ fail |
| q26 | ✅ | ✅ |
| q27 | ❌ fail | ❌ fail |
| q28 | ✅ | ✅ |
| q29 | ❌ fail | ✅ |
| q30 | ✅ | ❌ fail |
| q31 | ❌ fail | ✅ |
| q32 | ✅ | ✅ |
| q33 | ✅ | ✅ |
| q34 | ✅ | ✅ |
| q35 | ✅ | ❌ fail |
| q36 | ✅ | ❌ fail |
| q37 | ✅ | ❌ fail |
| q38 | ✅ | ❌ fail |
| q39 | ✅ | ✅ |
| q40 | ✅ | ✅ |
| q41 | ❌ fail | ❌ fail |
| q42 | ✅ | ✅ |
| q43 | ✅ | ✅ |
| q44 | ✅ | ✅ |
| q45 | ❌ fail | ✅ |
| q46 | ✅ | ❌ fail |
| q47 | ✅ | ❌ fail |
| q48 | ✅ | ✅ |
| q49 | ✅ | ❌ fail |
| q50 | ❌ fail | ❌ fail |
| q51 | ✅ | ❌ fail |
| q52 | ✅ | ✅ |
| q53 | ✅ | ✅ |
| q54 | ✅ | ✅ |
| q55 | ✅ | ✅ |
| q56 | ❌ fail | ❌ fail |
| q57 | ❌ fail | ✅ |
| q58 | ❌ fail | ✅ |
| q59 | ✅ | ✅ |
| q60 | ✅ | ✅ |
| q61 | ✅ | ✅ |
| q62 | ✅ | ❌ fail |
| q63 | ✅ | ✅ |
| q64 | ✅ | ❌ fail |
| q65 | ✅ | ❌ fail |
| q66 | ❌ fail | ❌ fail |
| q67 | ❌ fail | ✅ |
| q68 | ✅ | ✅ |
| q69 | ✅ | ✅ |
| q70 | ✅ | ✅ |
| q71 | ✅ | ✅ |
| q72 | ✅ | ✅ |
| q73 | ✅ | ❌ fail |
| q74 | ✅ | ❌ fail |
| q75 | ❌ fail | ❌ fail |
| q76 | ✅ | ❌ fail |
| q77 | ❌ fail | ❌ fail |
| q78 | ✅ | ❌ fail |
| q79 | ❌ fail | ❌ fail |
| q80 | ❌ fail | ❌ fail |
| q81 | ✅ | ❌ fail |
| q82 | ✅ | ✅ |
| q83 | ✅ | ✅ |
| q84 | ✅ | ✅ |
| q85 | ✅ | ❌ fail |
| q86 | ✅ | ✅ |
| q87 | ✅ | ✅ |
| q88 | ✅ | ❌ fail |
| q89 | ✅ | ✅ |
| q90 | ✅ | ✅ |
| q91 | ✅ | ✅ |
| q92 | ✅ | ✅ |
| q93 | ✅ | ✅ |
| q94 | ✅ | ✅ |
| q95 | ❌ fail | ❌ fail |
| q96 | ✅ | ✅ |
| q97 | ❌ fail | ✅ |
| q98 | ❌ fail | ✅ |
| q99 | ✅ | ✅ |
