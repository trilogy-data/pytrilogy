# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| db-only | 78/99 | q01, q02, q03, q04, q05, q06, q07, q09, q10, q11, q12, q13, q14, q15, q16, q17, q19, q20, q21, q22, q24, q25, q26, q28, q29, q32, q33, q34, q35, q36, q37, q38, q39, q40, q42, q43, q44, q46, q47, q48, q49, q52, q53, q54, q55, q57, q60, q61, q62, q63, q64, q65, q66, q68, q69, q70, q71, q72, q73, q74, q76, q82, q83, q84, q85, q87, q88, q89, q90, q91, q92, q93, q94, q95, q96, q97, q98, q99 | — |
| db+schema | 72/99 | q08, q41, q45, q50, q58, q59, q67, q81, q86 | q02, q05, q16, q17, q20, q36, q42, q49, q57, q66, q83, q93, q94, q95, q98 |
| enriched | 60/99 | q27, q51 | q05, q06, q14, q16, q17, q24, q25, q29, q35, q37, q39, q41, q44, q45, q49, q50, q57, q59, q62, q63, q67, q72, q81, q84, q90, q93, q94, q97, q99 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db-only | 0.79 | 16,248,427 |
| db+schema | 0.73 | 18,380,873 |
| enriched | 0.61 | 35,052,700 |

## Per-query matrix

| query | db-only | db+schema | enriched |
|---|---|---|---|
| q01 | ✅ | ✅ | ✅ |
| q02 | ✅ | ❌ fail | ✅ |
| q03 | ✅ | ✅ | ✅ |
| q04 | ✅ | ✅ | ✅ |
| q05 | ✅ | ❌ fail | ❌ fail |
| q06 | ✅ | ✅ | ❌ fail |
| q07 | ✅ | ✅ | ✅ |
| q08 | ❌ fail | ✅ | ✅ |
| q09 | ✅ | ✅ | ✅ |
| q10 | ✅ | ✅ | ✅ |
| q11 | ✅ | ✅ | ✅ |
| q12 | ✅ | ✅ | ✅ |
| q13 | ✅ | ✅ | ✅ |
| q14 | ✅ | ✅ | ❌ fail |
| q15 | ✅ | ✅ | ✅ |
| q16 | ✅ | ❌ fail | ❌ fail |
| q17 | ✅ | ❌ fail | ❌ fail |
| q18 | ❌ fail | ❌ fail | ❌ fail |
| q19 | ✅ | ✅ | ✅ |
| q20 | ✅ | ❌ fail | ✅ |
| q21 | ✅ | ✅ | ✅ |
| q22 | ✅ | ✅ | ✅ |
| q23 | ❌ fail | ❌ fail | ❌ fail |
| q24 | ✅ | ✅ | ❌ fail |
| q25 | ✅ | ✅ | ❌ fail |
| q26 | ✅ | ✅ | ✅ |
| q27 | ❌ fail | ❌ fail | ✅ |
| q28 | ✅ | ✅ | ✅ |
| q29 | ✅ | ✅ | ❌ exhausted |
| q30 | ❌ fail | ❌ fail | ❌ fail |
| q31 | ❌ fail | ❌ fail | ❌ fail |
| q32 | ✅ | ✅ | ✅ |
| q33 | ✅ | ✅ | ✅ |
| q34 | ✅ | ✅ | ✅ |
| q35 | ✅ | ✅ | ❌ fail |
| q36 | ✅ | ❌ fail | ✅ |
| q37 | ✅ | ✅ | ❌ fail |
| q38 | ✅ | ✅ | ✅ |
| q39 | ✅ | ✅ | ❌ fail |
| q40 | ✅ | ✅ | ✅ |
| q41 | ❌ fail | ✅ | ❌ fail |
| q42 | ✅ | ❌ fail | ✅ |
| q43 | ✅ | ✅ | ✅ |
| q44 | ✅ | ✅ | ❌ fail |
| q45 | ❌ fail | ✅ | ❌ fail |
| q46 | ✅ | ✅ | ✅ |
| q47 | ✅ | ✅ | ✅ |
| q48 | ✅ | ✅ | ✅ |
| q49 | ✅ | ❌ fail | ❌ fail |
| q50 | ❌ fail | ✅ | ❌ fail |
| q51 | ❌ fail | ❌ fail | ✅ |
| q52 | ✅ | ✅ | ✅ |
| q53 | ✅ | ✅ | ✅ |
| q54 | ✅ | ✅ | ✅ |
| q55 | ✅ | ✅ | ✅ |
| q56 | ❌ fail | ❌ fail | ❌ fail |
| q57 | ✅ | ❌ fail | ❌ fail |
| q58 | ❌ fail | ✅ | ✅ |
| q59 | ❌ fail | ✅ | ❌ fail |
| q60 | ✅ | ✅ | ✅ |
| q61 | ✅ | ✅ | ✅ |
| q62 | ✅ | ✅ | ❌ fail |
| q63 | ✅ | ✅ | ❌ fail |
| q64 | ✅ | ✅ | ✅ |
| q65 | ✅ | ✅ | ✅ |
| q66 | ✅ | ❌ fail | ✅ |
| q67 | ❌ fail | ✅ | ❌ fail |
| q68 | ✅ | ✅ | ✅ |
| q69 | ✅ | ✅ | ✅ |
| q70 | ✅ | ✅ | ✅ |
| q71 | ✅ | ✅ | ✅ |
| q72 | ✅ | ✅ | ❌ timeout |
| q73 | ✅ | ✅ | ✅ |
| q74 | ✅ | ✅ | ✅ |
| q75 | ❌ fail | ❌ fail | ❌ fail |
| q76 | ✅ | ✅ | ✅ |
| q77 | ❌ fail | ❌ fail | ❌ fail |
| q78 | ❌ fail | ❌ fail | ❌ fail |
| q79 | ❌ fail | ❌ fail | ❌ fail |
| q80 | ❌ fail | ❌ fail | ❌ fail |
| q81 | ❌ fail | ✅ | ❌ fail |
| q82 | ✅ | ✅ | ✅ |
| q83 | ✅ | ❌ fail | ✅ |
| q84 | ✅ | ✅ | ❌ fail |
| q85 | ✅ | ✅ | ✅ |
| q86 | ❌ fail | ✅ | ✅ |
| q87 | ✅ | ✅ | ✅ |
| q88 | ✅ | ✅ | ✅ |
| q89 | ✅ | ✅ | ✅ |
| q90 | ✅ | ✅ | ❌ fail |
| q91 | ✅ | ✅ | ✅ |
| q92 | ✅ | ✅ | ✅ |
| q93 | ✅ | ❌ fail | ❌ fail |
| q94 | ✅ | ❌ fail | ❌ fail |
| q95 | ✅ | ❌ fail | ✅ |
| q96 | ✅ | ✅ | ✅ |
| q97 | ✅ | ✅ | ❌ fail |
| q98 | ✅ | ❌ fail | ✅ |
| q99 | ✅ | ✅ | ❌ fail |
