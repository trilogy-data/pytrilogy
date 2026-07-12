# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| db-only | 77/99 | q01, q03, q04, q05, q07, q09, q10, q11, q12, q13, q14, q15, q16, q19, q21, q22, q24, q25, q26, q27, q28, q29, q30, q32, q33, q34, q35, q36, q37, q38, q39, q40, q43, q44, q45, q46, q48, q49, q50, q52, q53, q54, q55, q56, q59, q60, q62, q64, q65, q67, q68, q69, q70, q71, q72, q73, q74, q75, q76, q77, q78, q81, q82, q84, q85, q87, q88, q89, q90, q91, q92, q93, q94, q95, q96, q97, q99 | — |
| enriched | 79/99 | q02, q06, q08, q17, q23, q31, q41, q42, q47, q57, q58, q61, q63, q80, q83, q98 | q01, q14, q16, q38, q44, q55, q67, q72, q76, q81, q87, q92, q94, q95 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db-only | 0.78 | 14,822,138 |
| enriched | 0.80 | 27,923,654 |

## Per-query matrix

| query | db-only | enriched |
|---|---|---|
| q01 | ✅ | ❌ fail |
| q02 | ❌ fail | ✅ |
| q03 | ✅ | ✅ |
| q04 | ✅ | ✅ |
| q05 | ✅ | ✅ |
| q06 | ❌ fail | ✅ |
| q07 | ✅ | ✅ |
| q08 | ❌ fail | ✅ |
| q09 | ✅ | ✅ |
| q10 | ✅ | ✅ |
| q11 | ✅ | ✅ |
| q12 | ✅ | ✅ |
| q13 | ✅ | ✅ |
| q14 | ✅ | ❌ fail |
| q15 | ✅ | ✅ |
| q16 | ✅ | ❌ fail |
| q17 | ❌ fail | ✅ |
| q18 | ❌ fail | ❌ fail |
| q19 | ✅ | ✅ |
| q20 | ❌ fail | ❌ fail |
| q21 | ✅ | ✅ |
| q22 | ✅ | ✅ |
| q23 | ❌ fail | ✅ |
| q24 | ✅ | ✅ |
| q25 | ✅ | ✅ |
| q26 | ✅ | ✅ |
| q27 | ✅ | ✅ |
| q28 | ✅ | ✅ |
| q29 | ✅ | ✅ |
| q30 | ✅ | ✅ |
| q31 | ❌ fail | ✅ |
| q32 | ✅ | ✅ |
| q33 | ✅ | ✅ |
| q34 | ✅ | ✅ |
| q35 | ✅ | ✅ |
| q36 | ✅ | ✅ |
| q37 | ✅ | ✅ |
| q38 | ✅ | ❌ fail |
| q39 | ✅ | ✅ |
| q40 | ✅ | ✅ |
| q41 | ❌ fail | ✅ |
| q42 | ❌ missing | ✅ |
| q43 | ✅ | ✅ |
| q44 | ✅ | ❌ fail |
| q45 | ✅ | ✅ |
| q46 | ✅ | ✅ |
| q47 | ❌ fail | ✅ |
| q48 | ✅ | ✅ |
| q49 | ✅ | ✅ |
| q50 | ✅ | ✅ |
| q51 | ❌ fail | ❌ fail |
| q52 | ✅ | ✅ |
| q53 | ✅ | ✅ |
| q54 | ✅ | ✅ |
| q55 | ✅ | ❌ fail |
| q56 | ✅ | ✅ |
| q57 | ❌ fail | ✅ |
| q58 | ❌ fail | ✅ |
| q59 | ✅ | ✅ |
| q60 | ✅ | ✅ |
| q61 | ❌ fail | ✅ |
| q62 | ✅ | ✅ |
| q63 | ❌ fail | ✅ |
| q64 | ✅ | ✅ |
| q65 | ✅ | ✅ |
| q66 | ❌ fail | ❌ fail |
| q67 | ✅ | ❌ fail |
| q68 | ✅ | ✅ |
| q69 | ✅ | ✅ |
| q70 | ✅ | ✅ |
| q71 | ✅ | ✅ |
| q72 | ✅ | ❌ fail |
| q73 | ✅ | ✅ |
| q74 | ✅ | ✅ |
| q75 | ✅ | ✅ |
| q76 | ✅ | ❌ fail |
| q77 | ✅ | ✅ |
| q78 | ✅ | ✅ |
| q79 | ❌ fail | ❌ fail |
| q80 | ❌ fail | ✅ |
| q81 | ✅ | ❌ fail |
| q82 | ✅ | ✅ |
| q83 | ❌ fail | ✅ |
| q84 | ✅ | ✅ |
| q85 | ✅ | ✅ |
| q86 | ❌ fail | ❌ fail |
| q87 | ✅ | ❌ fail |
| q88 | ✅ | ✅ |
| q89 | ✅ | ✅ |
| q90 | ✅ | ✅ |
| q91 | ✅ | ✅ |
| q92 | ✅ | ❌ fail |
| q93 | ✅ | ✅ |
| q94 | ✅ | ❌ fail |
| q95 | ✅ | ❌ fail |
| q96 | ✅ | ✅ |
| q97 | ✅ | ✅ |
| q98 | ❌ fail | ✅ |
| q99 | ✅ | ✅ |
