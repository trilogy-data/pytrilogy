# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| db-only | 64/99 | q01, q03, q04, q05, q07, q08, q09, q10, q12, q13, q14, q16, q17, q19, q21, q22, q24, q25, q26, q28, q29, q32, q33, q34, q36, q37, q38, q39, q40, q41, q42, q43, q44, q46, q47, q48, q49, q52, q55, q57, q59, q60, q61, q62, q65, q68, q73, q74, q76, q82, q84, q85, q86, q87, q88, q89, q90, q91, q92, q94, q96, q97, q98, q99 | — |
| db+schema | 70/99 | q06, q15, q27, q30, q45, q51, q53, q63, q64, q66, q67, q71, q72, q78, q79 | q01, q16, q28, q29, q41, q47, q49, q86, q98 |
| ingest | 39/99 | q20, q58, q70 | q01, q05, q06, q13, q14, q15, q16, q24, q27, q30, q32, q34, q36, q38, q41, q42, q44, q46, q51, q53, q59, q60, q62, q63, q64, q66, q67, q68, q71, q72, q73, q74, q76, q78, q79, q84, q87, q88, q89, q90, q96, q97, q99 |
| enriched | 57/99 | q02, q11, q93, q95 | q04, q13, q14, q17, q21, q24, q25, q30, q34, q38, q39, q46, q49, q51, q58, q59, q62, q64, q66, q67, q68, q72, q73, q76, q78, q79, q84, q87, q88 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db-only | 0.65 | 14,151,790 |
| db+schema | 0.71 | 18,907,887 |
| ingest | 0.39 | 72,563,431 |
| enriched | 0.58 | 52,776,220 |

## Per-query matrix

| query | db-only | db+schema | ingest | enriched |
|---|---|---|---|---|
| q01 | ✅ | ❌ fail | ❌ fail | ✅ |
| q02 | ❌ fail | ❌ fail | ❌ fail | ✅ |
| q03 | ✅ | ✅ | ✅ | ✅ |
| q04 | ✅ | ✅ | ✅ | ❌ fail |
| q05 | ✅ | ✅ | ❌ timeout | ✅ |
| q06 | ❌ fail | ✅ | ❌ fail | ✅ |
| q07 | ✅ | ✅ | ✅ | ✅ |
| q08 | ✅ | ✅ | ✅ | ✅ |
| q09 | ✅ | ✅ | ✅ | ✅ |
| q10 | ✅ | ✅ | ✅ | ✅ |
| q11 | ❌ fail | ❌ fail | ❌ fail | ✅ |
| q12 | ✅ | ✅ | ✅ | ✅ |
| q13 | ✅ | ✅ | ❌ fail | ❌ fail |
| q14 | ✅ | ✅ | ❌ timeout | ❌ fail |
| q15 | ❌ fail | ✅ | ❌ fail | ✅ |
| q16 | ✅ | ❌ fail | ❌ fail | ✅ |
| q17 | ✅ | ✅ | ✅ | ❌ fail |
| q18 | ❌ fail | ❌ fail | ❌ fail | ❌ fail |
| q19 | ✅ | ✅ | ✅ | ✅ |
| q20 | ❌ fail | ❌ fail | ✅ | ✅ |
| q21 | ✅ | ✅ | ✅ | ❌ fail |
| q22 | ✅ | ✅ | ✅ | ✅ |
| q23 | ❌ fail | ❌ fail | ❌ fail | ❌ fail |
| q24 | ✅ | ✅ | ❌ fail | ❌ fail |
| q25 | ✅ | ✅ | ✅ | ❌ fail |
| q26 | ✅ | ✅ | ✅ | ✅ |
| q27 | ❌ fail | ✅ | ❌ fail | ✅ |
| q28 | ✅ | ❌ fail | ✅ | ✅ |
| q29 | ✅ | ❌ fail | ✅ | ✅ |
| q30 | ❌ fail | ✅ | ❌ fail | ❌ fail |
| q31 | ❌ fail | ❌ fail | ❌ fail | ❌ fail |
| q32 | ✅ | ✅ | ❌ fail | ✅ |
| q33 | ✅ | ✅ | ✅ | ✅ |
| q34 | ✅ | ✅ | ❌ fail | ❌ fail |
| q35 | ❌ fail | ❌ fail | ❌ fail | ❌ fail |
| q36 | ✅ | ✅ | ❌ fail | ✅ |
| q37 | ✅ | ✅ | ✅ | ✅ |
| q38 | ✅ | ✅ | ❌ exhausted | ❌ fail |
| q39 | ✅ | ✅ | ✅ | ❌ fail |
| q40 | ✅ | ✅ | ✅ | ✅ |
| q41 | ✅ | ❌ fail | ❌ fail | ✅ |
| q42 | ✅ | ✅ | ❌ fail | ✅ |
| q43 | ✅ | ✅ | ✅ | ✅ |
| q44 | ✅ | ✅ | ❌ fail | ✅ |
| q45 | ❌ fail | ✅ | ✅ | ✅ |
| q46 | ✅ | ✅ | ❌ fail | ❌ fail |
| q47 | ✅ | ❌ fail | ✅ | ✅ |
| q48 | ✅ | ✅ | ✅ | ✅ |
| q49 | ✅ | ❌ fail | ✅ | ❌ exhausted |
| q50 | ❌ fail | ❌ fail | ❌ fail | ❌ fail |
| q51 | ❌ fail | ✅ | ❌ fail | ❌ fail |
| q52 | ✅ | ✅ | ✅ | ✅ |
| q53 | ❌ fail | ✅ | ❌ fail | ✅ |
| q54 | ❌ fail | ❌ fail | ❌ fail | ❌ fail |
| q55 | ✅ | ✅ | ✅ | ✅ |
| q56 | ❌ fail | ❌ fail | ❌ exhausted | ❌ fail |
| q57 | ✅ | ✅ | ✅ | ✅ |
| q58 | ❌ fail | ❌ fail | ✅ | ❌ fail |
| q59 | ✅ | ✅ | ❌ fail | ❌ fail |
| q60 | ✅ | ✅ | ❌ fail | ✅ |
| q61 | ✅ | ✅ | ✅ | ✅ |
| q62 | ✅ | ✅ | ❌ fail | ❌ fail |
| q63 | ❌ fail | ✅ | ❌ fail | ✅ |
| q64 | ❌ fail | ✅ | ❌ fail | ❌ fail |
| q65 | ✅ | ✅ | ✅ | ✅ |
| q66 | ❌ fail | ✅ | ❌ fail | ❌ fail |
| q67 | ❌ fail | ✅ | ❌ fail | ❌ fail |
| q68 | ✅ | ✅ | ❌ fail | ❌ fail |
| q69 | ❌ fail | ❌ fail | ❌ fail | ❌ fail |
| q70 | ❌ fail | ❌ fail | ✅ | ✅ |
| q71 | ❌ fail | ✅ | ❌ fail | ✅ |
| q72 | ❌ fail | ✅ | ❌ fail | ❌ fail |
| q73 | ✅ | ✅ | ❌ fail | ❌ fail |
| q74 | ✅ | ✅ | ❌ fail | ✅ |
| q75 | ❌ fail | ❌ fail | ❌ fail | ❌ fail |
| q76 | ✅ | ✅ | ❌ fail | ❌ fail |
| q77 | ❌ fail | ❌ fail | ❌ fail | ❌ fail |
| q78 | ❌ fail | ✅ | ❌ fail | ❌ fail |
| q79 | ❌ fail | ✅ | ❌ fail | ❌ fail |
| q80 | ❌ fail | ❌ fail | ❌ fail | ❌ exhausted |
| q81 | ❌ fail | ❌ fail | ❌ fail | ❌ fail |
| q82 | ✅ | ✅ | ✅ | ✅ |
| q83 | ❌ fail | ❌ fail | ❌ fail | ❌ fail |
| q84 | ✅ | ✅ | ❌ fail | ❌ fail |
| q85 | ✅ | ✅ | ✅ | ✅ |
| q86 | ✅ | ❌ fail | ✅ | ✅ |
| q87 | ✅ | ✅ | ❌ exhausted | ❌ fail |
| q88 | ✅ | ✅ | ❌ fail | ❌ fail |
| q89 | ✅ | ✅ | ❌ fail | ✅ |
| q90 | ✅ | ✅ | ❌ fail | ✅ |
| q91 | ✅ | ✅ | ✅ | ✅ |
| q92 | ✅ | ✅ | ✅ | ✅ |
| q93 | ❌ fail | ❌ fail | ❌ fail | ✅ |
| q94 | ✅ | ✅ | ✅ | ✅ |
| q95 | ❌ fail | ❌ fail | ❌ fail | ✅ |
| q96 | ✅ | ✅ | ❌ fail | ✅ |
| q97 | ✅ | ✅ | ❌ fail | ✅ |
| q98 | ✅ | ❌ fail | ✅ | ✅ |
| q99 | ✅ | ✅ | ❌ fail | ✅ |
