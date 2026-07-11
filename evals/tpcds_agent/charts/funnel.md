# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| db-only | 75/99 | q01, q02, q03, q04, q05, q07, q08, q09, q10, q11, q12, q13, q15, q16, q18, q19, q21, q22, q24, q25, q26, q27, q28, q29, q32, q33, q34, q35, q36, q37, q38, q39, q40, q42, q44, q46, q47, q48, q50, q52, q53, q54, q55, q57, q58, q59, q60, q61, q62, q63, q64, q65, q67, q68, q69, q71, q73, q74, q75, q76, q77, q78, q82, q84, q85, q87, q88, q89, q90, q91, q92, q94, q95, q96, q99 | — |
| db+schema | 82/99 | q06, q14, q17, q20, q45, q49, q51, q72, q79, q80, q81, q86, q93 | q02, q16, q18, q27, q36, q58 |
| enriched | 80/99 | q23, q30, q41, q43, q70, q83, q97, q98 | q11, q14, q16, q17, q25, q26, q27, q29, q47, q51, q57, q67, q79, q81, q82, q95 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db-only | 0.76 | 13,037,651 |
| db+schema | 0.83 | 18,075,244 |
| enriched | 0.81 | 32,624,749 |

## Per-query matrix

| query | db-only | db+schema | enriched |
|---|---|---|---|
| q01 | ✅ | ✅ | ✅ |
| q02 | ✅ | ❌ fail | ✅ |
| q03 | ✅ | ✅ | ✅ |
| q04 | ✅ | ✅ | ✅ |
| q05 | ✅ | ✅ | ✅ |
| q06 | ❌ fail | ✅ | ✅ |
| q07 | ✅ | ✅ | ✅ |
| q08 | ✅ | ✅ | ✅ |
| q09 | ✅ | ✅ | ✅ |
| q10 | ✅ | ✅ | ✅ |
| q11 | ✅ | ✅ | ❌ fail |
| q12 | ✅ | ✅ | ✅ |
| q13 | ✅ | ✅ | ✅ |
| q14 | ❌ fail | ✅ | ❌ fail |
| q15 | ✅ | ✅ | ✅ |
| q16 | ✅ | ❌ fail | ❌ fail |
| q17 | ❌ fail | ✅ | ❌ fail |
| q18 | ✅ | ❌ fail | ✅ |
| q19 | ✅ | ✅ | ✅ |
| q20 | ❌ fail | ✅ | ✅ |
| q21 | ✅ | ✅ | ✅ |
| q22 | ✅ | ✅ | ✅ |
| q23 | ❌ fail | ❌ fail | ✅ |
| q24 | ✅ | ✅ | ✅ |
| q25 | ✅ | ✅ | ❌ fail |
| q26 | ✅ | ✅ | ❌ fail |
| q27 | ✅ | ❌ fail | ❌ fail |
| q28 | ✅ | ✅ | ✅ |
| q29 | ✅ | ✅ | ❌ fail |
| q30 | ❌ fail | ❌ fail | ✅ |
| q31 | ❌ fail | ❌ fail | ❌ fail |
| q32 | ✅ | ✅ | ✅ |
| q33 | ✅ | ✅ | ✅ |
| q34 | ✅ | ✅ | ✅ |
| q35 | ✅ | ✅ | ✅ |
| q36 | ✅ | ❌ fail | ✅ |
| q37 | ✅ | ✅ | ✅ |
| q38 | ✅ | ✅ | ✅ |
| q39 | ✅ | ✅ | ✅ |
| q40 | ✅ | ✅ | ✅ |
| q41 | ❌ fail | ❌ fail | ✅ |
| q42 | ✅ | ✅ | ✅ |
| q43 | ❌ fail | ❌ fail | ✅ |
| q44 | ✅ | ✅ | ✅ |
| q45 | ❌ fail | ✅ | ✅ |
| q46 | ✅ | ✅ | ✅ |
| q47 | ✅ | ✅ | ❌ fail |
| q48 | ✅ | ✅ | ✅ |
| q49 | ❌ fail | ✅ | ✅ |
| q50 | ✅ | ✅ | ✅ |
| q51 | ❌ fail | ✅ | ❌ fail |
| q52 | ✅ | ✅ | ✅ |
| q53 | ✅ | ✅ | ✅ |
| q54 | ✅ | ✅ | ✅ |
| q55 | ✅ | ✅ | ✅ |
| q56 | ❌ fail | ❌ fail | ❌ fail |
| q57 | ✅ | ✅ | ❌ fail |
| q58 | ✅ | ❌ fail | ✅ |
| q59 | ✅ | ✅ | ✅ |
| q60 | ✅ | ✅ | ✅ |
| q61 | ✅ | ✅ | ✅ |
| q62 | ✅ | ✅ | ✅ |
| q63 | ✅ | ✅ | ✅ |
| q64 | ✅ | ✅ | ✅ |
| q65 | ✅ | ✅ | ✅ |
| q66 | ❌ fail | ❌ fail | ❌ fail |
| q67 | ✅ | ✅ | ❌ fail |
| q68 | ✅ | ✅ | ✅ |
| q69 | ✅ | ✅ | ✅ |
| q70 | ❌ fail | ❌ fail | ✅ |
| q71 | ✅ | ✅ | ✅ |
| q72 | ❌ fail | ✅ | ✅ |
| q73 | ✅ | ✅ | ✅ |
| q74 | ✅ | ✅ | ✅ |
| q75 | ✅ | ✅ | ✅ |
| q76 | ✅ | ✅ | ✅ |
| q77 | ✅ | ✅ | ✅ |
| q78 | ✅ | ✅ | ✅ |
| q79 | ❌ fail | ✅ | ❌ fail |
| q80 | ❌ fail | ✅ | ✅ |
| q81 | ❌ fail | ✅ | ❌ fail |
| q82 | ✅ | ✅ | ❌ fail |
| q83 | ❌ fail | ❌ fail | ✅ |
| q84 | ✅ | ✅ | ✅ |
| q85 | ✅ | ✅ | ✅ |
| q86 | ❌ fail | ✅ | ✅ |
| q87 | ✅ | ✅ | ✅ |
| q88 | ✅ | ✅ | ✅ |
| q89 | ✅ | ✅ | ✅ |
| q90 | ✅ | ✅ | ✅ |
| q91 | ✅ | ✅ | ✅ |
| q92 | ✅ | ✅ | ✅ |
| q93 | ❌ fail | ✅ | ✅ |
| q94 | ✅ | ✅ | ✅ |
| q95 | ✅ | ✅ | ❌ fail |
| q96 | ✅ | ✅ | ✅ |
| q97 | ❌ fail | ❌ fail | ✅ |
| q98 | ❌ fail | ❌ fail | ✅ |
| q99 | ✅ | ✅ | ✅ |
