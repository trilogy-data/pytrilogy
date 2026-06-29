# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| db-only | 72/99 | q01, q03, q04, q05, q06, q07, q08, q09, q10, q11, q12, q13, q14, q15, q16, q17, q19, q21, q22, q25, q26, q27, q28, q30, q32, q33, q34, q35, q36, q37, q38, q39, q40, q41, q42, q43, q44, q46, q47, q48, q49, q52, q53, q55, q57, q58, q59, q60, q62, q63, q64, q65, q68, q69, q71, q72, q73, q74, q75, q76, q82, q83, q84, q85, q87, q88, q89, q91, q92, q94, q96, q99 | — |
| db+schema | 75/99 | q02, q29, q45, q61, q67, q70, q79, q81, q86, q90, q98 | q06, q30, q36, q49, q58, q69, q83, q84 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db-only | 0.73 | 13,556,169 |
| db+schema | 0.76 | 16,724,420 |

## Per-query matrix

| query | db-only | db+schema |
|---|---|---|
| q01 | ✅ | ✅ |
| q02 | ❌ fail | ✅ |
| q03 | ✅ | ✅ |
| q04 | ✅ | ✅ |
| q05 | ✅ | ✅ |
| q06 | ✅ | ❌ fail |
| q07 | ✅ | ✅ |
| q08 | ✅ | ✅ |
| q09 | ✅ | ✅ |
| q10 | ✅ | ✅ |
| q11 | ✅ | ✅ |
| q12 | ✅ | ✅ |
| q13 | ✅ | ✅ |
| q14 | ✅ | ✅ |
| q15 | ✅ | ✅ |
| q16 | ✅ | ✅ |
| q17 | ✅ | ✅ |
| q18 | ❌ fail | ❌ fail |
| q19 | ✅ | ✅ |
| q20 | ❌ fail | ❌ fail |
| q21 | ✅ | ✅ |
| q22 | ✅ | ✅ |
| q23 | ❌ fail | ❌ fail |
| q24 | ❌ fail | ❌ fail |
| q25 | ✅ | ✅ |
| q26 | ✅ | ✅ |
| q27 | ✅ | ✅ |
| q28 | ✅ | ✅ |
| q29 | ❌ fail | ✅ |
| q30 | ✅ | ❌ fail |
| q31 | ❌ fail | ❌ fail |
| q32 | ✅ | ✅ |
| q33 | ✅ | ✅ |
| q34 | ✅ | ✅ |
| q35 | ✅ | ✅ |
| q36 | ✅ | ❌ fail |
| q37 | ✅ | ✅ |
| q38 | ✅ | ✅ |
| q39 | ✅ | ✅ |
| q40 | ✅ | ✅ |
| q41 | ✅ | ✅ |
| q42 | ✅ | ✅ |
| q43 | ✅ | ✅ |
| q44 | ✅ | ✅ |
| q45 | ❌ fail | ✅ |
| q46 | ✅ | ✅ |
| q47 | ✅ | ✅ |
| q48 | ✅ | ✅ |
| q49 | ✅ | ❌ fail |
| q50 | ❌ fail | ❌ fail |
| q51 | ❌ fail | ❌ fail |
| q52 | ✅ | ✅ |
| q53 | ✅ | ✅ |
| q54 | ❌ fail | ❌ fail |
| q55 | ✅ | ✅ |
| q56 | ❌ fail | ❌ fail |
| q57 | ✅ | ✅ |
| q58 | ✅ | ❌ fail |
| q59 | ✅ | ✅ |
| q60 | ✅ | ✅ |
| q61 | ❌ fail | ✅ |
| q62 | ✅ | ✅ |
| q63 | ✅ | ✅ |
| q64 | ✅ | ✅ |
| q65 | ✅ | ✅ |
| q66 | ❌ fail | ❌ fail |
| q67 | ❌ fail | ✅ |
| q68 | ✅ | ✅ |
| q69 | ✅ | ❌ fail |
| q70 | ❌ fail | ✅ |
| q71 | ✅ | ✅ |
| q72 | ✅ | ✅ |
| q73 | ✅ | ✅ |
| q74 | ✅ | ✅ |
| q75 | ✅ | ✅ |
| q76 | ✅ | ✅ |
| q77 | ❌ fail | ❌ fail |
| q78 | ❌ fail | ❌ fail |
| q79 | ❌ fail | ✅ |
| q80 | ❌ fail | ❌ fail |
| q81 | ❌ fail | ✅ |
| q82 | ✅ | ✅ |
| q83 | ✅ | ❌ fail |
| q84 | ✅ | ❌ fail |
| q85 | ✅ | ✅ |
| q86 | ❌ fail | ✅ |
| q87 | ✅ | ✅ |
| q88 | ✅ | ✅ |
| q89 | ✅ | ✅ |
| q90 | ❌ fail | ✅ |
| q91 | ✅ | ✅ |
| q92 | ✅ | ✅ |
| q93 | ❌ fail | ❌ fail |
| q94 | ✅ | ✅ |
| q95 | ❌ fail | ❌ fail |
| q96 | ✅ | ✅ |
| q97 | ❌ fail | ❌ fail |
| q98 | ❌ fail | ✅ |
| q99 | ✅ | ✅ |
