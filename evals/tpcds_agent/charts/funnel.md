# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| db-only | 74/99 | q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11, q12, q13, q14, q19, q20, q21, q22, q24, q26, q28, q29, q32, q33, q34, q35, q36, q37, q38, q39, q40, q42, q43, q44, q46, q47, q48, q49, q52, q53, q54, q55, q56, q58, q60, q61, q62, q64, q65, q67, q68, q69, q71, q72, q73, q74, q75, q76, q78, q81, q82, q83, q84, q85, q87, q88, q89, q90, q91, q92, q94, q95, q96, q99 | — |
| db+schema | 71/99 | q15, q17, q25, q27, q45, q50, q57, q59, q63, q70, q86, q93, q98 | q14, q20, q24, q49, q56, q58, q67, q71, q75, q76, q78, q81, q83, q84, q94, q95 |
| ingest | 37/99 | q41 | q01, q02, q05, q14, q21, q24, q27, q34, q35, q38, q39, q45, q46, q47, q50, q56, q59, q61, q62, q63, q64, q65, q67, q68, q69, q70, q71, q72, q73, q74, q75, q76, q78, q81, q82, q83, q84, q85, q86, q87, q88, q89, q90, q91, q92, q93, q94, q95, q96, q98, q99 |
| enriched | 52/99 | — | q06, q11, q14, q17, q24, q25, q35, q41, q49, q50, q59, q60, q62, q64, q67, q70, q73, q74, q75, q76, q78, q81, q82, q86, q87, q88, q89, q90, q91, q92, q93, q94, q95, q96, q98, q99 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db-only | 0.75 | 0 |
| db+schema | 0.72 | 0 |
| ingest | 0.61 | 0 |
| enriched | 0.61 | 0 |

## Per-query matrix

| query | db-only | db+schema | ingest | enriched |
|---|---|---|---|---|
| q01 | ✅ | ✅ | ❌ fail | ✅ |
| q02 | ✅ | ✅ | ❌ fail | ✅ |
| q03 | ✅ | ✅ | ✅ | ✅ |
| q04 | ✅ | ✅ | ✅ | ✅ |
| q05 | ✅ | ✅ | ❌ fail | ✅ |
| q06 | ✅ | ✅ | ✅ | ❌ fail |
| q07 | ✅ | ✅ | ✅ | ✅ |
| q08 | ✅ | ✅ | ✅ | ✅ |
| q09 | ✅ | ✅ | ✅ | ✅ |
| q10 | ✅ | ✅ | ✅ | ✅ |
| q11 | ✅ | ✅ | ✅ | ❌ fail |
| q12 | ✅ | ✅ | ✅ | ✅ |
| q13 | ✅ | ✅ | ✅ | ✅ |
| q14 | ✅ | ❌ fail | ❌ fail | ❌ fail |
| q15 | ❌ fail | ✅ | ✅ | ✅ |
| q16 | ❌ fail | ❌ fail | ❌ fail | ❌ fail |
| q17 | ❌ fail | ✅ | ✅ | ❌ fail |
| q18 | ❌ fail | ❌ fail | ❌ fail | ❌ fail |
| q19 | ✅ | ✅ | ✅ | ✅ |
| q20 | ✅ | ❌ fail | ✅ | ✅ |
| q21 | ✅ | ✅ | ❌ fail | ✅ |
| q22 | ✅ | ✅ | ✅ | ✅ |
| q23 | ❌ fail | ❌ fail | ❌ fail | ❌ fail |
| q24 | ✅ | ❌ fail | ❌ fail | ❌ fail |
| q25 | ❌ fail | ✅ | ✅ | ❌ fail |
| q26 | ✅ | ✅ | ✅ | ✅ |
| q27 | ❌ fail | ✅ | ❌ fail | ✅ |
| q28 | ✅ | ✅ | ✅ | ✅ |
| q29 | ✅ | ✅ | ✅ | ✅ |
| q30 | ❌ fail | ❌ fail | ❌ fail | ❌ fail |
| q31 | ❌ fail | ❌ fail | ❌ fail | ❌ fail |
| q32 | ✅ | ✅ | ✅ | ✅ |
| q33 | ✅ | ✅ | ✅ | ✅ |
| q34 | ✅ | ✅ | ❌ fail | ✅ |
| q35 | ✅ | ✅ | ❌ fail | ❌ fail |
| q36 | ✅ | ✅ | ✅ | ✅ |
| q37 | ✅ | ✅ | ✅ | ✅ |
| q38 | ✅ | ✅ | ❌ error | ✅ |
| q39 | ✅ | ✅ | ❌ fail | ✅ |
| q40 | ✅ | ✅ | ✅ | ✅ |
| q41 | ❌ fail | ❌ fail | ✅ | ❌ fail |
| q42 | ✅ | ✅ | ✅ | ✅ |
| q43 | ✅ | ✅ | ✅ | ✅ |
| q44 | ✅ | ✅ | ✅ | ✅ |
| q45 | ❌ fail | ✅ | ❌ fail | ✅ |
| q46 | ✅ | ✅ | ❌ fail | ✅ |
| q47 | ✅ | ✅ | ❌ fail | ✅ |
| q48 | ✅ | ✅ | ✅ | ✅ |
| q49 | ✅ | ❌ fail | ✅ | ❌ fail |
| q50 | ❌ fail | ✅ | ❌ fail | ❌ fail |
| q51 | ❌ fail | ❌ fail | ❌ fail | ❌ fail |
| q52 | ✅ | ✅ | ✅ | ✅ |
| q53 | ✅ | ✅ | ✅ | ✅ |
| q54 | ✅ | ✅ | ✅ | ✅ |
| q55 | ✅ | ✅ | ✅ | ✅ |
| q56 | ✅ | ❌ fail | ❌ fail | ✅ |
| q57 | ❌ fail | ✅ | ✅ | ✅ |
| q58 | ✅ | ❌ fail | ✅ | ✅ |
| q59 | ❌ fail | ✅ | ❌ fail | ❌ fail |
| q60 | ✅ | ✅ | ✅ | ❌ fail |
| q61 | ✅ | ✅ | ❌ fail | ✅ |
| q62 | ✅ | ✅ | — | ❌ fail |
| q63 | ❌ fail | ✅ | — | ✅ |
| q64 | ✅ | ✅ | — | ❌ fail |
| q65 | ✅ | ✅ | — | ✅ |
| q66 | ❌ fail | ❌ fail | — | ❌ fail |
| q67 | ✅ | ❌ fail | — | ❌ fail |
| q68 | ✅ | ✅ | — | ✅ |
| q69 | ✅ | ✅ | — | ✅ |
| q70 | ❌ fail | ✅ | — | ❌ fail |
| q71 | ✅ | ❌ fail | — | ✅ |
| q72 | ✅ | ✅ | — | ✅ |
| q73 | ✅ | ✅ | — | ❌ fail |
| q74 | ✅ | ✅ | — | ❌ fail |
| q75 | ✅ | ❌ fail | — | ❌ fail |
| q76 | ✅ | ❌ fail | — | ❌ fail |
| q77 | ❌ fail | ❌ fail | — | ❌ fail |
| q78 | ✅ | ❌ fail | — | ❌ fail |
| q79 | ❌ fail | ❌ fail | — | ❌ fail |
| q80 | ❌ fail | ❌ fail | — | ❌ fail |
| q81 | ✅ | ❌ fail | — | ❌ fail |
| q82 | ✅ | ✅ | — | ❌ fail |
| q83 | ✅ | ❌ fail | — | ✅ |
| q84 | ✅ | ❌ fail | — | ✅ |
| q85 | ✅ | ✅ | — | ✅ |
| q86 | ❌ fail | ✅ | — | — |
| q87 | ✅ | ✅ | — | — |
| q88 | ✅ | ✅ | — | — |
| q89 | ✅ | ✅ | — | — |
| q90 | ✅ | ✅ | — | — |
| q91 | ✅ | ✅ | — | — |
| q92 | ✅ | ✅ | — | — |
| q93 | ❌ fail | ✅ | — | — |
| q94 | ✅ | ❌ fail | — | — |
| q95 | ✅ | ❌ fail | — | — |
| q96 | ✅ | ✅ | — | — |
| q97 | ❌ fail | ❌ fail | — | — |
| q98 | ❌ fail | ✅ | — | — |
| q99 | ✅ | ✅ | — | — |
