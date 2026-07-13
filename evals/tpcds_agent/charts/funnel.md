# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| ingest | 64/99 | q03, q04, q07, q08, q09, q10, q11, q12, q19, q20, q21, q22, q25, q28, q29, q30, q32, q33, q35, q36, q37, q39, q40, q41, q42, q43, q44, q47, q48, q49, q51, q52, q53, q54, q55, q57, q58, q59, q60, q61, q62, q63, q65, q67, q69, q70, q71, q76, q77, q78, q81, q82, q83, q86, q87, q88, q89, q91, q92, q93, q94, q95, q97, q98 | — |
| enriched | 79/99 | q02, q05, q06, q13, q15, q16, q23, q24, q26, q27, q34, q38, q45, q46, q50, q56, q66, q68, q73, q75, q79, q80, q84, q85, q90, q96 | q10, q21, q29, q30, q35, q47, q51, q59, q69, q77, q83 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| ingest | 0.65 | 56,444,424 |
| enriched | 0.80 | 35,916,902 |

## Per-query matrix

| query | ingest | enriched |
|---|---|---|
| q01 | ❌ fail | ❌ fail |
| q02 | ❌ fail | ✅ |
| q03 | ✅ | ✅ |
| q04 | ✅ | ✅ |
| q05 | ❌ fail | ✅ |
| q06 | ❌ fail | ✅ |
| q07 | ✅ | ✅ |
| q08 | ✅ | ✅ |
| q09 | ✅ | ✅ |
| q10 | ✅ | ❌ fail |
| q11 | ✅ | ✅ |
| q12 | ✅ | ✅ |
| q13 | ❌ fail | ✅ |
| q14 | ❌ fail | ❌ fail |
| q15 | ❌ fail | ✅ |
| q16 | ❌ fail | ✅ |
| q17 | ❌ fail | ❌ fail |
| q18 | ❌ fail | ❌ fail |
| q19 | ✅ | ✅ |
| q20 | ✅ | ✅ |
| q21 | ✅ | ❌ fail |
| q22 | ✅ | ✅ |
| q23 | ❌ fail | ✅ |
| q24 | ❌ fail | ✅ |
| q25 | ✅ | ✅ |
| q26 | ❌ fail | ✅ |
| q27 | ❌ fail | ✅ |
| q28 | ✅ | ✅ |
| q29 | ✅ | ❌ fail |
| q30 | ✅ | ❌ fail |
| q31 | ❌ fail | ❌ fail |
| q32 | ✅ | ✅ |
| q33 | ✅ | ✅ |
| q34 | ❌ fail | ✅ |
| q35 | ✅ | ❌ fail |
| q36 | ✅ | ✅ |
| q37 | ✅ | ✅ |
| q38 | ❌ fail | ✅ |
| q39 | ✅ | ✅ |
| q40 | ✅ | ✅ |
| q41 | ✅ | ✅ |
| q42 | ✅ | ✅ |
| q43 | ✅ | ✅ |
| q44 | ✅ | ✅ |
| q45 | ❌ fail | ✅ |
| q46 | ❌ fail | ✅ |
| q47 | ✅ | ❌ fail |
| q48 | ✅ | ✅ |
| q49 | ✅ | ✅ |
| q50 | ❌ fail | ✅ |
| q51 | ✅ | ❌ fail |
| q52 | ✅ | ✅ |
| q53 | ✅ | ✅ |
| q54 | ✅ | ✅ |
| q55 | ✅ | ✅ |
| q56 | ❌ fail | ✅ |
| q57 | ✅ | ✅ |
| q58 | ✅ | ✅ |
| q59 | ✅ | ❌ fail |
| q60 | ✅ | ✅ |
| q61 | ✅ | ✅ |
| q62 | ✅ | ✅ |
| q63 | ✅ | ✅ |
| q64 | ❌ fail | ❌ fail |
| q65 | ✅ | ✅ |
| q66 | ❌ fail | ✅ |
| q67 | ✅ | ✅ |
| q68 | ❌ fail | ✅ |
| q69 | ✅ | ❌ fail |
| q70 | ✅ | ✅ |
| q71 | ✅ | ✅ |
| q72 | ❌ fail | ❌ fail |
| q73 | ❌ fail | ✅ |
| q74 | ❌ fail | ❌ fail |
| q75 | ❌ fail | ✅ |
| q76 | ✅ | ✅ |
| q77 | ✅ | ❌ fail |
| q78 | ✅ | ✅ |
| q79 | ❌ fail | ✅ |
| q80 | ❌ fail | ✅ |
| q81 | ✅ | ✅ |
| q82 | ✅ | ✅ |
| q83 | ✅ | ❌ fail |
| q84 | ❌ fail | ✅ |
| q85 | ❌ exhausted | ✅ |
| q86 | ✅ | ✅ |
| q87 | ✅ | ✅ |
| q88 | ✅ | ✅ |
| q89 | ✅ | ✅ |
| q90 | ❌ fail | ✅ |
| q91 | ✅ | ✅ |
| q92 | ✅ | ✅ |
| q93 | ✅ | ✅ |
| q94 | ✅ | ✅ |
| q95 | ✅ | ✅ |
| q96 | ❌ fail | ✅ |
| q97 | ✅ | ✅ |
| q98 | ✅ | ✅ |
| q99 | ❌ fail | ❌ fail |
