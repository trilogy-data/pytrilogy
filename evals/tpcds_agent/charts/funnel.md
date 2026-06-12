# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| ingest | 40/99 | q03, q04, q07, q08, q09, q11, q12, q13, q17, q19, q20, q21, q22, q26, q28, q32, q33, q36, q37, q40, q42, q43, q44, q47, q48, q55, q57, q59, q61, q63, q70, q71, q82, q85, q86, q91, q92, q94, q95, q98 | — |
| enriched | 57/99 | q02, q06, q10, q15, q29, q31, q34, q39, q45, q46, q49, q52, q53, q58, q60, q68, q72, q74, q75, q80, q88, q89, q93, q96, q99 | q04, q32, q44, q47, q59, q70, q94, q95 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| ingest | 0.40 | 78,972,416 |
| enriched | 0.58 | 76,370,625 |

## Per-query matrix

| query | ingest | enriched |
|---|---|---|
| q01 | ❌ fail | ❌ fail |
| q02 | ❌ exhausted | ✅ |
| q03 | ✅ | ✅ |
| q04 | ✅ | ❌ fail |
| q05 | ❌ fail | ❌ crashed |
| q06 | ❌ fail | ✅ |
| q07 | ✅ | ✅ |
| q08 | ✅ | ✅ |
| q09 | ✅ | ✅ |
| q10 | ❌ fail | ✅ |
| q11 | ✅ | ✅ |
| q12 | ✅ | ✅ |
| q13 | ✅ | ✅ |
| q14 | ❌ exhausted | ❌ fail |
| q15 | ❌ fail | ✅ |
| q16 | ❌ fail | ❌ fail |
| q17 | ✅ | ✅ |
| q18 | ❌ fail | ❌ fail |
| q19 | ✅ | ✅ |
| q20 | ✅ | ✅ |
| q21 | ✅ | ✅ |
| q22 | ✅ | ✅ |
| q23 | ❌ fail | ❌ fail |
| q24 | ❌ fail | ❌ fail |
| q25 | ❌ fail | ❌ fail |
| q26 | ✅ | ✅ |
| q27 | ❌ fail | ❌ fail |
| q28 | ✅ | ✅ |
| q29 | ❌ fail | ✅ |
| q30 | ❌ fail | ❌ fail |
| q31 | ❌ fail | ✅ |
| q32 | ✅ | ❌ fail |
| q33 | ✅ | ✅ |
| q34 | ❌ fail | ✅ |
| q35 | ❌ fail | ❌ fail |
| q36 | ✅ | ✅ |
| q37 | ✅ | ✅ |
| q38 | ❌ fail | ❌ fail |
| q39 | ❌ fail | ✅ |
| q40 | ✅ | ✅ |
| q41 | ❌ fail | ❌ fail |
| q42 | ✅ | ✅ |
| q43 | ✅ | ✅ |
| q44 | ✅ | ❌ fail |
| q45 | ❌ fail | ✅ |
| q46 | ❌ fail | ✅ |
| q47 | ✅ | ❌ fail |
| q48 | ✅ | ✅ |
| q49 | ❌ exhausted | ✅ |
| q50 | ❌ fail | ❌ fail |
| q51 | ❌ fail | ❌ fail |
| q52 | ❌ error | ✅ |
| q53 | ❌ fail | ✅ |
| q54 | ❌ fail | ❌ fail |
| q55 | ✅ | ✅ |
| q56 | ❌ timeout | ❌ fail |
| q57 | ✅ | ✅ |
| q58 | ❌ timeout | ✅ |
| q59 | ✅ | ❌ fail |
| q60 | ❌ fail | ✅ |
| q61 | ✅ | ✅ |
| q62 | ❌ fail | ❌ error |
| q63 | ✅ | ✅ |
| q64 | ❌ exhausted | ❌ exhausted |
| q65 | ❌ fail | ❌ fail |
| q66 | ❌ fail | ❌ fail |
| q67 | ❌ fail | ❌ fail |
| q68 | ❌ fail | ✅ |
| q69 | ❌ fail | ❌ fail |
| q70 | ✅ | ❌ fail |
| q71 | ✅ | ✅ |
| q72 | ❌ fail | ✅ |
| q73 | ❌ fail | ❌ fail |
| q74 | ❌ fail | ✅ |
| q75 | ❌ fail | ✅ |
| q76 | ❌ fail | ❌ fail |
| q77 | ❌ fail | ❌ fail |
| q78 | ❌ exhausted | ❌ fail |
| q79 | ❌ fail | ❌ fail |
| q80 | ❌ timeout | ✅ |
| q81 | ❌ fail | ❌ fail |
| q82 | ✅ | ✅ |
| q83 | ❌ fail | ❌ fail |
| q84 | ❌ fail | ❌ fail |
| q85 | ✅ | ✅ |
| q86 | ✅ | ✅ |
| q87 | ❌ fail | ❌ fail |
| q88 | ❌ fail | ✅ |
| q89 | ❌ fail | ✅ |
| q90 | ❌ fail | ❌ fail |
| q91 | ✅ | ✅ |
| q92 | ✅ | ✅ |
| q93 | ❌ fail | ✅ |
| q94 | ✅ | ❌ fail |
| q95 | ✅ | ❌ fail |
| q96 | ❌ fail | ✅ |
| q97 | ❌ fail | ❌ fail |
| q98 | ✅ | ✅ |
| q99 | ❌ fail | ✅ |
