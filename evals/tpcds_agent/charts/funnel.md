# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| ingest | 46/99 | q01, q03, q04, q07, q08, q09, q10, q11, q12, q13, q16, q19, q20, q21, q22, q26, q27, q28, q29, q32, q33, q34, q37, q39, q42, q43, q45, q47, q48, q52, q53, q55, q57, q61, q63, q72, q74, q82, q85, q86, q89, q91, q92, q94, q95, q98 | — |
| enriched | 47/99 | q06, q25, q36, q41, q49, q58, q60, q68, q70, q71, q93, q96, q99 | q01, q12, q13, q20, q27, q32, q33, q34, q45, q72, q82, q95 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| ingest | 0.47 | 78,072,228 |
| enriched | 0.47 | 56,846,367 |

## Per-query matrix

| query | ingest | enriched |
|---|---|---|
| q01 | ✅ | ❌ fail |
| q02 | ❌ fail | ❌ exhausted |
| q03 | ✅ | ✅ |
| q04 | ✅ | ✅ |
| q05 | ❌ timeout | ❌ fail |
| q06 | ❌ fail | ✅ |
| q07 | ✅ | ✅ |
| q08 | ✅ | ✅ |
| q09 | ✅ | ✅ |
| q10 | ✅ | ✅ |
| q11 | ✅ | ✅ |
| q12 | ✅ | ❌ fail |
| q13 | ✅ | ❌ fail |
| q14 | ❌ fail | ❌ fail |
| q15 | ❌ fail | ❌ fail |
| q16 | ✅ | ✅ |
| q17 | ❌ fail | ❌ fail |
| q18 | ❌ fail | ❌ fail |
| q19 | ✅ | ✅ |
| q20 | ✅ | ❌ fail |
| q21 | ✅ | ✅ |
| q22 | ✅ | ✅ |
| q23 | ❌ fail | ❌ fail |
| q24 | ❌ fail | ❌ fail |
| q25 | ❌ fail | ✅ |
| q26 | ✅ | ✅ |
| q27 | ✅ | ❌ fail |
| q28 | ✅ | ✅ |
| q29 | ✅ | ✅ |
| q30 | ❌ fail | ❌ fail |
| q31 | ❌ fail | ❌ fail |
| q32 | ✅ | ❌ fail |
| q33 | ✅ | ❌ fail |
| q34 | ✅ | ❌ fail |
| q35 | ❌ fail | ❌ fail |
| q36 | ❌ fail | ✅ |
| q37 | ✅ | ✅ |
| q38 | ❌ exhausted | ❌ fail |
| q39 | ✅ | ✅ |
| q40 | ❌ fail | ❌ fail |
| q41 | ❌ fail | ✅ |
| q42 | ✅ | ✅ |
| q43 | ✅ | ✅ |
| q44 | ❌ fail | ❌ fail |
| q45 | ✅ | ❌ fail |
| q46 | ❌ fail | ❌ fail |
| q47 | ✅ | ✅ |
| q48 | ✅ | ✅ |
| q49 | ❌ exhausted | ✅ |
| q50 | ❌ fail | ❌ fail |
| q51 | ❌ fail | ❌ fail |
| q52 | ✅ | ✅ |
| q53 | ✅ | ✅ |
| q54 | ❌ fail | ❌ fail |
| q55 | ✅ | ✅ |
| q56 | ❌ fail | ❌ fail |
| q57 | ✅ | ✅ |
| q58 | ❌ fail | ✅ |
| q59 | ❌ exhausted | ❌ fail |
| q60 | ❌ fail | ✅ |
| q61 | ✅ | ✅ |
| q62 | ❌ fail | ❌ fail |
| q63 | ✅ | ✅ |
| q64 | ❌ fail | ❌ fail |
| q65 | ❌ fail | ❌ fail |
| q66 | ❌ fail | ❌ fail |
| q67 | ❌ fail | ❌ fail |
| q68 | ❌ fail | ✅ |
| q69 | ❌ fail | ❌ fail |
| q70 | ❌ fail | ✅ |
| q71 | ❌ fail | ✅ |
| q72 | ✅ | ❌ fail |
| q73 | ❌ fail | ❌ fail |
| q74 | ✅ | ✅ |
| q75 | ❌ fail | ❌ fail |
| q76 | ❌ exhausted | ❌ fail |
| q77 | ❌ timeout | ❌ fail |
| q78 | ❌ exhausted | ❌ exhausted |
| q79 | ❌ fail | ❌ fail |
| q80 | ❌ exhausted | ❌ fail |
| q81 | ❌ fail | ❌ fail |
| q82 | ✅ | ❌ fail |
| q83 | ❌ fail | ❌ fail |
| q84 | ❌ fail | ❌ fail |
| q85 | ✅ | ✅ |
| q86 | ✅ | ✅ |
| q87 | ❌ fail | ❌ fail |
| q88 | ❌ fail | ❌ fail |
| q89 | ✅ | ✅ |
| q90 | ❌ fail | ❌ fail |
| q91 | ✅ | ✅ |
| q92 | ✅ | ✅ |
| q93 | ❌ fail | ✅ |
| q94 | ✅ | ✅ |
| q95 | ✅ | ❌ fail |
| q96 | ❌ fail | ✅ |
| q97 | ❌ exhausted | ❌ fail |
| q98 | ✅ | ✅ |
| q99 | ❌ fail | ✅ |
