# Parameter-shifted TPC-DS refs (queries 01-20) — validation

Shifted variants of the first 20 TPC-DS eval queries so the canonical benchmark
parameter values (manufacturer 128, month 11, state TN, year 2000, ...) are not
recognizable. Structure, ordering, LIMITs, and output columns are byte-identical
to the canonical files in `tests/modeling/tpc_ds_duckdb/` — only parameter
literals changed. Baseline is the repo's canonical FILE (which already deviates
from PRAGMA defaults in places), not the PRAGMA spec.

Validated 2026-08-12 against `evals/tpcds_agent/.cache/tpcds_sf1.duckdb`
(opened read_only). Every shifted query returns non-empty results and its
output values differ from the canonical run (first-rows comparison; for
single-row aggregates, the values themselves were checked non-NULL and
different).

| query | canonical rows | shifted rows | params changed (old → new) | notes |
|---|---|---|---|---|
| 01 | 100 | 100 | d_year 2000 → 1999 | s_state 'TN' NOT shiftable: all 12 stores at SF=1 are TN. 1.2 multiplier is spec-fixed, kept. |
| 02 | 53 | 52 | year pair 2001/2001+1 → 1999/1999+1 | Day-of-week names and the 53-week offset are structural, kept. |
| 03 | 89 | 100 | i_manufact_id 128 → 277; d_moy 11 → 12 | Shifted hits the 100-row LIMIT (canonical 89). |
| 04 | 6 | 7 | year pair 2001/2001+1 → 1999/1999+1 (all six channel/year cells) | |
| 05 | 100 | 100 | date window 2000-08-23..2000-09-06 → 1999-06-10..1999-06-24 | Same 14-day span, all 3 channel CTEs shifted together. |
| 06 | 46 | 45 | d_year 2001 → 2000; d_moy 1 → 5 | 1.2 price multiplier is spec-fixed, kept. |
| 07 | 100 | 100 | gender M → F; marital S → W; education College → 4 yr Degree; d_year 2000 → 2001 | Promotion channel flags ('N') structural, kept. |
| 08 | 5 | 5 | d_qoy 2 → 3; d_year 1998 → 2000 | 400-zip curated list untouched (runtime `zips` param, per instructions). |
| 09 | 1 | 1 | thresholds 74129/122840/56580/10097/165306 → 631342/98763/743812/27452/601159 | Bucket ranges (1-20 ... 81-100) and r_reason_sk=1 structural, kept. New thresholds flip buckets 1/3/5 to the avg-net-paid branch (canonical: all avg-discount), so output columns 1, 3, 5 visibly differ; 2 and 4 keep the same branch. |
| 10 | 6 | 6 | counties Rush/Toole/Jefferson/Dona Ana/La Porte → Murray/Crook/Union/Sioux/Elbert; d_year 2002 → 2001; d_moy window 1..1+3 → 2..2+3 | New county set chosen to match canonical customer volume (~930 customers). |
| 11 | 90 | 97 | year pair 2001/2001+1 → 1999/1999+1 (all four channel/year cells) | |
| 12 | 100 | 100 | categories Sports/Books/Home → Music/Jewelry/Shoes; dates 1999-02-22..1999-03-24 → 2001-05-12..2001-06-11 | Same 30-day span. |
| 13 | 1 | 1 | d_year 2001 → 1998; demo combos (M,Advanced Degree)/(S,College)/(W,2 yr Degree) → (D,Secondary)/(U,Primary)/(M,4 yr Degree); state triples (TX,OH,TX)/(OR,NM,KY)/(VA,TX,MS) → (GA,MO,GA)/(IA,NC,NE)/(KY,GA,MN) | Sales-price and net-profit ranges kept (selectivity anchors); duplicate-state pattern in first triple preserved. All four aggregates non-NULL and differ from canonical. |
| 14 | 100 | 100 | year frame 1999..1999+2 → 1998..1998+2 (all 6 occurrences); target year 1999+2 → 1998+2; d_moy 11 → 12 | Whole frame shifted coherently. |
| 15 | 100 | 100 | 9 zips 85669...81792 → 54975,69532,75804,71087,68877,60169,74289,50411,56614; states CA/WA/GA → NC/IL/MN; d_qoy 2 → 3; d_year 2001 → 2000 | >500 price literal is spec-fixed, kept. All 9 new zips verified present in customer_address. |
| 16 | 1 | 1 | ship-date window 2002-02-01..2002-04-02 → 2001-05-01..2001-06-30; ca_state GA → KY | Same 60-day span. cc_county 'Williamson County' NOT shiftable: all 6 call centers at SF=1 are Williamson County. Shifted: 102 orders vs canonical 233. |
| 17 | 23 | 18 | d1.d_year 2001 → 1999; d2/d3 year lists (2001,2002) → (1999,2000) | |
| 18 | 100 | 100 | gender F → M; education Unknown → Primary; birth months (1,6,8,9,12,2) → (3,4,5,7,10,11); d_year 1998 → 2000; states (MS,IN,ND,OK,NM,VA,MS) → (KS,IL,MN,FL,WI,SD,KS) | 7-literal state list with one duplicate preserved (canonical duplicates MS; shifted duplicates KS). |
| 19 | 100 | 100 | i_manager_id 8 → 20; d_moy 11 → 12; d_year 1998 → 2001 | zip-mismatch condition structural, kept. |
| 20 | 100 | 100 | categories Sports/Books/Home → Music/Jewelry/Shoes; dates 1999-02-22..1999-03-24 → 2001-05-12..2001-06-11 | Same shift family as q12 (q20 is the catalog twin). |

## Partial / non-shiftable parameters (data constraints at SF=1)

- **q01 `s_state = 'TN'`** — every store row is TN; any other state empties the result. Year shifted instead.
- **q16 `cc_county = 'Williamson County'`** — every call center row is Williamson County. Date window + ship state shifted instead.
- Spec-fixed (non-parameter) literals kept everywhere: 1.2 multipliers (q01, q06), q09 bucket ranges, q15 `> 500`, promotion flags (q07), day names (q02).

## Prompts

`prompts_shifted.json` holds `{"id", "prompt_shifted"}` for ids 1-20: the
original prompt text verbatim with only parameter mentions (years, months,
ids, categories, states, counties, zips, thresholds, derived phrases like
"though ratios compare to ~2002 ranges") updated to the shifted values.
q08's `params` zip list is unchanged and is still expected to be injected at
runtime.
