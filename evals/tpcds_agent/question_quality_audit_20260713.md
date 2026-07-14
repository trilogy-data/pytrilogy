# TPC-DS question quality audit — 2026-07-13

## Scope

Reviewed all 99 prompts in `query_prompts.json`, with targeted comparison to
the canonical Trilogy queries for prompts with ambiguous grain, scope, output,
or ordering language. This audit covers question quality only; it does not
classify agent or framework failures.

## Priority 1: semantic or canonical mismatch

### Q05 — sale/return outlet terminology

The updated ordering language is now clear: order by the two displayed output
labels. One misleading phrase remains:

```text
returns ... use a non-null return store identifier by grouping
```

That branch covers store, catalog, and web entities. Replace `return store
identifier` with `return-side outlet identifier`. Also close the missing
parenthesis after the activity/exclusion sentence.

### Q42 — output columns are underspecified

The prompt says to report total sales by category, but the canonical returns:

1. year;
2. numeric category id;
3. category;
4. total extended sales price.

The ordering also names year and numeric category id, which hints at the
missing fields but does not unambiguously put them in the output. State the
four output columns and their order explicitly.

### Q65 — prompt adds ordering keys absent from the canonical

The prompt says:

```text
Order by store name, item description, store, then item
```

The canonical orders only by store name and item description. `store` and
`item` are also ambiguous between surrogate keys and displayed properties.
Remove the final `store, then item` unless the reference is intentionally
changed to include hidden tie-breakers.

### Q83 — percentage wording conflicts with ordinary interpretation

The canonical literal formula is:

```text
channel quantity / three-channel total / 3 * 100
```

Calling this “a percentage of the three-channel total averaged across
channels” invites the conventional but different formula
`channel / (total / 3) * 100`. Keep the benchmark's literal behavior, but name
it a score rather than a percentage and state the exact arithmetic once.

### Q84 — relationship and output grain are grammatically ambiguous

The current text has several competing readings:

```text
for each all demographic matches between any customer ... and any store return
on the customers customer-demographic profile type equals ...
```

Rewrite as two populations and one match:

- eligible customers: current home city and household-income restrictions;
- eligible store-return lines: recorded point-in-time customer-demographic SK;
- match any eligible customer to any eligible return line when their
  demographic SKs are equal;
- preserve one row per `(customer SK, ticket number, item SK)` but display only
  customer code and formatted name.

This wording is especially important because the customer need not be the
customer associated with the return.

## Priority 2: scope or identity is correct but hard to parse

### Q02

Remove the duplicated space and parenthetical fragment. Explicitly say that
the 2001 condition determines which base week-sequence rows are displayed; it
does not enter the weekday totals or split a week by calendar year.

### Q16

Fix `when calculation` to `when calculating`. State the two order-level tests
before the report-line filters: multiple warehouses and absence of any return
are determined from all lines of the order, independently of ship date,
shipping state, and call-center county.

### Q17

The opening sentence chains three facts and four time/key conditions without a
clear subject. Split it into store-sale, store-return, and catalog-sale
populations, followed by the two authored matches. Then list the 15 output
columns explicitly: three blocks of count, average, sample standard deviation,
and coefficient, after the three dimensions.

### Q18

The intended grain is clear, but the prompt exposes implementation details
(`first projecting`, `literal ROLLUP`, hidden grouping indicators). Retain the
required rollup hierarchy and display ordering, but phrase the line weighting
as business semantics: every catalog-sale line contributes once, including
when multiple lines share dimension values.

### Q23

This is one of the longest questions and mixes three population definitions,
two threshold calculations, and final output in a single paragraph. Break it
into named business stages: frequent items, best customers, qualifying
February sales, and final report. Add the missing sentence boundary after the
frequent-item definition.

### Q30

Fix `who who`, replace the semicolon after the threshold clause with a period,
and name the two states consistently:

- return-location state defines each total and its comparison average;
- current-home state `GA` is applied only after the threshold comparison.

### Q35

The output-order sentence is difficult to parse (`a group of 1. the field; 2.
the count...`). Replace it with an explicit ordered column list. Close the
missing parenthesis before the ordering sentence.

### Q39

Add the missing comma in `(warehouse SK, item SK, month-of-year)`. The two
month columns are constants after pairing, so ordering by them has no effect;
either remove those ordering requirements or explain that they are retained
only for canonical column-order parity.

### Q44

Avoid calling ascending average net profit “best” and descending average net
profit “worst”; those business labels contradict the numeric direction. Call
the arms `ascending-profit` and `descending-profit`, or explicitly say that
the benchmark uses those labels despite the counterintuitive direction.

### Q57

Replace “you'll need 1998/2000 boundary months” with the exact input window:
all months of 1999 plus December 1998 and January 2000. State that only 1999
rows survive after lag/lead calculation.

### Q59

Fix `in the next the year 2002`. “Keep only pairs where there is one week_seq”
could imply uniqueness validation; use “retain only matched pairs having both
a 2001 row and its 2002 counterpart.”

### Q64

The semantics appear specified, but the 1,900-character single paragraph is
hard to verify. Break it into four stages: eligible catalog-return items,
eligible store-sale/return rows, first aggregation, and 1999/2000 self-pair.
Keep the exact output column order in a separate final sentence.

### Q67

Change `coalescing a null product to zero` to `treating a null result of
per-unit sales price times quantity as zero`. “Product” can otherwise be read
as the item product.

### Q77

The prompt is accurate but implementation-heavy. Preserve the independent
date scopes and outlet mappings, while replacing “Union the channel results”
with “Combine the three channel result sets as rows.”

### Q81

Replace `in this circa 2000/not null set` with a direct scope statement:
compute the state average only from 2000 return totals having a recorded
return-address state. State explicitly that the final `GA` condition refers to
the returning customer's current home address and is applied after computing
the state averages.

### Q86

Fix `For web sales whose sold in the year 2000` to `For web sales sold in
2000`. The remainder accurately distinguishes category subtotals, detail rows,
and grand total.

### Q94

The parenthetical scope correction comes too late. Lead with the order-level
eligibility set, determined over all order lines and returns, and only then
describe the ship-date, state, and company filters applied to report lines.
This should mirror the clearer structure already used by Q95.

## Priority 3: mechanical cleanup

- Q08 contains mojibake in the ZIP-parameter parenthetical (`ā€”`).
- Q19 contains the same mojibake before “for each”.
- Q20 has a doubled space before `limit`.
- Q30 has `who who` and lowercase `limit` after a sentence boundary.
- Q35 has an unclosed parenthesis and inconsistent numbered-list punctuation.
- Q63 uses code-like `non_null store sks`; use “sales with a recorded store
  surrogate key.”
- Q84 has `customers customer-demographic`; use the possessive form and rewrite
  the sentence rather than applying only a typo fix.
- Q86 has `whose sold`.
- Q93 has `ke null customer sks`; use “keep null customer SKs as a null group.”
- Q97 uses `item key fk`; use `item surrogate key` consistently.

## Generally clear prompts

The remaining prompts are sufficiently explicit about their business
population, grain, measures, output order, and ordering for evaluation. Some
are necessarily long because the canonical TPC-DS logic is long; length alone
is not a reason to rewrite them. In particular, prompts that deliberately
distinguish point-of-sale attributes from current customer attributes should
retain that distinction.

## Recommended edit order

1. Fix Q42 and Q65 canonical mismatches.
2. Rewrite Q84, Q17, Q35, Q81, and Q30 for unambiguous population/scope flow.
3. Clarify Q83's literal formula.
4. Apply the mechanical cleanup batch.
5. Restructure Q23 and Q64 without changing their semantics.
6. Run a JSON parse check, then targeted replays for materially rewritten
   questions before another full baseline.

## Measured rewrite results

### Q01 — hidden output grain guidance accepted

The question requires one result row per `(return-customer SK, return-store SK)`
while displaying only customer business ID. Agents correctly computed the
surrogate-key aggregates but omitted the undisplayed keys from the final
select, so Trilogy deduplicated repeated customer IDs at the visible output
grain. The nested group-average syntax example now demonstrates preserving an
undisplayed group key with the `--` prefix.

| Guidance | Pass rate | Mean prompt tokens | Mean iterations |
|---|---:|---:|---:|
| Explicit surrogate-grain prompt only | 1/10 | 146,587 | 7.9 |
| Plus hidden group-key example | 9/10 | 156,010 | 8.2 |

The clearer surrogate-grain prompt and generic example are retained. Artifact:
`results/repeat_q01_20260714-122257_enriched`.

### Q05 — no further question change

The latest miss added a hidden rollup-level field as the leading sort key even
though the question says to sort only by channel label and entity identifier.
Current rollup guidance already says to use exactly the requested ordering and
explicitly warns that adding a level key changes which rows survive a limit.
This is an agent-instruction miss, not remaining question ambiguity.

### Q11 — same hidden identity pattern under evaluation

The latest candidate aggregated the four channel/year buckets by displayed
customer attributes rather than billing-customer SK. This can merge surrogate
customer versions and erase the canonical grain. Q11 is being replayed without
a prompt change after the generic hidden-group-key guidance improvement to test
whether the Q01 lift generalizes.

### Q02 — rejected

Compared ten enriched repetitions at scale factor 1:

| Version | Pass rate | Mean prompt tokens | Mean iterations |
|---|---:|---:|---:|
| Existing prompt | 10/10 | 599,188 | 17.3 |
| Explicit `week sequence + 53` rewrite | 5/10 | 620,879 | 19.2 |

The rewrite was reverted. Although it made the pairing more literal, it
encouraged less reliable direct-pair formulations and did not reduce churn.

Artifacts:

- Before: `results/repeat_q02_20260713-234809_enriched`
- After: `results/repeat_q02_20260713-235644_enriched`

### Q02 — richer default syntax example also rejected

With the original question restored, the existing period-over-period syntax
example was temporarily expanded to demonstrate aggregate-by-period-and-series,
partitioned `lead`, full window input, and a post-window display filter.

| Guidance | Pass rate | Mean prompt tokens | Mean iterations |
|---|---:|---:|---:|
| Existing compact example | 10/10 | 599,188 | 17.3 |
| Expanded multi-series example | 9/10 | 679,773 | 20.0 |

The expanded example was reverted. It increased both exploration and token
usage without improving correctness. The intervening
`repeat_q02_20260714-001906_enriched` run is invalid and excluded: all workers
were denied network access before their first model turn. Valid expanded-example
artifacts are in `results/repeat_q02_20260714-003158_enriched`.

### Q17 — explicit three-fact row-combination rewrite rejected

This comparison ran after the tuple-membership/discovery framework fix.

| Version | Pass rate | Mean prompt tokens | Mean iterations |
|---|---:|---:|---:|
| Existing prompt | 8/10 | 599,397 | 15.3 |
| Explicit row-level three-fact matching | 6/10 | 910,715 | 19.8 |

The rewrite was reverted. Explicitly describing the pre-aggregation fanout
encouraged agents to reconstruct three physical fact branches instead of
discovering the curated `catalog_store_returns` analysis model. Further lift
should come from model discovery or the analysis model's description, not a
more implementation-shaped business question.

Artifacts:

- Before: `results/repeat_q17_20260714-000622_enriched`
- After: `results/repeat_q17_20260714-011045_enriched`

### Q17 — curated-model description rejected

Clarifying the `catalog_store_returns` model description did not improve model
selection. With the original question unchanged, the pass rate fell from 8/10
to 6/10, mean prompt tokens rose from 599,397 to 642,530, and mean iterations
were effectively flat (15.3 to 15.1). The description change was reverted.

Artifact: `results/repeat_q17_20260714-015915_enriched`.

### Q59 — hidden store-SK clarification rejected

The original projected-key planner bug was verified fixed: minimized two-rowset
queries with store and offset-week scoped joins now plan correctly. The first
post-fix baseline nevertheless retained severe churn.

| Version | Pass rate | Mean prompt tokens | Mean iterations |
|---|---:|---:|---:|
| Post-fix existing prompt | 5/10 | 721,456 | 18.4 |
| Explicit hidden store-SK matching | 6/10 | 830,703 | 22.6 |

The identity clarification was reverted. Its small correctness increase did not
offset worse token use, two 50-iteration runs, and a 2.44M-token maximum. The
long traces require another framework investigation rather than further prompt
specificity.

Artifacts:

- Before: `results/repeat_q59_20260714-012345_enriched`
- After: `results/repeat_q59_20260714-013452_enriched`

### Q59 — matched-pair guidance improvement accepted

The generic self-pair syntax example previously demonstrated a row-preserving
`union join` even though its prose described matched period pairs. Adding
explicit presence filters reduced null-row churn, but the remaining long trace
showed that `union join` itself was still the wrong default for an intersection.
The example now uses a composite `subset join` when only matched pairs should
remain and reserves `union join` for comparisons that intentionally preserve
unmatched periods.

The intermediate presence-filter improvement already produced a clear gain:

| Guidance | Pass rate | Mean prompt tokens | Mean iterations |
|---|---:|---:|---:|
| Original self-pair example | 5/10 | 721,456 | 18.4 |
| Explicit union-side presence | 7/10 | 382,319 | 14.3 |

Artifact: `results/repeat_q59_20260714-015029_enriched`.

### Q17 — rejected

Compared ten enriched repetitions at scale factor 1 after the tuple-membership
discovery fix:

| Version | Pass rate | Mean prompt tokens | Mean iterations |
|---|---:|---:|---:|
| Existing prompt | 8/10 | 599,397 | 15.3 |
| Explicit three-population/match rewrite | 3/10 | 423,921 | 12.3 |

The rewrite was reverted. It reduced churn by causing agents to commit sooner,
but it pushed them toward manually rebuilding the three-way physical join and
substantially reduced correctness. The existing wording more often led agents
to the curated combined model.

Artifacts:

- Before: `results/repeat_q17_20260714-000622_enriched`
- After: `results/repeat_q17_20260714-001532_enriched`

### Q18 — accepted

Corrected two factual ambiguities: `coupon_amt` is the recorded per-line coupon
amount and must not be divided by quantity; output columns remain in rollup
order even though rows are sorted country/state/county/item.

| Version | Pass rate | Mean prompt tokens | Mean iterations |
|---|---:|---:|---:|
| Existing prompt | 7/10 | 236,476 | 10.0 |
| Coupon/output-order clarification | 9/10 | 262,619 | 10.8 |

The rewrite was retained. The modest token increase is acceptable for the
20-point correctness improvement.

Artifacts:

- Before: `results/repeat_q18_20260714-002634_enriched`
- After: `results/repeat_q18_20260714-003138_enriched`

### Q23 — blocked by framework defect

The existing prompt baseline passed 1/10, averaged 1,022,129 prompt tokens,
and exhausted one repetition at 2,572,923 tokens. Repetition 9 exposed a new
renderer bug: a supported scalar subquery was emitted as a `$1` bind parameter
whose Python value was a `SubqueryItem`, causing DuckDB
`NotImplementedException`.

Question rewriting stopped before an after-run because this is not a clean
wording experiment. See `bug_q23_scalar_subquery_rendered_as_duckdb_parameter.md`.

Artifact: `results/repeat_q23_20260714-003601_enriched`.
