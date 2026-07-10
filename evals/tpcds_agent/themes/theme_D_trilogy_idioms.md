# Theme D — Trilogy idioms (5 of 23)

**One line:** genuine Trilogy authoring idioms the agent got wrong — the prompt is clear and the
canonical `.preql` is correct, but the idiom is non-obvious and the guidance/examples don't cover
the exact wrinkle. Fix = guidance / syntax-example additions (+ one prompt replicate).

---

## D1 — Benchmark-average scope contaminated by an output filter (q30, q81)

Comparing each entity to a population-wide average (e.g. "> 1.2× the average return per state")
while the OUTPUT is filtered to a subset (e.g. home state = GA). A plain `sum(x) by k` referenced
in `HAVING`, with the subset filter in the top-level `where`, makes Trilogy push that filter INTO
the aggregate-input CTE — so the benchmark average is taken over the FILTERED subset, not the
whole population.

- **Quote (q81):** *"compare each customer's total to 1.2× the average of all customer totals in
  their state … Also filter: customer's own current home address state = 'GA'."* — understood the
  average must span "all customer totals," yet lumped the GA filter into the same top-level WHERE,
  not realizing WHERE narrows the `avg()` input.
- **Error signature (q81):** 382 rows vs ref 367 (+43/−28); the per-state threshold was lowered
  because the average was computed over the GA-home subset only.
- **The `nested-aggregate-group-average` syntax example is copied verbatim and does not show this
  case** (it has no secondary output filter that must stay OUT of the aggregate input).

**Fix:** (a) q81 is the exact twin of q30 (already reworded) — replicate the q30 prompt
clarification. (b) update the `nested-aggregate-group-average` example with the "compute the
benchmark over the full population, then filter the output rows without contaminating it — put the
threshold in `WHERE` (computes over WHERE-unfiltered input) or stage the average in a separate
rowset" note.

## D2 — Filter groups BEFORE the rollup, not after (q14)

"Keep only leaf groups exceeding the average, THEN roll up" requires staging the surviving leaves
in a rowset and rolling up the survivors. The agent used `by rollup (dims) … having total > avg`,
which filters every level AFTER the rollup — so subtotals aggregate below-average combos too.
Already diagnosed + prompt reworded; the general idiom (filter-leaves-then-rollup vs
having-on-rollup) belongs in the `rollup` example NOTES.

## D3 — Window functions must see the full set (compute before a row-reducing filter) (q47)

`lag`/`lead`/moving windows must be computed BEFORE any `where`/`having` that prunes rows,
otherwise the window skips filtered-out neighbors. The agent isolated `row_number` in an
unfiltered CTE but inlined `lag`/`lead` in the filtered final select, so `lead` jumped over a
dropped neighbor month.

- **Quote (q47):** plan step *"4. Filter … deviation > 0.1  /  5. Add lag and lead from adjacent
  months"* — intended filter-then-neighbors. It even read the harness warning *"BE CAREFUL with
  window functions and the where clause - you must have all the rows required for the window
  range."*
- **Fix:** the guidance exists but isn't sticky — strengthen the `window-period-over-period`
  example: derive `lag`/`lead` as `auto`/rowset window concepts at the aggregate grain (before the
  final HAVING), exactly as the canonical does.

## D4 — Counting over union arms / set-ops: count a guaranteed-non-null key, and pre-aggregate in the arm (q76, q38)

Two faces of Trilogy's "count(k) counts DISTINCT non-null k" + "union/rowset arms dedup to their
select grain":
- **q76:** selected a raw `row_counter` (constant 1) into each `union(...)` arm with no aggregate,
  then `count(row_counter)` outside. Each arm dedups to grain → count undercounts (catalog/Books
  Q1: 3 vs 9). Canonical pre-aggregates `sum(row_flag)` INSIDE each arm. Quote: *"This looks
  correct! … `line_count` ranges from 1 to 315"* — validated by column-stats only.
- **q38:** the NULL-safe 3-way INTERSECT correctly keeps the all-NULL-name group (107 rows), but
  `count(last_name)` skips NULLs → 0. Must count a guaranteed-non-null output like `sale_date`.
  Quote: *"even stepping through two at a time gives 0 … none overlap into all three."* — its own
  probes used `count(last_name)`, cementing the wrong "no overlap" conclusion. (Also Theme A: the
  null-safe set semantics kept the null group the agent then miscounted.)
- **Fix:** the `union-stack-channels` / `filtered-aggregate` examples already warn "count the row
  key, not a non-unique sub-key" and "arms dedup to grain" — but agents miss it. Add an explicit
  "to COUNT rows across a union/intersect, pre-aggregate a count inside each arm (`sum(1)`), or
  count a guaranteed-non-null grain key — never a nullable display column or a raw passthrough."

---

## Fix summary
Mostly syntax-example NOTES + one prompt replicate (q81←q30). Lower total query count than A/C but
each is a clean, generalizable idiom lesson. Recovery: ~5 (q14, q30, q47, q76, q81), and the
example fixes harden many others beyond the 23.
