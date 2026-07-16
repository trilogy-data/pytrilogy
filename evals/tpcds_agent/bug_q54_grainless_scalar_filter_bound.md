# q54 — NOT a framework bug: `concept ? predicate` is a per-row filter, not a scalar (idiom/guidance issue)

**Reclassified 2026-07-15 after review.** Original probe called this a bucket-A silent codegen bug; that is
**wrong**. This is the defined behavior of an inline `auto` macro over a row-level filter. Correct bucket:
**D (agent idiom)** with a minor **B** affordance angle (silent empty result is unfriendly).

## What the agent did
Wrote `auto dec_ms <- ss.date.month_seq ? ss.date.year=1998 and ss.date.month_of_year=12` and used it as a
filter bound: `where ss.date.month_seq >= dec_ms + 1 and ss.date.month_seq <= dec_ms + 3`. Result: 0 rows.

## Why that is correct, not a bug
- `auto` is a **reusable inline macro**: referencing `dec_ms` templates its definition into the expression.
- `concept ? predicate` is a **row-level filtered value at the concept's own grain**, rendered as
  `CASE WHEN predicate THEN concept ELSE NULL END`. It does **not** collapse the grain — it is not a scalar.
  Proof: `select ss.date.month_seq, dec_ms` returns `dec_ms` per-row (NULL wherever the row is not Dec-1998),
  i.e. `(2400, None) (2399, None) …` — not a broadcast constant.
- `dec_ms`'s definition is written over `ss.date.month_seq`, the same concept the outer WHERE compares. So the
  faithful macro expansion is `D_MONTH_SEQ >= (CASE WHEN Dec-1998 THEN D_MONTH_SEQ ELSE NULL END) + 1`, i.e.
  each qualifying row asks `x >= x + 1` (false) and every other row gets `x >= NULL` (not true) → 0 rows.
  Plain SQL given the same self-referential predicate does the identical thing.
- The `select dec_ms;` → single value `1187` that looked like "it's a scalar" is a red herring: alone, the
  filter applies as a row-drop and dedups to one distinct value; it was never a broadcast constant. The
  per-row-vs-drop difference is the documented WHERE-scope rule (a filter co-selected with an unfiltered
  concept must NOT restrict that concept, so it becomes a CASE), and is consistent.

## The correct idiom (what the agent should have written)
Collapse the grain with an aggregate so the bound is a genuine scalar:
```
auto dec_ms <- max(ss.date.month_seq ? ss.date.year=1998 and ss.date.month_of_year=12);
where ss.date.month_seq >= dec_ms + 1 and ss.date.month_seq <= dec_ms + 3;
```
Verified on HEAD: renders the bound as a joined aggregate ref (`"agg"."dec_ms" + 1`), no per-row CASE,
returns rows. `min(...)` works the same way. (The canonical `query54.preql` sidesteps it entirely by
hardcoding 1188/1190.)

## The other pitfall (also not a bug): bare `max(...)` is responsive-grain
A global aggregate scalar from a **separate** `import raw.date as d` (`max(d.month_seq ? …)`) used in
store_sales' WHERE → `DisconnectedConceptsException`. This is **correct**: `max(...)` with no explicit `by`
is *responsive-grain* — in `select ss.item.id, … where … max(...)` it resolves as `max BY ss.item.id`, which
is genuinely multi-row and has no join path to the standalone `date` table. (Inspecting the concept in
isolation shows abstract/SINGLE_ROW, but that is only because there is no surrounding grain — do not conclude
"grainless" from an isolated inspection.)

**Working idioms for a broadcast scalar bound:**
- `max(d.month_seq ? …) by *` — persistently grainless; verified to resolve and cross-join in every position
  (WHERE bound, `select key, dec_ms`, inline in WHERE).
- `max(ss.date.month_seq ? …)` off the fact's **own connected** date role — works because it connects.

## Guidance takeaways (the real fixes)
1. Document clearly that `concept ? predicate` is a **per-row filter at the concept's grain**, not a scalar;
   to use a filtered value as a scalar bound, wrap it in an aggregate (`max`/`min`).
2. (Weak, optional B) The engine could warn when a concept is compared against a filtered version of *itself*
   at the same grain (self-referential `x >= x+1`-shaped predicate) — but SQL wouldn't warn either, so this is
   nice-to-have, not a bug.

## Correction note
This file previously asserted a `base.py:1332` codegen bug and "hardcoding is the only working idiom." Both
were wrong: the per-row CASE is the correct expansion of a row-level filter, and `max(...)` is a working
scalar idiom.
