# q59 token sink — internal crash in the OUTER→INNER join optimizer

**Target:** q59 in `evals/tpcds_agent/results/20260703-151512/` — 4,376,400 tokens.
Passed (100 correct rows) but only after massive thrash. Trilogy v0.3.288.

## Symptom (what the agent saw)

q59 = weekly store sales, this-year (2001) vs next-year (2002), matched on
`(store, week=week-52)`, report per-day ratios. The natural Trilogy shape is
two `where … select …` rowsets joined with the new `union join`
(SUBSET/UNION joins that replace LEFT/FULL). Every attempt to `union join` two
rowsets returned an opaque **internal** error (`"Unexpected error in …"`, exit 1,
no location), repeated across messages 84–100 of
`agent_log.q59.conversation.txt`:

```
Unexpected error: _pair_side_fully_matches() takes 7 positional arguments but 8 were given
Unexpected error: 'UpgradeOuterFromKeySetEquivalence' object has no attribute 'subset_binding_sources'
```

The first fires on a **composite-key** union join
(`… and this_year.week = next_year.week - 52`); the second on a **single-key**
union join and on a plain multi-day `auto … by ss.store.text_id`. Because the
error is an internal Python exception with no query location, the agent could
not tell which construct was at fault. It abandoned rowset+join entirely, tried
window `lead`, tried self-joins, churned through store.id-vs-text_id fan-out
(expected semantics, not a bug), and finally stumbled into a workaround: encode
the composite key as a string (`concat(text_id,'-',week_seq)`) and do a
**single-key** `union join this_year.k = next_year.k` — which happened to dodge
the crashing path. That workaround is the final `workspace/query59.preql`.

## Minimal repro (self-contained BODY)

```
import raw.store_sales as ss;
auto total <- sum(ss.sales_price) by ss.store.text_id, ss.date.week_seq, ss.date.year;
with this_year as
where ss.date.year = 2001
select ss.store.text_id as code, ss.date.week_seq as week, total;
with next_year as
where ss.date.year = 2002
select ss.store.text_id as code, ss.date.week_seq as week, total;
select this_year.code, next_year.code
union join this_year.code = next_year.code
    and this_year.week = next_year.week - 52
limit 5;
```

At the eval-time build this raised the `_pair_side_fully_matches`/
`subset_binding_sources` internal error. On the **current working tree** it
runs clean and returns correct rows.

## Trigger matrix (current working tree — all repros now PASS; crash is fixed)

| # | Construct | Eval-time result | Current-tree result |
|---|-----------|------------------|---------------------|
| A | `union join a.k = b.k` on concat single key | (agent's workaround) OK | 312 rows |
| B | `union join a.code=b.code and a.week=b.week-52` (composite) | **CRASH** `_pair_side_fully_matches 7≠8` | 312 rows — **identical to A** |
| D | `union join a.code = b.code` (single, non-unique) | CRASH `subset_binding_sources` | 16 854 rows (correct fan-out; code alone isn't unique) |
| — | multi-day `auto … by ss.store.text_id`, no join | CRASH `subset_binding_sources` | OK |
| C | `subset join this_year = next_year` (bare rowset, no `.key`) | n/a | `UndefinedConceptException` (parser wants a key concept — see below) |

Key result: **B == A == 312 rows**. The composite `and` union join now resolves
as a true composite AND (no cross-product fan-out) — exactly what the agent
needed and feared it couldn't get. The only thing standing between the agent and
the direct answer was the internal crash.

## Root cause (file:line) + regression status

`trilogy/core/optimizations/value_set_join_upgrade.py`, class
`UpgradeOuterFromKeySetEquivalence` (the FULL/LEFT/RIGHT→INNER narrowing pass
that runs on every OUTER/UNION join). Two mid-refactor breakages:

1. **Arg-count desync** — `_pair_side_fully_matches(...)` (def ~L465) was called
   (`_narrow_directionally`, ~L713/L728) with a `self.subset_binding_sources`
   8th argument the definition no longer accepted → `TypeError: takes 7 …
   but 8 were given`.
2. **Missing attribute** — `__init__` stopped setting `self.subset_binding_sources`
   while a call site still read it → `AttributeError`.

Both are regressions from the **join refactor** (`dcc62ed78 union_checkpoint`
→ `956e7303b hacky_joins` → `35f8157b6 checkpoint`, the SUBSET/UNION-join
replacement of LEFT/FULL). `git show` confirms the `subset_binding_sources`
parameter/attribute pair was introduced there. HEAD (`35f8157b6`) has def+call
both 8-arg and sets the attribute; the **eval ran on a dirtier intermediate**
of that window (v0.3.288 was last bumped at `36deef863`, pre-refactor, and never
re-bumped), which is where the def/call fell out of sync.

**Already being fixed in the uncommitted working tree.** `git diff HEAD` on this
file (24 ins / 27 del) removes `subset_binding_sources` entirely and makes
`_pair_side_fully_matches` a clean 7-arg (`domain_graph, subset_join_map,
scoped_canonical`). With those edits the current engine no longer crashes on any
repro above, and `workspace/query59.preql` runs to 100 correct rows.

## Classification

- **Real framework bug (regression), primary token-sink driver:** the two
  internal crashes above. Being fixed in the working tree — verify the fix lands
  and add a join-matrix cell for `union join` (single + composite key) between
  two `where … select …` rowsets so this can't silently regress again.
- **Residual guidance gap (not a hard bug now):** even with the crash gone, the
  opaque `"Unexpected error"` (no query location) gave the agent nothing to act
  on and sent it down window/self-join dead ends; and the docs' `and` vs `= c`
  composite-key semantics led it to *believe* composite `union join` fans out
  when it does not (B proves it doesn't). A worked composite-key `union join`
  example + a location-bearing error would have saved most of the 4.3M tokens.
- **Agent errors (clear messages, not framework):** SQL-ism `… on a=b`
  (clean parse error), `FROM` keyword (`Syntax [101]`), misplaced `by code, week`
  (clean parse error), store.name-vs-text_id fan-out (expected grain semantics,
  correctly handled with `max(name) by text_id`). Case C's bare
  `subset join rs = rs` (no `.key`) is a malformed key, not a supported whole-
  rowset form — design doc only shows `subset join a.k = b.k`.
