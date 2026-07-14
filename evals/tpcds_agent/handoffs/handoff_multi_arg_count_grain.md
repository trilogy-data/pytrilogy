# Feature handoff — `grain(a, b, c)`: a total grain marker, so `count()` can count combinations

**Status: LANDED 2026-07-14.** Tests: `tests/engine/test_grain_function.py`.
Motivation is empirical — see "Why". Related: `handoff_framework_bugs_20260714.md` (q87, q14).

## What shipped (differs from the design below in one important way)

`grain()` is **pure syntactic sugar** — a parse-time desugar, NOT a new `FunctionType`:

```
grain(a, b, c)  ->  hash(concat_ws(<US>, coalesce(cast(a as string), <RS>), ...), 'md5')
```

Built from the EXISTING `CAST` / `COALESCE` / `CONCAT_WS` / `HASH` function types, so every dialect's
own spelling is inherited and **no dialect code was written at all**. One hydrator
(`parsing/v2/rules/function_rules.py fgrain`), one `SyntaxNodeKind`, and a grammar rule in each
backend (lark + pest; pest needed a `maturin develop` rebuild). No planner work — counting the
combinations falls out of the standard rule that an aggregate's population is the distinct grain of
its arguments.

**Semantics, as verified against DuckDB (`test_grain_function.py`):**

| spelling | means | over a `rid`-grained table with 7 rows / 5 distinct name-date combos |
|---|---|---|
| `count(grain(a, b))` | **rows** of the population (the `count(*)` replacement) | 7 |
| `count_distinct(grain(a, b))` | **distinct combinations** | 5 |
| `count(<nullable property>)` | rows where it is non-null — **the q87 bug** | 3 |

These two are NOT interchangeable, and the difference is deliberate: a property's grain IS the grain
of its defining key (unchanged, documented semantics), so if the tuple is COARSER than the population
the row count and the combination count legitimately differ. **They coincide in both motivating
cases** — q14's args ARE the fact's grain keys (157,093 lines) and q87's rowset grain IS the 3-tuple
(47,298) — which is why either spelling answers those questions. When in doubt about "distinct
combinations", use `count_distinct(grain(...))`; it is right regardless of population grain.

An earlier draft proposed a first-class `FunctionType.GRAIN` that would DECLARE its grain to be its
arguments (forcing a dedupe even when the args are properties of a finer key). **Rejected by owner:
the grain of a property is the grain of its key — do not change that semantics.**

---

## Original design notes (retained for context)

**Supersedes an earlier multi-arg-`count(a,b,c)` proposal, which was scrapped**: it needed a new
aggregate FunctionType, an arity special-case in `count`, a bespoke NULL rule that contradicted
unary `count`, and bespoke handling for `?` / `count_distinct` / `group by`. `grain()` needs none of
that.

## The gap

Trilogy has no `*` row-marker (`count(*)` → Syntax [223]) — correctly, because with automatic joins
"the number of rows" is meaningless until you say *whose* rows. The guidance is "count a non-null
grain key", which works only when a SINGLE key names the grain you mean. **There is no spelling for
"count rows at a COMPOSITE grain"** — and composite is the normal case.

`count()` itself is NOT buggy: `count(<property>)` returns the count of the key defining that
property's grain, and skips NULLs — documented and consistent. The problem is that an agent with a
composite grain in mind has to pick ONE column, and both wrong picks are silent:

| it picks | it gets | why |
|---|---|---|
| a **coarser** key | distinct values of that key | population dedupes to the arg's grain |
| a **nullable** property | rows where that property is non-null | `count(col)` skips NULLs |

## Why (both silent count failures in the 2026-07-13 enriched run were exactly this)

* **q87** — prompt: *"count of distinct customer-name / **date combinations**"*. Intent:
  `count(grain(fname, lname, sale_date))`. It counted `lname` (nullable) → **46,446 instead of
  47,298**; 852 tuples with a NULL last name silently dropped.
* **q14** — prompt: *"count of sale line items (**order + item id combinations**)"*. Intent:
  `count(grain(order_id, item.sk))`. It counted `item.id` (coarser) → **9,000 items instead of
  157,093 lines**.

Two of the run's worst failures, one missing primitive. Both prompts literally said "combinations".

## Design

> `grain(a, b, c)` is a ROW-LEVEL function whose **grain is its arguments** and whose **value is
> total** — non-null for every combination of those keys, including combinations with a NULL member.

That is the whole feature. Everything else falls out of machinery that already exists:

* **`count()` is unchanged.** Its documented rule ("count the non-null values of the argument, at the
  argument's grain") applied to `grain(a,b,c)` yields the number of distinct `(a,b,c)` combinations,
  because the planner ALREADY dedupes an aggregate's population to its argument's grain. Verified on
  HEAD — `select count(line_no)` (a sub-key) emits exactly this shape today:
  ```sql
  WITH wakeful as (SELECT "lines"."line_no" FROM "lines" GROUP BY 1)
  SELECT count("wakeful"."line_no") FROM "wakeful"
  ```
* **NULL-preservation is a property of the VALUE, not a special counting rule.** `grain()` is never
  null, so `count()` never skips a combination. No arity-dependent NULL behavior anywhere; nothing
  inconsistent to explain.
* **`?` composes for free.** `count(grain(a,b) ? cond)` is the ordinary filtered-aggregate idiom —
  `?` wraps the marker in a CASE, `count` skips the NULLs. (A multi-arg `count` had no coherent
  spelling for this: `?` binds to the immediately-prior expression, so `count(a, b ? cond)` would
  filter only `b`.)
* **The grain-match collapse needs NO new entry.** `AGGREGATE_GRAIN_MATCH_MAP[COUNT]` is already
  `CASE WHEN {args[0]} IS NOT NULL THEN 1 ELSE 0 END` (`dialect/base.py:679`); since `grain()` is
  total, that formula already returns the correct `1` on the collapse path.

## Rendering — a NULL-SAFE HASH (owner decision 2026-07-14)

The value must be **countable**, not merely non-null. An earlier design rendered a total constant
(`greatest(case when a is null then 1 else 1 end, …)`) — always `1`, prune-safe, and adequate for
`count()`, but **not countable BY VALUE**: `count_distinct(grain(a,b))` would render
`count(distinct 1)` → `1`. It would also collapse `group by grain(...)` into one group and project a
column of `1`s. A value that lies. **Rejected.**

```
grain(a, b, c)
  ->  md5(concat_ws(<sep>,
              coalesce(cast(a as varchar), <null_sentinel>),
              coalesce(cast(b as varchar), <null_sentinel>),
              coalesce(cast(c as varchar), <null_sentinel>)))
```

Four properties, all load-bearing:

1. **TOTAL** — the `coalesce` sentinel means a NULL member still produces a hash, so `count()` never
   skips a combination. This is what NULL-preservation rests on; `count` itself is unchanged.
2. **COUNTABLE / INJECTIVE** — distinct combinations produce distinct hashes, so
   `count(grain(...))` == `count_distinct(grain(...))` == the number of combinations. **This
   restores the "for a key, count == count_distinct" invariant** instead of puncturing it, and it is
   why there is NO restriction on where `grain()` may appear: project it, group by it,
   distinct-count it — all sound.
3. **PRUNE-SAFE BY CONSTRUCTION** — every argument is referenced in the emitted expression. The args
   are load-bearing as the dedupe CTE's `GROUP BY` keys but are not otherwise read by the outer
   SELECT; a rendering that did NOT reference them (e.g. a bare `1`) would leave any "unused column"
   pass — `optimizations/hide_unused_concept.py`, which has already caused one silent set-op bug —
   free to drop them, silently COLLAPSING the dedupe and INFLATING the count. Safety by
   construction, not an out-of-band "do not prune" flag a later pass could ignore.
4. **Usable as a surrogate key** — a stable fingerprint of the combination, which is a bonus the
   constant form could never offer.

**Encoding rules (get these right or the injectivity is a lie):**
* `<sep>` and `<null_sentinel>` must be characters that cannot appear in the cast values — use
  control chars (e.g. unit separator `chr(31)` as the separator, record separator `chr(30)` as the
  NULL sentinel). Without a NULL sentinel distinct from the empty string, `('', NULL)` and
  `(NULL, '')` collide.
* Residual (accept and document): a value containing the sentinel chars could collide. Length-prefix
  each member if we ever need a hard guarantee.
* `cast(x as varchar)` must be deterministic for DATE/FLOAT within a dialect (it is); the hash is
  NOT stable ACROSS dialects. Fine for counting; do not persist a `grain()` hash and compare it
  against one produced by another engine.

**Existing pieces to reuse:** `FunctionType.HASH` (`core/enums.py:337`, → `md5(...)` at
`dialect/base.py:496`) and `FunctionType.CONCAT_WS` (`core/enums.py:252`, `dialect/base.py:643`).
BigQuery already prefers `FARM_FINGERPRINT` (`dialect/bigquery.py:252`) — a natural per-dialect
override, as is DuckDB's native `hash(...)` over a struct.

**Perf:** the hash is computed on the DEDUPED rows (one per combination), not per fact row, so cost
scales with the number of combinations. A later optimization MAY substitute a cheaper total marker
when the ONLY use is `count(grain(...))` (no distinct/group-by/projection) — but only if the keys are
PROVEN to survive into the GROUP BY. Do not do that first; the arg references are the safety property.

## Target SQL

```
select count(grain(so.fname, so.lname, so.sdate)) as c;      -- the q87 shape
```
```sql
WITH tuples as (
    SELECT "so"."fname", "so"."lname", "so"."sdate"
    FROM "store_only" as "so"
    GROUP BY 1, 2, 3)                       -- NULL is an ordinary group value
SELECT count(md5(concat_ws(chr(31),
    coalesce(CAST("tuples"."fname" AS VARCHAR), chr(30)),
    coalesce(CAST("tuples"."lname" AS VARCHAR), chr(30)),
    coalesce(CAST("tuples"."sdate" AS VARCHAR), chr(30))))) as "c"
FROM "tuples"                               -- 47,298 — NULL-lname tuples INCLUDED
```
`count_distinct(...)` of the same expression returns the same 47,298.

## Implementation

1. **`FunctionType.GRAIN`** — row-level (NOT an aggregate). Args: 2+ row-level concepts/expressions
   (1 is degenerate but harmless; reject an aggregate argument with a clear error).
2. **Grain metadata**: the function's grain = the union of its args' grains. This is the load-bearing
   part — it is what makes `count()` dedupe the population to `(a,b,c)`.
3. **Type**: string (the hash). It is a real, countable, groupable value — no usage restriction.
4. **Render** (`dialect/base.py` `FUNCTION_MAP`): the null-safe hash form above, reusing
   `HASH`/`CONCAT_WS`. Optional per-dialect overrides: BigQuery `FARM_FINGERPRINT`, DuckDB native
   `hash()` over a struct.
5. **Grammar, BOTH backends**: lark (`parsing/trilogy.lark`) and pest
   (`scripts/dependency/src/trilogy.pest`); pest is the default and needs a `maturin develop` rebuild.
   **Check for a keyword collision** with the `grain (…)` datasource-DDL clause and the `by <grain>`
   vocabulary — different contexts, but confirm in both grammars.
6. **Guidance** — this is the payoff. Syntax [223] (`parsing/v2/errors.py:87`) currently only teaches
   the single-key form; it should say: *"to count rows at a composite grain, count its grain:
   `count(grain(order_id, item.sk))`"*. Same for `ai/constants.py` and the `filtered-aggregate`
   syntax example (`ai/syntax_examples.py:122`).

## Tests

* q87 shape: NULL tuple member is COUNTED, not dropped (47,298, not 46,446).
* q14 shape: composite count = line count (157,093), not distinct items (9,000).
* **Anti-prune regression (the critical one)**: assert the arg columns survive in the generated CTE's
  `GROUP BY` and that the count is not inflated. Silent otherwise — this is the failure mode the
  rendering exists to prevent, so it must be pinned by a test, not by the rendering alone.
* **`count(grain(a,b))` == `count_distinct(grain(a,b))`** — the invariant. Pin it; it is the reason
  the hash exists.
* Injectivity edge: `('', NULL)` and `(NULL, '')` must NOT collide (this is what the distinct NULL
  sentinel buys — test it explicitly, it is the classic encoding bug).
* Grain-match collapse path (existing COUNT entry, no new map entry) — a single-row group with a
  NULL member still counts as 1.
* `count(grain(a,b) ? cond)` filters combinations correctly.
* `grain()` projected / grouped by / used as a surrogate key — all sound now (no v1 restriction).
* Empty population → 0.
* Parses on BOTH backends; no collision with datasource `grain (…)` DDL.
