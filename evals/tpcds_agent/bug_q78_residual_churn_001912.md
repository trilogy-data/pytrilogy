# q78 — residual 655k churn after the FULL-from-LEFT fix (run 20260629-001912)

Run: `evals/tpcds_agent/results/20260629-001912` (deepseek-chat). q78 burned **655k
tokens**, FAILED (result differs from reference), no sentinel in the coarse scan. The
agent's final `workspace/query78.preql` returns 74 rows from a *web-anchored 2-way LEFT
join* that drops catalog-only customers (semantically wrong) — a fallback it reverted to
after a **600s timeout** killed its (correct-intent) FULL-join attempt.

## 1. Prior FULL-from-LEFT bug — CONFIRMED FIXED

`bug_q78_disconnected_never_returned_churn.md`'s defect (3-way scoped LEFT silently
lowered to symmetric FULL) is fixed. Re-verified read-only via `generate_sql` against the
run workspace (`scratchpad/repro_q78e.py`): a 3-way store→web,catalog scoped LEFT now
renders store-anchored

```
FROM "vacuous"(store)
LEFT OUTER JOIN "late"(web)        ON store.cust = web.cust AND store.item = web.item
LEFT OUTER JOIN "cooperative"(cat) ON store.cust = cat.cust AND store.item = cat.item
```

No FULL, no `coalesce(...)=store` mis-anchor. Both LEFT keys (item AND cust) are present.

## 2. Remaining obstacle — the FULL combine of web+catalog (the 655k churn)

q78 needs the "other channel" total = web **FULL** catalog (union of both populations,
coalesce-0 the missing side). The agent correctly reasoned this (msg 33) and tried FULL.
**Two framework defects made the only natural FULL forms either explode or degenerate**,
and since the failure was a silent 600s timeout (no error to react to), the agent
abandoned FULL and shipped a wrong web-anchored INNER/LEFT fallback.

### Defect A — chained `=` join group does adjacent zip-pairing (silent garbage pairs)

The docs (`trilogy/ai/syntax_examples.py:437`) say *"`full join a = b = c` chains one full
group"*. The agent read this as "chain a composite key" and wrote the natural form:

```
full join web_agg.item_id = cat_agg.item_id = web_agg.cust_id = cat_agg.cust_id
```

`_resolve_join_group` (`trilogy/parsing/v2/rules/select_statement_rules.py:196-224`)
turns a chain into ADJACENT pairs via `zip(resolved, resolved[1:])`. Verified at parse
time (`env.parse`) the above yields THREE pairs:

```
web_agg.item_id => cat_agg.item_id   (valid)
cat_agg.item_id => web_agg.cust_id   (GARBAGE — item joined to customer)
web_agg.cust_id => cat_agg.cust_id   (valid)
```

Adjacency pairing is correct only for chaining the SAME concept across N sources
(`store.item = web.item = cat.item`, which works — see §1). For a *composite* key it
emits a cross-concept garbage pair. The only guard (line 210, `la == ra`) catches a
literal self-join, not the `item == cust` cross-link. Downstream join resolution can't
coherently source the garbage link and silently drops keys. In the full q78 chained form
`...yr = ...yr = ...item = ...item = ...cust = ...cust`, the **only surviving join
condition was the leading `yr = yr` pair** (`generate_sql`, `scratchpad/repro_q78c.py`):
`FULL JOIN "cooperative" on "concerned"."web_agg_yr" = "cooperative"."cat_agg_yr"`.

### Defect B — a constant-literal join key (`2000 as yr`) renders as `... on 1=1`

All three channel rowsets project `2000 as yr` (a grain-less constant). When `yr` is a
join key, the planner splits it into a separate CTE and joins it back with an empty key
set → `_build_joinkeys` returns `["1=1"]` (`trilogy/dialect/common.py:253-254, 318`):

```
SELECT ... FROM "young" FULL JOIN "questionable" on 1=1
```

This appears even in the *correct* per-key separate-clause form when `yr` is included
(`scratchpad/repro_q78c.py sep`). Dropping the constant `yr` from the join (keying on
item+cust only) yields a clean plan with no `1=1` (`scratchpad/repro_q78d.py B`).

### Combined effect = cartesian explosion → 600s timeout, no error

Defect A collapsed the agent's chained 6-key FULL join onto the single `yr = yr` pair;
Defect B makes a constant-key join a cross product. Net SQL: `web FULL JOIN catalog on
2000 = 2000` = full web×catalog cartesian (millions × millions) → **subprocess timed out
after 600s** (conversation msg 38). With no error to anchor on, the agent dropped FULL
entirely (msg 39) and reverted to `other_agg = web LEFT JOIN catalog` (web-anchored, drops
catalog-only customers) + `store LEFT JOIN other` → 74 rows, fails the reference.

## 3. Minimal repros (read-only `generate_sql` against the run workspace)

- `scratchpad/repro_q78c.py chained` — agent's exact 6-key chain → FULL JOIN on the
  constant `yr` only + a top-level `FULL JOIN ... on 1=1` (the cartesian).
- `scratchpad/repro_q78c.py sep`   — 3 separate `full join` clauses WITH `yr` → correct
  item+cust ON, but still a degenerate `FULL JOIN questionable on 1=1` for the constant.
- `scratchpad/repro_q78d.py A`     — chained two real keys `item = ... = cust` → FULL
  JOIN drops `cust`, keys only on `item` (Defect A in isolation).
- `scratchpad/repro_q78d.py B`     — separate clauses, item+cust, no constant → CLEAN
  `FULL JOIN ON cust AND item`. **This is the form that actually works.**
- `scratchpad/repro_q78e.py`       — 3-way scoped LEFT, confirms §1 fix.

The working form the agent never reached: drop `2000 as yr` from the rowsets/join and
combine with **separate** `full join web_agg.item_id = cat_agg.item_id` /
`full join web_agg.cust_id = cat_agg.cust_id` clauses (NOT one chain).

## 4. Root cause (file:line)

- **Defect A (primary, the timeout trigger):**
  `trilogy/parsing/v2/rules/select_statement_rules.py:205-224` `_resolve_join_group` —
  `zip(resolved, resolved[1:])` adjacency pairing of a `=` chain emits cross-concept
  garbage pairs for composite keys; only `la == ra` literal self-join is guarded.
- **Defect B (constant join key → cross product):**
  constant-literal join key split into its own CTE with empty `joinkey_pairs` →
  `trilogy/dialect/common.py:253-254` / `318` render `1=1`.
- **Contributing guidance gap:** `trilogy/ai/syntax_examples.py:425-437` — "`= c` chains
  keys into ONE group" / "`full join a = b = c` chains one full group" reads as license
  to chain a composite key; it only supports chaining ONE concept across N sources.
  Composite keys must use `and` / separate clauses, which the wording underplays for FULL.

## Classification

**(a) Framework codegen/resolution BUG, ×2** — both run cleanly and produce no error:
A silently mis-pairs a chained composite join key; B turns a constant join key into a
cross product. Together they make the natural FULL-combine of two constant-year rowsets a
cartesian explosion that times out at 600s. Compounded by a **(c) guidance ambiguity** in
the chain-syntax docs. Severity high: silent wrong/exploding plans with no error, which is
exactly what denies the agent a signal and drives six-figure token churn. Do NOT fix
(per task).
