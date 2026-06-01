# Engine bug handoff: a SELECT `as <name>` that reuses an existing `auto`/derived concept name silently discards the SELECT expression and emits the old concept's lineage

## RESOLVED (2026-06-01)

Fixed by raising a clear **recursive self-reference** error at finalize time
(`trilogy/parsing/v2/select_finalize.py`, in the `ConceptTransform` branch of
`finalize_select_statement`): a `select <expr> as foo` whose `<expr>` references
`foo` itself now raises `InvalidSyntaxException` instead of silently emitting the
original `foo`. Renaming the output is the fix (the alias must stay visible to
sibling calculations, so renaming the *input* is not an option — cf. `a+1 -> a,
a+2 -> b`: which `a`?).

Regression tests in `tests/test_parse_engine_v2.py`:
`test_parse_text_v2_select_transform_self_reference_raises`,
`test_parse_text_v2_select_self_referential_shadow_of_auto_raises`,
`test_parse_text_v2_select_non_self_referential_shadow_commits`.

**Corrections to the original diagnosis below:** the two suggested fixes
(`select_finalize.py` MANUAL replace; `author.py` `as_lineage` prefer
`local_concepts`) were both implemented and verified to be **no-ops
end-to-end**. The built `SelectLineage`/`BuildSelectLineage` *does* correctly
carry the case-when in `local_concepts`, but the planner
(`source_query_concepts` / the concept graph) keys concepts by **address**, so
the output `local.b1` (case-when) and the inner reference `local.b1` (the
`count`) collapse to one node and the datasource-resolvable `count` wins. The
working `out_b1` variant works *only* because its output address differs from
the inner reference. Also note: a **non**-self-referential shadow (`auto b1 <-
count(...); select avg(x) as b1`) already resolved correctly (the `avg` wins) —
only the self-referential form was broken. Making the self-referential form
actually *compute* would require giving the output a distinct internal address,
which breaks the sibling-visibility contract; raising is the correct behavior.

---

## Summary

When a query declares a concept (e.g. with `auto foo <- ...`) and then a later
`SELECT` aliases a *different* expression with the **same name** (`select <expr>
as foo`), the engine silently ignores the SELECT expression and computes the
**original** concept instead. No error, no warning — just wrong numbers.

This is a silent-wrong-results bug (the worst kind) and an easy footgun: an agent
naturally names an output the same as an intermediate (`auto bucket1 <-
count(...)` … `select case when bucket1 > k then avg(x) end as bucket1`).

It sank **q09 / ingest** in the TPC-DS agent eval (run `20260601-143003_ingest`):
the agent defined five `auto bucket_N <- count(...)` quantity-band counts, then
selected `case when bucket_N > threshold then avg(...) else avg(...) end as
bucket_N`. The emitted SQL contained only the raw `count` aggregates; every
`case-when avg` was dropped. The plain-SQL baselines and the **enriched** Trilogy
leg passed — enriched only because its canonical answer writes the `case-when`
inline in the SELECT (no `auto` of the same name to shadow it).

## Minimal, deterministic repro

```
import raw.store_sales as ss;
auto b1 <- count(ss.quantity ? ss.quantity between 1 and 20);
select case when b1 > 5 then avg(ss.list_price) else avg(ss.wholesale_cost) end as b1 limit 1;
```

Run against any ingested TPC-DS workspace (`trilogy run repro.preql duckdb`).

Confirmed by inspecting the generated SQL + result (against
`results/20260601-143003_ingest/workspace`):

| SELECT alias | `CASE` in generated SQL? | value returned |
|---|---|---|
| `... end as b1` (reuses the `auto b1` name) | **No** | `550263` ← the raw `count(b1)` |
| `... end as out_b1` (distinct name) | Yes | `75.77` ← the correct `case-when avg` |

Same expression, only the alias name differs. Reusing the name silently swaps in
the wrong computation.

## Root cause (traced + confirmed)

Two cooperating sites:

1. **`trilogy/parsing/v2/select_finalize.py:555-566`** — when finalizing a SELECT
   output whose address already exists in the environment, the code only writes
   the SELECT's expression back into `env.concepts` if the *existing* concept's
   `concept_source == ConceptSource.SELECT`:
   ```python
   if existing is None:
       context.add_top_level_concept(x.content.output, meta=meta)          # fires only when name is new
   elif existing.metadata.concept_source == ConceptSource.SELECT:           # fires only for prior SELECT-defined
       context.semantic_state.replace_concept(...)
   # else: an `auto`/MANUAL concept with this name — NEITHER branch fires
   ```
   An `auto foo <- ...` registers `foo` with `ConceptSource.MANUAL`. So when the
   SELECT reuses `foo`, neither branch runs: `env.concepts['local.foo']` keeps the
   **stale MANUAL lineage** (the `count`). The correct SELECT expression IS written
   into `select.local_concepts` (line 571), but only there.

2. **`trilogy/core/statements/author.py:144`** — `SelectStatement.as_lineage`
   resolves the selection through `environment.concepts`, not `local_concepts`:
   ```python
   selection=[environment.concepts[x.concept.address].reference for x in self.selection]
   ```
   So it picks up the stale MANUAL `count` lineage and plans SQL for that,
   discarding the `case-when` that lives in `local_concepts`.

The two maps disagree and the wrong one wins.

## Suggested fix

Either site closes it; doing both is safest:

- **select_finalize.py**: also replace when the existing concept is a local
  `MANUAL`/derived definition, not just `SELECT` — i.e. extend the guard to
  `concept_source in (ConceptSource.SELECT, ConceptSource.MANUAL)` (or, more
  conservatively, raise a clear "alias `foo` shadows an existing concept" error
  rather than silently keeping the old one).
- **author.py `as_lineage`**: prefer `self.local_concepts` over
  `environment.concepts` when resolving a selection address that exists in both:
  `(self.local_concepts.get(addr) or environment.concepts[addr]).reference`.

A loud error is arguably the right product behavior regardless — shadowing an
existing concept name with a different SELECT expression is almost always an
agent/author mistake.

## Suggested regression assertions

- The 3-line repro returns `75.77…` (the avg), and its generated SQL contains a
  `CASE`/`avg`, not a bare `count`.
- An assert that `select <expr> as X` where `auto X <- <other>` exists either
  reflects `<expr>` in the output or raises — never silently emits `<other>`.

## Agent-facing note (separate, lower priority)

Until fixed, agents should not reuse an `auto`/derived name as a SELECT alias.
The enriched canonical answer avoids it by writing the `case-when` inline. A line
in the language reference ("never alias a SELECT output with the name of an
existing concept") would reduce exposure — the existing reference already says
"Never alias fields with existing names" but does not flag that the failure is
*silent*.

- Observed on `trilogy 0.3.275`.
- Distinct from the other open handoffs (`bug_aggregate_render_derived_in_by_grain.md`,
  `bug_merge_in_subselect_missing_cte.md`, `recursion_bug_handoff.md`).
