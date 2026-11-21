# Grouping sets as a call form: language handoff

The next PR after #1075. Goal: `rollup(...)`, `cube(...)` and
`grouping_sets(...)` become the way to write multi-level grouping, and the
select-level `by rollup|cube|grouping sets` clause is removed.

## Where things stand

- Planning is done. Every keyed `by rollup` read (rowset body, inline
  `(select ...)`, top-level statement) already lowers to a one-arm relational
  TVF: `RollupSelectStatement` / `RollupSelectLineage`, planned by
  `gen_rollup_select`. See seam 10 of
  `docs/query_gen_simplification_audit_2026_10.md`.
- The surface is unchanged. There is no call syntax yet; the only authoring
  form is the select clause, and `Renderer` prints a rollup TVF as its arm
  select.
- The lowering entry points are `lower_grouping_select` (`tvf_rules.py`, a
  finalized select to the TVF) and `lower_statement_grouping`
  (`rowset_rules.py`, a top-level statement to `SelectStatement.relational`).
  The call form should reach the same two functions, not a new path.

## The syntax

Decided:

- The arm is a parenthesized select, as for `union` / `except` / `intersect`
  (`tvf_rel_arg`). No bare field-list shorthand: that waits for a generic TVF
  argument syntax that would apply to every TVF.
- The grouping spec follows the arm.

```
from rollup((select department, course, sum(credits) as t where credits > 2),
            department, course) -> (dept, course, t)
select dept, course, t order by dept;

with r as rollup((select department, sum(credits) as t), department) -> (dept, t);
```

| Form | Arguments | Replaces |
|---|---|---|
| `rollup(arm, k1, k2, ...)` | ordered keys: the subtotal hierarchy | `by rollup (k1, k2)` |
| `cube(arm, k1, k2, ...)` | keys, every combination | `by cube (k1, k2)` |
| `grouping_sets(arm, (k1, k2), (k1), ())` | one tuple per set, `()` the grand total | `by grouping sets (...)` |

Rules carried over from the clause:

- Keys are concepts, not expressions (the `grouping()` restriction in
  `_validate_grouping_args_are_concepts`). Name an expression first.
- A key need not be projected by the arm.
- `grouping(k)` / `grouping_id(...)` are written inside the arm. The flags
  the parser injects stay hidden.
- No keys (`rollup(arm)`) is one plain group and stays a plain select.
- Mixed specs (SQL's `rollup(a), cube(b)`) stay unsupported; `grouping_sets`
  covers them.

Open, decide before building:

1. **Output signature.** `union` requires `-> (...)` because arms align by
   position. A rollup has one arm, so the signature could be optional, the
   outputs taking the arm's names. Recommended: optional. If required, every
   migrated query gains a signature line.
2. **`grouping_sets` spelling.** One token with an underscore is the cheap
   option. `grouping sets(...)` needs its own lexer token in both grammars
   (as `_BY_GROUPING_SETS` has now).
3. **Top-level statement form.** Either `from rollup(...) -> (...) select ...`
   only (as `union` today), or also a bare `rollup(...) -> (...);` statement.
   The first needs no new statement kind.

## What the PR has to change

Grammar, both together (parity is tested):

- `trilogy/parsing/trilogy.lark`: `tvf_union_invocation` and its siblings at
  the `_tvf_invocation` rule; add the three forms and their lead tokens beside
  `_UNION`.
- `trilogy/scripts/dependency/src/trilogy.pest`: the same rules; `GROUPING_LEAD`
  and `select_grouping` go when the clause does.
- `trilogy/parsing/v2/syntax.py`: new `SyntaxNodeKind` members beside
  `TVF_UNION_INVOCATION`.

Hydration:

- `tvf_rules.py`: a hydrator per form that builds the `AggregateGrouping`
  from the key arguments, sets it on the arm select, finalizes the arm and
  calls `lower_grouping_select`. `tvf_select_statement` and
  `rowset_derivation_statement` already accept a `_tvf_invocation`.
- With an explicit `-> (...)` the output names are the signature's, as in
  `_lower_set_op`; without one they are the arm's (today's behaviour).

Rendering:

- `Renderer.to_string(RollupSelectStatement)` prints the arm select. It must
  print the call form instead, and round-trip it.

Removing the clause:

- Drop `select_grouping` from both grammars and from `select_statement`.
- Replace it with a targeted migration error, as the removed join types have
  (`_JOIN_MIGRATION_HINT` in `select_statement_rules.py`): keep the lead
  lexing long enough to say "write `rollup((select ...), keys)`".
- `trilogy/parsing/v2/errors.py` has several hints that recommend
  `by rollup`; `select_finalize.py`'s `grouping()` errors do too.
- A merge arm and a union arm that carry a spec are still mutated selects.
  With the clause gone they can only be written as a nested call, which
  lowers through the rowset path, so this resolves itself; check it does.

Migration:

- 41 test `.py` files and 13 test `.preql` files use the clause, including
  TPC-DS queries 18, 22, 27, 36, 67, 70, 86.
- Agent guidance: `trilogy/ai/constants.py`, `trilogy/ai/syntax_examples.py`,
  `trilogy/scripts/file_helpers/preql_validation.py`.
- Eval references under `evals/` (reference queries and the tpcds agent
  notes); committed result directories are history and stay.

## Gates

- `tests/engine/test_duckdb_rollup_statement_laws.py`: the statement against
  DuckDB. Port its bodies to the call form; keep the SQL oracle.
- `tests/engine/test_duckdb_rollup_tvf_laws.py`: rowset reads against the
  statement.
- The lark / pest parity tests, and the renderer round-trip tests.
- TPC-DS SQL size budgets (`tests/modeling/tpc_ds_duckdb/test_queries.py`):
  the call form should render the same SQL as the clause does now.

## Not part of this

Deleting the grouping mode from `BuildAggregateWrapper` and the planner sites
that track grouping-set padding (seam 10, steps (a) to (c)) is independent of
the syntax and can land before or after.

Known defect to keep in mind while migrating persist tests:
`persist ... from select ... by rollup` fails at the warehouse today because
`to_datasource` lists hidden outputs and a rollup hides its injected flags.
