# Bug: q04 validated query regenerates invalid SQL during scoring

## Severity

Fatal correctness/reliability bug. The same `.preql` file executes successfully
twice in the agent session, then regenerates invalid SQL in the scorer.

Successful local validation therefore does not guarantee that an unchanged
query can be executed again.

## Run

```text
evals/tpcds_agent/results/20260711-185953_enriched
```

Artifacts:

```text
workspace/query04.preql
agent_log.q04.jsonl
report.json
```

## Observed sequence

The agent's final staged query executed successfully at `19:14:05Z` and returned
six customers. It executed the same file again at `19:14:22Z` and returned the
same six customers.

The eval scorer then reparsed/regenerated that candidate and failed in DuckDB:

```text
status: error
detail: execute: ProgrammingError: Binder Error:
Values list "rambunctious" does not have a column named "w01_cid"
```

No agent edit occurred between the second successful validation and scoring.

## Candidate shape

The query creates six annual per-customer rowsets:

```text
s01, s02, c01, c02, w01, w02
```

It first combines store and catalog:

```preql
with sc as
select
    s02.cid,
    s02.fn,
    s02.ln,
    s02.pf,
    s01.val as sv01,
    s02.val as sv02,
    c01.val as cv01,
    c02.val as cv02
union join s02.cid = s01.cid = c01.cid = c02.cid
having
    sv01 > 0 and cv01 > 0 and sv02 > 0 and cv02 > 0
    and (cv02 / cv01) > (sv02 / sv01);
```

It then combines `sc` with the two web rowsets:

```preql
select
    sc.cid as customer_id,
    sc.fn as first_name,
    sc.ln as last_name,
    sc.pf as preferred_cust_flag
union join sc.cid = w01.cid = w02.cid
having
    w01.val > 0 and w02.val > 0
    and (sc.cv02 / sc.cv01) > (w02.val / w01.val)
    and (sc.cv02 / sc.cv01) > (sc.sv02 / sc.sv01)
order by
    customer_id asc nulls first,
    first_name asc nulls first,
    last_name asc nulls first,
    preferred_cust_flag asc nulls first
limit 100;
```

The complete candidate is preserved at `workspace/query04.preql` in the run.

## Invalid scorer SQL

The scorer generated a CTE named `rambunctious` with this projection:

```sql
rambunctious as (
SELECT
    divergent.sc_s02_fn as sc_s02_fn,
    divergent.sc_s02_ln as sc_s02_ln,
    divergent.sc_s02_pf as sc_s02_pf,
    coalesce(divergent.sc_s02_cid, waggish.w02_cid) as sc_s02_cid,
    coalesce(divergent.sc_s02_cid, waggish.w02_cid) as w02_cid
...
)
```

It does **not** project `w01_cid`.

Later generated joins nevertheless reference it repeatedly:

```sql
INNER JOIN dapper
  ON rambunctious.sc_s02_cid = dapper.w01_cid
 AND rambunctious.w01_cid = dapper.w01_cid
 AND rambunctious.w02_cid = dapper.w01_cid
```

DuckDB correctly rejects that SQL because `rambunctious.w01_cid` does not
exist.

## Expected behavior

SQL generation for an unchanged environment and query must be deterministic in
all semantically relevant respects. In particular:

- every generated column reference must be projected by its source CTE;
- repeated generation must either succeed consistently or fail consistently;
- scorer generation must not depend on prior queries executed in the agent
  process; and
- cache state, set iteration, generated CTE names, or discovery traversal order
  must not change required-key propagation.

If `w01_cid` is required by a downstream join, the planner must preserve it in
`rambunctious`. If it is redundant, the downstream predicate must not reference
it.

## Suspected failure

The final three-way relation is represented through several synthesized
filter/join CTEs (`dapper`, `cool`, `courageous`, `vast`, `rambunctious`). A
downstream join reconstructs all members of the authored equivalence group, but
projection pruning removes one member (`w01_cid`) from an upstream CTE.

The fact that the agent executions succeeded while scorer generation failed
suggests an unstable ordering or state-sensitive choice between equivalent
source plans, not merely a permanently unsupported query shape.

Likely areas:

```text
trilogy/core/processing/join_resolution.py
trilogy/core/processing/node_generators/
trilogy/core/processing/nodes/merge_node.py
trilogy/core/optimizations/
trilogy/core/models/build.py
```

Audit unordered set/dict traversal wherever equivalence-group keys are selected
for projection or pseudonym/coalesce chains.

## Regression tests

### Determinism loop

In fresh environments, generate SQL for the preserved q04 candidate at least
100 times:

```python
sql_variants = {engine.generate_sql(text)[-1] for _ in range(100)}
```

Every variant must execute successfully and return the same result. Exact SQL
text may vary only if all variants remain structurally valid and equivalent.

### Warm-state matrix

Generate and execute the query:

1. in a fresh process;
2. twice in the same environment;
3. after generating unrelated queries;
4. after a prior failed planning attempt;
5. in the agent CLI path; and
6. through `score_query_timed`.

The agent and scoring paths must agree.

### Structural invariant

Parse or inspect every generated CTE reference and assert that each qualified
column exists in the referenced CTE projection. Specifically pin the authored
equivalence group:

```text
sc.cid = w01.cid = w02.cid
```

Projection pruning must retain every key required by later predicates.

## Related q04 failure

Earlier candidates in the same trajectory produced guarded planner failures and
a long-running planner attempt. Those are documented separately in:

```text
handoff_q04_multi_rowset_union_join_planner_hang.md
```

This report is specifically about the final candidate succeeding in agent
validation but failing when regenerated for scoring.
