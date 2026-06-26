# Bug: WHERE with multiple cross-rowset comparisons over scoped-joined rowsets → cryptic `Have {…} and need …` resolution error (q11 / q23)

**Status:** FIXED 2026-06-26. Root cause: `gen_rowset_node` (node_generators/rowset_node.py)
got the WHERE pushed up as `conditions` but, with `local_optional` empty, early-returned the
bare rowset node and dropped the predicate — the operand rowsets (only-in-WHERE) were never
sourced. The legacy enrichment path can't help because `possible_joins` excludes ROWSET outputs,
filtering out the shared scoped-join key that is actually present. Fix: when `conditions` reference
concepts outside the rowset's outputs, source the rowset output + those operand rowsets as one
merge and apply the predicate there (MergeNode infers the cust_id join via pseudonym equivalence).
Regression test `tests/test_scoped_join_cross_rowset_multi_where.py`. An alternative "Option B"
(extend the discovery injection trigger to rowsets) was explored and found insufficient on its own
— see `evals/tpcds_agent/handoff_cross_rowset_where_via_injection_option_b.md`. Original report
(repro + bisection) preserved below.
**Surfaced by:** TPC-DS q11 (run `20260626-200411`) and q23 (run `20260626-193952`) — the canonical
"compare growth/ratio across periods or channels" shape. NOT a regression (q11 passed a prior run
with a different formulation; recent shorthand/q64 fixes are unrelated).
**Severity:** MEDIUM-HIGH — blocks a whole TPC-DS query family (q11/q23/q74-style period-over-period
comparisons); the error message is also unhelpful.

## Symptom

```
Syntax error: Have {'RowsetNode<store_2001.cust_code,store_2001.cust_id,store_2001.fname...3 more>': None}
and need store_2001.rev > 0 and web_2001.rev > 0 and divide(parenthetical(subtract(
web_2002.rev@Grain<web_2002.cust_...))) ...
```

A `SyntaxError` (raised from discovery/resolution). The node "has" only ONE rowset
(`store_2001`) but the WHERE predicate "needs" outputs from several — the planner can't co-locate the
multi-rowset condition into a resolvable node.

## Reproducing shape (q11)

Four rowsets (store/web × 2001/2002), each `with NAME as where … select cust_id, sum(...) as rev`,
joined on a shared customer key, then a WHERE comparing revenues across them:

```trilogy
... 4 rowsets: store_2001, store_2002, web_2001, web_2002 ...
select store_2001.cust_code, ...
inner join store_2001.cust_id = store_2002.cust_id
inner join store_2001.cust_id = web_2001.cust_id
inner join store_2001.cust_id = web_2002.cust_id
where store_2001.rev > 0
  and web_2001.rev > 0
  and (web_2002.rev - web_2001.rev) / web_2001.rev
        > (store_2002.rev - store_2001.rev) / store_2001.rev   -- cross-rowset compare
order by ... limit 100;
```

## Bisection (what triggers it)

| variant | WHERE predicate | result |
|---|---|---|
| 4-way scoped join, **no** cross-rowset compare (`store_2001.rev>0 and web_2001.rev>0`) | — | **OK** |
| **single** cross-rowset compare (`… and web_2002.rev > store_2002.rev`) | 1 compare | **OK** |
| 2-rowset **arithmetic** (`web_2002.rev - web_2001.rev > 0`) | arithmetic, 2 rowsets | **OK** |
| **multiple** cross-rowset compares (`web_2002.rev>store_2002.rev and web_2001.rev>store_2001.rev`) | 2 compares | **ERR** |
| original ratio compare (arithmetic, spans all 4) | — | **ERR** |

So: the **4-way scoped join is fine**, a **single** cross-rowset comparison is fine, and
**arithmetic is NOT the trigger**. The failure needs a WHERE that carries **two or more comparisons
referencing outputs across ≥3 distinct scoped-joined rowsets** — the planner resolves one
cross-rowset predicate against the join result but cannot co-locate a second, leaving the residual
condition unsourceable (`Have {one rowset} and need {multi-rowset predicate}`).

## Likely fix area

The scoped-join result should expose ALL joined rowsets' outputs as one resolvable grain so a WHERE
spanning them filters post-join. Today a multi-rowset WHERE predicate is pushed/resolved against a
node that only carries one rowset (`store_2001`), so the rest dangles. Inspect where query-scoped
joins build the post-join filter scope and how multi-rowset WHERE conditions are assigned to nodes
(discovery / `Have…need` resolution). The `Have {…} and need …` message should also be replaced with
a clear user-facing error if some forms are genuinely unsupported. Same family touched by the q11
scoped-join operand-order work ([[project_scoped_join_left_anchor_operand_order_bug]]).

## Repro harness

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260626-200411/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
eng.generate_sql(open('q11.preql').read())   # SyntaxError: Have {...} and need ...
```
