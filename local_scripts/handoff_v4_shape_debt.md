# Handoff — v4 shape debt: remaining work

Owner: unassigned. Prereqs: none. Optimizer code is shared with v3, so every
change needs a full one-process off/on A/B over both corpora.

## Current headline

Measured 2026-07-31 over every TPC-DS and TPC-H query with the completed shape
fixes applied:

| suite | v3 chars | v4 chars | ratio | smaller | identical | larger |
|---|---:|---:|---:|---:|---:|---:|
| TPC-DS (109) | 520,595 | 434,623 | **0.835** | 41 | 38 | 30 |
| TPC-H (23) | 32,551 | 30,285 | **0.930** | 10 | 13 | 0 |

No TPC-H query is larger under v4.

## Performance follow-up

TPC-H and TPC-DS now share the selective-repeat/minimum timing harness. Filtered
aggregate predicates move before grouping only when the group itself or every
consumer rejects the empty-group result. This reduced warmed TPC-H q20 execution
from the original 4.86x outlier to 13.5 ms against a 9.9 ms reference. Its
remaining gap is candidate-key correlation: Trilogy still groups every dated
lineitem before joining the small forest/Canada partsupp set.

## Gates

1. Render both corpora twice in one process, with the change monkeypatched off
   and on. Accept only named wins and byte-identical non-targets.
2. `pytest tests/modeling/tpc_ds_duckdb/test_queries.py -m "not adventureworks_execution"`
3. `pytest tests/join_matrix tests/engine tests/core/processing tests/modeling/join_resolution tests/test_scoped_join.py`
4. `pytest tests/modeling/tpc_h`
5. `mypy trilogy`, targeted Ruff `--select E,F,I`, and Black.

Several failures in this area are order-dependent. Always run whole files, not
only isolated examples.
