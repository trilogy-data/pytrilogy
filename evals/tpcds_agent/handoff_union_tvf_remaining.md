# Handoff: remaining `union(...)` TVF work (resolution + performance)

**Status:** OPEN. The `union(...)` **crashes are fixed**; what's left is (1) a
resolution failure on the multi-base-model channel shape and (2) slow planning.
This doc supersedes `bug_union_tvf_upstream_map_crash.md` and
`bug_union_rollup_crossmodel_recursion.md` (both now crash-fixed).

## Already fixed — do NOT redo

Verified on the current engine (2026-06-12):
- **`find_source` "Could not find upstream map for multiselect"** (q05 enriched,
  single `all_sales` model, channel relabel + order-by) — now **generates in
  0.3s**. Fixed via `_carry_order_by_concepts` + `UnionCTE.replace_dependency`.
- **`RecursionError` building a self-referential union output** — now a clean
  `InvalidSyntaxException` ("output column X aliases X, which is itself the union
  output — use a distinct name"). Fixed in `function_to_concept`.
- **Bare `SyntaxError` from `find_source`** — now a `RuntimeError` (internal-error
  class), no longer mislabeled as user syntax feedback.

Regression tests live in `tests/engine/test_duckdb_rowset.py`
(`test_tvf_union_order_by_grouped_away_column`,
`test_tvf_union_output_alias_self_reference_errors`).

## Remaining issue 1 — multi-base-model union can't resolve (the real blocker)

A `union(...)` whose arms come from **separate base models** (the ingest channel
models `raw.store_sales` / `raw.catalog_sales` / `raw.web_sales`), each emitting a
**constant channel literal** (`'store' as ch`) + dimension keys + measures,
consumed by an outer aggregate, fails to resolve:

```
UnresolvableQueryException: Could not resolve connections for query with output
  ['local._store_ch_ch<Purpose.CONSTANT>', 'local._store_ch_ent<...BASIC>',
   'local._store_ch_gross<...METRIC>', ...] from current model.
```

Repro (deterministic, ~15s to fail):

```python
import sys; sys.path.insert(0, 'evals'); from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260612-133004_ingest/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
eng.generate_sql(Path('evals/_repros/q05_union_constant_channel_unresolved.preql').read_text())
# -> UnresolvableQueryException: Could not resolve connections ...
```

The **enriched** counterpart (single unified `all_sales` model) now WORKS — so the
break is specific to unioning arms across *different* base models, where the
constant channel column (`_*_ch<CONSTANT>`) and the per-arm mangled outputs don't
connect to a shared grain.

The same family also surfaces in the q14 cross-channel shape (3-arm union over
`ss`/`cs`/`ws` + rollup + cross-model HAVING) as
`UndefinedConceptException: Concept '_virt_msl_… not found in environment` once the
self-referential-alias collisions are renamed away. Treat them as one bug: the
multi-model union's output concepts (constant + `_virt_msl_*` align placeholders)
aren't resolvable downstream.

This is the **single most common channel-query shape** (q05, q14, q56, q77 all
reach for it) and the highest-value fix — agents now bounce off a clean error
instead of crashing, but still can't complete these queries.

## Remaining issue 2 — union planning is slow (perf cliff)

Even when it only reaches an error, the multi-model union above takes **~15s** to
plan. In the live eval one enriched q05 union attempt took **214s** on a single
`trilogy run`, which (plus normal agent latency) blew the 900s wall clock →
`score=timeout` (not iteration exhaustion). The q05 crash doc noted the renderer
walked "~1.2MB of `render_concept_sql` self-recursion"; that deep lineage walk is
the likely cost center and is worth profiling independently of the resolution fix.

## Repro fixtures

- `evals/_repros/q05_union_constant_channel_unresolved.preql` — issue 1 (ingest,
  multi-model union, "Could not resolve connections").
- (the q14 3-arm union + rollup shape, for the `_virt_msl_*` variant, is in the
  trace `results/20260612-180707_ingest/agent_log.q14.jsonl`.)

## Context

Found across the full-99 rebaseline (`results/20260612-180707_*`) and the
non-pass rerun (`results/20260612-203218_*`). Machinery: `BuildUnionSelectLineage`
/ `BuildMultiSelectLineage` (`build.py`), `Derivation.TVF_UNION`, `UnionNode`,
`UnionCTE`, and the discovery/resolution path that connects union outputs to a
query grain.
