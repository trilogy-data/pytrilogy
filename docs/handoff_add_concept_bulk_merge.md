# Handoff: bulk-insert the imported-concept merge

## STATUS: LANDED 2026-08-04 (s71)

Follow-on to increment (1) of `docs/handoff_immutable_import_envs.md`
(`ee9ab12a6`), which took the `with_namespace` copy fan-out to zero. This was
what remained of `Environment.add_import`.

## What landed

`NamespaceProjection` now carries a precomputed `bulk_concepts` /
`bulk_hidden` / `bulk_safe`, and `Environment._bulk_merge_projected_concepts`
replaces the whole per-concept loop with `data.update()` + a `hidden` union
when it can prove nothing collides. Everything else falls back to
`_merge_imported_concept` unchanged — the loop is still the definition of
correct behavior.

The fast path declines when:

- an explicit `concepts` import filter is set (per-edge, cannot ride on the
  shared projection — `test_concept_filter_is_per_edge_over_a_shared_projection`),
- a concept overlay is installed (it changes what `validate_concept` considers
  present),
- `bulk_safe` is False: a struct concept (`generate_related_concepts` derives
  further concepts, order-sensitively) or a key written twice with different
  objects (the loop's durable-signature dedup picks a winner; a dict does not),
- any target key is already in the importer's `concepts.data`.

## Measured (warm corpus, 132 `query*.preql`, tpc_ds_duckdb + tpc_h)

Same process, alternating legs, bulk path forced off vs on:

| metric | loop | bulk |
|---|---|---|
| `add_concept` calls | 52,775 | **4,076** |
| `validate_concept` calls | 52,020 | **3,321** |
| `add_datasource` (control) | 5,366 | 5,366 |
| corpus parse, median of 5 | 0.698s | **0.613s** (1.14x) |
| q64 / q77 / q03 parse (median of 20) | 19.4 / 15.2 / 2.8ms | 18.7 / 13.7 / 2.4ms |

**The win is 1.14x, not the 1.3x this handoff predicted.** The prediction came
from cProfile's 0.188s `cum` for `add_concept`, but at 52k calls most of that
is profiler per-call overhead; the real wall recovered is 0.085s. Judging by
call counts was still right — 92.3% of `add_concept` calls are gone — but the
wall conversion in the original writeup was too generous.

## What the instrumentation actually showed

Better than assumed. Over all 143 aliased import edges in the corpus:

```
edges                    143
projected concepts    48,699
dirty entries              0   <- zero collisions, every edge
edges with a filter        0
edges with structs         0
edges under an overlay     0
default-namespace hits     0
```

So the intersection test is empty *everywhere*, and the fast path is
all-or-nothing per edge without cost. The 583 `dedup_to_durable` / 601
`validate_concept` collisions cited in the original writeup do **not** come
from the aliased path at all — they live in the bare-import merge
(`projection is None`) and the ~1,700 non-import `add_concept` callers.

Both concerns the original writeup flagged as "the two things that make it not
a one-liner" turned out to be cheaper than described:

1. **`generate_related_concepts` order-sensitivity is not a date-concept
   problem.** It only fires on `StructType` — `generate_date_concepts` (the
   function with the `if address in environment.concepts: continue` guard) is
   reached from `enrich_environment`, not from `add_concept`. So the fast path
   just declines on struct concepts and the question never arises.
2. **The per-edge `concepts` filter** is handled by declining, not by applying
   the filter in the bulk path. It is 0 of 143 edges in the corpus.

## Second landing: don't build suggestions for a swallowed lookup

A re-profile after the bulk merge put `difflib` in the top 5 of a *successful*
parse. `EnvironmentConceptDict.get()` was calling `__getitem__` and catching
`UndefinedConceptException` — but `raise_undefined` runs
`_find_similar_concepts` (an O(concepts) difflib pass) to build the
"Suggestions:" text first, and `get()` throws the whole exception away.

Over the warm corpus that was **450 of 450 calls wasted** — every single
`_find_similar_concepts` call was under a `.get()`. `__getitem__` /
`raise_undefined` now take `suggest: bool = True`, threaded through the
recursive re-lookups, and `get()` passes `suggest=False`. Suggestions on
genuine user-facing errors are unchanged and still pinned
(`tests/test_undefined_concept.py`, `tests/test_rowset_output_shorthand.py`).

`_find_similar_concepts`: **450 -> 0** calls per warm corpus parse.

## Session total

Both changes, alternating legs in one process, 11 reps:

| | pre-session | current |
|---|---|---|
| `add_concept` | 52,775 | 4,076 |
| `_find_similar_concepts` | 450 | 0 |
| `add_datasource` (control) | 5,366 | 5,366 |
| corpus parse, min | 0.638s | **0.492s** (1.30x) |
| corpus parse, median | 0.738s | **0.543s** (1.36x) |

So the 1.3x the original writeup predicted *was* reachable — just not from the
merge alone; it took the merge plus the suggestion fix.

## Considered and declined: caching the closure text re-hash

The other profile hotspot is file I/O: 1,922 `open()` calls per warm corpus
parse, all from `import_service.py::_store_lookup` re-reading every file in an
entry's transitive closure to re-hash it (33 distinct files, ~16 reads per
parse — already deduped *within* a parse by `text_lookup`, so the cost is
across parses).

Patching it out entirely is worth **~0.074s (1.13x)** — the largest single
remaining item. It was still declined: that re-hash *is* the store's mechanism
for noticing a file edit (`test_transitive_content_invalidation`). The safe
version caches on `(path, mtime, size)` — a stat instead of a read — which is
strictly weaker than a content hash (mtime granularity, same-second edits) and
would buy less than the 0.074s upper bound. On an LSP/interactive path a
missed edit is a much worse failure than 50ms. Revisit only with a real
invalidation story.

`CustomFunctionFactory.__call__`'s `copy.deepcopy` (173 top-level calls, 61k
inner, ~0.1s tottime) is the third item — each `def` macro expansion needs its
own copy, so this is semantically load-bearing and not a free win.

## What is left

`add_concept` is down to 4,076 calls, of which ~2,300 are the **bare-import**
merge (`import x`, no alias — objects shared, no projection built). Applying
the same treatment there would recover maybe 0.008s. Not worth the risk; the
bare path is also where the remaining collisions live, so it would not be a
clean pure-insert case.

Parse is ~3% of a corpus render, so this was for interactive/LSP latency, not
throughput. Re-profile before assuming anything else in parse is on top.

## Gates run

Run once per landing (both times, clean):

- `tests/parsing/test_import_env_store.py` + `test_reparse_content_version.py`
  — 20 tests (18 prior + 2 new: `test_bulk_merge_matches_the_per_concept_loop`,
  which asserts the fast path fires *and* that forcing it off yields the same
  environment, and `test_bulk_merge_declines_for_a_filtered_import`).
- Corpus byte-identity, four legs in ONE process (loop-warm / bulk-cold /
  bulk-warm / loop-cold): **132/132 identical on every leg**.
- Full suite `-m "not adventureworks_execution"`: 7137 passed, 110 skipped,
  1 xpassed, 0 failed.
- `ruff check --select E,F,I`, `mypy trilogy` (348 files clean), `black`.

For the suggestion change specifically, the suggestion-text assertions in
`tests/test_undefined_concept.py`, `tests/test_rowset_output_shorthand.py`,
`tests/core/test_exceptions.py` and `tests/test_imports.py` are the gate that
the `suggest=False` flag did not leak onto the user-facing error path.
