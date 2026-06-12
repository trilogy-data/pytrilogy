# Follow-up: undefined-concept suggestions can't see CTE / named-statement outputs

**Status:** OPEN (found 2026-06-12, enriched eval q64 trajectory). A partial fix for
the *ranking* half landed the same day; this doc is the remaining *candidate-set* half.

## Symptom

An agent (q64) wrote a query whose final `select` / `inner join` referenced
`y1999.item_id`, but the column the `with y1999 as …` CTE actually exposes is
`y1999.agg.item_id` (it selected `agg.item_id` without an `as`, so the column kept
its source namespace). The error was:

```
Undefined concept: y1999.item_id. Suggestions: ['ss.item.id', 'cs.item.id', 'cr.item.id']
```

The right answer (`y1999.agg.item_id`) was **not** suggested — only unrelated
`*.item.id` fuzzy matches were. The agent then thrashed instead of being pointed at
the real path.

## Root cause

Two independent issues; only the first is fixed.

1. **(FIXED 2026-06-12) Ranking.** `_find_similar_concepts`
   (`trilogy/core/models/environment.py`) had no namespace-prefix signal, so even when
   the real path *was* a candidate it ranked behind same-leaf/fuzzy noise. Fixed by an
   ordered-subsequence ("partial path") match ranked first — `y1999.item_id` now
   subsequence-matches `y1999.agg.item_id` and leads. Tests in
   `tests/test_undefined_concept.py` (`test_find_similar_partial_path_*`,
   `test_is_subsequence_is_ordered`).

2. **(OPEN — this task) Candidate set is empty for CTE outputs mid-parse.** The
   suggestion was wrong because `y1999.agg.item_id` **was not in the candidate set at
   the raise site at all** — no heuristic can surface a concept it can't see.

   The raise comes from `EnvironmentConceptDict.raise_undefined`
   (`environment.py`, ~line 247, reached via `__getitem__` on the join-key lookup),
   which calls `self._find_similar_concepts(key)` with **no `extra_keys`**. During a
   single combined parse of a multi-statement query, `with … as` / `rowset` outputs are
   committed to `environment.concepts` only at *end of parse*, and the named-statement
   prefixed concepts are never staged into this dict-level call. So at the moment the
   3rd statement fails, `self.keys()` has no `y1999.*` / `agg.*` and `extra_keys` is not
   passed.

   Contrast: the `select_finalize.py` raise sites (`_raise_undefined`,
   `raise_collected_undefined`) *do* pass `extra_keys=_staged_addresses(context)` and so
   can see staged rowset outputs. The dict-level join-key path is the one that's blind.

### Reproduce (minimal, no tpcds model)

```python
from trilogy.core.models.environment import Environment
env = Environment()
env.parse("key id int;\nproperty id.color string;\nproperty id.name string;\n"
          "datasource items (id:id, color:color, name:name) grain (id) address items;")
src = (
    "with agg as select id as item_id, name as product_name where color = 'red';\n"
    "with y1999 as select agg.item_id, agg.product_name where agg.item_id > 0;\n"
    "select y1999.product_name inner join y1999.item_id = y1999.item_id;"
)
env.parse(src)   # UndefinedConceptException: Undefined concept: y1999.item_id. (suggestions: [])
```

Spying confirms: at the raise, both `self.keys()` and `extra_keys` contain **zero**
`y1999.*`/`agg.*` entries. But after the same statements parse *separately*,
`env.concepts` does contain `y1999.agg.item_id` (leaf `item_id`) — and a direct
`env.concepts._find_similar_concepts('y1999.item_id')` then returns
`['y1999.agg.item_id', 'agg.item_id']`. So the only thing missing is candidate
visibility at the dict-level raise during a combined parse.

## What "fixed" should look like

When `raise_undefined` (or `_find_similar_concepts`) runs, the candidate set should
include the referenceable outputs of every named statement defined so far in the parse,
i.e. for each entry in `environment.named_statements`, the addresses
`{statement_name}.{strip_local(output.address)}`.

Reconstruction note: `named_statements['y1999'].output_components` carry addresses like
`agg.item_id` → prefix to `y1999.agg.item_id` (matches the committed form ✓). But `agg`'s
own outputs come back as internal `local._agg_item_id` (aliased via `as item_id`), which
do **not** naively reconstruct to the committed `agg.item_id`. So enumerate from the
public/exposed output names, not the raw lineage addresses — find where the committed
`{name}.{col}` concepts are derived at end-of-parse (search `add_rowset_concept`,
`rowset_to_concepts_v2`, `select_finalize.rowset_to_concepts`) and reuse that mapping, or
expose a helper on `SelectLineage`/`Environment` that yields a named statement's public
output addresses.

### Wiring obstacle

`EnvironmentConceptDict` has **no back-reference** to its `Environment`, so it can't read
`named_statements` from inside `raise_undefined`. Options:
- Give the dict an optional weak ref to the owning environment (set on attach), and
  enumerate named-statement outputs into `extra_keys` when raising. Cleanest but touches
  the dict's lifecycle.
- Route the dict-level join-key lookups through a raise helper that *does* have the
  environment/parse context (mirror what `select_finalize` already does), and pass
  `extra_keys` there.
- Have the parser commit named-statement public concepts incrementally (as each `with`
  finalizes) rather than only at end-of-parse, so `environment.concepts` is populated by
  the time a later statement references them. This also fixes *resolution*, not just
  suggestions — verify it doesn't change scoping/shadowing semantics for same-named
  columns across statements.

Pick based on whether the goal is "better error message" (extra_keys plumbing) or
"the reference should resolve" (incremental commit — larger blast radius, needs the full
suite + a tpcds run).

## Validation

- Minimal repro above should suggest `y1999.agg.item_id` first.
- Re-run the real trajectory: enriched q64 via
  `python evals/tpcds_agent/repeat_query.py --query-id 64 --repeats 10 --scale-factor 1`
  (q64 is the failing-and-expensive one — ~41 iters / 2.1M tokens in the 20260612 run).
- `tests/test_undefined_concept.py` must stay green; add a parse-level test that the CTE
  output is suggested for a dropped-namespace reference (the current tests are dict-level
  only because the candidate-visibility bug blocks an end-to-end one).

## Files

- `trilogy/core/models/environment.py` — `EnvironmentConceptDict.raise_undefined`
  (~247), `_find_similar_concepts` (~340, partial-path match already added),
  `_is_subsequence` helper, `add_rowset` / `named_statements` (~474).
- `trilogy/parsing/v2/select_finalize.py` — `_staged_addresses`, `_raise_undefined`,
  `raise_collected_undefined` (the sites that *do* pass `extra_keys`; mirror their
  pattern).
- `trilogy/parsing/v2/rowset_semantics.py`, `rules/rowset_rules.py`,
  `rules_context.add_rowset_concept` — where named-statement public concepts are derived.
- `tests/test_undefined_concept.py` — existing coverage.
