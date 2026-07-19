# Handoff — `explore` exposes `_`-prefixed private concepts from the target file

**Status:** OPEN agent-facing framework bug. Discovered in enriched TPC-DS q16
on 2026-07-17. Exposure of a private implementation field directly initiated
a 75-iteration, 3.38M-token agent exhaustion.

## Summary

Trilogy uses a leading underscore to mark private/internal concepts that model
consumers should not use directly. `trilogy explore` correctly suppresses
`_`-prefixed leaves inside imported namespaces, but it exposes them when they
are declared locally in the file being explored.

The q16 agent ran:

```text
trilogy explore raw/catalog_sales.preql
```

The default v2 concept listing included:

```text
_returned_order_number int?; # The catalog_returns order number matched to this line.
auto is_returned <- _returned_order_number is not null;
```

The agent then used `cs._returned_order_number` instead of the intended public
`cs.is_returned` concept. Its attempted zero-return aggregation produced null
on zero matches, and the agent spent the remainder of the run probing and
reinterpreting the data.

This is not an `include-hidden` request: the agent used ordinary `explore`, so
private concepts should never have appeared.

## Evidence

- Run: `evals/tpcds_agent/results/20260717-023235_enriched`
- Trajectory: `agent_log.q16.{jsonl,conversation.txt}`
- Crash summary: `crash.q16.txt`
- Model: `tests/modeling/tpc_ds_duckdb/catalog_sales.preql`

The first exposure is in `agent_log.q16.conversation.txt` around lines
1282–1308, in the result of the unfiltered explore call. It appears under the
local namespace's `properties` array:

```json
{
  "grain": "<item.sk, order_number>",
  "properties": [
    "_returned_order_number int?; # The catalog_returns order number matched to this line.",
    "auto is_returned <- _returned_order_number is not null; # True if this line was later returned, else False."
  ]
}
```

The agent subsequently ran:

```text
trilogy explore raw/catalog_sales.preql \
  --regex 'returned_order_number|catalog_returns|_return'
```

That ordinary regex-filtered call exposed the private field again around lines
1885–1900. Regex filtering must not bypass private visibility unless
`--include-hidden` is explicitly provided.

The causal transition is explicit in the next assistant message:

> The catalog_sales model has `_returned_order_number` which links to catalog
> returns.

It then authored queries directly against that field.

## Root cause

`trilogy/scripts/explore.py::_imported_payload` already has the intended rule:

```python
if leaf.startswith("_"):
    continue
```

However, `build_concepts_payload` sends local concepts directly to
`_grouped_decls` without applying equivalent filtering:

```python
local_items = [(a, c) for a, c in concept_items if c.namespace == DEFAULT_NAMESPACE]
namespaces = _grouped_decls(env, local_items)
```

When exploring `raw/catalog_sales.preql` itself, `_returned_order_number` is a
local concept, not an imported one, so it bypasses `_imported_payload` and is
rendered publicly.

The rich/group render paths should be audited for the same asymmetry. The
visibility decision should happen once before any renderer, grouping, regex,
or namespace-dedup path rather than being implemented only inside
`_imported_payload`.

## Minimal reproduction

From the repository root:

```powershell
.venv\Scripts\python.exe -m trilogy.scripts.trilogy explore `
  tests/modeling/tpc_ds_duckdb/catalog_sales.preql
```

Or through the agent harness's JSON mode:

```text
trilogy explore raw/catalog_sales.preql
```

Observed: `_returned_order_number` appears in the concepts payload without
`--include-hidden`.

Also reproduce the regex bypass:

```text
trilogy explore raw/catalog_sales.preql --regex '_returned_order_number'
```

Observed: the private field is returned without `--include-hidden`.

## Expected behavior

- By default, exclude any concept whose display leaf begins with `_`, whether
  it is local, imported, role-played, or selected through `--regex`.
- Preserve public derived concepts such as `is_returned`, even when their
  lineage references a private concept. Rendering the public declaration may
  show its expression; it should not make the private concept itself a listed,
  discoverable API surface.
- `--include-hidden` should explicitly restore private concepts for debugging
  and model-authoring workflows.
- Continue suppressing `__` internal namespaces and builtin concepts according
  to the existing `--include-builtins` behavior.

## Regression tests

Add coverage in `tests/scripts/test_explore.py` or
`tests/scripts/test_json_output.py` using a model containing:

```trilogy
key id int;
property <id>._raw_flag int?;
auto public_flag <- _raw_flag is not null;
```

Assert across JSON and rich output:

1. Default exploration omits `_raw_flag` as a listed concept.
2. `public_flag` remains visible.
3. `--regex '_raw_flag'` returns no private concept by default.
4. `--include-hidden` exposes `_raw_flag`.
5. The same behavior holds when the model is explored directly and when it is
   imported under an alias.
6. `--show concepts`, `--show groups`, and `--show all` behave consistently.

Add a fixture-level regression asserting that default exploration of
`catalog_sales.preql` does not list `_returned_order_number` but does list
`is_returned`.

## Acceptance criteria

- No `_`-prefixed concept is discoverable through ordinary `explore`, including
  regex-filtered exploration.
- `--include-hidden` remains the single explicit opt-in.
- Public concepts backed by private implementation fields remain visible and
  usable.
- Existing imported-namespace filtering and conformed-role deduplication are
  unchanged.
- Targeted explore tests pass.
