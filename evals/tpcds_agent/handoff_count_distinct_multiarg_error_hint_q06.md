# Handoff: `count_distinct(a, b)` parse error should point at `count(grain(a, b))` (q06)

**Status: small, well-scoped error-UX improvement. From
`results/20260810-211903_enriched_noise/agent_log.q06.jsonl`.**

## What happened

SQL habit: `COUNT(DISTINCT a, b)`. The agent wrote the direct translation:

```trilogy
import raw.item as i;

select
    count_distinct(i.id) as n_id,
    count_distinct(i.id, i.category) as n_id_cat,
    count_distinct(i.id, i.category, i.current_price) as n_id_cat_price
;
```

`file write` refused it with a bare grammar dump:

```
refused to write 'inspect5.preql': not syntactically valid Trilogy.
Parse error:
  --> 5:24
  |
5 |     count_distinct(i.id, i.category) as n_id_cat,
  |                        ^---
  |
  = expected dot_tail, bracket_tai...
```

The agent recovered by hand-rolling `count_distinct(concat(i.id::string, '|',
i.category::string))` — which works but is exactly the delimiter-collision
pattern `grain(...)` exists to replace. The idiomatic form is
`count(grain(i.id, i.category))`.

## The task

When a parse failure sits at a comma inside a `count_distinct(...)` call (i.e.
the user supplied more than one argument), append a hint to the error:

> `count_distinct` takes one argument. To count distinct combinations of
> several fields, use `count(grain(a, b))`.

Implementation notes:

- Both parser backends matter (lark + pest/rust; pest is the default and needs
  a maturin rebuild — from repo ROOT only). Prefer implementing the hint at
  the shared error-formatting layer above the backends if one exists (the
  `file write` refusal path and `run` both surface parse errors), so it does
  not need to be duplicated per grammar. A cheap heuristic on the failing
  source line (`count_distinct(` before the error column, `,` at/near it) is
  acceptable — this is a hint, not a diagnosis.
- Grep for how existing targeted hints are attached (e.g. the rename-to-self
  error seen in the same run: "Output column 'x' renames 'local.x' back to..."
  — that error names its cause and is a good model).
- Test: a parse-refusal message for multi-arg `count_distinct` contains
  `count(grain(`. One test per surfaced path (`file write` refusal, `run`
  parse error) is enough.

## Why it matters

The language reference documents `grain(...)` counting, but the doc is long
and agents pattern-match from SQL first. The error site is the one place the
hint is guaranteed to be read at exactly the right moment.
