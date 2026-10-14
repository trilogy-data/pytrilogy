# Datasource assertions (`assert` / `prior`)

A managed datasource's refresh replaces its asset with whatever the build
produced. Before this feature nothing let the model say "this build is
implausible, keep the old asset": a portal that served 100 rows instead of
1.47M refreshed successfully and published them over the city. `validate
datasource` runs on the published asset, after the damage, and checks types
and grain rather than volumes.

## Syntax

Any number of `assert` clauses on a datasource, after the refresh clauses and
before `state`:

```trilogy
datasource cobog_tree_info (
    tree_id, city, data_source, ...
)
grain (tree_id)
file `.../cobog_tree_info.parquet`
freshness by cobog_published_data_updated_through
# relative: no source may lose more than 25% of its rows in one build
assert count(tree_id) by data_source >= 0.75 * prior(count(tree_id) by data_source)
# absolute floor, independent of history
assert count(tree_id ? data_source = 'BOGOTA_OPENDATA') >= 1000000
;
```

An assertion is a boolean over aggregates of one build of the datasource.
`prior(expr)` is the same expression evaluated over the asset the build is
about to replace. Aggregates may carry a `by` grain; every grouped term in one
assertion must share the same grain, which is also the only column the
condition may name outside an aggregate. The condition can only read columns
the datasource stores (or row-level derivations of them), for the same reason
its `where` can: it checks what the build writes.

`prior` is spelled as a function rather than a namespace (`count(prior.tree_id)`)
because grouping must line up between the two sides: wrapping the whole
grouped aggregate makes the `by` keys the join key by construction, where a
per-concept prefix would group the two sides on different concepts.

## When it runs

Every persist of the datasource, whether from `trilogy refresh`, a
`persist into` statement, or `Executor.update_datasource`. A dry run never
reaches it.

- **File targets** are checked on the staged file (`staged_write`) before the
  rename. A failure deletes the staged object and leaves the asset's bytes and
  mtime untouched, local and remote alike. An incremental append stages the
  full new asset (existing rows plus new), so the check sees the whole file.
- **Table targets** are checked inside the write transaction, after the
  INSERT and before the commit; a failure rolls the write back on engines with
  transactional DDL. Engines whose native writer commits on its own (BigQuery
  via `SupportsNativePersist`) can only report the failure afterwards.
- `prior(...)` terms are read **before** the write, since a table overwrite
  destroys the previous asset in place.

Any failing assertion raises `DatasourceAssertionError`, which a refresh wraps
in `RefreshAssetError`, so the refresh and any cloud run fail with the message
in the error. The message names each failing assertion, its group, and every
measured value:

```
built: 2 assertion(s) failed; the asset was not replaced.
  - `count(id) >= 0.75 * prior(count(id))`: count(id) = 720, prior(count(id)) = 1000
  - `count(id) by source >= 0.75 * prior(count(id) by source)` for source = 'B': group has no rows in the new build; prior(count(id) by source) = 100
```

## Semantics at the edges

- **Groups** are outer-joined between the two sides. A group present in the
  previous asset and absent from the build fails ("group has no rows in the
  new build"). A group present in the build and absent from the previous
  asset passes with a warning: a new source has no history to be judged by.
- **No previous asset** (first build, missing or unreadable file, a table
  with no rows for a grouped assertion, or a previous schema missing a column
  the assertion needs): every assertion using `prior` passes with a warning.
  Assertions without `prior` still apply, so an absolute floor catches a bad
  first build. Schema drift is deliberately not an error, or a new column
  could never roll out.
- **NULL** never satisfies an assertion on the new side (reported as such).
  A NULL previous value passes with a warning, like a missing one.

## How it is evaluated

`trilogy/core/assertions.py` splits a condition into its aggregate terms
outside `prior` (read from the new build), the arguments of each `prior`
(read from the previous asset), the shared `by` grain, and a template with
placeholders where the terms were. `trilogy/execution/assertions.py` runs one
trilogy select per side, pinned to the datasource at that physical address by
swapping in a repointed copy and hiding every other datasource, then joins
the two result sets on the grain in Python and checks each row by
substituting the values into the template and running it as a constant query.
The comparison itself therefore keeps the engine's semantics rather than
reimplementing them.

`prior` is `FunctionType.PRIOR` so it rides the ordinary expression
machinery (namespacing, argument validation, rendering); it has no SQL form,
and rendering it outside an assertion raises.

## Overrides, dry runs and `validate datasource`

- **`trilogy refresh <file> --skip-validation <datasource>`** (repeatable,
  comma-separated) builds the named datasources without evaluating their
  assertions, for a drop the model's guard would refuse but you know to be
  legitimate (a dataset migration that correctly shed 29% of rows). It is
  carried on `RefreshPolicy.skip_assertions`, the single CLI-to-plan mapping,
  so it reaches the single-file and directory paths alike. The skip is never
  silent: a warning line under the asset's "Refreshing" line, an executor log,
  and an `asset_assertions` record with `skipped: true` in the run report. A
  name that matches no datasource, or one declaring no `assert`, is rejected
  up front. The next build's `prior` is the new baseline, so a skip never
  needs repeating.
- **`refresh --dry-run`** lists each stale asset's assertions under its
  "Would refresh" line and emits the same `asset_assertions` record, so a
  reviewer can see which checks gate a build before running it.
- **`validate datasource <name>`** runs the datasource's assertions against
  the asset as published, as `assertion` rows beside the type, grain and
  domain checks; a failure fails the validation like any other. There is one
  asset and no previous one, so an assertion that reads `prior(...)` is
  listed with `ran = false` rather than evaluated.

## Not yet done

- A `--check-assertions` mode that builds to staging and evaluates without
  swapping.
- A committable one-shot override for cloud jobs, which cannot pass flags.
- Reusable assertions attached from an import (one relative guard shared by
  many city models).
