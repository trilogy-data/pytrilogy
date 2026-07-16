# Handoff — `union join` of two aggregates on their SOLE grouping key → dangling merge-join column (BinderException)

**Status:** OPEN framework codegen bug. Loud (DuckDB rejects the emitted SQL).
Independent of rowsets — reproduces in a bare top-level select. Discovered while
reviewing the rowset/join boundary after fixing
[`bug_q64_reused_joined_rowset_self_pair_disconnect_timeout.md`]. Not the same
bug as q64 (that was rowset-slice sourcing of a collapsed key, now fixed); this
is deeper, in the coalescing-join completion merge.

Per project rule: a `BinderException` from Trilogy-GENERATED SQL is ALWAYS a
framework bug — the engine must compile it or reject it with a clear authored
error, never emit SQL the database rejects.

## Symptom

`union join` (any coalescing join — `union`/`full` collapse a key group) between
two aggregates **whose only grouping column is the join key**, selecting just the
key and a measure, emits SQL whose completion merge references a column the
grouped CTE never projects:

```
(_duckdb.BinderException) Binder Error:
Values list "abundant" does not have a column named "ci_k"
```

## Minimal repro

Against the standard `tests/modeling/tpc_ds_duckdb` model (sf=0.01 engine),
NO rowset wrapper, NO downstream reference:

```trilogy
import store_sales as ss;
import catalog_sales as cs;

with ci as select cs.item.sk as k, sum(cs.ext_list_price) as amount;
with sa as select ss.item.sk as k, sum(ss.list_price) as lp;

select sa.k, sa.lp
union join sa.k = ci.k
having ci.amount is not null;
```

## Trigger matrix

| Shape | Result |
|---|---|
| `sa` groups by **key only** + measure (above) | **BinderException** |
| `sa` groups by key + a second dim (`ss.store.sk`) + measure | Pass |
| `sa` groups by key + two dims + measure (real q64 `base`) | Pass |
| Same key-only shape but `full join` instead of `union join` | Pass |
| Same key-only shape, wrapped in a rowset + sliced (q64 core) | BinderException (post-q64-fix; was a render sentinel pre-fix) |

The discriminator is exact: **the aggregate's only grouping column is the join
key.** Any additional grouping dimension makes the merge well-formed. This is why
the real q64 (`base` groups by product_name, store_name, zips, addresses, year…)
does not hit it, and why the q64 fix is correct and sufficient for that query —
but the key-only variant remains broken.

## Root cause (the emitted SQL)

`sa` materializes as CTE `abundant` grouped on the join key:

```sql
abundant as (
  SELECT "questionable"."sa_k" as "sa_k", "questionable"."sa_lp" as "sa_lp"
  FROM "questionable" GROUP BY 1, 2)
```

`abundant` has columns `sa_k`, `sa_lp` only. The completion merge then emits:

```sql
SELECT coalesce("abundant"."sa_k","wakeful"."ci_k") as "sa_k",
       "abundant"."sa_lp" as "sa_lp"
FROM "abundant"
  INNER JOIN "cheerful" on "abundant"."sa_k" = "cheerful"."ci_k"
  INNER JOIN "wakeful"  on "abundant"."ci_k" = "wakeful"."ci_k"      -- <-- abundant.ci_k does not exist
                      AND "abundant"."sa_k" = "wakeful"."ci_k"       -- (this operand is already correct)
```

The second `wakeful` join condition is doubly specified: `abundant.ci_k =
wakeful.ci_k AND abundant.sa_k = wakeful.ci_k`. The first operand
(`abundant.ci_k`) is spurious — `abundant` groups by `sa_k`, and `ci_k` is only
the coalesced output alias, not a column of `abundant`. The correct join is
`abundant.sa_k = wakeful.ci_k` alone (already present as the AND term).

The join-inference produces a phantom `abundant.ci_k` operand because the
coalesced group's canonical (`ci.k`) is treated as available on the anchor side
even though, at the key-only grain, the anchor projects the pseudonym member
(`sa.k`) rather than the canonical. With an extra grouping dim the inference
routes differently and the phantom operand is not emitted.

## Fix locus

The coalescing-join completion merge / join-condition inference —
`trilogy/core/processing/nodes/merge_node.py` and
`trilogy/core/processing/utility.py::get_node_joins` (whichever assembles the
`MergeNode` parent join for a distinct-scoped-join group). The merge must relate
the anchor by the key column it actually projects (the group member it grouped
on), not by the group canonical when the anchor doesn't expose it. Suppressing
the redundant first operand (keep only `abundant.sa_k = wakeful.ci_k`) is the
concrete symptom to eliminate; verify it generalizes to three-way groups
(`a=b=c`) and to the rowset-wrapped path (q64 key-only variant).

## Secondary observation (lower priority, likely separate)

`subset join` on the same sole-key shape raises a **clean**
`UnresolvableQueryException` ("no complete sources found for output concepts
{'sa.k'}") rather than a BinderException. That is contract-compliant (clean
authored error, not leaked SQL), but semantically a `subset join sa.k = ci.k`
narrowing `sa` to `ci`'s key set arguably should resolve. Worth confirming
whether the disconnect is intended (subset marks the member partial on its own
scope — cf. the q59 self-recursion guard) or a coverage gap.

## Coverage

`tests/test_rowset_generation_matrix.py` now parametrizes cross-rowset shapes
over `left` and `union` join types and includes the coalescing-key-referenced-
downstream shape (the q64 fix). The key-only union-join shape above is added
there as an `xfail` pinned to this handoff; flip it to a passing cell when fixed.
