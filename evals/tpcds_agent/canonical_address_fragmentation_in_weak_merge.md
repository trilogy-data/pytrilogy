# Why the Steiner injection can't bridge two rowsets — canonical-address fragmentation

> **RESOLVED 2026-06-30.** `island_rowsets_for_weak_merge` now reshapes the
> weak-merge graph (sever rowset↔content by canonical address across all grains +
> per-rowset output hub), and `determine_induced_minimal_nodes` exempts `ROWSET`
> from the `filter_downstream` purge. `gen_merge_node` bridges the two rowsets
> over the derived join key. Regression test:
> `tests/.../test_rowset_derived_join_key_enrichment.py::test_weak_merge_bridges_derived_rowset_join`.
> This is the canonical-discovery capability; the rowset-priority enrichment path
> still uses the localized `rowset_node.py` materialize fix (delegating it to
> `gen_merge_node` re-enters the rowset and recurses), so both coexist. The notes
> below are the original diagnosis, kept for the design rationale.


Context: making `determine_induced_minimal_nodes` / `resolve_weak_components`
(`node_generators/node_merge_node.py`) bridge two rowsets joined on a
**derived-expression** key (`inner join a.grp + 1 = b.grp`, then `sum(b.tot)` by
`a.grp`). The minimal example is the q02 driver; harness in
`tests/generators/test_rowset_derived_join_key_enrichment.py`.

## TL;DR

The bridge edge `a.grp ↔ joinkey ↔ b.grp` **exists and is traversable**. The only
reason the network-discovery (Steiner) injection returns `None` is that the two
rowset **measures** (`a.tot`, `b.tot`) connect to their own grain key *only*
through an **aggregate** node, and aggregates are always pruned — so each measure
is an isolated component, and the Steiner tree over `{a.grp, a.tot, b.tot}` can't
span them. Linking each rowset's outputs to one another (a per-rowset hub) closes
the gap. The catch that wasted earlier attempts: graph nodes are keyed on the
**canonical** address, not the friendly one — match the wrong one and the hub
attaches to nothing.

## The two independent multiplicities ("many versions of the canonical address")

A `BuildConcept` carries both `.address` (friendly, e.g. `a.grp`) and
`.canonical_address` (internal, e.g. `a._virt_3746183633448277`). The reference
graph keys every node as `c~<canonical_address>@Grain<...>`, and
`environment.canonical_concepts[<canonical_address>]` maps back to the friendly
concept. Two separate things create "many versions":

### 1. friendly → canonical aliasing (1:1, stable, NOT the problem)

Rowset outputs are aliases (`select grp as grp` inside rowset `a`), so the
friendly name resolves to an internal `_virt_*` canonical:

| friendly concept | `.canonical_address` (graph node base) |
|---|---|
| `a.grp`  | `a._virt_3746183633448277` |
| `a.tot`  | `a._virt_4442502139159310` |
| `b.grp`  | `b._virt_merge_4438086194343205` |
| `b.tot`  | `b._virt_6947647709256901` |
| `local._virt_func_add_1352…` (the join key `a.grp+1`) | `local._virt_func_add_6829860569878987` |

This is consistent and reversible. The **trap**: any helper that groups graph
nodes by rowset output must use the *canonical* address. `concept.address` is
`a.grp`; `extract_address(node)` is `a._virt_3746…`. Matching friendly-vs-graph
silently attaches to zero nodes (this is exactly why the first hub experiments
produced "no steiner tree" — the hub linked nothing).

### 2. per-grain node instances of one canonical concept (the real fragmentation)

The same canonical concept appears as a **separate graph node per grain** it is
referenced at. Observed instances for this query:

| canonical | grain instances in the graph |
|---|---|
| `a._virt_3746…` (`a.grp`)   | `@Grain<a.grp>`, `@Grain<local.grp>` |
| `a._virt_4442…` (`a.tot`)   | `@Grain<Abstract>`, `@Grain<a.grp>` |
| `b._virt_merge_4438…` (`b.grp`) | `@Grain<b.grp>`, `@Grain<local.grp>` |
| `b._virt_6947…` (`b.tot`)   | `@Grain<Abstract>`, `@Grain<b.grp>` |
| `local._virt_func_add_6829…` (join key) | `@Grain<Abstract>`, `@Grain<a.grp,local._a_grp,local.grp>` |

The default-grain node (what `concept_to_node(c.with_default_grain())` and the
Steiner `nodelist` use) is just *one* of these instances, and instances of the
same concept are **not** auto-connected to each other.

## The actual bridge edges (undirected view)

Edges that exist among the bridge-relevant nodes:

```
a.grp[Grain<local.grp>]        — joinkey[Grain<a.grp,local._a_grp,local.grp>]
joinkey[Grain<a.grp,…>]        — b.grp[Grain<b.grp>]
joinkey[Grain<a.grp,…>]        — b.grp[Grain<local.grp>]
a.grp[Grain<local.grp>]        — joinkey[Grain<Abstract>]
b.grp[Grain<local.grp>]        — joinkey[Grain<Abstract>]
```

So `a.grp ↔ joinkey ↔ b.grp` is a real, connected path — and the
`a.grp` default-grain node (`@Grain<local.grp>`) is the very instance that sits on
it. The cross-rowset bridge is fine.

What's missing: `a.tot` and `b.tot`. Their only edges go to `_virt_agg_sum_*`
(AGGREGATE) nodes, and `determine_induced_minimal_nodes` unconditionally drops
`CONSTANT`/`AGGREGATE`/`FILTER`. After that removal `a.tot[Abstract]` and
`b.tot[Abstract]` are isolates → removed → not in the final tree → `None`.

## Why both passes still failed before

`gen_merge_node` runs two passes, `filter_downstream` ∈ {True, False}:

- **`filter_downstream=True`** additionally drops every non-`ROOT`,
  non-materialized node — which includes the rowset outputs *themselves* and the
  BASIC join key. Nothing survives. (Fix: exempt `ROWSET`; the join key needs to
  survive too — it's the bridge.)
- **`filter_downstream=False`** keeps `ROWSET`/`BASIC`, so `a.grp`/`b.grp`/joinkey
  survive and the cross-rowset bridge is intact — but `a.tot`/`b.tot` still
  isolate on the aggregate prune. (Fix: hub-link each rowset's outputs.)

## Validated fix shape (connectivity proven)

With, on the search graph used by `resolve_weak_components`:
1. exempt `ROWSET` derivation from the `filter_downstream` purge, and
2. add a per-rowset hub node linking **all grain-instances whose
   `extract_address` ∈ {canonical addresses of that rowset's outputs}**,

the three targets `{a.grp, a.tot, b.tot}` collapse into a single connected
component (verified: components `[7, 7, 7]`, was `[7, 10, 11]`). The Steiner tree
then spans them via `a.tot — hub_a — a.grp — joinkey — b.grp — hub_b — b.tot`,
injecting the join key exactly as intended.

Open design questions to settle together:
- Hub node naming/stripping so it is pure connectivity glue and never mistaken
  for a concept downstream (`extract_ds_components`, `subgraphs_to_merge_node`).
- Whether to also reconcile the per-grain instances generally (a deeper change)
  or keep the hub localized to the weak-merge graph.
- Whether the localized `rowset_node.py` materialize fix can then be retired in
  favor of this canonical path.
