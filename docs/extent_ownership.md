# Extent ownership: who manufactures a `~` span's extension rows

A column-level `~` binding licenses **domain extension**: unmatched members of
that key's dimension enter the result once, carrying their own attributes, with
every concept outside the key's functional closure NULL
(`trilogy/core/processing/partial_bridging.py`).

The semantics say those rows exist. They do not say which part of the plan
builds them, and for a long time nothing else did either. Each branch that
touched a `~` key padded its own joins, so the same extension member was
manufactured two or three times over; the FINAL assembly then picked one copy
to anchor on and either reunited the rest null-safely or read them through a
plain equality that threw them away. A plan containing a join whose rows the
same plan discards is a logical-plan defect, and the only layer that could see
it was the CTE optimizer, which is why the first fix lived there
(`PruneInvisibleOuterJoins`, `docs/handoff_field_report_residual_stitch.md`;
since deleted, the planner no longer builds these joins).

Extent routing is now a decision, taken once, before any node is built.

## The election

`trilogy/core/processing/v4_helper/extent_ownership.py`, called at the end of
`build_group_graph` once the FINAL contract is known.

1. **Which spans are in play.** A licensed key qualifies when the statement
   projects it, or projects something it functionally determines, because an
   extension row exists to carry one dimension member's own attributes. A `~` FK
   that only shows up as a join axis licenses nothing, so the election returns
   empty and the whole mechanism is inert (that is the common case: TPC-DS and
   TPC-H never demand one).
2. **Who owns each span.** Among the groups that expose the key, the most
   downstream wins: its rows have already absorbed everything upstream, so
   routing extent there keeps one copy instead of one per branch. Ties break
   toward the group that owns the key as a member (the dimension span) over one
   merely carrying it as a join column.
3. **Keep the families together.** A group exposing *every* span takes them all.
   Split ownership manufactures the same member in two branches, and the FINAL
   merge can then only reunite them by pairing padding null-safely, which is the
   cross-family hazard the reunion machinery exists to survive.
4. **Who may pad.** The owner and its ancestors: the extension rows have to
   reach the owner somehow. Every other group is **extent-free** for that span.

The result rides on the FINAL sink's `GroupAttrs.extent_ownership`.

## What extent-free means

`build_strategy_node` scopes `environment.extent_free_spans` around each group's
build, and every `MergeNode` captures it at construction, before the node
resolves: a routing decided after resolution would leave the padding's nullable
marks behind. Three things follow, all in
`trilogy/core/processing/join_resolution.py`:

- **`~` grants no row intent.** With a clean fact/dimension split the fact side
  anchors and equality sheds the members it never referenced. When both sides
  bind the key `~`, `partial_binding_sources` decides: two projections of the
  SAME leaf binding cover the same subset, so the span drops out of the typing
  and the remaining keys decide; PEER facts (sales and returns each referencing
  their own slice of the group domain) each hold rows the other lacks, and
  dropping either side's is a chasm rather than an extension, so their typing
  stands whoever owns the extent.
- **Inherited padding is absence, not content.** A shared ancestor may
  legitimately pad on the way to the owner (`extension_padded_addresses` finds
  exactly the addresses it padded, and only for span-keyed joins). Downstream of
  the owner's branch those NULLs are somebody else's rows, so they do not make a
  key nullable here and do not drive preservation or null-safe pairing.
- **No host, no reunion.** A suppressed span is not a licensed key for hosting
  or for the `family_anchored` exemption, so an extent-free merge gets neither.

Declining to extend narrows the branch, and it has to say so: the merge marks
every suppressed span it holds a `~` binding for as PARTIAL
(`MergeNode._extent_free_partials`). That is what makes the assembly above
preserve the owner instead of INNER-joining it against a branch that no longer
pads itself to the full domain, and it is why
`_tighten_joins_for_filtered_branches` stops treating such a branch as the
population: a row missing from it is a member its facts never bound, not a row
the WHERE rejected.

## Identity

`QueryDatasource.extent_free_spans` is folded into `_compute_identifier`, for
the same reason `limit` is: the same sources joined preserving a dimension's
unmatched members and joined discarding them are different relations. Without
it a d1 (WHERE-phase) scan and its d0 twin, routed differently because one sits
on the path to the owner, collide under one CTE name and their join lists
concatenate. The suffix is added only for spans some source actually binds `~`,
so scans the routing cannot bite keep their names.

It is deliberately NOT inherited by wrappers. A group or projection over a
narrowed scan is narrowed too, and consumers that need to know ask
`deep_extent_free_spans`, but folding the inherited set into wrapper names
splits CTEs that should stay shared (q29 grew by a third when it did).

## One source of truth

`_cover_groups_for_mandatory` reads the election rather than re-deriving
ownership from built outputs. That is load-bearing, not tidiness: a predicted
election that diverges from the actual one leaves a contributor dangling and the
statement fails to bind at render time.

## What this leaves behind

The reunion machinery (`_gate_nullable_by_host`'s `family_anchored` keep, the
padding-provenance matrix) is now a safety net for plans that still split a
span across owners, not the mechanism that makes the common case correct.

`PruneInvisibleOuterJoins` is gone. It survived this landing because it still
changed two statements outside the tpc corpus (gcat's aggregate query, thelook
`adhoc04`), and in both the dead join was keyed on something that is not a `~`
span at all: invisible CONTRIBUTORS, a separate defect class. Both are now
fixed at the planner (`docs/handoff_invisible_contributor_joins.md`,
`docs/handoff_contributor_reachability.md`), and
`tests/optimization/test_no_invisible_contributor_joins.py` asserts the field
report's plan joins nothing it does not read.
