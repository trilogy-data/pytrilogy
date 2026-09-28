# A dim peel built beside the region domains that cover it (adhoc04)

## Status: FIXED 2026-09-28 (hypotheses 1 and 2 below, in code; 3 not needed).

Two rules, guarded by `tests/core/processing/test_v4_dim_peel_not_built.py`:

- **Hypothesis 1, in `_split_root_dimension_clusters`** (`_peels_a_cluster`):
  a candidate entity key (single or composite grain) must determine some
  other member of the bucket but NOT every other member. A key that
  determines the whole bucket is the bucket's own row key (`local.id`
  determining its FK columns), and a peel keyed by it rescans the same table
  beside nothing at a coarser grain. Composite grains needed the same bound:
  without it adhoc04's peel just moved to `dim:local.id|order.id`.
- **Hypothesis 2, `_drop_unread_peels_the_domain_took`** (right after the
  domain pass): a peel keyed by the region's span whose every primary member
  the domain took, and which NO other group reads (no concept-graph successor
  of a member outside the peel), is deleted and its concept nodes remapped
  to the domain. Left standing (thelook q06/q07, tpc_h adhoc04) it was
  sourced once more and dropped at FINAL as covered. The reader exception is
  load-bearing: in `tests/engine/test_region_dimension_attributes.py` the
  peel is the BASIC's solid-side provider of `tier` for `tier_amount` (INNER
  on the fact), while the domain is the padded side FINAL reads; remapping
  such a peel onto the domain made FINAL take `name`/`tier` from the solid
  contributor and customer 3 lost them (3 wrong-rows failures).

- **Follow-up, `_keep_extension_families_together`**: hypothesis 1's bound
  stranded a region. With `item_id` and `{item_id, order_id}` rejected as the
  field report's row key, `product_id` no longer peeled at all while `user_id`
  still peeled onto `order_id`, so only ONE cluster carried a demanded span and
  the merge had nothing to merge. A cluster carrying the span alone mixes no
  region, `add_region_domain_buckets` found no solid source, and `{user_id}`
  got no domain: the election handed its extent to the peel, which padded
  `user_id` off the `orders` table it hung from and stopped reading the items'
  own `~user_id` binding (an extra CTE, 57 -> 66 lines). The merge now also
  checks its result: a carrying cluster must hold something ABSENT on the
  region too, or its members stay in the bucket they were peeled from, which
  does mix. Guarded by
  `test_extent_ownership::test_every_span_is_owned_by_its_region_domain`,
  which failed from `a36122952` until this. The field report's FINAL merge also
  loses its re-scan of `items` (four scans -> three).

Verified: adhoc03/adhoc04/q06/q07/q19 rows and sorted-row md5 unchanged
against a baseline worktree; `source_repeats.py` over the three corpora
369 -> 363 `plan_source` calls (one per dead peel); the dead-group census
(`dead_groups.py`) shows no dead ROOT on any of the six files. The remaining
struck-through groups there are the inlined `item_margin` BASIC and, in
adhoc03/q19, the plain root read through the regraft's untagged rebuild (the
second question below, untouched).

### Original observation

Follows `docs/handoff_duplicate_source_requests.md` (fixed at `45ecbb025` /
`e8f8840a0`). After those fixes adhoc04 still builds one group whose node
never reaches the FINAL tree, and that group is the subject here.

## The case

`tests/modeling/thelook_duckdb/adhoc04.preql`:

```
select order.id, id, user.id, product.id, revenue, margin, total_order_revenue;
```

`order_items` binds `~user.id` and `~product.id`, so the keyspace has three
regions: the order lines, `{product.id}` (products with no line), and
`{user.id}` (users with no line). Rows are correct: 940 = 900 lines + 20 users
+ 20 products with no lines.

```bash
.venv/Scripts/python.exe local_scripts/plan_debugger/trace_query.py tests/modeling/thelook_duckdb/adhoc04.preql \
  --rows --setup tests.modeling.thelook_duckdb.db_build:seed --open
```

Step numbers below are from that trace as of `09b7e4970`; they shift as the
planner changes, but the step titles don't.

## What the grouping produces

After "region domain buckets added" there are three ROOT buckets with
overlapping addresses:

| bucket | members | what its rows are |
|---|---|---|
| `grp:root:root:∅:dim:local.id` | `user.id`, `product.id` | each order line's FK values (base region) |
| `grp:root:root:∅:extent:product.id` | `product.cost`, `product.id` | every product: the `{product.id}` region's domain |
| `grp:root:root:∅:extent:user.id` | `user.id` | every user: the `{user.id}` region's domain |

The addresses overlap, but the rows don't: the peel holds the line's product,
the domain all products. So the peel is not a duplicate of the domains. It is,
however, redundant in this plan:

- **"group graph materialized":** the peel has NO edges. Nothing reads it.
- **"FINAL added, concept sets computed, sources regrafted":** its only edge
  is a merge into FINAL. The same pass hands `user.id` and `product.id` to the
  line-level aggregate `grp:aggregate:d0:local.id|order.id:input:local.id`
  (FD passengers of `local.id`), which is already a FINAL contributor.
- **FINAL** drops the peel as covered (`_fold_covered_contributors`,
  `strategy_builder.py` ~line 2016). Its node is still built by
  "built grp:root:root:∅:dim:local.id": a full source search over
  `order_items`. The viewer strikes that step through as "not in FINAL tree".

## Why the peel exists, and why it gains nothing here

`_split_root_dimension_clusters` (`v4_helper/group_graph.py` ~line 919) peels
"single-entity FD dimension clusters out of a keyed ROOT bucket" so wide
dimension projections source "from their own dim tables keyed by the entity
id", avoiding fact joins. Its candidate entity key must be a downstream
grouping key that determines other members.

Here the chosen key is `local.id`, the FACT's own key: `user.id` and
`product.id` are FK columns stored on `order_items`, determined by
`local.id` only because the fact row carries them. No dimension table is keyed
by `local.id`, so the peel sources from `order_items` again. That is the
opposite of the peel's stated purpose.

## Hypotheses (untested)

1. **The peel should not form when its entity key is the fact's own grain key**
   (or, more generally, when no datasource other than the one the parent
   bucket already reads can bind the cluster keyed by that key). The docstring's
   benefit requires a separate dim table.
2. **The peel predates region domains and may be vestigial beside them.** A
   comment at FINAL (`strategy_builder.py` ~line 4285) says a peel "was the
   only thing putting that member on the extension row; the domain carries
   such a member now (`add_region_domain_buckets`)". Here both peel members
   are span keys that domains cover. Check whether any remaining test depends
   on a peel whose members are all output-demanded spans.
3. **Even when a peel is right, building it before the cover is known is the
   cost.** FINAL's cover election (`_fold_covered_contributors`) decides it is
   unneeded only after its node exists. If step 9's concept sets already show
   another FINAL contributor carrying every peel output, the build loop could
   skip it. That is a broader change: it moves part of FINAL's election earlier.

Start with 1: it is the narrowest, and it matches the docstring's own
rationale.

## A second, related question: the domain feed and the regraft

In "group graph materialized", both domains feed the BASIC group
`grp:basic:d*:local.id:sig:…` (`item_margin = sale_price - product.cost`).
That is deliberate: `feed_region_domains_to_present_scalars`
(`v4_helper/region_domains.py` ~line 539) wires a row-stream derivation that
reads something a domain carries (`sale_price - cost` reads `product.cost`)
to "the domain of every region it is NULL on the padding of", citing this very
query: "thelook: `users LEFT JOIN order_items FULL JOIN products` stays one
SELECT".

In the next pass, `_regraft_group_sources` (`group_graph.py` ~line 3849)
replaces those edges: the BASIC group now reads a synthetic
`grp:root:root:dim:local.id` (`order_items` ⋈ `products`, all six columns),
and the domains feed only FINAL. The final plan computes `item_margin` in the
`cooperative` CTE over the solid join, and the domains pad at FINAL.

Is the regraft meant to override the domain feed? Either:
- the feed is still wanted, and the regraft is undoing it; check what
  `null_on_padding` promises for `margin` on the extension rows (it is NULL
  either way here, since `sale_price` is absent there); or
- the regraft's solid source is right, and the feed is dead work for this
  shape. Then the feed's docstring example (this query) no longer describes
  what happens.

Rows are correct either way today; this is about which pass owns the decision.

## Verification for any change

- adhoc04 rows: 940, and the same sorted-row md5 as before the change.
- `local_scripts/plan_debugger/source_repeats.py` over the thelook, TPC-H and
  TPC-DS globs: the source call count should drop.
- The full suite, plus a before/after comparison of the committed
  `zquery<N>.log` files: diffs should be limited to CTEs the peel produced.
- The viewer: after the fix, adhoc04's step list should strike nothing through
  except groups intentionally absorbed (the `item_margin` BASIC group is
  folded into a merge node, which is expected).
