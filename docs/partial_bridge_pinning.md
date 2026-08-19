# Partial-key bridges: pin-healing and the unconstrained-span error

Implementation: `trilogy/core/processing/partial_bridging.py`, invoked once per
single-stage SELECT in `query_processor.get_query_node`, after the build
environment materializes and before `generate_graph`. Tests:
`tests/engine/test_duckdb_partial_key_assembly.py`,
`tests/modeling/tpc_ds_duckdb/test_partial_key_assembly_shapes.py`.

## The semantics

A structural column-level `~` binding (`user_id: ~user_id`) licenses domain
extension: unmatched members of that key's dimension enter the result once,
carrying their own attributes, with every concept outside the key's functional
closure NULL. That is well-defined for a SINGLE key
(`select user_id, total_amount` keeps the user who never ordered) but not for a
span: when a query needs two keys related and the only datasource relating them
binds a needed key `~`, unmatched members of one side have no defined
counterpart on the other (customer list x product list related only by a
partial sales list). Trilogy no longer guesses at a shape for that span.

Two rules, sharing one analysis, applied at one seam so search, condition
routing, join planning and the optimizers all see the same judgment:

## 1. Pin-healing (`heal_pinned_partials`)

An extension row for key `k` is NULL at every concept outside `k`'s FD closure.
So if the statement WHERE proves non-null (`condition_proves_non_null`) a
bound concept OUTSIDE that closure, no extension row for `k` survives — the
binding is complete *for this statement*. The affected datasources are
replaced (copy-on-write; shared build caches are never mutated) with the
`Modifier.PARTIAL` mark dropped, and the query plans as a plain star anchored
on the fact instead of the anchor-LEFT + coalesce extension scaffolding it
otherwise builds and then filters.

Guards, each load-bearing:

- **Structural `~` only.** A table-level partial stamp
  (`partial datasource ... complete where`) is a row-subset contract the union
  machinery completes across siblings; healing it breaks that assembly
  (`test_partial_key_union_matrix`).
- **Sibling anchor blocks.** If another row-source carries the key inside a
  LARGER grain (store_sales anchoring store_returns' `~` grain keys), the pin
  does not shrink the population to this datasource's rows — the key stays
  partial and the sibling-stitch machinery owns the merge.
- **Killers must be bound and component-local.** A derived tautology
  (`coalesce(x, 5) is not null`) or a concept from a disconnected subgraph
  (attached via a cross-join gate) is non-null on extension rows too and
  proves nothing.
- **Single-stage WHERE only.** `then where` stages see populations the
  combined WHERE has not filtered yet, so staged statements skip both rules.

Note the pin on a key NEVER heals that key itself: `where user_id is not null`
is satisfied by user_id's own extension rows. Keys heal each other —
`where user_id is not null and product_id is not null` kills both extension
families, which is why that is the canonical pin for a two-key span.

## 2. The unconstrained-span error (`validate_partial_bridges`)

After healing, take the ROW-level referenced concepts (outputs plus WHERE row
args; aggregates collapse their internals and contribute nothing) and map each
to its home keys — the `required` set. A datasource relates its bound keys
SAFELY when either:

- at most ONE required key is a live (un-killed) structural `~` there. A
  single extension family is well-defined: unmatched members appear once with
  NULLs elsewhere (`select user_id, state, total_amount` keeps the user who
  never ordered; `select order_id, product_id, total_pair_cost` keeps the
  never-sold product). Partial keys the query never asks about license no
  extension rows and do not count.
- its row identity anchors the output: every grain component is complete-bound
  and in `required` (`select store_id, product_id, order_id` through an orders
  fact binding both FKs `~` — each row is a fact row or one dimension's
  extension row).

For every needed key pair, check connectivity twice: once through safe
datasources' bindings, once through all bindings. A pair connected only
through UNSAFE bridges — a multi-`~`-required datasource with no row anchor,
the customer x product x partial-sales shape — raises
`UnconstrainedPartialBridgeException`, which carries the offending keys, the
bridging datasources, and a ready-to-paste `suggestion`
(`where <k1> is not null and <k2> is not null`) that makes the same query
generate as rule 1's star. Pairs with any safe path are left to normal
discovery; pairs with no path at all fall through to the ordinary
disconnection error.

## Known residual

The by-key-aggregate shape (`min(amount) by user_id` compared against a row
value, selected beside additional keys and metrics) still trips the
keyless-join guard EVEN pinned — it reproduces with no `~` in the model at
all, so it is a pre-existing discovery defect, not a partial-bridging one.
Pinned as xfail(strict) with correct expected rows in
`test_forked_with_status_pinned` / `test_forked_full_column_set_pinned`; the
FINAL-merge cover strands each key on a different computed group with no
shared join axis (a spine-contributor reassignment was prototyped and
reverted — see git history — because eager key-carrying corrupted sibling
aggregate joins).
