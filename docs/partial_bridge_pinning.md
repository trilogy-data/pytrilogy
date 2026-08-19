# Partial keys, value NULLs, and pin-healing

Implementation: `trilogy/core/processing/partial_bridging.py` (pin-healing),
invoked once per single-stage SELECT in `query_processor.get_query_node`,
after the build environment materializes and before `generate_graph`; join
licensing in `trilogy/core/processing/join_resolution.py`
(`get_join_type`, `get_modifiers`, `ensure_content_preservation`). Tests:
`tests/engine/test_duckdb_partial_key_assembly.py`,
`tests/engine/test_duckdb_nullability_matrix.py`,
`tests/modeling/tpc_ds_duckdb/test_partial_key_assembly_shapes.py`,
`tests/modeling/thelook_duckdb/test_thelook_queries.py` (adhoc01/adhoc02).

## Two kinds of NULL, two orthogonal declarations

A bound column can deviate from its key's domain in two independent ways, and
each has its own mark:

- **`~` (partial): the column covers a SUBSET of the key's members.** The
  missing members exist in the dimension but have no row here. `~` licenses
  *domain extension*: unmatched members of that key's dimension enter the
  result once, carrying their own attributes, with every concept outside the
  key's FD closure NULL (a *padding* NULL — absence, not a value).
- **`?` (nullable): the column carries NULL as a VALUE.** A guest order's
  NULL customer is data, not absence. Value NULLs form their own group under
  aggregation, survive as fact rows at every grain, pair null-safely only
  with other value NULLs, and are *removed* by a `is not null` filter.

They compose: `c_sk: ~?customer.sk` says "subset of customers, and some rows
have no customer at all". Conflating them is the classic SQL trap; the planner
keeps them apart end to end:

- `get_modifiers` uses null-safe equality only when BOTH sides' NULLs are
  values (`nulls_are_values`); padding never pairs with a value NULL, and
  padding pairs with padding only within the same extension family.
- `get_join_type` grants row-preservation (FULL) only under a license: a `~`
  partial on the connecting key, an authored `full join`/`union join`
  declaration, or nullable-key preservation.
- `ensure_content_preservation` propagates preservation through a join chain:
  once padding is in the accumulated stream, later joins must stay
  LEFT-preserving so padded rows (NULL join keys) survive — but that creates
  no license for the new right relation. A dimension reached off the spine
  (a transitive FK, `customer -> current_address`) joins LEFT unless its own
  key is marked `~` somewhere; upgrading it to FULL leaked orphan dimension
  rows. A join keyed ON the preserving join's own coalesced spine keys keeps
  both-ways preservation (every family carries the spine).

## The span shape (customer x product through a partial fact)

`select customer.sk, product.sk, total` over a fact binding both keys `~`
generates the union-of-branches shape — no pin required:

| customer | product | total | provenance |
| --- | --- | --- | --- |
| c1 | p1 | 50 | fact row |
| c2 | NULL | 90 | fact row, NULL product **value** (needs `?`) |
| NULL | p2 | 80 | fact row, NULL customer **value** (needs `?`) |
| c3 | NULL | NULL | customer extension (`~` license) |
| NULL | p3 | NULL | product extension (`~` license) |

Real pairs exactly once; one extension row per unmatched member per `~` side;
extension families never cross-pair (no invented pairings, no all-NULL-key
rows). The same holds at attribute grain (`state x brand`) and for spans
sourced through a multi-`~` aggregate. Note the residual reader ambiguity is
inherent to tabular output: `(NULL, p2, 80)` (a real order with no customer)
and `(NULL, p3, NULL)` (a never-sold product) both render NULL in the
customer column; the measure column distinguishes them.

Value NULLs in a bound column MUST be declared `?`. Undeclared value NULLs
are a modeling error the planner cannot detect: it is entitled to assume the
column is non-null, so `is not null` pins may be dropped as tautological and
NULL-keyed fact rows may be silently lost or kept depending on plan shape.

## Pin-healing (`heal_pinned_partials`)

An extension row for key `k` is NULL at every concept outside `k`'s FD
closure. So if the statement WHERE proves non-null
(`condition_proves_non_null`) a bound concept OUTSIDE that closure, no
extension row for `k` survives — the binding is complete *for this
statement*. The affected datasources are replaced (copy-on-write; shared
build caches are never mutated) with the `Modifier.PARTIAL` mark dropped, and
the query plans as a plain star anchored on the fact instead of the
extension scaffolding it otherwise builds and then filters.

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
  combined WHERE has not filtered yet, so staged statements skip healing.

Note the pin on a key NEVER heals that key itself: `where user_id is not
null` is satisfied by user_id's own extension rows. Keys heal each other —
`where user_id is not null and product_id is not null` kills both extension
families, which is why that is the canonical pin for a two-key span. On a
`~?` binding the pin ALSO filters the value-NULL fact rows — healing narrows
the plan, the rendered WHERE still applies.

## History

An `UnconstrainedPartialBridgeException` guard (`validate_partial_bridges`)
used to reject multi-`~` spans whose row identity was absent from the output,
because two assembly defects corrupted the generated shape (an unlicensed
transitive-dimension FULL leaking orphan rows, and NULL-key row manufacture
for `?`-bound dims). Both are fixed and the guard is deleted; the span
generates the table above.

## Known residual

The by-key-aggregate shape (`min(amount) by user_id` compared against a row
value, selected beside additional keys and metrics) still trips the
keyless-join guard EVEN pinned — it reproduces with no `~` in the model at
all, so it is a pre-existing discovery defect, not a partial-bridging one.
Pinned as xfail(strict) with correct expected rows in
`test_forked_with_status_pinned` / `test_forked_full_column_set`; the
FINAL-merge cover strands each key on a different computed group with no
shared join axis (a spine-contributor reassignment was prototyped and
reverted — see git history — because eager key-carrying corrupted sibling
aggregate joins).
