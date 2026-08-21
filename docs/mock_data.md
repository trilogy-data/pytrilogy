# Mock data (`trilogy unit`)

`trilogy/dialect/mock.py` synthesizes a table per datasource so a model can be
validated — and queried — without a warehouse. `mock datasources a, b;` is the
statement; `trilogy unit` issues it for every datasource in scope.

The generator's job is not "plausible-looking values". It is to produce data in
which **the model's own claims are observably true**, so a query answered
against mocks fails when the planner gets the model wrong. Four rules carry
that, and each exists because the alternative made some real regression
invisible.

## The row-count contract

One value pool per concept (`MockManager.mock_concept`); key pools are dense,
non-repeating sequences. `create_mock_table` sizes the table to
`min(row_target, product of the grain pool sizes)` and fills its grain columns
from `grain_indices`, so every table sees the same distinct-value set per
concept — which `validate_multi_datasource_concept` compares.

`grain_indices` is what makes a composite grain more than a diagonal. The widest
component cycles its pool; each later one *also* advances on every lap of the
ones before it, by a step chosen coprime to its pool size. Writing `i = q*d + r`
with `d` the product of the earlier sizes, component `j` takes index
`(r + q*(d + step)) mod L`, which is injective in `q` for each `r` — so the
tuple is unique across the whole cross product rather than repeating at the lcm
of the sizes, while `d >= L` keeps each component's first `L` rows a full walk
of its pool. Cycling independently instead gave a bridge grained on two
100-member keys 100 of its 10,000 pairs, one per member: nothing in the mock
was ever many-to-many.

Each pool is also rotated by `cycle_offset`, a per-concept constant. Without it
two dense keys cycling from zero pair one-to-one, `user_id` equals `product_id`
on every row, and a join through the wrong key is indistinguishable from the
right one.

## Facts fan out over their dimensions

`datasource_depths` puts each datasource at a level: a table binding another's
whole grain, without being grained on it, is a fact about that entity.
`row_target = scale_factor * FANOUT_FACTOR ** depth`, so thelook mocks 100
customers, 300 orders and 900 sale lines rather than 100 of each. Composite
grains count both ways — a junction grained on `(user, group)` references both
entities and sits above them, and anything binding that pair sits above the
junction.

Height alone isn't enough — `MockManager.multiplied` decides *how* a foreign key
repeats. Every member appears at least once (a complete binding that misses
values reads as a model error, not as noise), and the surplus rows are dealt out
at random, so members have 1–8 facts rather than exactly three. A uniform ratio
would hide skew handling, and a 1:1 mock hides row multiplication entirely: an
aggregate that double-counts on a join returns the right answer when every join
is 1:1. Drawing each column's surplus independently also decorrelates two
foreign keys of equal cardinality, which lockstep cycling pins to a diagonal.

A concept's pool sizes to the **shallowest** table binding it — its owning
entity. Sizing it to the deepest would give a denormalized property more
distinct values in the fact than in the dimension, which
`validate_multi_datasource_concept` reads as missing values.

## Partial (`~`) bindings cover a strict prefix

A `~` column samples `partial_pool` — the same deterministic prefix of the key's
domain for every partial binding of that concept. The uncovered tail is the
population `~` exists to describe: never-ordered customers, never-sold products.

Cycling the full pool instead makes LEFT and INNER return identical rows, so no
unit-tier run can catch a join-type regression on a partial bridge.
`validate_multi_datasource_concept` is unaffected — it already skips
non-complete bindings when collecting coverage.

## Functionally determined columns are looked up, not cycled

When a single-key table is mocked, `register_dependencies` records
`key value -> dependent value` for each of its other columns. A later table that
binds both that key and one of those dependents reads the dependent through the
mapping (`MockManager._determinant`).

`order_items` binds `order_id` and a redundant `user_id`; `orders` establishes
`order_id -> user_id`. Index cycling keeps those consistent only while both
tables have equal row counts, and they diverge the moment a grain lcm cap, a
domain-capped type, or a different scale factor enters. A redundant key that
disagrees manufactures non-matches on every join built from both keys —
catastrophically so on FULL joins. Mock data must never contain an FD violation
the real data cannot.

The lookup is skipped when the column is `~` and the mapping already covers the
whole domain: inheriting a complete source would erase the partiality.

## Rollups are computed, not synthesized

`rollup_datasources` classifies a target as a rollup when it is non-root, binds
at least one derived concept, and binds nothing that isn't either derived or
bound by another target. Those are built last, by `derive_datasource`: a select
of their columns is planned against the already-mocked tables and the result is
written to the address — the same move a hand-written fixture makes with a
CTAS. `~` columns get an `is not null` pin so the rollup holds recorded
combinations only, rather than inheriting the bridge's extension rows.

Synthesizing them instead gives metric columns unrelated to any mocked fact
(the same question answered through the pre-aggregate and through the base
table disagrees), date parts unrelated to the datetime they derive from, and
key combinations the fact says never happened.

A rollup whose leaves aren't reachable from the mocked tables falls back to
synthesis with a warning.

## Declared constraints are honoured, not just declared

- **`NULLABLE`** columns get `NULL_FRACTION` of their rows emptied
  (`punch_nulls`). Never grain components — a NULL there is a grain violation —
  and never at the cost of a distinct value, which
  `validate_multi_datasource_concept` would read as a datasource missing data.
  Nulling runs *after* the dependency pass: a NULL determinant has nothing for
  its dependents to look up by.
- **A datasource's own `where`** restricts the pools of the columns it
  constrains (`literal_domains(..., "where")`). Mocking `where status =
  'Complete'` with every status validates the model against rows its own
  declaration says cannot exist. Conjunctions of `=` and `in` against literals
  are honoured; anything else is logged rather than silently dropped.
- **`complete where`** (`non_partial_for`) is the opposite move —
  `MockManager.complete_slice` has to *make* the claim true. The slice is where
  the source is missing nothing, and the planner may treat it as complete for a
  query implying the predicate, so the mock biases the filter column toward the
  admitted values until the slice is at least as tall as the key domain, then
  deals the full domain of every `~` key across it. Rows outside keep the
  partial prefix. A key fixed by the grain or by another table's dependency
  can't be widened; that is logged.
- **`partial datasource`** needs nothing special — the parser stamps `PARTIAL`
  onto every column, so the `~` rules above already apply.
- **Declared ranges** seat their endpoints in the pool (`with_bounds`).
  Inclusive/exclusive and off-by-one bugs live on a range's edges, and a pool
  that samples the interior never lands on one.
- **Declared regexes** are sampled, not skipped (`mock_pattern`). A pattern is a
  domain: `validate_datasource` runs the same regex back over the mocked column.
  The forms a type declaration uses are covered; lookaround and backreferences
  still raise rather than emit values that fail the model's own validator.
- **Traits** with a closed or bounded domain get real values from
  `TRAIT_GENERATORS` (`::country_code`, `::city`, `::latitude`, …), keyed by
  trait name and guarded by the base type they produce, since trait names are
  global. `register_trait_mock` extends it. Unregistered string traits fall back
  to the unique-value-per-row default described below.

## Namespaced bindings of one column share one pool

`canonical_column_map` unions concept addresses that share a
(physical address, column alias). One model can reach the same table through
several namespaces — thelook's `users` is bound once as `user.id` and again,
through the orders import, as `order.user.id`. Both write the same physical
table, so independent pools make the second write contradict the first.

`address_column_map` closes the other half: each datasource writes its *whole*
table, so where several bind one address the table gets the union of their
columns. Without it the last write wins and drops columns the other bindings
need. A column stays partial only if every binding of it is partial — one
complete binding means the physical column covers its domain.

## What mock data still cannot do

Values are arbitrary within their declared type. **A model that leaves a
categorical column as bare `string` mocks as a unique value per row**, which
makes every group-by a no-op and every literal filter (`where brand = 'Brand
01'`) an empty result. Declare the domain — `enum<string>[...]`, `int[18..78]` —
and the generator respects it. `tests/modeling/thelook_duckdb` does this for
exactly that reason.

Mocking is seeded (`MOCK_SEED`) and restores the caller's RNG state: a fixture
that changes shape between runs is not a fixture.

`docs/handoff_mock_fidelity.md` tracks what is still missing.

## Where it is used

- `mock datasources a, b with (scale_factor=500);` ->
  `handle_processed_mock_statement`. `scale_factor` sizes the shallowest entity;
  facts above it still fan out per level.
- `mock_environment(environment, executor, targets=..., scale_factor=...)` is
  the same thing callable from Python.
- `trilogy unit [--scale N]` ->
  `trilogy/scripts/testing.py::execute_script_for_unit` ->
  `validate_environment(mock=True)`.
- The agent tier's mock image -> `validate_agent.py::_materialize_mock_tables`,
  which runs the same generator through `mock_environment` with an
  `address_for` that names the `_mock_name` stand-in tables.
- `tests/modeling/thelook_duckdb` and `evals/thelook_agent` both seed
  themselves from mocks and assert the same four invariants
  (`assert_properties`): never-ordered customers exist, never-sold products
  exist, the redundant FK agrees, and no fact key is NULL.
