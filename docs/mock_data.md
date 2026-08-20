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
non-repeating sequences. `create_mock_table` fills each independent column by
cycling its pool — row `i` gets `pool[(i + offset) % len(pool)]` — and sizes the
table to `min(scale_factor, lcm(grain pool sizes))`. The grain tuple stays
unique through that lcm (equal rows require `i ≡ j` modulo every component's
length), so a composite grain over small domains fills the combination space
instead of capping at its smallest column, and every table sees the same
distinct-value set per concept — which `validate_multi_datasource_concept`
compares.

`offset` is `cycle_offset`: a per-concept rotation. Without it two dense keys
cycling from zero pair one-to-one, `user_id` equals `product_id` on every row,
and a join through the wrong key is indistinguishable from the right one.

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

## Namespaced bindings of one column share one pool

`canonical_column_map` unions concept addresses that share a
(physical address, column alias). One model can reach the same table through
several namespaces — thelook's `users` is bound once as `user.id` and again,
through the orders import, as `order.user.id`. Both write the same physical
table, so independent pools make the second write contradict the first.

## What mock data still cannot do

Values are arbitrary within their declared type. **A model that leaves a
categorical column as bare `string` mocks as a unique value per row**, which
makes every group-by a no-op and every literal filter (`where brand = 'Brand
01'`) an empty result. Declare the domain — `enum<string>[...]`, `int[18..78]` —
and the generator respects it. `tests/modeling/thelook_duckdb` does this for
exactly that reason.

Mocking is seeded (`MOCK_SEED`) and restores the caller's RNG state: a fixture
that changes shape between runs is not a fixture.

## Where it is used

- `mock datasources ...;` -> `handle_processed_mock_statement`.
- `trilogy unit` -> `trilogy/scripts/testing.py::execute_script_for_unit` ->
  `validate_environment(mock=True)`.
- The agent tier's mock image -> `validate_agent.py::_materialize_mock_tables`
  (synthesis only; it has no live environment to derive rollups against).
- `tests/modeling/thelook_duckdb` seeds its entire partial-bridge battery from
  mocks (`db_build.py`), and asserts the four invariants shared with
  `evals/thelook_agent`.
