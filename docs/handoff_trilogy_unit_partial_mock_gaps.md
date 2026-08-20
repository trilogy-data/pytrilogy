# Handoff: make `trilogy unit` mock data respect partial (`~`) semantics

## Goal

`trilogy unit` validates models against mocked datasources
(`trilogy/dialect/mock.py`). We wanted to use it to seed
`tests/modeling/thelook_duckdb` — the partial-bridge regression battery — and
could not: the generator produces data in which partial (`~`) bindings are
indistinguishable from complete ones. We hand-rolled a deterministic generator
(`tests/modeling/thelook_duckdb/db_build.py`) instead. These are the gaps to
close so mock seeding works for partial-key models, both for that battery and
for unit-validating production models like the thelook `sales_reporting` asset
(see `trilogy-cloud/docs/bug-pytrilogy-partial-key-join-explosion-2026-08.md`).

## How the mocker works today (the 30-second version)

One value pool per concept (`MockManager.mock_concept`); key pools are dense
sequences (`1..scale_factor`). `create_mock_table` (mock.py:432) fills every
column by cycling its concept's pool — row `i` gets `pool[i % len(pool)]` —
and sizes the table to `min(scale_factor, lcm(grain pool sizes))`. The comment
at mock.py:443 documents why: the grain tuple stays unique through the lcm,
and every table sees the same distinct-value prefix per concept, which
`validate_multi_datasource_concept` compares.

## Gap 1: `~` never reaches the generator — no extension rows exist

`mock_datasource` (mock.py:486) iterates `datasource.concrete_columns` and
ignores `ColumnAssignment.modifiers` entirely. A fact column bound
`user_id: ~user.id` cycles the *same full pool* as the users table's key
column, so every user appears in every fact table.

Consequences:

- The populations `~` exists to describe are empty: no never-ordered users, no
  never-sold products. `db_build.py::_assert_properties` (never_ordered > 0,
  never_sold > 0) is exactly what mock data fails.
- LEFT vs INNER become row-identical, so a unit-tier run can never catch a
  join-type regression on a partial bridge, and any future execution-backed
  validation of `~` marks has nothing to observe.

Fix sketch: thread the column's modifiers from `mock_datasource` into
`create_mock_table`; a `Modifier.PARTIAL` column samples a deterministic
**strict prefix** of the key pool (e.g. first 80%, minimum `len - 1`), the
same prefix for every partial binding of that concept so partial sources stay
mutually consistent and the uncovered tail is stable. This is compatible with
`validate_multi_datasource_concept`: it already skips non-complete bindings
when collecting coverage (`trilogy/core/validation/concept.py:49`), so only
complete bindings must keep full-pool coverage — which they do.

## Gap 2: cross-table FK/FD consistency is coincidental

`order_items` binds both `order_id` and `user_id`; `orders` establishes
`order_id → user_id`. The mock keeps these consistent only because every
column cycles in lockstep by row index with equal-length pools: row `i` pairs
`order_pool[i]` with `user_pool[i]` in both tables. The pairing desynchronizes
whenever row counts diverge — a grain lcm cap, a domain-capped type (enum,
bool, `ValidatedType` range, short date span) in one table's grain, or
different scale factors — and then the same order carries different users in
different tables.

That is precisely the "redundant join key that disagrees" pathology from the
trilogy-cloud bug report: joins are built from every shared key, and a
dependent key that disagrees manufactures non-matches (catastrophically so on
FULL joins). Mock data should never manufacture FD violations the real data
cannot contain.

Fix sketch: an entity registry. For each key concept, generate one canonical
tuple of its dependent values (properties and FK targets) per key value; any
column whose concept is functionally determined by a key the same datasource
binds looks the value up by that row's key, instead of cycling independently.
Row-index cycling remains for genuinely independent columns.

## Gap 3: bound derived concepts are mocked independently of their lineage

A managed aggregate datasource — `daily_sales` / `user_product_sales` in the
battery's `sales_agg.preql`, `sales_reporting` in production — binds derived
concepts: `revenue: revenue` at a coarser grain, `order_date:
order.created_at.date`. `mock_concept` mocks each bound concept from its
datatype alone, so:

- metric columns are random floats unrelated to any mocked base fact — a
  query routed through the pre-agg and the same query routed through the base
  fact return contradicting numbers in unit mode;
- `order.created_at.date` gets dates unrelated to the mocked
  `order.created_at` datetimes;
- the pre-agg's key combinations are pool cross-product prefixes, not the
  pairs the mocked fact actually emits — once Gap 1 lands, the rollup would
  "contain" pairs the fact says never sold.

Fix sketch: mock in dependency order. Root datasources first; then, for a
non-root datasource, derive bound-lineage columns by executing the binding's
defining query over the already-mocked tables (the engine can already render
and run it — this is what `persist` does) rather than synthesizing columns.
That makes pre-aggs consistent by construction, the same move
`db_build.py` makes with its CTAS statements.

## Acceptance criteria

1. Mock-seeding the thelook model (`tests/modeling/thelook_duckdb/*.preql`,
   or `evals/thelook_agent/enriched_model`) satisfies the four
   `_assert_properties` invariants: never-ordered users exist, never-sold
   products exist, redundant user FK agrees between `orders` and
   `order_items`, no NULL fact FKs.
2. On that mock data, the unpinned extension query (`query06.preql` shape) and
   the pinned span (`query01.preql` shape) return **different** row sets —
   the property that lets unit-tier validation see join-type regressions.
3. `trilogy unit` runs green over the battery directory including
   `sales_agg.preql`, with pre-agg metrics summing to the mocked fact.
4. (Nice-to-have) the battery gains a mock-seeded smoke test alongside
   `db_build.py`, or replaces it once 1–3 hold.

## Pointers

- Generator: `trilogy/dialect/mock.py` — `MockManager.mock_concept` (:403),
  `create_mock_table` (:432, cycling contract comment at :443),
  `mock_datasource` (:486). `DEFAULT_SCALE_FACTOR` (:30).
- Modifiers on bindings: `trilogy/core/models/datasource.py` —
  `ColumnAssignment.modifiers` (:139), `is_complete` (:162).
- The validation contract the pool-prefix design serves:
  `trilogy/core/validation/concept.py::validate_multi_datasource_concept`
  (:20; partial bindings skipped at :49).
- Unit entrypoints: `trilogy/scripts/testing.py::unit` (:604),
  `execute_script_for_unit` (:237); mock statement handling in
  `trilogy/dialect/mock.py::handle_processed_mock_statement` (:465).
- The generator the mock should be able to replace:
  `tests/modeling/thelook_duckdb/db_build.py` (invariants in
  `_assert_properties`, pre-agg CTAS at the bottom of `seed`).
