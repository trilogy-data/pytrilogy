# Spec: Foreign-Key Inference for `trilogy ingest`

Status: proposed (spike)
Owner: unassigned
Surfaced by: `evals/tpcds_agent` — see "Background" below.

## Problem

`trilogy ingest` (especially `trilogy ingest --all`) introspects and emits one
datasource per table, **independently**. Foreign-key columns become plain
scalar properties:

```
# raw/store_returns.preql  (ingested)
property <return_time_sk,ticket_number>.returned_date_sk int;
property <return_time_sk,ticket_number>.store_sk int;
```

`returned_date_sk` and `store_sk` are not linked to `date_dim`'s or `store`'s
keys. Trilogy joins datasources by **shared concepts**, so a model built purely
from `ingest` has no join paths — any multi-table query fails:

```
UnresolvableQueryException: Could not resolve connections for query ...
```

Today the only remedy is the manual `--fks table.col:ref_table.col` flag, which
requires the user to already know every relationship. For a 24-table schema
(TPC-DS) that is ~30 relationships of hand-entry, and an agent building a model
has no way to discover them.

## Goal

`trilogy ingest` infers foreign-key relationships automatically and wires the
generated datasources so related tables share key concepts — producing a
**joinable** model with no manual `--fks`.

Acceptance: `trilogy ingest --all` against the TPC-DS DuckDB database
(`evals/tpcds_agent` builds one at sf=0.01) yields a model where a cross-table
query — e.g. TPC-DS query 1, `store_returns × date_dim × store × customer` —
resolves and runs, with no `--fks` argument.

## Approach — two stages

The DuckDB TPC-DS tables carry no FK constraints (dsdgen emits plain tables), so
inference must work from column names and data, not `information_schema`.

### Stage 1 — candidate generation by fuzzy name match

For each non-key column of each table, propose candidate `(to_table, to_column)`
references. `ingest` already strips a per-table prefix via `canonicalize_names`
(`sr_store_sk` → `store_sk`, `s_store_sk` → `store_sk`), so match on the
canonical names:

- Treat columns ending in `_sk` / `_id` / `_key` / `_fk` as candidate FKs.
- **Exact canonical match**: candidate column `store_sk` → any table whose
  primary key canonicalizes to `store_sk`.
- **Suffix / substring match**: `returned_date_sk` → a table keyed `date_sk`
  (`date_dim`). This is the case exact matching misses and is why a fuzzy stage
  is needed — `<qualifier>_<name>_sk` should propose the `<name>_sk`-keyed table.
- **Name-stem ↔ table-name match**: `customer_sk` → table `customer`.

Output: a set of candidate edges, each with a name-match confidence. Be
generous here — false positives are filtered in Stage 2.

### Stage 2 — value-overlap verification (sniffing)

For each candidate `from_table.from_col → to_table.to_col`, verify with data:

- The referenced column should be unique (a key) — sanity check.
- Containment check, e.g.:
  ```sql
  SELECT count(*) AS unmatched
  FROM (SELECT DISTINCT from_col FROM from_table WHERE from_col IS NOT NULL) f
  LEFT JOIN (SELECT DISTINCT to_col FROM to_table) t ON f.from_col = t.to_col
  WHERE t.to_col IS NULL;
  ```
- `unmatched == 0` → **complete** containment → strong FK, accept.
- High overlap (≥ threshold, e.g. 95%) → **subset** match → likely FK, accept
  (tolerates sampling noise / dirty data).
- Low overlap → reject.
- When several candidates survive for one column, keep the best by overlap.

Must be efficient — bound work with sampling / `LIMIT` on large warehouses; do
not full-scan unnecessarily. `ingest` already pulls sample rows
(`get_table_sample`); reuse where possible, fall back to a bounded query.

## Wiring the result

Accepted FKs should feed the **existing** application path — produce the same
structure `parse_foreign_keys` yields and reuse `apply_foreign_key_references`
(`trilogy/scripts/ingest_helpers/foreign_keys.py`) so the FK column binds to the
referenced table's key concept. Explicit `--fks` continues to work and should
override / merge with inferred relationships.

Report inferred FKs in the ingest summary, visibly distinct from explicit ones,
so the user can see and correct what was wired.

## Integration points

- `trilogy/scripts/ingest.py` — `ingest()` flow, `create_datasource_from_table`.
  Inference is a **second pass**: it needs every table's schema + primary keys +
  sample before it can match, so collect introspection results first, then infer,
  then wire, then write.
- `trilogy/scripts/ingest_helpers/foreign_keys.py` — `parse_foreign_keys`,
  `apply_foreign_key_references` (reuse, don't duplicate).
- `trilogy/dialect/base.py` — `get_table_schema`, `get_table_primary_keys`,
  `get_table_sample`, `list_tables`.

## Flags

- Gate the new behavior so it is opt-out-able — e.g. `--infer-fks` /
  `--no-infer-fks` (default-on vs default-off is an open decision).
- Consider a level control: name-only vs name+value sniffing, since sniffing
  has a query cost.

## Testing

- Unit: the name-match heuristic (prefix stripping, suffix/substring, stem↔table)
  on TPC-DS-style names.
- Unit: value-overlap check on small synthetic DuckDB tables — matching,
  non-matching, and subset-overlap cases.
- End-to-end: `ingest --all` on a small multi-table DuckDB → a cross-table
  Trilogy query resolves.
- Real target: `ingest --all` on TPC-DS sf=0.01 → the `evals/tpcds_agent`
  queries resolve.

## Edge cases / risks

- Composite keys — TPC-DS has them (`store_returns` grain is
  `(return_time_sk, ticket_number)`).
- Self-referential FKs.
- One column with multiple plausible targets — disambiguate by value overlap.
- The `*_date_sk` family — many tables reference `date_dim`; ensure the fuzzy
  stage proposes it and value-sniffing confirms.
- Performance on large/remote warehouses — sniffing must be sampled/bounded.

## Background

This gap was found by `evals/tpcds_agent`, an on-demand eval that drives the
`trilogy agent` loop through a TPC-DS modeling task. The agent reached the point
of writing correct Trilogy queries, but every query failed to resolve because
`ingest --all` produced disconnected table models. That eval is the natural
end-to-end validation harness for this work.
