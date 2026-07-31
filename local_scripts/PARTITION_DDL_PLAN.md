# Handoff: physical partitioning for the remaining engines

Follow-up to the partition-aware state diff. That diff made `partition by` a
**logical** declaration that drives per-slice state, expected-slice discovery,
and per-slice replacement on write — on every dialect. This is about making it
reach **physical DDL** on the engines where it currently doesn't.

Current coverage:

| engine | physical partitioning | status |
| --- | --- | --- |
| BigQuery | `PARTITION BY <expr>` | done |
| Presto / Trino | `WITH (partitioned_by = ARRAY[...])` | done |
| DuckDB, SQLite | none exists for tables | not possible |
| Postgres, MySQL, SQL Server | declarative, but needs per-slice DDL | **this doc** |
| Snowflake | none; `CLUSTER BY` is a different thing | **decision, not work** |

---

## The blocker that shapes all of it

A partitioned parent table on Postgres/MySQL/SQL Server is **unusable until its
child partitions or boundaries exist**. Emitting the parent clause alone turns a
working table into one that refuses every write:

- **Postgres** — `ERROR: no partition of relation "t" found for row`
- **MySQL** — `RANGE`/`LIST` require explicit `PARTITION p0 VALUES ...` definitions
- **SQL Server** — needs `CREATE PARTITION FUNCTION` + `CREATE PARTITION SCHEME`
  as separate objects before the table

So this is not "add a `render_partition_clause` override". It is: **create each
slice's partition at the moment that slice is first written.** Per-slice state is
what makes that tractable — we now know which slices exist and which are being
written — but the plumbing to carry it is not there yet.

## The one real design problem

**Slice values never reach the dialect.** Both structures carry only names:

```python
# trilogy/core/statements/execute.py
class PersistQueryMixin:
    partition_by: list[str]          # column ALIASES
    partition_types: list[DataType]  # their types
    # ...no values

class CreateTableInfo:
    partition_keys: list[str] = field(default_factory=list)   # column names only
```

And the staged replace (`BaseDialect.generate_partitioned_insert_statements`)
stages *from the select*, so at render time nothing knows which slices the
statement will touch — that is deliberate, and it is what makes the DELETE and
the INSERT agree on the same key set.

The values DO exist upstream: a slice-scoped refresh has them on
`StaleAsset.partitions` (`list[PartitionObservation]`, values keyed by physical
column), and `Executor.update_datasource(partitions=...)` already turns them
into a `WHERE`. They stop at the executor.

**Recommended approach:** carry the slice values onto `ProcessedQueryPersist`
(e.g. `partition_values: list[dict[str, PartitionValue]]`, populated only when
the persist is slice-scoped), and give `BaseDialect` a new hook:

```python
def render_partition_provisioning(self, target, partition_by, partition_values) -> list[str]:
    """DDL that must exist before rows for these slices can be written. Empty on
    engines whose partitioning needs no per-slice objects."""
    return []
```

prepended inside `generate_partitioned_insert_statements`. Base returns `[]`, so
BigQuery/Presto/DuckDB are untouched. Postgres/MySQL/SQL Server override it.

Non-slice-scoped writes (a full `APPEND` with no stale-slice filter) have no
values, so provisioning is a no-op — which means **a full rebuild on these
engines still needs the parent to be creatable**. Decide up front whether
`partition by` on Postgres implies "always slice-scoped writes" or whether a
`DEFAULT` partition catches the rest. See the Postgres notes below; this is the
main open question and it is a semantics decision, not an implementation detail.

## Per-engine notes

### Postgres

```sql
CREATE TABLE t (...) PARTITION BY LIST (order_date);
CREATE TABLE IF NOT EXISTS t_20240103 PARTITION OF t FOR VALUES IN ('2024-01-03');
```

- Child names must be generated and stable — derive from the partition id, but
  **hash it**: identifiers cap at 63 bytes and values can be long or contain
  characters that need quoting. Keep the mapping recoverable (a comment on the
  child, or a deterministic hash the state file can reproduce).
- `LIST` fits our model (discrete slice values) far better than `RANGE`. `RANGE`
  needs an upper bound we have no way to infer.
- **The partition key must be part of every `PRIMARY KEY`/`UNIQUE` constraint.**
  Our `grain` becomes the PK in `datasource_to_create_table_info`, so a
  partitioned datasource whose grain excludes the partition column will be
  rejected by Postgres. Validate this at build time with a clear message rather
  than letting Postgres complain.
- A `DEFAULT` partition makes the table writable without provisioning, but you
  cannot later add a partition covering rows already sitting in `DEFAULT`
  without detaching it first. Attractive, and a trap.
- `DELETE`+`INSERT` on a partitioned parent works normally, so the staged replace
  needs no change.

### MySQL

- `PARTITION BY LIST (...)` needs integer-valued expressions; `LIST COLUMNS(...)`
  accepts dates/strings and is the one to use.
- Same unique-key rule as Postgres, and MySQL enforces it more aggressively:
  **every** unique key must contain all partition columns.
- Adding a partition is `ALTER TABLE t ADD PARTITION (PARTITION p... VALUES IN (...))`
  — DDL, so it implicitly commits. Fine here, worth knowing.
- Note MySQL already overrides `render_partition_delete` with a multi-table
  `DELETE ... JOIN` (it rejects a correlated `EXISTS` on the delete target).

### SQL Server

Heaviest of the three: partition function + scheme are **database-level objects**,
not table-level, so provisioning is not idempotent per table in the same way.

```sql
CREATE PARTITION FUNCTION pf_t (date) AS RANGE RIGHT FOR VALUES ('2024-01-03', ...);
CREATE PARTITION SCHEME ps_t AS PARTITION pf_t ALL TO ([PRIMARY]);
CREATE TABLE t (...) ON ps_t(order_date);
```

- Extending is `ALTER PARTITION FUNCTION pf_t SPLIT RANGE ('2024-01-04')`, which
  can be expensive if it splits a populated range.
- Only `RANGE` exists — no `LIST` — so discrete slice values must be mapped to
  boundaries, and the boundary set must be kept sorted and deduplicated.
- Realistically the lowest-value of the three. Consider deferring until asked.

## Testing

- Unit/DDL: extend `tests/dialect/test_create_table_partitioning.py`. It already
  parametrizes every dialect and asserts non-partitioning ones stay clean —
  update `PARTITIONING_DIALECTS` as engines land.
- Provisioning sequence: extend `tests/dialect/test_partitioned_append.py`, which
  asserts the exact statement list per dialect.
- **Behaviour must be proven against a live engine, not a rendered string.** The
  failure modes here (a parent that refuses writes, a unique-key rejection, a
  `DEFAULT` partition swallowing rows) all pass a string assertion happily. See
  `tests/scripts/test_partition_round_trip.py` for the shape — the out-of-band
  `DELETE` trick makes "did this actually work?" observable. Postgres and MySQL
  are dockerised in this repo; SQL Server is not.

## Do not

- **Do not reorder columns** to satisfy a partitioning constraint. The persist
  `INSERT` is positional, so a table whose column order differs from the
  datasource's gets written to wrong. Presto raises instead, deliberately.
- **Do not emit a parent partition clause without provisioning.** A silently
  unwritable table is strictly worse than the current unpartitioned one.
- **Do not infer physical partitioning from anything but explicit `partition by`.**
  It is a user declaration, and on some engines it costs money (see below).

---

## Snowflake: a decision, not a task

Snowflake has no declarative partitioning — micro-partitioning is automatic.
`CLUSTER BY (col)` is the nearest analogue, and I left it out on purpose.

**It is not just "some cost", it is structurally mismatched to how we write.**

Automatic Clustering is a serverless service billed in credits *separately from
warehouse compute*, and it is charged by how much data it reclusters. Reclustering
is triggered by DML that disorders the table against the clustering key — and our
partitioned append is `DELETE` a whole slice then `INSERT` it back. That is close
to a worst case: every refresh of every slice hands the clustering service a large
disordered region to rewrite, and it recurs on **every** refresh, not once.

Worse, we would mostly be paying for something we already get. Writing one slice
per bulk `INSERT` naturally lands that slice's rows in their own micro-partitions,
so Snowflake already prunes well on the partition key without a clustering key.
The realistic gain is limited to fragmentation that accumulates over many
DELETE+INSERT cycles.

Snowflake's own guidance also points away from it: clustering is aimed at large
(multi-terabyte) tables that are queried far more than they are written. A
partitioned fact table refreshed daily is the opposite profile.

**Recommendation: do not emit `CLUSTER BY` from `partition by`.** If it is wanted,
make it a separate explicit declaration in the model (a `cluster by` clause), so
nobody incurs a recurring serverless bill as a side effect of declaring how their
data is sliced.

If you want to measure before deciding, on a table clustered by hand:
`SYSTEM$CLUSTERING_INFORMATION('<table>', '(<col>)')` for whether clustering would
help, and `ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY` for what it actually costs.
I have not benchmarked this and the credit rate depends on your contract — treat
the argument above as the shape of the problem, not a quantified number.
