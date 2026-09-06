# BigQuery partitioned appends

An `append into <ds> by <key>` onto a datasource with `partition by` must
replace exactly the slices its select produced. On BigQuery there are two
implementations of that, and which one runs depends on the engine you are
connected through.

## Which key types are allowed

`BaseDialect.SUPPORTED_PARTITION_KEY_TYPES` decides what an append may key on —
`date` and `timestamp` by default. BigQuery sets it to the same map its DDL
partitions with, so a `datetime` column is legal in both the `CREATE` and the
append rather than only the first. The check runs where the persist is
generated, not at parse time, because what can key a partition is a property of
the engine being written to.

## The SQL form (renderable, always the fallback)

The shared staged replace: create a temp table shaped like the target, insert
the select into it, delete the target rows whose partition keys the staged rows
cover, insert, drop. BigQuery emits these as **one script** rather than five
statements, because a BigQuery temp table only lives for the length of the
multi-statement query that declared it.

This is what `trilogy generate-sql`, `show`, and Trilogy Studio render, and what
runs on any connection that is not the native BigQuery engine. It is null-safe
(the `__NULL__` slice is a real slice and is replaced like any other) and works
for a multi-column `partition by`, though BigQuery itself only partitions
physically on one column.

## The native form (default when running through `BigQueryEngine`)

`trilogy/dialect/bigquery_persist.py` performs the same write through the jobs
API instead:

1. `CREATE TABLE ... LIKE <target>` makes a staging table with the target's
   schema, partitioning and clustering, and one `INSERT` job fills it;
2. `INFORMATION_SCHEMA.PARTITIONS` names the slices it produced — metadata
   only, no scan, and the ids it reports *are* the decorators;
3. one **copy** job per slice, `staging$id` → `target$id`, with
   `WRITE_TRUNCATE`. A copy job is free, consumes no slots, and replaces that
   partition atomically.

The target is read once instead of three times, the delete is not billed, and
an interrupted run leaves each slice either wholly old or wholly new.

Step 1 is `CREATE ... LIKE` plus an `INSERT`, not a single query job writing to
a destination table, because **a select names its output columns after
concepts** (`events_created_at_date`) while a datasource declares its own
(`date`). A destination write would stage the concept names, which no copy job
can move onto the target and no partition spec can name. `LIKE` brings the
target's names, and the rows land positionally — exactly what the SQL form does
with its temp table. The staging table also carries an `expiration_timestamp`
from the same statement, so a process killed before the drop cannot leave it
billing storage.

**The null slice is the exception.** BigQuery reports it as `__NULL__` but
rejects that as a decorator (`Invalid date partitioned partition key:
__NULL__`), so there is no copy job for it. It is replaced by a `DELETE` +
`INSERT` pair inside a `BEGIN/COMMIT TRANSACTION`, which keeps the replace
atomic but does bill a scan of that partition.

### When it declines

Declining is always safe — the SQL above runs instead. It happens when:

- the persist is not an `APPEND`, or names more than one partition column;
- the address does not resolve to `project.dataset.table` (a copy job needs a
  dataset, so a bare table name cannot be addressed);
- the target table does not exist, or is not time-partitioned on the column
  being written — including ingestion-time partitioning, which is keyed on load
  time rather than on any column we write.

Partition **granularity** is not restricted: the staging table copies the
target's own `type_`, so the two tables agree on partition ids whether that is
DAY, HOUR, MONTH or YEAR.

### Turning it off

```python
Dialects.BIGQUERY.default_executor(conf=BigQueryConfig(native_partition_swap=False))
```

The switch exists so the same write can be run both ways and the rows compared;
that comparison is `tests/engine/bigquery/test_bigquery_partition_swap.py`, and
it is the only thing that shows the two implementations agree.

## Measured

Same append, same 200k-row source, run three ways against live BigQuery. "old"
is the scripted `EXECUTE IMMEDIATE` loop this replaced. No null slice, because
the old loop cannot represent one.

| partitions | | wall clock | jobs | bytes billed |
| --- | --- | --- | --- | --- |
| 1  | old | 9.9s | 5 | 30 MiB |
| 1  | new-sql | 9.3s | 5 | 40 MiB |
| 1  | **native** | **5.2s** | 4 | **10 MiB** |
| 10 | old | 50.9s | 23 | 210 MiB |
| 10 | new-sql | 12.5s | 6 | 40 MiB |
| 10 | **native** | **7.8s** | 12 | **10 MiB** |
| 30 | old | 119.2s | 64 | 610 MiB |
| 30 | new-sql | 10.0s | 6 | 40 MiB |
| 30 | **native** | **9.5s** | 32 | **10 MiB** |

Measured before the staged write was split into `CREATE ... LIKE` + `INSERT`;
add one job to each native row. The DDL is metadata-only, so the bytes and the
wall clock are unaffected.

The old loop scaled linearly in every column — 2 DML statements per partition —
and is gone. Between the two survivors:

- **Bytes are flat for native at 10 MiB** regardless of slice count: the source
  is read once and the copy jobs are free. The SQL form costs 40 MiB because
  the staged rows are written, scanned for the delete, and scanned again for
  the insert.
- **Wall clock is flat for both.** It is flat for the SQL form because its
  statement count is fixed. It is flat for native only because copy jobs are
  *submitted* concurrently — see `COPY_SUBMIT_WORKERS`. Submitting them in a
  serial loop cost 0.5s of HTTP round-trip each and made native the slower
  option past ~10 slices (22.6s at 30). BigQuery itself was never the limit:
  measured over 30 slices, submission fell 13.7s -> 1.4s while the wait stayed
  at ~6s, so the copies were always running concurrently.

Native is therefore the better choice at every width measured, and there is no
slice count at which the SQL form is worth choosing for speed.

### Quotas worth knowing

Copy jobs are free but counted: BigQuery caps copy jobs per destination table
per day. Irrelevant for hourly or daily runs; check it before fanning a single
day's backfill across thousands of slices, where the SQL form's fixed statement
count may be the better shape.

## Overwrites

An OVERWRITE is not an append and takes neither form above. It renders as a
single `CREATE [OR REPLACE] TABLE t (...) [PARTITION BY ...] AS <select>`:
BigQuery's multi-statement transactions accept DML and temp-table DDL only, so
the portable DDL-then-INSERT pair cannot be made atomic there and would leave
an empty table behind a failed insert. The column list carries the declared
types and descriptions; the select fills it positionally, exactly as the INSERT
did. Engines with transactional DDL keep the pair and rely on the executor
rolling the implicit transaction back on failure.
