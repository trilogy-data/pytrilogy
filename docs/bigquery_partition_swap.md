# BigQuery partitioned appends

An `append into <ds> by <key>` onto a datasource with `partition by` must
replace exactly the slices its select produced. On BigQuery there are two
implementations of that, and which one runs depends on the engine you are
connected through.

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

1. one query job stages the select into a table partitioned like the target;
2. `INFORMATION_SCHEMA.PARTITIONS` names the slices it produced — metadata
   only, no scan, and it spells the NULL slice `__NULL__`, which is also that
   slice's partition decorator;
3. one **copy** job per slice, `staging$id` → `target$id`, with
   `WRITE_TRUNCATE`. A copy job is free, consumes no slots, and replaces that
   partition atomically.

The target is read once instead of three times, the delete is not billed, and
an interrupted run leaves each slice either wholly old or wholly new.

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

### Quotas worth knowing

Copy jobs are free but counted: BigQuery caps copy jobs per destination table
per day. Irrelevant for hourly or daily runs; check it before fanning a single
day's backfill across thousands of slices, where the SQL form's fixed statement
count may be the better shape.
