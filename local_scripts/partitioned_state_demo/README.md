# Partition-aware state, end to end

A worked example of using `trilogy`'s state file as **the unit of refresh for a
partitioned table**: probe state, take the stale partitions as a work list, fan
out one run per partition, and merge the per-partition deltas back into one
state file.

Everything runs locally against DuckDB. No cloud, no credentials.

---

## The problem this solves

Before this, a state snapshot said one thing per asset: fresh, stale, or
unknown. For a partitioned table that verdict is nearly useless —
`daily_orders is behind` tells an orchestrator to rebuild *everything*, when in
practice one day arrived late and the other 700 are fine. A table-level
`MAX(updated_at)` also cannot see a **missing** partition at all: if yesterday
never loaded, the max still reads as today's, and the table looks caught up.

So the state format now records per-partition state for any datasource
declaring `partition by`:

```jsonc
{
  "datasource_id": "daily_orders",
  "partition_by": [{ "column": "order_date", "concept_address": "local.order_date" }],
  "partitions_complete": true,
  "partitions": [
    {
      "partition_id": "order_date=2024-01-02",
      "values": { "order_date": "2024-01-02" },
      "observed": true,
      "expected": true,
      "status": "stale",
      "stale_reason": "'max_updated_at' behind: 2024-01-05 06:01:00 < 2024-01-06 09:30:00",
      "row_count": 2,
      "observed_watermarks": [...],
      "expected_watermarks": [...],
      "run_id": "..."
    }
  ]
}
```

Each slice carries **both sides** of its own comparison, exactly the way a whole
datasource does, so a reader can re-derive the verdict rather than trust it.

Two probes produce those sides, one query each regardless of partition count:

| side | what it is | how |
| --- | --- | --- |
| observed | the slices the table actually holds | `GROUP BY` the partition columns on the physical table |
| expected | the slices the roots say should exist | a trilogy query with every non-root datasource hidden, so only authoritative sources can answer |

`partition missing` (expected, not observed) is the case a table-level watermark
structurally cannot express, and it is the reason this exists.

---

## Run it

From this directory, using the repo venv:

```bash
# Windows
../../.venv/Scripts/python.exe orchestrate.py --reset
# Linux/Mac
../../.venv/bin/python orchestrate.py --reset
```

`--reset` drops the warehouse and state, reseeds the source feed, and creates
the target table **empty**. Then it runs the four-step loop and prints each
step. Expected output, abbreviated:

```
== 1. probe ==
  ✗ daily_orders
      partitioned by (order_date): 4 partition(s), 4 stale
        ✗ order_date=2024-01-01 — partition missing
        ✗ order_date=2024-01-02 — partition missing
        ✗ order_date=2024-01-03 — partition missing
        ✗ order_date=2024-01-04 — partition missing

== 2. plan ==   (the stale slices, straight out of state.json)
== 3. fan out (4 slice(s), 4 worker(s)) ==
== 4. merge ==
  ✓ daily_orders
      partitioned by (order_date): 4 partition(s), 0 stale
```

Then the incremental case — one day's rows get edited upstream:

```bash
../../.venv/Scripts/python.exe seed.py --late-day 2024-01-02
../../.venv/Scripts/python.exe orchestrate.py
```

```
  ✗ daily_orders
      daily_orders: freshness 'max_updated_at' behind: 2024-01-05 06:03:00 < 2024-01-06 09:30:00
      partitioned by (order_date): 4 partition(s), 1 stale
        ✗ order_date=2024-01-02 — 'max_updated_at' behind: 2024-01-05 06:01:00 < 2024-01-06 09:30:00
...
== 3. fan out (1 slice(s), 4 worker(s)) ==
```

The asset-level line still says the whole table is behind. The partition line
says which day, and only that day rebuilds.

### Doing it by hand

The orchestrator is 200 lines of subprocess calls; nothing is hidden. The same
loop as raw CLI:

```bash
# 1. probe -> a state file
trilogy state model.preql -o state/state.json

# 2. plan: read state.json, take every partition with "status": "stale"

# 3. fan out, one per stale partition, each writing its OWN delta file
trilogy run build_partition.preql \
    --param load_date=2024-01-02 \
    --state-file state/deltas/order_date=2024-01-02.json \
    --state-partition order_date=2024-01-02

# 4. merge the deltas back
trilogy state-merge state/state.json state/deltas/*.json
```

---

## Parallelism with a file-backed state store

This is the part worth reading.

**The rule: one writer per file, and a merge that is order-independent.**

A worker that refreshes `order_date=2024-01-02` and then writes a state file has
a problem — the post-run probe sees the *whole* table, including slices its
peers are concurrently rewriting. Publishing that would let the last writer win
on data it never owned. Two mechanisms fix it:

1. **`--state-partition <id>`** (repeatable; env `TRILOGY_STATE_PARTITION`)
   narrows the written snapshot to the slices this run owns and stamps
   `partitions_complete: false` — "these slices, and nothing about the others".
2. **`trilogy state-merge base.json delta...`** overlays scoped deltas by
   `partition_id` and lets a complete snapshot replace the list. Because each
   worker only speaks for slices it owns, **the merged result does not depend on
   the order the deltas are listed in**.

So the concurrency story is: N workers write N distinct files with no
coordination at all, and a single-writer coordinator folds them together
afterwards. No lock on the state file, no read-modify-write race, no
last-writer-wins. A worker crashing loses exactly its own slice, and the next
probe puts it straight back on the work list — the merge is idempotent, so a
delta can be replayed.

The `partition_id` is what makes this work, so it is built to be stable across
processes: hive-style `col=value`, keyed on the **physical column** (never the
namespaced concept address, which differs per script) and rendered canonically
(ISO for dates, an explicit `__NULL__` token) so two drivers can't split one
slice in two.

### The warehouse, not the state store, is the bottleneck here

DuckDB opens an on-disk database with a **single-writer lock**, so four
concurrent `trilogy run` processes against `warehouse.duckdb` cannot all
connect. `orchestrate.py` therefore takes a crude file lock around each
warehouse write. That is a property of DuckDB, not of partitioned state:

```bash
# on BigQuery / Snowflake / Postgres, delete the lock and nothing else changes
python orchestrate.py --no-warehouse-lock
```

The state plane never needs the lock — the fan-out writes four distinct delta
files concurrently either way.

### Writes are idempotent per partition

`APPEND` on a datasource declaring `partition by` no longer appends blindly. The
dialect stages the new rows, deletes the partition keys they cover, and inserts:

```sql
CREATE TEMPORARY TABLE "_stage" AS SELECT * FROM "daily_orders" WHERE 1=0;
INSERT INTO "_stage" <select>;
DELETE FROM "daily_orders" WHERE EXISTS (
  SELECT 1 FROM "_stage"
  WHERE ("_stage"."order_date" = "daily_orders"."order_date"
      OR ("_stage"."order_date" IS NULL AND "daily_orders"."order_date" IS NULL)));
INSERT INTO "daily_orders" SELECT * FROM "_stage";
DROP TABLE "_stage";
```

Re-running a partition is a no-op on row count and never touches a neighbour,
which is what makes a retried worker safe. The key match is null-safe on
purpose: a NULL partition key is a real slice (the state format names it
`__NULL__`), and a bare `=` — or the row-value `IN` this started as — would
leave it behind to re-append on every run.

Dialects differ only where they must: BigQuery keeps its scripted per-partition
delete, SQL Server stages with `SELECT ... INTO` into a `#temp`, and MySQL uses
a multi-table `DELETE ... JOIN` with `<=>` because it rejects a correlated
`EXISTS` on the delete target. This staged form is the portable default that
replaced an `INSERT OVERWRITE` no supported engine accepted.

The DELETE reads the *staged* keys, not a literal partition list, so **one
statement replaces exactly the N slices its select produced** — writing a set of
partitions is the same operation as writing one. That is what makes chunking a
scheduling decision rather than a correctness one.

---

## Files

| file | role |
| --- | --- |
| `model.preql` | root order feed (CSV) + `daily_orders`, `partition by order_date`, `freshness by max_updated_at` |
| `create.preql` | creates the target table, empty — the bootstrap state |
| `build_partition.preql` | one partition's work: `parameter load_date` + `APPEND daily_orders WHERE order_date = load_date` |
| `seed.py` | writes `data/orders.csv`; `--late-day` ages one day's rows |
| `orchestrate.py` | the probe → plan → fan out → merge loop |
| `trilogy.toml` | DuckDB at `warehouse.duckdb` |

Generated at runtime: `warehouse.duckdb`, `data/orders.csv`, `state/state.json`,
`state/deltas/*.json`.

---

## What this demo does not do yet

- **`trilogy refresh` narrows to stale slices but does not fan out.** It refreshes
  them in one statement (see below), which is usually what you want. Running
  slices as N concurrent processes is the orchestrator's job — that is what
  `orchestrate.py` does, and the tradeoff is retry granularity and warehouse
  parallelism, neither of which the language can size for you.
- **A partition-scoped delta is not usable as `--state-input`.** It speaks for
  only some slices, so seeding from it would report the rest as absent. Feed the
  merged file. (A complete snapshot seeds slices exactly as it seeds
  watermarks.)
- **`partition by` reaches physical DDL on BigQuery and Presto/Trino only.**
  Everywhere else it remains a logical declaration that still drives state,
  slice discovery, and per-slice replacement on write. The remaining engines and
  why each is deferred are written up in
  [`../PARTITION_DDL_PLAN.md`](../PARTITION_DDL_PLAN.md).
- Expected partitions come from a full scan of the roots. On a real warehouse
  you would want that bounded (a lookback window), which the model has no way to
  express today.
