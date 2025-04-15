

### New Test Case

Go [here](https://github.com/duckdb/duckdb/tree/main/extension/tpcds/dsdgen/queries).

Copy SQL to query.sql.

Use that to create query.preql.

You may need to expand the .preql input models.

While iterating, if the first row is a mismatch, you're generally pretty far off.

If a later row is a mismatch, some common error patterns are:
- implicit null dropping on joins [trilogy will join on field and null matching] - explicitly filter out nulls if needed

## Performance Debugging

run timing_debug.py

analyze flame graph using snakeviz
