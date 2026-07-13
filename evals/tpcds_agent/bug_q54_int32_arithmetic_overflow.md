# Bug: q54 generated INT32 arithmetic overflows at execution

## Summary

The enriched q54 agent produced a valid segmentation expression based on a
large customer revenue total. Trilogy generated DuckDB SQL that cast the rounded
segment to a 32-bit integer before multiplying by 50. DuckDB then rejected the
generated query:

```text
Out of Range Error: Overflow in multiplication of INT32 (105249413 * 50)!
```

The query later passed only after the agent manually changed the multiplication
operand to `bigint`. Requiring the agent to anticipate backend intermediate
integer overflow caused 1.09M tokens of churn.

## Artifacts

- Run: `evals/tpcds_agent/results/20260712-204357_enriched`
- Trajectory: `agent_log.q54.jsonl` / `agent_log.q54.conversation.txt`
- Passing candidate: `workspace/query54.preql`
- Reference: `tests/modeling/tpc_ds_duckdb/query54.preql`

The failing expression was equivalent to:

```preql
auto segment <- round(total_spend / 50, 0)::int;
select
    segment,
    segment * 50 as segment_base;
```

Generated SQL included:

```sql
cast(round(sum(SS_EXT_SALES_PRICE) / 50, 0) as int) * 50
```

The aggregate made the segment large enough that the intermediate INT32 product
overflowed even though the arithmetic result is otherwise valid.

## Expected behavior

Arithmetic type inference should choose an execution type capable of holding the
result. At minimum, multiplying an integer expression derived from a decimal or
aggregate by an integer literal should not silently retain a narrow INT32
intermediate when the backend can evaluate it as BIGINT/DECIMAL.

Acceptable fixes include:

- promote integer multiplication to BIGINT when either operand derives from an
  aggregate/decimal cast;
- preserve the underlying numeric/decimal type through `round` and multiplication;
- insert a safe backend cast based on inferred result bounds;
- reject a provably unsafe explicit narrowing cast with an authored overflow-risk
  diagnostic.

An unhandled backend `OutOfRangeException` is not acceptable generated-query
behavior.

## Likely fix area

Inspect numeric result-type inference for `MULTIPLY` and DuckDB rendering of the
Trilogy `int` type. Determine whether `int` is intentionally INT32 everywhere;
if so, the multiplication result still needs widening rules independent of the
operand's declared storage type.

## Regression coverage

Add DuckDB execution tests covering:

1. `large_int * 50` where the result exceeds INT32 but fits INT64;
2. rounded aggregate-derived values multiplied by an integer literal;
3. explicit `::int` followed by arithmetic;
4. equivalent `::bigint` and decimal controls;
5. generated SQL contains a widened intermediate or otherwise executes without
   overflow.

