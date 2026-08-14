# Bug: q89 `trilogy unit` cannot mock precision-bearing `NumericType`

**Reproduced OPEN 2026-08-13 against HEAD.** This is a deterministic unit-test
mocking gap. It was exposed during q89, but it was not the primary reason q89
failed the rebaseline: that trajectory had already been contaminated by the
concurrent-run DuckDB lock/process-leak cascade.

## Summary

The q89 agent invoked:

```text
trilogy unit answer_840315271.preql
```

The CLI failed with an unhandled framework error:

```text
Unexpected error in answer_840315271.preql:
Mocking not implemented for datatype Numeric(15,2)
```

`trilogy unit` mocks every datasource column in the imported model. The curated
`store_sales` model includes a precision-bearing
`ext_sales_price numeric(15,2)::usd`, so unit validation fails before it can
validate the query. Bare `numeric` is supported; `numeric(p,s)` is represented
by `NumericType` and misses the mocker's `DataType.NUMERIC` branch.

## Artifacts

- Run: `evals/tpcds_agent/results/20260813-125008_enriched`
- Task: `task.q89.txt`
- Trajectory: `agent_log.q89.jsonl` / `agent_log.q89.conversation.txt`
- Triggering event: q89 JSONL event at `2026-08-13T16:07:41Z`
- Unstaged candidate:
  `workspace/_worker_1/answer_840315271.preql`
- Precision-bearing model field:
  `tests/modeling/tpc_ds_duckdb/store_sales.preql:32`
- Mock implementation: `trilogy/dialect/mock.py:108-194`

The candidate's business query does not use `ext_sales_price`; unit mode mocks
the entire imported datasource, so any unsupported column type poisons all unit
validation of that model.

## Minimal reproduction

The type-dispatch bug reproduces without TPC-DS or a database:

```python
from trilogy.core.models.core import NumericType
from trilogy.dialect.mock import mock_datatype

numeric = NumericType(15, 2)
mock_datatype(numeric, numeric, 3)
```

Current result:

```text
NotImplementedError: Mocking not implemented for datatype Numeric(15,2)
```

The equivalent enum-level type is already supported:

```python
mock_datatype(DataType.NUMERIC, DataType.NUMERIC, 3)  # succeeds
```

## Trigger matrix

| Declared/mock datatype | Outcome |
|---|---|
| `DataType.NUMERIC` / bare `numeric` | Generates numeric mock values |
| `NumericType(15,2)` / `numeric(15,2)` | `NotImplementedError` |
| `numeric(15,2)::usd` | Trait unwrap reaches `NumericType(15,2)`, then fails |
| `trilogy run` against real data | Does not use the mocker; this specific error is absent |
| `trilogy unit` importing a datasource with any precision numeric column | Whole validation aborts, even if the query does not reference that column |

## Root cause and likely fix area

`NumericType` is a concrete datatype object with a `data_type` property returning
`DataType.NUMERIC` (`trilogy/core/models/core.py:130-147`). The mock dispatcher,
however, compares its `datatype` argument directly to enum values:

- `trilogy/dialect/mock.py:143-152` handles `DataType.FLOAT`,
  `DataType.DOUBLE`, and `DataType.NUMERIC`;
- there is no `isinstance(datatype, NumericType)` branch or general concrete-type
  normalization;
- `trilogy/dialect/mock.py:122-130` unwraps a `TraitDataType` by recursively
  passing `full_type.type` as both arguments. For `numeric(15,2)::usd`, that
  value is still `NumericType(15,2)`, so it reaches the final
  `NotImplementedError` at line 194.

Fix direction: normalize precision-bearing concrete types to their base
`data_type`, or explicitly handle `NumericType`. Generated values should respect
the declared precision and scale; using `Decimal` would avoid introducing
binary-float artifacts into exact numeric tests.

## Expected behavior and regression coverage

`trilogy unit` must build a mock datasource for models containing
`numeric(p,s)` columns. Add tests covering:

1. direct `mock_datatype(NumericType(15,2), ...)`;
2. `numeric(15,2)::usd` through the trait-unwrapping path;
3. values fit the declared precision and scale;
4. key and non-key numeric columns;
5. a CLI-level `trilogy unit` fixture whose imported datasource contains a
   precision numeric column not referenced by the query;
6. existing bare `DataType.NUMERIC` behavior remains supported.

## Eval classification

This is a real framework/DX defect because a documented CLI command terminates
with an unhandled `NotImplementedError`. It is not the scored semantic cause of
q89 in `20260813-125008_enriched`: the agent called `unit` as a diagnostic after
the worker had accumulated repeated DuckDB file-lock failures. Track the
process-recovery/worker-reuse defect separately.

