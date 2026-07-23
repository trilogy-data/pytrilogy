# Concise Type Validators

Declaration-time value constraints on primitive types, written as a bracket
suffix on the base type. Two kinds:

- **Ranges** (numeric / date / datetime bases): `int[0..100]`
- **Regex** (string base): `string['[A-Z]+']`

```sql
key score int[0..100];              -- inclusive range
key temp float[-40.0..120.5];
key bucket int[0..10, 20..30];      -- comma = OR (union of ranges)
key exact int[42];                  -- single admitted value
key positive int[0..];              -- open-ended max
key capped int[..100];              -- open-ended min
key money numeric(20,5)[0..];       -- composite numeric base
key event_date date['2020-01-01'..'2024-12-31'];   -- quoted ISO literals
key event_ts datetime['2020-01-01 00:00:00'..'2024-12-31 23:59:59'];
key code string['[A-Z]+'];          -- full-match regex (re.fullmatch)
```

## Semantics

- Ranges are **inclusive** on both ends; `..` is the separator (avoids the
  negative-literal ambiguity `-` would create in both lexers).
- Regex patterns are quoted, Python syntax, and must match the **entire**
  value (`re.fullmatch`) — `'[A-Z]+'` means all-caps, not contains-a-capital.
- Validators constrain **non-null** values only; nullability remains the `?`
  modifier (`string['[A-Z]+']?` = all-caps or null).
- Allowed only at declaration sites (`key`/`property`/`properties(...)`
  inline properties/`param`), not in casts — a validator has no enforcement
  meaning in an expression position.

## Enforcement

1. **Datasource validation** (`trilogy.core.validation`): sampled rows are
   checked in `type_check`; out-of-domain values raise
   `DatasourceColumnBindingError` with a "violates declared domain" message.
2. **Query authoring**: `Comparison`/`Between` construction calls
   `constant_domain_violation` (`core/models/core.py`) and raises
   `SyntaxError` for provably-false predicates against literals:
   - `score = 250`, `score in (50, 250)` — value outside ranges / regex
   - `score > 150` when max is 100 (union envelope for multi-range)
   - `score between 150 and 200`
   Always-true predicates (`!= 500`, `not in (500,)`) are not flagged.
   `EnumType` is deliberately excluded: enum domains may be sampled/inferred,
   so non-member comparisons remain legal possibly-empty filters
   (`tests/engine/test_enum_unions.py`); ValidatedType domains are always
   user-declared.
3. **Mocks** (`trilogy.dialect.mock`): range-validated columns generate
   in-domain values; regex-validated columns raise `NotImplementedError`
   (loud, not best-effort).

## Model

`ValidatedType` (`core/models/core.py`) wraps a `DataType | NumericType` base
with `ranges: tuple[ValueRange, ...]` or `pattern: str`. Like
`EnumType`/`TraitDataType` it compares/hashes equal to its bare base type and
is stripped by `is_compatible_datatype`, so all structural machinery treats it
as the base; constraint checks read it explicitly. SQL rendering erases it to
the base type (`dialect/base.py render_expr`).

Both parser backends (lark + pest — pest changes require `maturin develop`)
produce the shared `VALIDATED_TYPE`/`RANGE_SPEC` syntax nodes hydrated once in
`parsing/v2/rules/concept_rules.py`.

## Not in v1 (follow-ups)

- Validators on `type` declarations / carried through `::trait` application.
- `::trait` suffix combined with a validator (`int[0..100]::percent`).
- Optimizer use of declared ranges in `reduce_expression` domain coverage.
- Validators on `tvf_output_item`, `struct_component`, function bindings.
- SQL-side (pushdown) validation of full tables rather than sampled rows.
