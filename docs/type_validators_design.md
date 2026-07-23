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
   checked in `type_check` (out-of-domain values raise
   `DatasourceColumnBindingError`), and `validate_declared_domains` runs a
   **full-table SQL-side scan** per domain-declaring column — ranges/enums as
   comparison predicates, regex as an anchored `REGEXP_CONTAINS` (`^(?:…)$` to
   mirror `re.fullmatch`; regex flavor is the executing dialect's). Both ride
   `validate_environment` / the `validate` statement.
2. **Query authoring**: `Comparison`/`Between` construction calls
   `constant_domain_violation` (`core/models/core.py`) and raises
   `SyntaxError` for provably-false predicates against literals:
   - `score = 250`, `score in (50, 250)` — value outside ranges / regex
   - `score > 150` when max is 100 (union envelope for multi-range)
   - `score between 150 and 200`
   `EnumType` domains get the same impossibility check (membership for `=`/`in`
   elements, min/max envelope for inequalities over numeric members).

   **Only provably-FALSE predicates are flagged — never tautologies.**
   Always-true predicates (`!= 500`, `not in (500,)`, domain-covering filters)
   parse untouched: through a nullable FK they carry load-bearing join
   null-rejection, and advising their removal caused a silent wrong result
   (`evals/tpcds_agent/bug_q16_enum_tautology_drops_joined_null_rejection.md`).
   Error messages advise fixing the constant or updating the declaration —
   never removing the predicate.
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

## Trait carry-through

`type pct int[0..100];` declares a validator-carrying custom type;
`int::pct` transfers the constraint onto the authored base (a
`TraitDataType` whose `.type` is the `ValidatedType`), so trait-applied
concepts are enforced everywhere validators are. `int[0..100]::pct` (explicit
brackets plus trait) is allowed when the constraints agree; differing
constraints from two sources is a hard "Conflicting validators" error.
Validators on union `type` declarations (`type x int[0..1] | string`) are
rejected. Validators also parse on `struct_component` fields and
`tvf_output_item` outputs.

## Optimizer integration

`reduce_expression` (condition_utility.py) treats declared ranges as the
coverage domain — same trust model as its existing `EnumType` branch (data
conformance is checked by datasource validation): `score >= 0` fully covers
`int[0..100]`, so CASE-exhaustiveness and datasource-injection completeness
proofs can use it. Every declared range must be covered; open bounds fall
back to the base type's bounds; regex domains are not reasoned about.

## Ingest inference & stdlib traits

`trilogy ingest` infers validators (`detect_numeric_bounds`, full-scan MIN/MAX
like enum detection): integer non-key columns get tight `[min..max]` (exact for
the ingested source; regenerated on re-ingest), continuous float/numeric
measures get only a structural `[0..]` sign bound when non-negative. Keys, enum
columns, and trait-assigned columns are skipped (a trait may carry its own
stdlib validator; observed partial bounds would conflict on re-parse).

String enforcement rides the **stdlib traits** instead of per-column regex
inference: `std.date` types carry ranges (`type month int[1..12];`, year/
quarter/week/day/hour/minute/second/day_of_week), `std.net` carries permissive
regexes matching the ingest value-gates (`email_address string['\S+@\S+']`,
ipv4, url), `std.color.hex` a hex-color pattern. Trait composition then
enforces them anywhere the trait is applied. An enum base composing with a
validated trait (`enum<int>[1,2,3,4]::quarter` — ingest's enum + rich-type
wrap) verifies the members against the validator and keeps the narrower enum.
Geography traits (`us_state`, `postal_code`, …) deliberately carry no regex:
their name-based detection has no value gate, so international/full-name data
would false-fail.

## Function bindings

`def clamped(x: int[0..100]) -> ...;` — validators parse on bindings and
`CustomFunctionFactory.__call__` rejects out-of-domain literal arguments at
author time (`@clamped(250)` errors; concept args pass through, enforced at
their own declaration).

## Follow-ups

- Trait-typed bindings (`x: int::pct`) demand the trait of the passed arg, so
  literal args fail the trait check before any domain check — carry-through
  for literals would need trait-inference relaxation.
