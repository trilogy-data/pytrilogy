# Bug B4: enum-typed numeric columns reject ordered comparisons (and the ingest heuristic that creates them)

**Status:** LAYER 1 FIXED 2026-06-06 — reframed as an overall enum-comparison
improvement. The parser now evaluates each comparison's satisfiability over the enum's
value domain (plus NULL when the concept is nullable) and raises a new
`InvalidComparison` with an actionable message whenever the result is constant:

- **Discriminating** filters — a literal/range/list that some members satisfy and others
  do not (`> 138504`, `between 100 and 300000`, `in (138504, 999)`, and `= 'USBOS'`
  against a multi-city enum) — pass through to a normal `Comparison`/`Between`. There is
  no special-casing of `=`/`in`; narrowing a wider enum to a subset is always valid.
- **Unsatisfiable** (no member matches — always False): `= 0`, `> max`, `between` outside
  the domain, `in` with no overlap → ``Comparison `w.sq_ft = 0` can never match enum
  field 'w.sq_ft' … It is always false and should be removed.``
- **Tautology on a non-nullable field** (every member matches): `> 0`, `between 0 and
  9999999` → ``Comparison `… > 0` matches every value … It is always true and should be
  removed.``
- **Match-all on a nullable field** (every member matches; the predicate's only effect is
  dropping NULLs): same predicates against a nullable enum → ``Comparison `… > 0` matches
  every value of nullable enum field … It only excludes nulls; simplify it to `<field> is
  not null`.``

The error message leads with the rendered comparison (operator included) so `> 0` is not
mistaken for matching `0`. Nullability is read from the concept's `Modifier.NULLABLE` via
the parse `RuleContext`.

**Operator coverage.** Validated: `=` `!=` `<` `>` `<=` `>=` (ordered/equality), `in` /
`not in` (membership), `between`, and `like` / `ilike` / `not like` / `not ilike` (SQL
pattern translated to a regex and run over a *string* enum's members; both the infix and
`like(col, 'pat')` call sites go through the shared `validate_enum_like`). Intentionally
skipped: `is` / `is not` (NULL checks — `is not null` is the canonical form we recommend),
`contains` (array op), `else` (case branch). LIKE on a non-string enum is left alone.

Implemented in `trilogy/parsing/v2/rules/expression_rules.py` (`comparison`,
`between_comparison` + `_enum_*` helpers) and `trilogy/core/exceptions.py`
(`InvalidComparison(InvalidSyntaxException)`). Tests in
`tests/engine/test_enum_unions.py` (`test_enum_unsatisfiable_comparison`,
`test_nullable_enum_match_all_suggests_is_not_null`,
`test_non_nullable_enum_match_all_is_tautology`,
`test_enum_discriminating_comparison_allowed`). The `boston_multi_enum.preql` fixture's
single-value `city` enum was widened to `['USBOS', 'USNYC']` (it was a simplification of
a real multi-city model), so its `complete where city = 'USBOS'` partitions discriminate.

Layer 2 (ingest auto-typer over-eagerly enum-typing continuous numeric measures) is
**still open** — see below.

**Original report follows.**

**Status:** OPEN (found 2026-06-06)
**Severity:** medium — agents cannot write natural numeric predicates (`> 0`, `between 100 and
500`) against a column the ingester typed as `enum<bigint>`. Burned ingest q66 (`warehouse_sq_ft`)
and contributed to q05 (`store_id` align mismatch).
**Two layers — fix at least the first, ideally both:**
1. **Parser/validator** `trilogy/parsing/v2/rules/expression_rules.py:211` (`comparison`) — the
   enum-domain check is applied to *ordered* comparisons, not just equality/membership.
2. **Ingest auto-typer** — types continuous numeric measures (square footage, floor space,
   employee counts) as `enum<bigint>` purely on low cardinality.

## Symptom

```
InvalidSyntaxException: Value 0 is not valid for enum field 'w.sq_ft'.
Allowed values: 138504, 294242, 621234, 977787.
```

Fires for `w.sq_ft > 0`, `w.sq_ft >= 100 and w.sq_ft <= 500`, etc. — any comparison whose
literal isn't a member of the enum domain. `coalesce(w.sq_ft, 0)` and `sum(w.sq_ft)` happen to
compile; the failure is specifically the comparison-domain validation.

## Deterministic reproduction (self-contained, no data)

Files preserved at **`evals/tpcds_agent/bug_B4_enum_repro/`** (`wh.preql`, `q.preql`).

```python
from pathlib import Path
from trilogy.core.query_processor import process_query
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.parsing.parse_engine_v2 import parse_text
ROOT = Path("evals/tpcds_agent/bug_B4_enum_repro")
text = Path(ROOT / "q.preql").read_text()
env, parsed = parse_text(text, root=ROOT)          # -> InvalidSyntaxException at parse/bind
process_query(env, parsed[-1])
```

`wh.preql` declares `property warehouse_sk.sq_ft enum<bigint>[138504, 294242, 621234, 977787]?;`
— mirroring exactly what `trilogy ingest` produced for `w_warehouse_sq_ft` in the eval. `q.preql`
is just `where w.sq_ft > 0`.

## Root cause (layer 1)

`expression_rules.py` `comparison()` validates, for ANY operator, that an int/str literal
compared against an `EnumType` concept is in `concept.datatype.values`:

```python
for concept_side, value_side in ((left, right), (right, left)):
    if isinstance(concept_side, ConceptRef) and isinstance(concept_side.datatype, EnumType):
        if isinstance(value_side, (str, int)) and value_side not in concept_side.datatype.values:
            raise InvalidSyntaxException(f"Value {value_side!r} is not valid for enum field ...")
```

A domain-membership check makes sense for `=` / `!=` / `IN` (a literal outside the domain can
never match). It is **wrong for ordered operators** (`>`, `>=`, `<`, `<=`) and for arithmetic —
those treat the enum as its underlying scalar, and a bound like `0` or `500` legitimately need
not be a domain member. **Fix:** restrict the validation to equality/membership operators; let
ordered comparisons fall through to a normal `Comparison` on the base type.

## Root cause (layer 2 — ingest heuristic)

`trilogy ingest` typed several continuous numeric measures as enums on cardinality alone:
`warehouse_sq_ft enum<bigint>[...]`, `store.floor_space enum<bigint>[...]`,
`store.number_employees enum<bigint>[...]`. These are quantities, not categories. Even with
layer-1 fixed, `align`/datatype-coherence breaks when one channel's id is `enum<string>` and
another's is plain `string` (q05: `Datatypes do not align for merged statements st_id`). Gate
enum inference so numeric/`*_sk`/`*_id`/measure-like columns are NOT enum-typed regardless of
cardinality (or only enum-type `string` categoricals).

## Provenance

TPC-DS agent eval, ingest q66 (`warehouse_sq_ft` monthly-sales-per-sqft) and q05 (`store_id`
cross-channel align). See inventory `token_burn_inventory_20260606.md`.
</content>
