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

## Related: align trait/type-coherence (FIXED 2026-06-06)

A cousin of this surfaced when validating the enum fix on q66: an `align` of two columns
whose types differ only by a trait or within the integer/float/numeric family
(`date_dim.moy` bigint vs `month(date)` int+`month`-trait) raised
`Datatypes do not align for merged statements month, have {BIGINT, TraitDataType(int, [month])}`.
`align_item_to_concept` (`trilogy/parsing/common.py`) grouped by exact inner type. Now it uses
`is_compatible_datatype` (which strips traits and unifies the numeric family) and picks a
trait-bearing/widened representative via `merge_datatypes`. Regression tests:
`tests/test_hydration_phases.py::TestStagedParser::test_multiselect_align_compatible_datatypes`
and `…_incompatible_datatypes_raise`.

## Enum/base compatibility + traits-over-enums + ingest date-part traits (FIXED 2026-06-06)

Follow-on to the align fix, all in one pass:
- **`enum<X>` ↔ base `X` compatibility.** `is_compatible_datatype`
  (`trilogy/core/models/core.py`) now strips an `EnumType` to its base (mirroring the existing
  trait stripping), so `enum<string>` aligns with bare `string` (q05 `st_id`) and `enum<int>`
  with `int`/`bigint`. Genuinely different bases (enum<string> vs int) still raise. Unit test:
  `tests/test_typing.py::test_is_compatible_datatype_enum_and_trait`.
- **Traits can wrap enums with a compatible base.** Because the trait-validity check in
  `concept_rules.py` routes through `is_compatible_datatype`, `enum<int>[…]::month` now validates
  (and an incompatible base like `enum<string>::float-trait` still raises). Requires the trait to
  be a declared type — the date-part traits live in `trilogy/std/date.preql` (added `quarter`,
  which the `quarter()` function produced but the std lib was missing).
- **Ingest now annotates date-part columns with std.date traits.** `detect_rich_type`
  (`trilogy/scripts/ingest_helpers/typing.py`) gained a `date` category: `moy`/`month_of_year`,
  `year`/`yr`, `dom`/`day_of_month`, `dow`/`day_of_week`, `qoy`/`quarter`, `woy`/`week` over an
  integer base, **gated by an inclusive value range** (e.g. month ∈ 1..12) so `d_month_seq`
  (~1200) is NOT misclassified. Emits `import std.date` + the trait. Tests:
  `tests/scripts/test_ingest.py::TestRichTypeDetection::test_date_part_detection` and
  `…_value_gate_rejects_out_of_range`; end-to-end via
  `tests/scripts/test_cli_consistency.py::test_ingest_output_passes_integration`.

STILL OPEN: Layer 2 proper — the ingest auto-typer still enum-types *continuous* numeric measures
(`warehouse_sq_ft`, `floor_space`) purely on low cardinality. That is now far less harmful (the
enum-comparison errors are actionable and enum/base aligns work), but it remains a separate
heuristic to tighten.

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
