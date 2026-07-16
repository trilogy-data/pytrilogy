# Handoff — missing `@` on a named function call produces an opaque parser error

**Status:** OPEN authored-diagnostic defect. Discovered in the enriched q66
trajectory while the agent composed monthly pivot helpers.

## Symptom

Trilogy requires user-defined functions declared with `def` to be invoked with
an `@` prefix. When the prefix is omitted, the parser points at the opening
parenthesis and emits only a generic expected-token list:

```text
def mon_sales_sqft(m) -> mon_sales(m) / s.warehouse.square_feet;
                                   ^---
expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail,
COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
```

This does not identify the actual correction:

```trilogy
def mon_sales_sqft(m) -> @mon_sales(m) / s.warehouse.square_feet;
```

## Minimal reproduction

```trilogy
def identity(x) -> x;
def doubled(x) -> identity(x) * 2;
```

The second definition should be rejected with an authored diagnostic explaining
that named-function calls require `@`, rather than an operator-oriented grammar
failure at `(`.

Correct form:

```trilogy
def doubled(x) -> @identity(x) * 2;
```

## Expected diagnostic

Suggested wording:

```text
Named function `identity` must be invoked with `@`; write `@identity(...)`.
```

The source span should cover `total(...)` or at minimum point at `total`, not
just the opening parenthesis.

## Why this is detectable

At the failure site:

1. the grammar has parsed an identifier followed immediately by `(` in an
   expression position;
2. an earlier `def identity(...)` exists in the same document/environment;
3. the valid named-function invocation differs only by the missing `@`.

That combination is specific enough to replace or augment the generic parser
expectation list without guessing.

## Likely fix area

Add a targeted parse-error enrichment where syntax diagnostics are converted
into authored errors. When the unexpected token is `(` after an identifier,
look up that identifier in the named-function registry visible at the failure
position. If it resolves, emit the `@name(...)` correction.

Do not broadly rewrite every `identifier(` parse failure: only use this message
when the identifier matches a known named function. Unknown names should retain
their normal undefined/parse diagnostic.

## Regression coverage

Add parser and CLI/file-write tests for:

1. calling an earlier named function without `@` produces the actionable
   diagnostic and source location;
2. `@identity(...)` parses successfully;
3. nested named-function calls receive the same diagnostic;
4. an unknown `missing(...)` identifier is not incorrectly reported as a known
   named function;
5. built-in functions such as `sum(...)` remain valid without `@`;
6. the CLI/file-write error never falls back to only the generic expected-token
   list for this recognized case.

## Artifact

The original q66 failure was:

```trilogy
def mon_sales(m) -> sum(combined_sales_line ? s.date.month_of_year = m);
def mon_sales_sqft(m) -> mon_sales(m) / s.warehouse.square_feet;
```

It occurred while writing `answer_3979964698.preql`; the write validator
correctly rejected the file, but its diagnostic did not expose the missing
prefix.
