# Bug handoff: a compound boolean (`and`/`or`) cannot be assigned to a derived concept (`auto`)

## Summary

A derived concept defined with `auto x <- <expr>` rejects a **compound boolean**
expression — anything joined by `and` / `or`, and also `between` (which desugars
to a range predicate). Single comparisons (`flag = 1`) and arithmetic (`v + 1`)
are fine. The exact same compound boolean parses and evaluates correctly inside a
`?` inline aggregate filter and inside a `where` clause, so the limitation is
specific to the **derived-concept assignment position**, not the boolean
expression itself.

This blocks the natural way to name a reusable row predicate, e.g.

```trilogy
auto in_sale <- (sales.date.date between '2000-08-23'::date and '2000-09-06'::date)
                and sales.channel_dim_text_id is not null;   # InvalidSyntaxException
```

which forces the condition to be copy-pasted into every `sum(... ? <cond>)` that
needs it (see `tests/modeling/tpc_ds_duckdb/query05.preql`, where the sale/return
window+dim gate is repeated across three aggregates).

## Minimal repro

`evals/tpcds_agent/repro_compound_boolean_auto.py` (prints the matrix below):

| form | result |
|---|---|
| `auto x <- v + 1` (arithmetic) | OK |
| `auto x <- flag = 1` (single comparison) | OK |
| `auto x <- flag = 1 and id = 1` (compound bool) | **FAIL** `InvalidSyntaxException` |
| `auto x <- d between a and b` | **FAIL** `HydrationError` |
| `select sum(v ? flag = 1 and id = 1)` (inline `?` filter) | OK |
| `where flag = 1 and id = 1 select id` | OK |

```
key id int;
property id.d date;
property id.flag int;
property id.v int;
datasource t (id, d, flag, v) grain (id)
query '''select 1 as id, date '2020-01-01' as d, 1 as flag, 10 as v''';

auto x <- flag = 1 and id = 1;   -- InvalidSyntaxException at the `and`
select x;
```

The parse error points at the `and`:
`expected dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT`
— i.e. after the first comparison the grammar only expects a continuation of an
arithmetic/scalar expression, never a logical operator. `between` fails one stage
later (`HydrationError`) rather than at parse.

## What narrows it

- `auto x <- <arith>` → OK.
- `auto x <- <single comparison>` → OK.
- `auto x <- <comparison> and/or <comparison>` → parse error at the operator.
- `auto x <- <between>` → HydrationError.
- Same compound bool inside `sum(x ? <cond>)` → OK.
- Same compound bool inside `where <cond>` → OK.

So both parser backends treat a derived-concept body as a scalar/arithmetic
expression that may *end* in a single comparison, but cannot contain a logical
combinator — even though the conditional grammar used by `?` and `where` can.

## Desired fix

Allow a full boolean expression (logical `and`/`or`, `between`, `is null`, etc.)
as a derived-concept body so a row predicate can be named once and reused, e.g.
`auto in_window <- d between a and b and flag = 1;` then `sum(v ? in_window)`.
Grammar lives in both backends — `trilogy/parsing/trilogy.lark` and the `.pest`
file (pest is the default; needs a `maturin develop --release` rebuild). The
conditional production already used by `?`/`where` is the shape to admit here.

## Repro assets

- `evals/tpcds_agent/repro_compound_boolean_auto.py` — runs the matrix above
  through `parse_text`; no DB needed.
