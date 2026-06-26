# Bug: undefined rowset member inside a function (`coalesce`) gives a confusing type error instead of "Undefined concept" (q11)

**Status:** FIXED 2026-06-26. Error-quality bug (not correctness/crash).

## Fix

`FunctionFactory.create_function` (trilogy/core/functions.py) now short-circuits when
any processed arg is an `UndefinedConcept`: it emits an UNKNOWN-typed `Function`
instead of running the output-type function (e.g. coalesce's same-type check). The
deferred undefined reference then surfaces in `finalize_select_statement`, which
already walks a transform's function args and raises the clean
`UndefinedConceptException` with suggestions — identical to the bare-select path.
General across all type-checking functions, not just coalesce. Genuine type
mismatches (all args defined) still raise as before.

Tests: tests/test_undefined_concept.py::test_undefined_concept_in_coalesce_raises_undefined_not_type_error
and ::test_genuine_coalesce_type_mismatch_still_raises

---

**Status (original):** OPEN — confirmed, 100% deterministic. Error-quality bug (not correctness/crash).
**Surfaced by:** TPC-DS q11 enriched eval (run `20260626-031753`) — cost ~3 iterations.
**Severity:** Low-Medium. The query is genuinely wrong (references a column the rowset doesn't
export), but the error misdirects the agent to a "type" problem instead of the real "undefined
member" problem.

## Symptom

Referencing a rowset member the rowset does **not** project, **as a function argument**, yields:

```
Syntax error: All arguments to coalesce must be of the same type, have
{<DataType.STRING>, <DataType.UNKNOWN>} for [ref:cinfo.login, ref:w01.web.billing_customer.login]
```

The same reference in a **bare select** gives the correct, actionable error:

```
Undefined concept: w01.web.billing_customer.login. Suggestion: …
```

(`w01` projects only `cust_id` + `rev`; `web.billing_customer.login` was never selected into it.)

## Reproduce

```trilogy
import raw.web_sales as web;
import raw.store_sales as store;
rowset w01 <- where web.date.year=2001
  select web.billing_customer.id as cust_id, sum(web.ext_list_price) as rev;
rowset c <- select store.customer.id as cust_id, store.customer.login as login;

-- (A) bare select  -> clean UndefinedConceptException
select c.login, w01.web.billing_customer.login as x inner join c.cust_id=w01.cust_id limit 5;

-- (B) inside coalesce -> confusing "{STRING, UNKNOWN}" type error
select coalesce(c.login, w01.web.billing_customer.login) as x inner join c.cust_id=w01.cust_id limit 5;
```

A → `UndefinedConceptException`; B → `InvalidSyntaxException` (coalesce type mismatch).

## Root cause (hypothesis)

In function/expression argument resolution, an unresolved rowset-member reference is assigned a
phantom concept of type `UNKNOWN` rather than raising `UndefinedConceptException`. The downstream
type-compatibility check on `coalesce` (which requires homogeneous arg types) then fires on
`{STRING, UNKNOWN}`. The bare-select path runs the undefined-concept check first, so it reports the
real problem.

## Suggested fix

Make function-argument concept resolution raise the same `UndefinedConceptException` (with the
"Suggestion: …" list of the rowset's actual outputs) that a bare reference does, *before* any
type-compatibility check. An undefined member should never resolve to an `UNKNOWN`-typed phantom.
Cheaper alternative: when a coalesce/type check sees an `UNKNOWN`-typed arg that traces to an
unresolved reference, re-raise as undefined-concept with the rowset's available outputs.

## Note — the rest of q11's thrash is NOT a bug

q11's dominant error (`cannot merge … 9 disconnected subgraphs`, ×5) is **correct** framework
behavior: the agent referenced raw `web.billing_customer.login`/`first_name`/`last_name` directly
in the final select without carrying them through any rowset (the rowsets aliased
`web.billing_customer.id` to `cust_id`, breaking the address path). Verified: carrying the columns
through the rowset resolves cleanly. That's agent construction on a hard 5-rowset two-channel YoY
query, not a framework defect.

## Repro harness

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260626-031753/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
eng.generate_sql(open('repro.preql').read())
```
