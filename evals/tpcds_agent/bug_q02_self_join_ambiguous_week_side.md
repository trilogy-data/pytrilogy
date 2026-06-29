# Bug: rowset self-join leaks a join-key forward-ref as a phantom output → FALSE "Ambiguous reference" on leaf-shorthand

**Status:** ✅ FIXED 2026-06-28 (claude). `_collapse_alias_matches` now drops a forward-only
match (no real concept) that is NOT a pending output — once a real match exists, every genuine
output is already a real concept, so a pure symbol-table leak (the self-join's other-side join
key `joined.nxt.wk`) is no longer counted toward ambiguity. Test:
`tests/test_rowset_output_shorthand.py::test_self_join_rowset_phantom_join_key_not_ambiguous`.
(Originally found TPC-DS agent eval q02, run `evals/tpcds_agent/results/20260628-194910`,
`agent_log.q02.jsonl` write at log line 62 → error at line 66.)
**Severity:** medium — `generate_sql` never runs; parse aborts with a misleading ambiguity error that
names a candidate (`joined_data.nxt_dow.wk`) which is **not actually a selectable output of the
rowset**. Fully qualifying to the real side (`joined_data.cur_dow.wk`) makes the whole query succeed,
so this is a pure false-positive that cost the agent iterations (q02 churned ~779k tokens this run).
**Area:** rowset leaf-shorthand resolution — `_resolve_rowset_suffix` /
`_collapse_alias_matches` / `_rowset_output_addresses` in `trilogy/parsing/v2/semantic_state.py`.

## Symptom

The agent modeled q02's 53-week offset as a scoped **self-join** of two `with` rowsets (`cur_dow`,
`nxt_dow`, each a per-`(week_seq, day_of_week)` sum), materialized into a third rowset `joined_data`,
then projected day-of-week pivots from it via leaf-shorthand `joined_data.wk` / `joined_data.dw`:

```trilogy
with joined_data as
select cur_dow.wk, cur_dow.dw, cur_dow.amt as cur_amt, nxt_dow.amt as nxt_amt
inner join cur_dow.wk + 53 = nxt_dow.wk
inner join cur_dow.dw   = nxt_dow.dw;

select
    joined_data.wk as week_seq,           -- <-- FALSE ambiguity error here
    round(sum(case when joined_data.dw = 0 then joined_data.cur_amt else null end) /
          sum(case when joined_data.dw = 0 then joined_data.nxt_amt else null end), 2) as sunday
where joined_data.wk in weeks_in_2001;
```

```
Syntax error in query02.preql: Ambiguous reference 'joined_data.wk':
matches ['joined_data.cur_dow.wk', 'joined_data.nxt_dow.wk']. Qualify the full path to disambiguate.
```

But `joined_data.nxt_dow.wk` is **not a real output** of the rowset — `nxt_dow.wk` appears only as a
**join key** (`cur_dow.wk + 53 = nxt_dow.wk`); the SELECT projects `cur_dow.wk` only. So the shorthand
`joined_data.wk` has exactly one genuine target and should resolve unambiguously.

## Proof it is a FALSE ambiguity

Model dir = the eval workspace (`raw.all_sales`), `generate_sql` only.

| variant | result |
|---|---|
| `select joined_data.wk ...` (leaf shorthand) | **FALSE Ambiguous** (`cur_dow.wk` vs `nxt_dow.wk`) |
| `select joined_data.cur_dow.wk ...` (every shorthand fully qualified to the cur side) | **OK** — `generate_sql` len ~3982 |
| `select joined_data.nxt_dow.wk;` (try to select the phantom directly) | **UndefinedConcept** — `joined_data.nxt_dow.wk` does not exist; suggestions list `joined_data.cur_dow.wk`, `joined_data.cur_dow.dw`, `joined_data.nxt_amt` but NOT `nxt_dow.wk` |
| `env.concepts.data` keys under `joined_data.` after parse | only `cur_amt`, `cur_dow.dw`, `cur_dow.wk`, `nxt_amt` — **no `nxt_dow.wk`** |

So the genuine outputs of `joined_data` are `{cur_dow.wk, cur_dow.dw, cur_amt, nxt_amt}`. The
ambiguity check invents `joined_data.nxt_dow.wk` as a second candidate; it is never materializable.

## Minimal repro

```python
from trilogy import Environment, Dialects
WS = "evals/tpcds_agent/results/20260628-194910/workspace"
env = Environment(working_path=WS)
ee  = Dialects.DUCK_DB.default_executor(environment=env)
text = """
import raw.all_sales as sales;
with cur_dow as
where sales.channel in ('WEB','CATALOG')
select sales.date.week_seq as wk, sales.date.day_of_week as dw, sum(sales.ext_sales_price) as amt;
with nxt_dow as
where sales.channel in ('WEB','CATALOG')
select sales.date.week_seq as wk, sales.date.day_of_week as dw, sum(sales.ext_sales_price) as amt;
with joined_data as
select cur_dow.wk, cur_dow.dw, cur_dow.amt as cur_amt, nxt_dow.amt as nxt_amt
inner join cur_dow.wk + 53 = nxt_dow.wk
inner join cur_dow.dw = nxt_dow.dw;
select joined_data.wk;        -- raises the false Ambiguous reference
"""
ee.generate_sql(text)
# Replace `joined_data.wk` with `joined_data.cur_dow.wk` -> generates valid SQL.
```

## Root cause (file:line)

1. `_rowset_output_addresses(prefix)` (`semantic_state.py:526-542`) returns **every** address under the
   rowset prefix, explicitly including `self._symbol_table.visible_addresses()` (symbol-table forward
   refs). The rowset body `inner join cur_dow.wk + 53 = nxt_dow.wk` registers `nxt_dow.wk` under the
   rowset namespace as the forward ref `joined_data.nxt_dow.wk`, even though it is only a **join key**,
   never a projected output. (`env.concepts.data` correctly omits it; the leak is symbol-table-only.)
2. `_resolve_rowset_suffix` (`semantic_state.py:574-608`) collects subsequence matches of
   `joined_data.wk` over that set → `{joined_data.cur_dow.wk, joined_data.nxt_dow.wk}`.
3. `_collapse_alias_matches` (`semantic_state.py:544-572`) is meant to drop stale forward-only matches,
   but only when a forward match is an **ordered subsequence of a real match**. Here
   `joined_data.nxt_dow.wk` (`[joined_data, nxt_dow, wk]`) is NOT a subsequence of the real
   `joined_data.cur_dow.wk` (`[joined_data, cur_dow, wk]`) — different middle segment — so the phantom
   survives. `real=[cur_dow.wk]`, `kept_forward=[nxt_dow.wk]` → 2 matches → false ambiguity raised at
   `semantic_state.py:603-606`.

## Verdict — FRAMEWORK defect (rowset self-join should disambiguate sides), NOT agent naming

The shorthand `joined_data.wk` has exactly one real target. The error stems from a join-key forward
ref leaking into the candidate set; a join key into the *other* rowset source is never a projected
output of `joined_data` and should not count toward ambiguity. Either (a) `_rowset_output_addresses`
should exclude symbol-table refs that are pure join keys / never become projected outputs, or (b)
`_collapse_alias_matches` should drop any forward-only match that has no corresponding real OR pending
projected-output concept (not just subsequence-of-a-real-match), so a self-join's mirrored
join-key column (`nxt_dow.wk`) is not treated as a phantom twin of the projected `cur_dow.wk`.

## Relation to priors

- **Same machinery, NEW trigger vs the q64 false-ambiguity fix** (`project_q64_nested_rowset_shorthand_false_ambiguity`,
  memory): that fix added `_collapse_alias_matches` to drop a FORWARD-only stale write-shorthand that
  is a *subsequence* of the real concept. This case is the **complementary gap**: the phantom forward
  ref is NOT a subsequence (it is the *other* self-join source's same-named column), so the existing
  subsequence guard doesn't catch it. A self-join is the distinguishing ingredient.
- **Distinct from `bug_q02_offset_week_virt_filter_resolution.md`** (the `_virt_filter` family): that
  report is about filtered membership/aggregate over a rowset output failing to *resolve*; this one is
  a *parse-time* shorthand ambiguity that aborts before resolution, and the underlying query (once
  qualified) resolves and generates valid SQL.

## Workaround (agent-side)

Fully qualify the side: `joined_data.cur_dow.wk` (and `joined_data.cur_dow.dw`). Confirmed to generate
valid SQL. An agent-info note that scoped self-join rowset outputs must be qualified by source side
would short-circuit the churn.
</content>
</invoke>
