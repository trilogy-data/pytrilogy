# Bug: a `having` condition at a grain finer than the SELECT changes the output grain (fan-out instead of filter)

**Severity: HIGH — SILENT wrong results.** No error; the query runs (`exit 0`) and returns multiplied rows with wrong aggregate values. Surfaced on TPC-H Q21 ("suppliers who kept orders waiting"), enriched leg.

## The contract being violated
**`having` must never change the SELECT's output grain.** It is a post-aggregation filter on the select-grain rows. When a `having` condition references concepts that are **not in the output projection** (here, a finer grain than the select grain), it must filter the select-grain rows via a **SEMIJOIN** (the select-grain row survives iff a qualifying finer-grain row exists) — never a regular join, which multiplies the select-grain rows.

## Symptom / reproduction (`generate_sql` + execute, TPC-H model)
```trilogy
import raw.lineitem as li;
select li.part.supplier.name as sn,
       count(li.order.id) by li.part.supplier.name as nw
where  li.part.supplier.nation.name = 'SAUDI ARABIA'
having count(li.part.supplier.id) by li.order.id >= 2;
```
| query | rows |
|---|---|
| **without** the `having` | **47** (one per Saudi supplier — the declared `{supplier.name}` grain) |
| **with** the order-grain `having` above | **4648** (each supplier fanned out across its qualifying orders; `nw` repeated verbatim) |

The select's only non-aggregate output is `supplier.name`, so the result grain is `{supplier.name}` and must stay 47 rows. The single order-grain `having` blows it to 4648.

## Mechanism (the generated SQL)
The `having`'s order-grain aggregate is materialized in its own CTE and then **INNER-JOINed back in on the finer key**, not semijoined:
```sql
-- abundant   = supplier-grain aggregate (nw = count by supplier.name), GROUP BY supplier.name
-- questionable = per-(supplier,order) rows ; thoughtful = orders where count(...) >= 2
uneven AS (
  SELECT abundant.nw, questionable.li_order_id, questionable.li_part_supplier_name
  FROM questionable INNER JOIN abundant
       ON questionable.li_part_supplier_name = abundant.li_part_supplier_name)   -- broadcasts nw per order
SELECT uneven.li_part_supplier_name AS sn, uneven.nw AS nw
FROM uneven INNER JOIN thoughtful
     ON uneven.li_order_id = thoughtful.li_order_id;                            -- INNER JOIN filters AND fans out
```
The final `INNER JOIN thoughtful ON li_order_id` both applies the filter **and** multiplies — one output row per qualifying order, with the supplier-level `nw` repeated. It should instead be `WHERE EXISTS (… thoughtful … same supplier)` / a semijoin that keeps one row per supplier.

`select distinct` (the agent's next move) only masks the row duplication — `nw` was still computed over the multiplied grain, so the counts are wrong regardless.

## Root cause (pointers)
`having` is applied as a plain condition/join rather than a grain-preserving semijoin:
- `trilogy/core/query_processor.py:692-712` and `:819-849` — `build_statement.having_clause` handling; `ds.add_condition(having)` (`:827`) and the having-source resolution (`:712`, `:849`) bring the finer-grain `having` concepts into the datasource as joined inputs.
- The defect: when the `having` conditional references concepts **not in the select output set** (i.e. finer than / off the select grain), the resulting source is INNER-JOINed (filter + multiply) instead of being attached as a semijoin keyed on the select grain.

## Fix direction
When resolving a `having` conditional, split it into concepts that ARE in the select projection vs concepts that are NOT:
- in-projection conditions → apply directly (current behavior is fine; grain unchanged).
- **off-projection (finer/other-grain) conditions → apply as a SEMIJOIN** against the select-grain keys (`WHERE <select keys> IN (SELECT <select keys> FROM <having source> WHERE <cond>)` / `EXISTS`), so the select-grain row is filtered, never duplicated.

Equivalently: the `having` source must be reduced (semijoin / existence) to the select grain before it touches the main query, exactly like membership/existence filters already do elsewhere — never an INNER JOIN that can fan out.

## Tests
- The repro above returns **47** rows (not 4648), and `nw` equals the no-`having` per-supplier count for the suppliers that survive the filter.
- A `having` referencing only projected (select-grain) concepts is unchanged (no regression).
- Multiple off-grain `having` conditions (TPC-H Q21 has three, at order grain) all filter the supplier rows without multiplying; row count stays at the supplier grain.
- An off-grain `having` that matches nothing drops the supplier (semijoin semantics), rather than producing zero-fanned rows.
