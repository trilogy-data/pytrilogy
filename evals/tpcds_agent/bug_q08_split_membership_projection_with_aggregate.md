# Bug: `x in split(param, ',')` as a projection fails when the select also contains an aggregate

Status: OPEN. Found in run `20260817-013108` (deepseek-v4-flash), q08, BOTH legs
(`ingest` and `enriched`). This was the trigger for the agent's whole q08 thrash
spiral (subsequent `array_contains` and `unnest` workaround attempts).

## Symptom

Membership over a `split()` of a string parameter, projected as a boolean
column, renders an `INVALID_REFERENCE_BUG` sentinel when any aggregate shares
the select:

```
Could not render the query: Missing source reference to
local._virt_func_split_4785012549328100. A planned reference has no backing
source CTE ...
```

The enriched leg hit the same construct via a rowset and got a different
surface error for the same underlying wiring gap:

```
Resolution error: Could not resolve condition existence arguments
['local._virt_func_split_4785012549328100']
```

## Minimal repro

```python
from trilogy import Dialects, parse

MODEL = """
key cust_id int;
property cust_id.zip string;
property cust_id.flag string;
datasource customers ( id: cust_id, z: zip, f: flag ) grain (cust_id) address cust_tbl;
parameter zips string default '1,2';
"""
env, _ = parse(MODEL)
Dialects.DUCK_DB.default_executor(environment=env).generate_sql(
    "select zip, count(cust_id ? flag = 'Y') as n, zip in split(zips, ',') as in_param;"
)
# ValueError: Missing source reference to local._virt_func_split_...
```

## Trigger matrix

| Shape | Result |
|---|---|
| `select zip, zip in split(zips, ',') as in_param;` | OK |
| `select zip where zip in split(zips, ',');` | OK |
| `rowset r <- select ... where zip in split(zips, ','); select r.p;` | OK |
| projection membership + aggregate in the same select | FAIL (sentinel) |

The sole trigger is the membership-over-split appearing as a PROJECTED column
in a select that also carries an aggregate. The aggregate flips the select into
a grouped plan and the virtual split concept's source CTE is not carried into
(or joined to) the group node.

## Field occurrences (exact bodies preserved in the run logs)

- `results/20260817-013108_ingest_deepseek_deepseek-v4-flash/agent_log.q08.jsonl`:
  `probe_store.preql` and `probe_cust.preql` (both with and without the
  `root.` import prefix; the prefixed retries hit this bug).
- `results/20260817-013108_enriched_deepseek_deepseek-v4-flash/agent_log.q08.jsonl`:
  `probe_mech.preql` (rowset variant, "Could not resolve condition existence
  arguments").

## Root-cause leads (not yet confirmed to file:line)

- Virtual function concepts get a canonical `_virt_func_*` address; the class
  collapse / probe-canonicalization for those addresses lives in
  `trilogy/core/processing/v4_helper/network_build.py:93` and
  `trilogy/core/processing/v4_helper/source_planning.py:933`.
- The sentinel is emitted from `trilogy/dialect/base.py:1642` /
  `base.py:1876` when the planned reference has no backing CTE.
- Compare against the passing WHERE path: the existence-subquery machinery
  (unnest-members EXISTS render) wires the split CTE correctly there; the
  projection-plus-aggregate path never attaches that CTE to the grouped node.

## Doctrine note

Per EVAL_LOOP_INSTRUCTIONS: any `INVALID_REFERENCE_BUG` sentinel from
generated SQL is a framework bug regardless of how unusual the authored query
is. The framework must either plan this or reject it with a clear authored
error ("membership over a computed array cannot be projected next to an
aggregate; move it to WHERE or a rowset").
