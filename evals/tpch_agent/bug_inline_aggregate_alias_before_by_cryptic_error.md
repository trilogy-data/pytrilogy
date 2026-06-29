# Bug: inline aggregate with `as <alias>` placed BEFORE `by <grain>` → cryptic parse error

**Severity:** low (parse-time, loud) but **high-friction** — the agent can't tell its `as`/`by` are just in the wrong order. Surfaced on TPC-H (an inline grouped aggregate in a `select`).

## Symptom
```trilogy
import raw.lineitem as li;
select li.order.id as oid,
       max(li.line_no ? li.receipt_date > li.commit_date) as late_flag by li.order.id
limit 20;
```
→
```
Syntax error: --> 2:150
... max(li.line_no ? li.receipt_date > li.commit_date) as late_flag ??? by li.order.id ...
  = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
```
The error fires at the `by` and lists clause keywords — it never says the real problem: **the `as <alias>` and the `by <grain>` are swapped.** The agent guessed for a turn before reordering.

## Boundary (reproduced via `generate_sql`)
| form | result |
|---|---|
| `max(...) as late_flag by li.order.id` (alias **before** `by`) | **CRYPTIC parse error** |
| `max(...) by li.order.id as late_flag` (alias **after** `by`) | OK |

Correct order for an inline grouped aggregate select-item is `<aggregate> by <grain> as <alias>`. With the alias first, the parser has already closed the select-item at `as late_flag`, so the trailing `by` looks like a misplaced top-level clause → the generic "expected metadata, limit, order_by, …" message.

## Root cause / where it lives
This is the third case in the `by`-placement family in `trilogy/parsing/v2/errors.py`; the other two already have friendly detectors:
- `detect_by_on_wrapped_aggregate` (`errors.py:276`) — `coalesce(sum(x),0) by …`
- `detect_by_on_non_aggregate` (`errors.py:357`) — `raw_identifier by …` (added from the q06 handoff)

The **alias-before-`by`** case (`<aggregate> as <alias> by <grain>`) has no detector, so it falls through to the raw Lark/PEST grammar error.

## Fix (recommended: friendly detector)
Add `detect_alias_before_by` (sibling of the two above): when a `by` clause appears immediately after an `as <identifier>` that itself follows an aggregate select-item, emit a Syntax-coded error:

> Syntax [NNN]: the `by <grain>` clause must come **before** the alias. Write `max(...) by li.order.id as late_flag`, not `max(...) as late_flag by li.order.id`.

Purely textual (no reparse), wired into the same friendly-error dispatch as the other `detect_*` functions, in both grammar backends. Reuse `_AGG_NAMES` / the existing helpers to confirm an aggregate precedes the `as`. (Naming the corrected ordering in the message is the high-leverage part.)

## Pointers
- `trilogy/parsing/v2/errors.py:276` — `detect_by_on_wrapped_aggregate` (pattern to copy)
- `trilogy/parsing/v2/errors.py:357` — `detect_by_on_non_aggregate` (closest sibling)
- `trilogy/parsing/v2/errors.py:~34` — the `Syntax [2xx]` code/message table (add the new code here)
- `_AGG_NAMES` / `_BY_NEAR_RE` in the same file — reuse for "aggregate precedes the `as`"

## Tests
- `select max(x) as f by a` → friendly Syntax error naming the correct `max(x) by a as f` order (not the token-list error)
- `select max(x) by a as f` still parses (no false positive)
- a genuine misplaced top-level `by` (no aggregate/alias before it) still gets its existing error (no regression)
- both grammar backends (Lark + PEST) give the same message
