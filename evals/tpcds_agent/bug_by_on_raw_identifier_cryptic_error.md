# Bug: `by` after a *raw* (non-aggregate) expression → cryptic grammar error (should suggest `group(x) by …`)

**Severity:** low (parse-time, loud) but **high-friction** — it derails the natural "one value per group" idiom and the agent can't decode the error. Surfaced in q06 (the "average each distinct item's price once, not weighted by sales" requirement), enriched run `20260629-030015`.

## Symptom
```trilogy
import raw.store_sales as store_sales;
auto item_price_by_cat <- item.current_price by item.id, item.category;
```
→
```
Parse error: --> 4:46
auto item_price_by_cat <- item.current_price ??? by item.id, item.category;
  = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail,
    COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
```
The grammar-token list gives the agent no clue that the real problem is **`by` needs an aggregate in front of it**. It then spent many turns guessing.

## Exact boundary (reproduced via `generate_sql`)
| form | result |
|---|---|
| `auto x <- item.current_price by item.id` (raw property + `by`) | **CRYPTIC parse error** |
| `auto x <- max(item.current_price) by item.id` (aggregate + `by`) | OK |
| `auto x <- group(item.current_price) by item.id` (`group()` + `by`) | OK |

So `by` is only legal after an aggregate, and **`group(x) by keys` is exactly the idiom the agent wanted** (take each item's price once, at item grain) — but nothing points there.

## Root cause
There is already a friendly detector, `detect_by_on_wrapped_aggregate` (`trilogy/parsing/v2/errors.py:269-318`), but it only fires when `by` follows a **non-aggregate wrapper that *contains* an aggregate** (`coalesce(sum(x),0) by …`). For a **bare expression with no aggregate at all** (`item.current_price by …`) it explicitly bails:
```python
# errors.py:~314 — only fires if an aggregate actually sits inside the wrapper
if _AGG_CALL_RE.search(text, open_paren + 1, i) is None:
    return None
```
…so the raw-identifier case falls through to the raw Lark/PEST grammar error. The `<expr> by <keys>` with **zero aggregates** is the uncovered case.

## Fix (option A — recommended: friendly error)
Add a sibling detector `detect_by_on_non_aggregate` (or widen the existing one) that fires when a `by` clause follows an expression containing **no** aggregate, and emit a Syntax-coded error like:

> Syntax [NNN]: `by <grain>` must follow an aggregate. To get distinct values per group keys, wrap it: `group(item.current_price) by item.id, item.category`. For a reduction, use `sum/avg/max(...) by ...`.

Wire it into the same friendly-error dispatch as the other `detect_*` functions (runs in both grammar backends, purely textual — no reparse), ordered **before** `detect_by_on_wrapped_aggregate` (or share a single entry point that distinguishes "wrapper has agg" vs "no agg anywhere"). Naming `group(x)` in the hint is the highest-leverage part — it's the precise construct for "distinct value per grain."

## Fix (option B — the user's musing, NOT recommended)
Auto-desugar `x by y,z` → `group(x) by y,z`. Rejected: ambiguous/surprising — a bare `expr by keys` reads like it could be a grain *annotation* rather than a group reduction, and silently inserting `group()` hides a real modeling decision (which reducer?). A clear error that *names* `group()` gets the ergonomics without the ambiguity.

## Pointers
- `trilogy/parsing/v2/errors.py:269-318` — `detect_by_on_wrapped_aggregate` (the near-miss; extend or add a sibling)
- `trilogy/parsing/v2/errors.py:319` / `:357` — sibling detectors (`detect_star_argument`, `detect_missing_signature_semicolon`) as the pattern to copy, incl. the `Syntax [2xx]` code + dispatch
- `_AGG_NAMES` / `_AGG_CALL_RE` / `_BY_NEAR_RE` in the same file — reuse for "is there any aggregate in this expr"

## Tests
- `auto x <- item.current_price by item.id` → friendly Syntax error naming `group(...) by ...` (not the token-list error)
- `auto x <- group(item.current_price) by item.id` and `max(...) by item.id` still parse (no false positive)
- `coalesce(sum(x),0) by item.id` still hits the existing wrapped-aggregate detector (no regression)
- both grammar backends (Lark + PEST) give the same message
