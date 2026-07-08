# q38 — misleading parse error: TVF `-> (...)` missing-`;` diagnostic suppressed by a second downstream error

**Run:** `evals/tpcds_agent/results/20260708-135136_enriched` — q38 PASS, but **684,058 tokens** burned.
**Verdict:** category (b) — a **misleading error for valid-looking Trilogy** (framework guidance defect). The construct IS supported; the diagnostic just fails to name the real cause when a *second* independent error coexists in the file.

## Symptom (what the agent hit)
q38 = 3-way INTERSECT (customers buying in store AND catalog AND web in 2000). The agent correctly reached for the `intersect(...) -> (last_name, first_name, sale_date)` TVF idiom. Its file had **two** independent authoring mistakes at once:
1. missing `;` after the `-> (...)` output signature, and
2. `count(*)` in the consuming select (`*` is not a valid Trilogy arg → Syntax [223]).

The parser reported only the first, and reported it **misleadingly**:
```
Parse error:  --> 27:39
) -> (last_name, first_name, sale_date)
                                      ^---
  = expected metadata, PURPOSE, or data_type
```
The caret points **inside the output tuple** and the wording ("expected … data_type") reads as "your output columns need type annotations." The agent believed it and churned through ~6 rewrites (conversation msgs 30–42): `-> (… string?)`, `-> (… string)`, `-> (… int)`, re-reading the `union`/`intersect` syntax examples — all dead ends. It only escaped by writing a **minimal** test whose consuming select was valid (`select test.last_name`), which produced the *correct* clear error `Syntax [222]: Missing ';'`, after which the fix was obvious (add `;`, and use `count(all_three.sale_date)`).

## Minimal repro (pest, no model needed)
```
with all_three as intersect(
  (where x.a is not null select x.b as last_name, x.c as first_name, x.d as sale_date)
) -> (last_name, first_name, sale_date)     <-- no `;`
<consuming select>
```

## Trigger matrix (identical TVF, only the consuming select changes)
| consuming statement after unterminated `-> (...)` | diagnosis |
|---|---|
| `select count(*) as cnt` (has a 2nd error) | **`--> 3:39 expected metadata, PURPOSE, or data_type`** (misleading, points into tuple) |
| `select all_three.last_name` (valid) | **`Syntax [222]: Missing ';'`** (correct, actionable) |
| `select count(all_three.sale_date) as cnt` (valid) | **`Syntax [222]: Missing ';'`** (correct) |
| same TVF **with** trailing `;` + valid select | **PARSES OK** |

So the exact same missing-`;` yields the friendly [222] *unless* a second error lives downstream — then it silently degrades to a raw pest error aimed at the wrong token.

## Root cause (file:line)
`trilogy/parsing/v2/pest_backend.py:333-335`:
```python
sig_pos = detect_missing_signature_semicolon(text, pos)
if sig_pos is not None and _pest_parses(text[:sig_pos] + ";" + text[sig_pos:]):
    return create_syntax_error(222, sig_pos, text)
```
`detect_missing_signature_semicolon` (`trilogy/parsing/v2/errors.py:482`) **correctly** locates the missing `;` (returns `sig_pos=155` here). But the confirmation gate reparses the **entire remaining file** with `;` inserted. Because `count(*)` downstream is itself invalid, that whole-file reparse returns **False**, so [222] is suppressed and diagnosis falls through to the generic raw-pest error.

Proof (reproduced):
- whole-file gate `_pest_parses(text[:sig_pos] + ";" + text[sig_pos:])` → **False** (count(*) defeats it)
- local-only gate `_pest_parses(text[:sig_pos] + ";")` → **True** (the TVF definition alone is well-formed)

The misleading wording/position comes from grammar `trilogy/scripts/dependency/src/trilogy.pest:495`:
`tvf_output_item = { … IDENTIFIER ~ (PURPOSE? ~ data_type ~ concept_nullable_modifier?)? ~ metadata? }` — output items *may* carry a type, so with no `;` pest's longest-match failure lands on the tuple's `)` and blames the optional `data_type`/`metadata`/`PURPOSE`.

The lark backend has the analogous whole-file gate at `trilogy/parsing/v2/lark_backend.py:192-196`.

## Fix direction (NOT applied)
Confirm [222] against the **local** construct only — `_pest_parses(text[:sig_pos] + ";")` — rather than the whole file. Then a coexisting downstream error (count(*)) no longer masks the missing-`;` diagnosis; each error surfaces with its own accurate message. (Guard both pest and lark backends.)

## Canonical confirmed
Final `query38.preql` (uses `intersect(...) -> (...);` + `count(all_three.sale_date)`) builds and returns **107**, matching the TPC-DS q38 reference; report status = pass.
