# CRITICAL: planner raises a builtin `SyntaxError` carrying internal planner repr for an unresolvable condition

**Severity: HIGH.** A *resolution/planner* dead-end is raised as a Python **`SyntaxError`** whose message is the engine's **internal repr** (`Have {'GroupNode<…>': None} and need local.a > multiply(2, local.b@Grain<…>)`). Two compounding defects:
1. **Wrong exception class** — it's a builtin `SyntaxError`, so the CLI labels it `Syntax error in <file>: …`. The agent then rewrites *syntax* (the query is syntactically fine) and burns turns getting nowhere.
2. **Leaked internals** — `GroupNode<…>`, `local.a`, `multiply(2, local.b@Grain<…>)` are planner/AST reprs that mean nothing to an author; no human-readable description of the actual problem.

Surfaced on q64 (cross-import aggregate comparison) and q23 (rowset output filtered only in WHERE) — both reach the same raise.

## Root cause (one line)
`trilogy/core/processing/concept_strategies_v3.py:280`:
```python
raise SyntaxError(f"Have {cond_dict} and need {str(context.conditions)}")
```
This is the `INCOMPLETE_CONDITION` dead-end (see the comment at `:192`, family at `:192`/`:265`/`:280`): a WHERE/HAVING/membership condition whose operands cannot be co-sourced into one query level. The engine gives up and emits this raw exception instead of a typed, human-readable resolution error.

## Triggering constructs (from the agent runs)
- **q64** — comparing two **cross-imported** aggregates: `sum(cs.ext_list_price) by cs.item.text_id` vs `2 * sum(cr.return_amount) by cr.item.text_id` with `catalog_sales`/`catalog_returns` imported separately. (The membership-in-WHERE variant instead raises a correctly-typed `DisconnectedConceptsException` "cannot merge all concepts" — so the SAME intent yields *two different* error qualities depending on phrasing; this one is the bad path.)
- **q23** — a rowset output filtered only in WHERE with a named-concept threshold RHS (`Have {'RowsetNode<…lifetime_total>': None} and need lifetime_total > local.threshold`).

`cond_dict` is `{<NodeRepr>: None}` (the unsatisfied source) and `context.conditions` is the AST of the condition with grain annotations — i.e., pure internals.

## Why it's critical
The mislabel actively *misdirects* the agent: a "Syntax error" tells it to fix syntax, but the query is syntactically valid — the planner just can't source the condition. In q64 this cost multiple write→run→rewrite cycles; the error gives no clue that the real issue is cross-import disconnection / an unsourceable filter operand.

## Fix direction (do NOT fix here)
1. **Stop raising builtin `SyntaxError`.** Raise a typed resolution exception (e.g. `UnresolvableQueryException` / `DisconnectedConceptsException`, whatever the resolver uses elsewhere) so the CLI categorizes it as a resolution/discovery error, not a syntax error.
2. **Human-readable message, no internal repr.** Describe the actual failure ("the filter/comparison condition references concepts that can't be sourced into one query — they split across grains/imports") and, where detectable, name the idiom that works (reach the second fact through the first's import, e.g. `cr.sales.*`; or add an explicit join/merge). Never print `GroupNode<…>` / `local.a` / `@Grain<…>` to the user.
3. Align with the sibling cross-import path that already raises `DisconnectedConceptsException` with a readable "cannot merge all concepts … N disconnected subgraphs" message — that path is the model for this one.

## Tests
- The q64 cross-import direct-comparison form and the q23 rowset-filtered-in-WHERE form raise a **typed resolution error** (not `SyntaxError`) with a message containing **no** `GroupNode`/`@Grain`/`local._virt` internals.
- The CLI surfaces it as a resolution/discovery error, not `Syntax error in <file>`.
- A genuinely syntactically-invalid file still raises a real syntax error (no regression).
