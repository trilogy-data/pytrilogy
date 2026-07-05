# `run` of a file with no executable statements reports success silently (0-statement false success)

**Severity: medium (UX / churn driver).** A `.preql` whose only statements are rowset/`with`/concept definitions and **no final consuming SELECT** runs as `Executing 0 statements… Execution Complete`, **exit 0**, surfaced to an agent as `{"ok": true, "rows": 0}` — a silent false success with no warning that the file is inert. On q64 the agent wrote such a file **3 separate times** and each time believed it had succeeded, then had to infer on its own that nothing ran — multiple write→run→diagnose cycles of pure churn.

## Symptom / minimal repro
```trilogy
import raw.store_sales as ss;
with base_agg as
select ss.item.id, count(ss.line_item) as line_count;
```
`run` →
```
Executing 0 statements...
Execution Complete   |  Statements: 0    (exit 0)
```
The file parsed and registered a rowset definition, but it has no statement that *produces output*, so nothing executes — yet it reports success.

## Root cause
- `trilogy/executor.py:711-730` (`parse_text_generator`): only output-producing statement types are "generatable" (`SelectStatement`, `PersistStatement`, `MultiSelectStatement`, `ShowStatement`, `RawSQLStatement`, `CopyStatement`, `ValidateStatement`, `CreateStatement`, `PublishStatement`, `MockStatement`, `ChartStatement`). A rowset/`with` derivation registers into the environment and yields nothing, so an all-definitions file yields `[]`.
- `trilogy/scripts/single_execution.py:251-281` (`execute_run_mode`): runs the empty list and calls `show_execution_summary(0, …, exception is None → True, 0)`. **No `len(queries) == 0` guard** to warn that the file parsed N definitions but has 0 executable statements.

## Fix direction (do NOT fix here)
Add a zero-executable-statement guard (at `single_execution.py:251` / `executor.py:711`) that, when a file parses successfully but yields **0 generatable statements**, emits a clear warning and a **non-zero / not-`ok:true`** signal — e.g. *"parsed N definitions but found no executable statement (a `with`/rowset/concept file does nothing on its own — add a final `select`)."* That single change removes the false-success churn (~6+ turns on q64).

## Tests
- A file of only `with`/rowset/`auto`/`property` definitions → warning + non-success signal naming "no executable statement; add a final select" (not `{"ok": true, "rows": 0}`).
- A normal file with a final `select` is unaffected.
- A file that legitimately produces 0 rows (a real `select` returning nothing) still reports `ok:true, rows:0` — the guard keys on *0 executable statements*, not 0 result rows.
