---
query: q58
tokens: 866218
status: PASS (heavy churn)
run: evals/tpcds_agent/results/20260708-135136_enriched
signature: "Unexpected error" (x3) — Unable to import
verdict: FRAMEWORK BUG (error-labeling), not a planner/SQL crash
kind: read-only investigation, NOT fixed
---

# q58 — `ImportError` (bad import path) mislabeled as "Unexpected error"

## Symptom

The q58 agent hit "Unexpected error" three times (866k tokens, eventual PASS). All
three are the SAME class: a bad import PATH raising an `ImportError` that the CLI
surfaces with the internal-crash prefix `Unexpected error in <file>: ...`, which reads
to the agent as a framework crash to retry rather than a fixable authoring mistake.
The agent re-issued the identical bad import once (messages 18→23, test_week.preql)
before pivoting, then hit it a third time on query58.preql itself.

Exact messages from `agent_log.q58.conversation.txt`:
- msg 19 & 23: `Unexpected error in raw\test_week.preql: Unable to import 'raw\raw\all_sales.preql': [Errno 2] No such file or directory: 'raw\\raw\\all_sales.preql'. Did you mean: all_sales?`
- msg 35: `Unexpected error in query58.preql: Unable to import '.\all_sales.preql': [Errno 2] No such file or directory: '.\\all_sales.preql'. Did you mean: raw.all_sales?`

The import layer already builds a clean, actionable message WITH a `Did you mean: …?`
suggestion — but that helpful `ImportError` is then relabeled "Unexpected error",
defeating its own guidance. This is the import-path cousin of the already-filed
`bug_parse_error_mislabeled_unexpected.md` (same handler, different exception type).

## Minimal repro (of the `Unexpected error`)

The failure is at PARSE/import time (before generate_sql), so the minimal repro
targets the two engine functions directly.

1. Raise the real `ImportError` (identical to the log):
```python
from trilogy.parsing.v2.import_service import _read_import_text
from trilogy.core.models.environment import Environment
env = Environment(working_path=str(ws))          # ws = q58 workspace
_read_import_text(str(ws/'raw'/'raw'/'all_sales.preql'), env)
# -> ImportError: Unable to import '...raw\raw\all_sales.preql':
#    [Errno 2] No such file or directory: '...'. Did you mean: raw.all_sales?
```

2. Feed that `ImportError` to the CLI handler — it is mislabeled:
```python
from trilogy.scripts.common import handle_execution_exception
handle_execution_exception(ImportError("Unable to import '...'. Did you mean: all_sales?"),
                           source="raw/test_week.preql")
# prints:  Unexpected error in raw/test_week.preql: Unable to import '...'
```

## Trigger matrix (`handle_execution_exception`, source='f.preql')

| Exception fed in                     | Label emitted        | Correct? |
|--------------------------------------|----------------------|----------|
| `ImportError` (bad import path)      | **Unexpected error** | NO — it's a fixable authoring mistake with a hint |
| `InvalidSyntaxException` / `SyntaxError` | Syntax error     | yes |
| `UndefinedConceptException`          | Syntax error         | yes |
| `Disconnected/UnresolvableQuery`     | Resolution error     | yes |
| `FunctionArgumentException`          | Type error           | yes |
| `RecursionError`                     | Resolution error (bug)| yes |
| generic `ValueError`                 | Unexpected error     | yes (genuinely unexpected) |

`ImportError` is the only user-fixable, hint-carrying error type that falls through to
the internal-crash bucket.

## Root cause (file:line)

Two-part, both in the CLI error-labeling path — the ENGINE raises correctly; only the
LABEL is wrong:

- `trilogy/parsing/v2/import_service.py:92` — `_read_import_text` catches the `OSError`
  from a missing `.preql` file and raises a clean, intentional
  `ImportError("Unable to import '{address}': {exc}.{hint}")` with a `Did you mean` hint
  (`import_service.py:88-92`, hint built at line 91). (A sibling path at
  `import_service.py:212-214` wraps nested parse failures as `ImportError` too.)
- `trilogy/scripts/common.py:679-706` — `handle_execution_exception` labels
  `UndefinedConceptException`, `SyntaxError/InvalidSyntaxException`,
  `Disconnected/UnresolvableQueryException`, `FunctionArgumentException`,
  `ConfigurationException`, and `RecursionError` as fixable, but has NO `ImportError`
  branch, so it falls to the `else` at **line 706** → `print_error(f"Unexpected error{location}: {e}")`.

Fix location (NOT applied): add an `isinstance(e, ImportError)` branch in
`handle_execution_exception` (common.py:679-706) that labels it e.g. `Import error{location}: {e}`
(the hint is already in the message). Mirrors the existing
`bug_parse_error_mislabeled_unexpected.md` fix guidance.

## Canonical / correctness

Not a planner or generated-SQL defect — this is purely an error string label. The
agent's final `query58.preql` in the workspace builds and executes cleanly via the
harness (generate_sql → 5 rows, matching the PASS verdict); the 866k churn came from
the agent treating the mislabeled import errors as internal crashes.
