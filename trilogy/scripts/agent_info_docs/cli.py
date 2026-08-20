CLI_DOC = """# Trilogy CLI and Workspace Operations

Use this drilldown to create, inspect, edit, validate, and execute scripts. For
language syntax use `agent-info query`; for models use `agent-info authoring`.

- `trilogy init [path]` - scaffold trilogy.toml, root/, jobs/, and an example.
- `trilogy run <file|dir> [dialect]` - execute scripts; supports `--param`, `--config`, `--parallelism`, and `--timeout <seconds>`.
- `trilogy run ... --dry-run` (`-n`) - compile every statement and print the SQL it WOULD issue, executing nothing. Use it to check codegen, or to read a persist's DDL, without spending a warehouse query. `--dry-run` means the same thing on `refresh`, `ingest`, `env publish`, `cloud sync`, and `cloud jobs|workspaces push`: report the writes, perform none.
- `trilogy explore <model.preql>` - inspect concepts/imports; imported dimensions arrive outlined - expand one with `--ns <alias>`, or narrow with `--regex`, `--purpose`, or `--show`.
- `trilogy file list [path] --recursive` - list files and model descriptions.
- `trilogy file read <path>` - read a file when exploration is insufficient.
- `trilogy file write <path>` - write stdin or use `--content`, `--from-file`, or `--from-url`.
- `trilogy file write <path> --run` (or `--run-and-delete`) - write, then execute like `trilogy run <path>` in ONE call; forwards `--param k=v` and `--timeout <seconds>`. `--run-and-delete` also removes the file afterwards (probe workflow: write+execute+cleanup in one call). Prefer these over separate write/run/delete calls.
- `--timeout <seconds>` cancels a statement in the warehouse once it overruns. A query that overruns a sane cap is almost always a shape mistake (an unintended fan-out), not a big scan, so the cancellation is the diagnosis: rewrite the query rather than raising the cap. Pick the cap from what a CORRECT query costs on this warehouse, not from patience.
- `trilogy file move|delete|exists ...` - manage workspace paths.
- `trilogy fmt <file|dir>` - format Trilogy scripts.
- `trilogy unit <file|dir>` - validate with mocked datasources.
- `trilogy integration <file|dir>` - validate against real data.
- `trilogy database list` - list physical tables and views.
- `trilogy database describe <table>` - inspect physical columns and types.
- `trilogy source describe <script.py>` - a script datasource's schema, pushdown, and a ready-to-paste `datasource` block.
- `trilogy source preview <script.py>` - run it through the IO contract (`--limit`, `--filter`, `--columns`) and print rows.
- `trilogy source check <script.py>` - confirm it implements the contract.

Typical existing-model workflow:

1. `trilogy file list . --recursive`
2. `trilogy explore <fact.preql>`, then `--ns <alias>` for the dimensions the question needs
3. `trilogy file write answer.preql --run` with the complete body on stdin —
   validates syntax, executes, and shows results in one call; re-issue the
   same call after edits. Use `--run-and-delete` for throwaway probes instead
   of deleting them in a separate call.

Adding a Python script datasource: write the script with `trilogy.io.run`, then
`trilogy source describe <script.py>` and paste the `datasource` block it prints.
Do NOT hand-write the block against a guessed schema.

Focused details: `agent-info report`, `ingest`, `config`, `state`, or `serve`.
"""
