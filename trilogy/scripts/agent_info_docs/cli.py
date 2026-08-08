CLI_DOC = """# Trilogy CLI and Workspace Operations

Use this drilldown to create, inspect, edit, validate, and execute scripts. For
language syntax use `agent-info query`; for models use `agent-info authoring`.

- `trilogy init [path]` - scaffold trilogy.toml, root/, jobs/, and an example.
- `trilogy run <file|dir> [dialect]` - execute scripts; supports `--param`, `--config`, and `--parallelism`.
- `trilogy explore <model.preql>` - inspect concepts/imports; narrow with `--regex`, `--purpose`, or `--show`.
- `trilogy file list [path] --recursive` - list files and model descriptions.
- `trilogy file read <path>` - read a file when exploration is insufficient.
- `trilogy file write <path>` - write stdin or use `--content`, `--from-file`, or `--from-url`.
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
2. `trilogy explore <fact.preql> --regex <business term>`
3. `trilogy file write answer.preql` with the complete body on stdin
4. `trilogy run answer.preql`

Adding a Python script datasource: write the script with `trilogy.io.run`, then
`trilogy source describe <script.py>` and paste the `datasource` block it prints.
Do NOT hand-write the block against a guessed schema.

Focused details: `agent-info report`, `ingest`, `config`, `state`, or `serve`.
"""
