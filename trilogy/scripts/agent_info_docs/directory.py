AGENT_INFO_DIRECTORY = """# Trilogy Agent Info Directory

Choose the bucket matching the task, then immediately call its drilldown. Do
not guess syntax here; detailed guidance lives one level down.

## Query authoring

- `trilogy agent-info query` - language, exploration, clause order, functions, and query workflow.
- `trilogy agent-info syntax` - list focused, copy-pasteable syntax examples.
- `trilogy agent-info syntax example <name>` - print one complete example.

## Model, script, and datasource authoring

- `trilogy agent-info authoring` - model and datasource authoring, including Python scripts.
- `trilogy agent-info datasources` - mappings, files, partial sources, and Python/Arrow sources.
  Writing a Python source? Wrap the function in `trilogy.io.run`, then generate the
  datasource block with `trilogy source describe <script.py>`.
- `trilogy agent-info ingest` - bootstrap models from tables, files, or cloud objects.
- `trilogy agent-info config` - trilogy.toml, engines, credentials, and feature flags.

## Creating, running, and managing projects and scripts

- `trilogy agent-info cli` - init, run, explore, files, formatting, tests, and database inspection.
- `trilogy agent-info report` - render Markdown reports to HTML or PNG.
- `trilogy agent-info state` - persisted execution state and run reports.
- `trilogy agent-info serve` - publish, fetch, and serve Trilogy models.
"""
