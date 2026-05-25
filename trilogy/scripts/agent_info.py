"""Agent info command - outputs AGENTS.md-style usage guide for AI agents."""

import click

from trilogy.ai.prompts import get_trilogy_prompt

AGENT_INFO_OUTPUT = """# Trilogy CLI - AI Agent Usage Guide

## Overview

Trilogy is a semantic ETL and reporting tool providing a SQL-like language with
optimizations. This CLI enables workspace management, script execution, testing,
and data ingestion.

Defaults are managed by a trilogy.toml config file; convention
will be to manage connection info there. CLI flags available for exceptions.

## Quick Start

```bash
# Initialize a new workspace
trilogy init [path]

# Run a script
trilogy run script.preql dialect [connection_args...]

# Run unit tests (mocked datasources)
trilogy unit script.preql

# Run integration tests (real connections)
trilogy integration script.preql dialect [connection_args...]
```

## Commands Reference

### trilogy init [path]

Create a new Trilogy workspace with default configuration and structure.

**Arguments:**
- `path` (optional): Directory to initialize (default: current directory)

**Creates:**
- `trilogy.toml` - Configuration file
- `assets/root/` - Directory for base data models (the `root` namespace)
- `jobs/` - Directory for job scripts
- `hello_world.preql` - Example script

**Example:**
```bash
trilogy init my_project
cd my_project
trilogy unit hello_world.preql
```

---

### trilogy run <input> [dialect] [options] [conn_args...]

Execute a Trilogy script or all scripts in a directory.

**Arguments:**
- `input` (required): Path to .preql file or directory
- `dialect` (optional): Database dialect (duckdb, postgres, snowflake, bigquery, etc.)
- `conn_args` (optional): Connection arguments passed to the database driver

**Options:**
- `--param KEY=VALUE`: Environment parameters (can be repeated)
- `--parallelism N`, `-p N`: Max parallel workers for directory execution
- `--config PATH`: Path to trilogy.toml configuration file
- `--env KEY=VALUE`, `-e KEY=VALUE`: Set env vars (or pass an env file path)
- `--import MODULE[:ALIAS]`: Prepend an `import` to an inline query. Repeatable.
  Accepts bare names (`flight`), filenames (`flight.preql`), or relative paths
  (`root/flight.preql`, converted to dotted form). Append `:ALIAS` to namespace
  the import so the query reaches its concepts as `ALIAS.*` — e.g.
  `--import raw/item:item` matches a file's `import raw.item as item;` and lets
  you write `item.current_price`. Without an alias, concepts import unprefixed.

**Examples:**
```bash
# Run single script with DuckDB
trilogy run query.preql duckdb

# Run with connection string
trilogy run etl.preql postgres "postgresql://user:pass@host/db"

# Run directory with parallelism
trilogy run jobs/ duckdb -p 4

# Run with parameters
trilogy run report.preql duckdb --param date=2024-01-01 --param region=US

# Inline query against a file's concepts — `:alias` namespaces them as alias.*
trilogy run --import flight.preql:flight "select flight.carrier, count(flight.id);" 

# Read the query from stdin (use `-` as input)
echo "select item.id limit 5;" | trilogy run --import raw/item:item - 
```

---

### trilogy explore <path>

The canonical schema-discovery tool. Parses a `.preql` file and prints every
concept (with purpose + datatype), every datasource, and every import that the
file's environment exposes. No dialect or connection needed.

**Imports chain in** — exploring a fact file lists the dimensions it imports
in the same output. After `import raw.store_sales as store_sales;` the
explore output for `raw/store_sales.preql` ALSO contains every concept under
`store_sales.customer.*`, `store_sales.date_dim.*`, `store_sales.item.*`, etc.
You do NOT need to call `explore` separately on each dimension file — one call
on the fact file shows the full queryable surface.

**Trilogy auto-resolves joins.** Trilogy automatically resolves
joins from the model's declared key/property relationships — there is no
manual `JOIN` clause in this language. If `store_sales.date_dim.year` shows
up in explore, you write `select store_sales.date_dim.year, ...;` and the
engine does the join planning. When using an existing model, you can 
typically query all fields safely.

Prefer this over `read_file` on a model file: the same content arrives as a
structured listing, smaller and easier to scan. Default `--show groups`
collapses concepts by namespace so a 300+ concept fact collapses to ~25 group
lines. Datatype is rendered using the `value::trait` authoring syntax
(`string::us_state`, `enum<'TN'>::us_state`) so what you see is what you
write.

**Arguments:**
- `path` (required): Path to a `.preql` file.

**Options:**
- `--show {groups|concepts|datasources|imports|all}`: Section to print
  (default: `groups` — concepts collapsed by namespace prefix). `concepts`
  gives the flat table; `all` adds datasources + imports.
- `--purpose NAME`: Filter concepts by purpose (`key`, `property`, `metric`,
  `constant`, `rowset`). Repeatable: `--purpose key --purpose property`.
- `--grep TEXT`: Case-insensitive substring filter over concept addresses.
  Repeatable: `--grep customer --grep date` matches concepts containing
  either needle.
- `--include-hidden`: Include concepts normally hidden from public view.
- `--include-builtins`: Include internal/builtin concepts (hidden by default).

**Examples:**
```bash
trilogy explore raw/store_sales.preql                    # full schema, grouped
trilogy explore raw/store_sales.preql --grep customer --grep date
trilogy explore raw/store_sales.preql --show concepts --purpose key --purpose property
```

---

### trilogy unit <input> [options]

Run unit tests on Trilogy scripts with mocked datasources. Always uses DuckDB.

**Arguments:**
- `input` (required): Path to .preql file or directory

**Options:**
- `--param KEY=VALUE`: Environment parameters
- `--parallelism N`, `-p N`: Max parallel workers
- `--config PATH`: Path to trilogy.toml

**Examples:**
```bash
# Test single file
trilogy unit test_query.preql

# Test entire directory
trilogy unit tests/ -p 4
```

---

### trilogy integration <input> [dialect] [options] [conn_args...]

Run integration tests on Trilogy scripts with real database connections. Integration tests
run validation that all datasources are configured properly. They do not execute code.

To set up new tables, run first then do integration.

**Arguments:**
- `input` (required): Path to .preql file or directory
- `dialect` (optional): Database dialect
- `conn_args` (optional): Connection arguments

**Options:**
- `--param KEY=VALUE`: Environment parameters
- `--parallelism N`, `-p N`: Max parallel workers
- `--config PATH`: Path to trilogy.toml

**Examples:**
```bash
# Integration test against Postgres
trilogy integration tests/ postgres "postgresql://localhost/testdb"
```

---

### trilogy fmt <input>

Format a Trilogy script file.

**Arguments:**
- `input` (required): Path to .preql file to format

**Example:**
```bash
trilogy fmt messy_script.preql
```

---

### trilogy render <input> [options]

Render a markdown *report* — prose mixed with embedded ```trilogy code blocks —
to a polished PNG or HTML file. `select` statements become tables and `chart`
statements become charts; setup blocks (declarations only) produce no output.

**Arguments:**
- `input` (required): Path to a markdown (`.md`) report file.

**Options:**
- `--to {png|html}`: Output format (default: `png`).
- `--theme {inter|editorial}`: Visual theme — font and colors (default: `inter`).
- `--out PATH`, `-o PATH`: Output path (default: input path with the format's extension).

**Examples:**
```bash
trilogy render quarterly.md --to png
trilogy render quarterly.md --to html --theme editorial
```

Run `trilogy agent-info report` for the full report markdown format reference.
Requires the `report` extra (`pip install pytrilogy[report]`); PNG output also
needs `playwright install chromium`.

---

### trilogy file <subcommand>

CRUD+ operations over pluggable storage backends. Agent-loop friendly: all
read/write/delete/move operations go through the CLI, so an agent with only
CLI access can manage workspace files. Currently supports the local
filesystem; future backends (GCS, S3, remote git models) will plug in via the
same commands.

**Subcommands:**
- `list [path] [--recursive/-r] [--long/-l]`: List entries at PATH (default `.`).
- `read <path>`: Write the file contents to stdout.
- `write <path> [--content/-c TEXT] [--escapes/-e] [--from-file SRC] [--from-url URL] [--no-create] [--quiet]`:
  Create or overwrite the file. If none of `--content`, `--from-file`, or
  `--from-url` is given, reads bytes from stdin. Use `--escapes` with
  `--content` to embed newlines as `\n` in a single-line string when working
  from shells without heredoc support (cmd.exe, some CI runners).
  `--from-url` fetches bytes from `http(s)://` or `file://` URLs — useful
  for pulling a hosted snippet (raw GitHub / gist) into the workspace.
- `delete <path> [--recursive/-r] [--force/-f]`: Delete a file or directory.
- `move <src> <dst>`: Rename or move between paths on the same backend.
- `exists <path>`: Prints `true`/`false`; exits non-zero if the path is missing.

**Examples:**
```bash
# Inline content (cross-shell safe)
trilogy file write scratch.preql --content "import flight; select count(id);"

# Multi-line via --escapes (portable across bash, zsh, PowerShell, cmd.exe)
trilogy file write scratch.preql -e -c "import flight;\nselect count(id);\n"

# Update an existing script from a local file (CI/agent-friendly)
trilogy file write reporting.preql --from-file snippets/reporting.preql

# Pull a hosted snippet (raw GitHub URL)
trilogy file write reporting.preql --from-url https://example.com/reporting.preql

# Inspect and list
trilogy file read reporting.preql
trilogy file list . --recursive --long

# Clean up
trilogy file delete old_assets/ --recursive
```

---

### trilogy database <subcommand> [options]

Inspect the database configured in `trilogy.toml` — the introspection you need
before `ingest` to know what tables exist and what columns they have.

**Subcommands:**
- `database list`: List all tables and views (one `name<TAB>type` per line).
- `database describe <table>`: Show a table's columns (one
  `column<TAB>type<TAB>nullable` per line).

**Options:**
- `--schema NAME`, `-s NAME`: Restrict to a single schema.

**Examples:**
```bash
# Discover the schema before building a model
trilogy database list
trilogy database describe store_sales
```

---

### trilogy ingest <sources> [dialect] [options] [conn_args...]

Bootstrap datasources from existing warehouse tables OR from data files
(local paths and remote URLs). Connects to a database, introspects schemas,
and generates Trilogy datasource definitions.

**Arguments:**
- `sources` (required unless `--all`): Comma-separated list of either table names OR file
  paths/URLs (cannot be mixed in one call). Supported file types: `.csv`,
  `.tsv`, `.parquet`. URL schemes: `https://`, `http://`, `gs://`, `gcs://`,
  `s3://`, `az://`.
- `dialect` (optional): Database dialect. File ingest forces `duckdb`.
- `conn_args` (optional): Connection arguments

**Options:**
- `--output PATH`, `-o PATH`: Output directory for generated files
- `--schema NAME`, `-s NAME`: Schema/database to ingest from (table mode only)
- `--config PATH`: Path to trilogy.toml
- `--fks SPEC`: Foreign key relationships (format: table.col:ref_table.col)
- `--name NAME`: Override the generated datasource name (single source only)
- `--all`: Ingest every table in the database (table mode; omit `sources`)

**Examples:**
```bash
# Ingest tables from DuckDB
trilogy ingest "users,orders,products" duckdb "path/to/db.duckdb"

# Ingest every table in the configured database in one step
trilogy ingest --all

# Ingest with schema and output directory
trilogy ingest "customers" postgres -s public -o raw/ "postgresql://localhost/db"

# Ingest with foreign key relationships
trilogy ingest "orders,customers" duckdb --fks "orders.customer_id:customers.id"

# Ingest a local CSV (DuckDB is auto-selected; dialect arg optional)
trilogy ingest ./data/orders.csv

# Ingest a remote parquet over HTTPS
trilogy ingest https://example.com/data/events.parquet --name events

# Ingest from a public GCS bucket
trilogy ingest gs://my-bucket/sales.parquet -o raw/
```

---

### trilogy public <subcommand> [options]

Browse and pull Trilogy models published in
[trilogy-public-models](https://github.com/trilogy-data/trilogy-public-models).

**Subcommands:**
- `list`: Print available models from the studio index.
- `fetch <model>`: Download a model's source files into a local directory.

**`trilogy public list` options:**
- `--engine NAME`, `-e NAME`: Filter by engine (e.g. `duckdb`, `bigquery`).
- `--tag NAME`, `-t NAME`: Filter by tag.

**`trilogy public fetch <model> [<path>]` arguments/options:**
- `<path>`: Optional target directory (default `./<model>`).
- `--no-examples`: Skip example scripts/dashboards.
- `--force`, `-f`: Overwrite an existing non-empty target directory.

Writes all components, a README.md from the model description, and a
`trilogy.toml` with the engine dialect and any setup SQL preconfigured, so the
directory is immediately usable with `trilogy refresh` / `trilogy serve`.

**Example:**
```bash
trilogy public list --engine duckdb
trilogy public fetch bike_data ./bike-demo
cd bike-demo && trilogy refresh . && trilogy serve .
```

---

### trilogy serve <directory> [engine] [options]

Start a FastAPI server to expose Trilogy models from a directory.
Requires `pytrilogy[serve]` extras.

**Arguments:**
- `directory` (required): Directory containing model files
- `engine` (optional): Engine type (default: generic)

**Options:**
- `--port N`, `-p N`: Port number (default: 8100)
- `--host HOST`, `-h HOST`: Host to bind (default: 0.0.0.0)
- `--timeout N`, `-t N`: Shutdown after N seconds

**Endpoints exposed:**
- `/` - Server info
- `/index.json` - List of available models
- `/models/<name>.json` - Specific model details
- `/files/<name>` - Raw .preql/.sql file content

**Example:**
```bash
trilogy serve ./models/ duckdb --port 8080
```

---

### trilogy agent <command> [options]

Pass off a multi-step orchestration task to an AI agent. (Not yet implemented)

**Arguments:**
- `command` (required): Natural language command

**Options:**
- `--context PATH`, `-c PATH`: Additional context files
- `--model NAME`, `-m NAME`: AI model to use
- `--interactive`, `-i`: Interactive mode with feedback

---

## Authoring Datasources

### Root Datasources

Prefixing a datasource declaration with the `root` keyword marks it as a source-of-truth that
Trilogy does not manage or refresh. Root datasources are external inputs — warehouse tables,
files, or scripts that are populated outside of Trilogy.

```trilogy
root datasource raw_rides (
    ride_id,
    rider_id,
    distance_miles,
    duration_minutes
)
grain (ride_id)
address source_schema.raw_rides;
```

**Key behaviors:**
- Root datasources are **not eligible for refresh** — they are never marked stale and will not
  be rebuilt by `trilogy run` or the refresh system.
- Derived (non-root) datasources that depend on root datasources will be checked for staleness
  relative to root watermarks when `freshness_by` is configured.
- The state store will still query root datasources for watermark values when a downstream
  datasource declares `freshness_by` pointing to a concept that lives on the root — no
  configuration on the root itself is needed or allowed.

**Convention:** place root datasource definitions in `assets/root/` so they can be imported
via `import root;` in downstream scripts. This is convention only — the `root` keyword is what
matters, not the file location.

```trilogy
# in a job or derived model:
import root;

auto total_rides <- COUNT(ride_id);
select total_rides;
```

---

### File-Based Datasources (Parquet, CSV)

Datasources declared with a `file` clause can be **read from and written to**. The file
extension determines how the file is handled — no extra configuration is needed.

| Extension | Behaviour |
|-----------|-----------|
| `.parquet` | `read_parquet(...)` / write parquet |
| `.csv` | `read_csv(...)` / write csv |
| `.tsv` | `read_csv(..., delim='\t')` / write tsv |
| `.py` | `uv_run(...)` — Arrow IPC read-only (see below) |

**Reading** — declare the datasource and query it like any other source:

```trilogy
key ride_id int;
property ride_id.distance_miles float;

root datasource raw_rides (
    ride_id,
    distance_miles
)
grain (ride_id)
file `./data/rides.parquet`;
```

Glob patterns are supported for multi-file reads:

```trilogy
file `./data/rides_*.parquet`;
```

**Writing** — use `state unpublished` to mark the datasource as a write target, then
populate it with `overwrite` or `persist`:

```trilogy
auto total_distance <- sum(distance_miles);

datasource ride_summary (
    total_distance
)
grain ()
file `./output/ride_summary.parquet`
state unpublished;

overwrite ride_summary;
```

`overwrite` replaces the file contents. `persist` appends. Both work with local paths and
cloud storage URIs (e.g. `gcs://bucket/path/out.parquet`) when the appropriate DuckDB
extension is enabled.

---

### Complete and Partial Datasources

By default a datasource is "complete" — it represents the full dataset for its grain. The
`partial` keyword declares that a datasource only covers a subset of rows, identified by a
`complete where` clause. This enables Trilogy to union multiple partial datasources together
when it needs the full population.

**Complete datasource (default):**
```trilogy
datasource orders (
    order_id,
    status,
    region
)
grain (order_id)
address all_orders;
```

**Partial datasource:**
```trilogy
partial datasource orders_us (
    order_id,
    status,
    region
)
grain (order_id)
address orders_us_table
complete where region = 'US';

partial datasource orders_eu (
    order_id,
    status,
    region
)
grain (order_id)
address orders_eu_table
complete where region = 'EU';
```

When Trilogy needs `order_id` it will union `orders_us` and `orders_eu` automatically. Partial
datasources can also carry `incremental by` for time-partitioned appends:

```trilogy
partial datasource orders_us (
    order_id,
    status,
    region,
    created_at
)
grain (order_id)
address orders_us_table
complete where region = 'US'
incremental by created_at;
```

The `root partial` combination is also valid for external partitioned sources (e.g. one
Arrow/file source per partition):

```trilogy
root partial datasource raw_us (
    id,
    value,
    region
)
grain (id)
complete where region = 'US'
file `./us_data.py`;
```

---

### Python Script Datasources (Arrow)

Trilogy supports using a Python script as a datasource. The script must write an Apache Arrow
IPC stream to `stdout`. This is powered by `uv run` under the hood, so the script can declare
its own dependencies via inline script metadata.

**Requirements:**
- DuckDB executor with `enable_python_datasources=True` in `DuckDBConfig`
- Script writes `pyarrow.Table` to `sys.stdout.buffer` using `pa.ipc.new_stream`
- Script is referenced with a `file` clause using a backtick path

**Datasource declaration (`.preql`):**
```trilogy
key row_index int;
property row_index.value int;

datasource my_source(
    index: row_index,
    value: value
)
grain (row_index)
file `./my_script.py`;
```

**Script template (`my_script.py`):**
```python
#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow"]
# ///

import sys
import pyarrow as pa

def emit(table: pa.Table) -> None:
    with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
        writer.write_table(table)

if __name__ == "__main__":
    table = pa.table({"index": [1, 2, 3], "value": [10, 20, 30]})
    emit(table)
```

**Enabling in Python:**
```python
from trilogy import Dialects, Environment
from trilogy.execution import DuckDBConfig

executor = Dialects.DUCK_DB.default_executor(
    environment=Environment(working_path=...),
    conf=DuckDBConfig(enable_python_datasources=True),
)
```

**Enabling via `trilogy.toml`:**
```toml
[engine]
dialect = "duckdb"

[engine.config]
enable_python_datasources = true
```

The column names in the Arrow table must match the column names declared in the datasource
mapping. The script runs in an isolated `uv` environment, so it can have dependencies that
differ from the main project.

---

## Configuration File (trilogy.toml)

```toml
[engine]
# Default dialect for execution
dialect = "duckdb"

# Max parallelism for multi-script execution
parallelism = 3

[setup]
# Startup scripts to run before execution
trilogy = ["setup.preql"]
sql = ["init.sql"]

[agent]
# Default LLM provider for AI features
# Valid values: openai, anthropic, google, openrouter
provider = "anthropic"

# Default model for the chosen provider
model = "claude-sonnet-4-6"
```

The `[agent]` section configures the default LLM provider and model used by `trilogy agent`
and any AI-assisted features. API keys are read from environment variables:
- `OPENAI_API_KEY` for OpenAI
- `ANTHROPIC_API_KEY` for Anthropic
- `GOOGLE_API_KEY` for Google
- `OPENROUTER_API_KEY` for OpenRouter

OpenRouter gives access to models from many providers through a single API and key.

## Supported Dialects

- `duckdb` / `duck_db` - DuckDB (default for unit tests)
- `sqlite` / `sqlite3` - SQLite
- `postgres` / `postgresql` - PostgreSQL
- `bigquery` - Google BigQuery
- `snowflake` - Snowflake
- `redshift` - Amazon Redshift
- `trino` - Trino/Presto
- `sql_server` - Microsoft SQL Server

## File Types

- `.preql` - Trilogy script files (main language)
- `.sql` - Raw SQL files (for setup scripts)
- `trilogy.toml` - Configuration file

## Common Workflows

### 1. Setting up a new project
```bash
trilogy init my_analytics
cd my_analytics
# Configure trilogy.toml with your dialect and connection
trilogy unit hello_world.preql
```

### 2. Ingesting existing tables
```bash
trilogy ingest "fact_sales,dim_customers,dim_products" postgres \\
    -s analytics -o assets/root/ "postgresql://localhost/warehouse"
```

### 3. Running ETL jobs
```bash
trilogy run jobs/ postgres -p 4 "postgresql://localhost/warehouse"
```

### 4. Testing before deployment
```bash
# Unit tests (fast, no connection needed)
trilogy unit .

# Integration tests (real connection)
trilogy integration . postgres "postgresql://localhost/testdb"
```

## Debug Mode

Add `--debug` flag to any command for verbose output:
```bash
trilogy --debug run query.preql duckdb
```
"""


REPORT_FORMAT_DOC = """# Trilogy Report Format - AI Agent Reference

## Overview

A Trilogy *report* is a standard markdown file with embedded Trilogy. Author it
as normal markdown, then run `trilogy render <file.md>` to produce a polished
PNG or HTML artifact. Fenced ```trilogy code blocks are executed and replaced
by their output:

- a `select` statement -> a formatted table
- a `chart` statement  -> a rendered chart
- declarations only (key / property / datasource / import / ...) -> no output

All other markdown (headings, prose, lists, links) renders normally. This lets
an agent author one markdown file mixing narrative with live query results and
hand back a finished report.

## Rendering

```bash
trilogy render report.md                     # -> report.png (default)
trilogy render report.md --to html           # -> report.html (interactive charts)
trilogy render report.md --theme editorial   # font + color theme
trilogy render report.md -o out/q3.png       # explicit output path
```

- `--to`: `png` (default) or `html`.
- `--theme`: `inter` (default) or `editorial`.

## Trilogy code blocks

Tag a fenced block `trilogy` to have it executed:

```trilogy
select region, revenue order by revenue desc;
```

Every trilogy block in the document runs against ONE shared executor, in
document order. Declarations in an earlier block are visible to later blocks, so
a report typically opens with a setup block that defines the data model:

```trilogy
key region string;
property region.revenue float;
property region.units int;

datasource sales (r: region, rev: revenue, u: units)
  grain (region)
  query '''
  select 'North' as r, 120000.0 as rev, 340 as u
  union all select 'South', 98000.0, 280
  ''';
```

Later blocks query the model:

```trilogy
select region, revenue, units order by revenue desc;
```

A block may hold multiple statements; each result-producing statement renders
in order. If a statement errors, the error is shown inline and the rest of the
report still renders.

## Charts

A `chart` statement renders as a chart in place:

```trilogy
chart layer bar ( x_axis <- region, y_axis <- revenue );
```

Full chart-statement syntax (layers, encodings, placements) is in the main
syntax reference — run `trilogy agent-info`.

## Side-by-side layout

By default each block spans the full content width. To place outputs in a row,
wrap blocks in a `:::row` container (a pandoc-style fenced div). Open with
`:::row` on its own line and close with `:::` on its own line; each block
inside becomes one equal-width column, and charts are sized to fit:

:::row
```trilogy
chart layer bar ( x_axis <- region, y_axis <- revenue );
```
```trilogy
chart layer bar ( x_axis <- region, y_axis <- units );
```
:::

## Complete example

A full report file (`quarterly.md`), shown indented:

    # Quarterly Sales

    ```trilogy
    key region string;
    property region.revenue float;
    datasource sales (r: region, rev: revenue)
      grain (region)
      query '''
      select 'North' as r, 120000.0 as rev
      union all select 'South', 98000.0
      ''';
    ```

    ## Revenue by region

    ```trilogy
    select region, revenue order by revenue desc;
    ```

    ## Visualized

    :::row
    ```trilogy
    chart layer bar ( x_axis <- region, y_axis <- revenue );
    ```
    ```trilogy
    chart layer line ( x_axis <- region, y_axis <- revenue );
    ```
    :::

Render: `trilogy render quarterly.md --to png`

## Notes

- Reports need the `report` extra: `pip install pytrilogy[report]`. PNG output
  additionally needs a browser: `playwright install chromium`.
- Reports execute on DuckDB. Make a report self-contained by declaring
  datasources with inline `query '''...'''` blocks or `file` clauses.
- Non-trilogy fenced blocks (python, sql, ...) are passed through unchanged.
"""


def get_agent_info_output() -> str:
    """Build the complete agent info output with CLI docs and syntax reference."""
    syntax_section = get_trilogy_prompt(
        intro="## Trilogy Language Syntax\n\nTrilogy is a SQL-inspired language with a built-in semantic layer. Use the following syntax reference when writing .preql files.",
    )
    return AGENT_INFO_OUTPUT + "\n" + syntax_section


@click.group(invoke_without_command=True)
@click.pass_context
def agent_info(ctx: click.Context) -> None:
    """Output comprehensive CLI documentation for AI agents.

    With no subcommand, prints the full AGENTS.md-style guide. Subcommands
    print focused references — e.g. `trilogy agent-info report`.
    """
    if ctx.invoked_subcommand is None:
        print(get_agent_info_output())


@agent_info.command("report")
def agent_info_report() -> None:
    """Print the Trilogy markdown report format reference."""
    print(REPORT_FORMAT_DOC)
