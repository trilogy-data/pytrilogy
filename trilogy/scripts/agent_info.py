"""Agent info command - outputs AGENTS.md-style usage guide for AI agents."""

import click

from trilogy.ai.prompts import get_trilogy_prompt
from trilogy.ai.syntax_examples import example_index, render_example

AGENT_INFO_OUTPUT = r"""# Agent Usage Guide

## Overview

Trilogy is a data access and transform language with
SQL-like syntax. This CLI enables workspace management, script execution, testing,
and data ingestion.

Trilogy operates on an abstract semantic model, not tables. 

## Commands Reference

### trilogy init [path]

Create a new workspace (default: current dir): `trilogy init [path]`. Scaffolds
`trilogy.toml`, `assets/root/` (the `root` namespace), `jobs/`, and a
`hello_world.preql` example.

---

### trilogy run <input> [dialect] [options] [conn_args...]

Execute a Trilogy script or all scripts in a directory.

**Arguments:**
- `input` (required): Path to .preql file or directory
- `dialect` (optional): Database dialect (duckdb, postgres, snowflake, bigquery, etc.)
- `conn_args` (optional): Connection arguments passed to the database driver

**Options:**
- `--param KEY=VALUE`: Script arameters (can be repeated)
- `--parallelism N`, `-p N`: Max parallel workers when executing
- `--config PATH`: Path to trilogy.toml configuration file
- `--env KEY=VALUE`, `-e KEY=VALUE`: Set env vars (or pass an env file path)
- `--import MODULE[:ALIAS]`: Prepend an `import` to an inline query. Repeatable.
  Use the SAME dotted form as in-file imports — `--import raw.item:item` becomes
  `import raw.item as item;`

**Examples:**
```bash
# Run a script with DuckDB
trilogy run query.preql duckdb

# Run with parameters
trilogy run report.preql duckdb --param date=2024-01-01 --param region=US

# Inline query against a file's concepts — dotted form, `:alias` namespaces them
trilogy run --import flight:flight "select flight.carrier, count(flight.id);"

# Read the query from stdin (use `-` as input)
echo "select item.id limit 5;" | trilogy run --import raw.item:item -
```
(Connection string and directory `-p N` parallelism work too — see Options.)

---

### trilogy explore <path>

Canonical schema-discovery tool. Parses a `.preql` file and prints
structured information.

**Trilogy auto-resolves joins.** 
An explore call
will show all imported join models that are accessible as well; joins
are not required to access those concepts.

Prefer explore over reading the raw model file (`trilogy file read`); 

**Arguments:**
- `path` (required): Path to a `.preql` file.

**Options:**
- `--show {groups|concepts|datasources|imports|all}`: Section to print
  (default: `groups` - concepts grouped by namespace). `concepts`
  gives the flat table; `all` adds datasources + imports.
- `--purpose NAME`: Filter concepts by purpose (`key`, `property`, `metric`,
  `constant`, `rowset`). Repeatable: `--purpose key --purpose property`.
- `--regex PATTERN`: Case-insensitive Python regex (re.search) over targets
  addresses. Repeatable - a match is kept if ANY supplied pattern matches. 
  metacharacters work (`date\.(year|week_seq)`). Uses the Python `re` flavor
- `--include-hidden`: Include concepts normally hidden from public view.
- `--include-builtins`: Include internal/builtin concepts (hidden by default).
- `--expand-roles`: Render each role of a shared dimension separately instead of
  collapsing them into one comma-separated key (see the JSON note below).

**Examples:**
```bash
trilogy explore raw/my_fact.preql                    # full schema, grouped
trilogy explore raw/my_fact.preql --regex customer --regex date
trilogy explore raw/my_fact.preql --regex 'date\.(year|week_seq)'
trilogy explore raw/my_fact.preql --show concepts --purpose key --purpose property
```

**Reading the JSON output: shared (conformed) dimensions.** A fact often binds the same
dimension type under several distinct roles (a date used as `date`, `return_date`,
`ship_customer.first_sales_date`, ...). These role namespaces share one identical schema, so
the JSON lists them **together in a single key, comma-separated, with the schema shown once**:

```json
"namespaced": {
  "household_demographics, customer.household_demographics": {
    "roles": {
      "household_demographics": {"direct": true},
      "customer.household_demographics": {"via": "customer"}
    },
    "concepts": [
      { "keys": ["household_demographics.demo_sk bigint;"] },
      { "grain": "household_demographics.demo_sk", "properties": ["dep_count bigint;"] }
    ]
  }
}
```

Every namespace in the comma-separated key exposes every listed concept. The declarations use
the first namespace as the example prefix; spell another role by replacing the prefix — e.g.
`customer.household_demographics.dep_count`.

**The listed names are NOT interchangeable.** Sharing a schema does not mean sharing meaning:
each name is a distinct semantic binding, and swapping one for another changes *which
dimension row a given fact row resolves to*, not just the spelling. Filtering
`household_demographics.dep_count` and `customer.household_demographics.dep_count` can return
different results. Pick the role whose meaning matches the question, using the `roles` map:

- `"direct": true` — the explored file's own binding. On a fact model this describes the
  fact event itself (e.g. the household demographics recorded on the sale).
- `"via": "X"` — reached through the imported binding `X`; it describes X's own relationship
  to the dimension (e.g. `customer.household_demographics` is the customer's household link,
  which need not match what was recorded on any particular sale).
- `"description"` — a note from the model author; when present it is authoritative.

When the question is about the fact/event itself, prefer the direct role; use a `via` role
only when the question asks about the related entity's attribute. A combined entry with no
`roles` map is a group of same-level aliases whose names alone distinguish them (e.g.
`sold_date` vs `ship_date`) — the same non-interchangeability still applies. A key with no
comma is a single namespace as usual. (Pass `--expand-roles` for the older
one-namespace-per-entry dump.)

---

### trilogy unit <input> [options]

Run unit tests with mocked datasources (no connection needed):
`trilogy unit <file|dir>`. Options: `--param KEY=VALUE`, `--parallelism/-p N`,
`--config PATH`.

---

### trilogy integration <input> [dialect] [conn_args...]

Validate that every datasource is correctly configured by sampling
real db data.
`trilogy integration <file|dir> <dialect> <conn>`. Same
`--param`/`-p`/`--config` options as `unit`.

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

### trilogy render

Render a report to PNG/HTML.
Run `trilogy agent-info report` for the command flags and the report format
reference. Use this when a user asks you to produce a report or readout
as a file.

---

### trilogy ingest

Bootstrap a Trilogy model from existing warehouse tables, files, or cloud
objects. 

---

### trilogy file <subcommand>

CRUD+ operations over filesystems. Local filesystem only.

**Subcommands:**
- `list [path] [--recursive/-r] [--long/-l]`: List entries at PATH (default `.`).
- `read <path>`: Read the file contents to stdout.
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

# Inspect and list
trilogy file read reporting.preql
trilogy file list . --recursive --long
```
(`--from-file`/`--from-url` write from a local path or URL; `delete`/`move`/`exists` round out the CRUD set.)

---

### trilogy database <subcommand> [options]

Direct database object inspection. Use in bootstrapping
and ingest. When working with a pre-curated model consume
that directly. 

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
trilogy database describe my_fact
```

---

## Authoring Datasources

When you need to author or edit a model
call `trilogy agent-info datasources` for 
the full reference.

---

## Configuration File (trilogy.toml)

Trilogy defaults are stored in this file. Run `trilogy agent-info config` 
for the full schema and API-key conventions. before making edits.

## File Types

- `.preql` - Trilogy script files (main language)
- `.sql` - Raw SQL files (for setup scripts)
- `trilogy.toml` - Configuration file

## Common Workflows

- **New project**: `trilogy init` → configure `trilogy.toml` dialect/connection → author models.
- **Bootstrap a model from existing data**: `trilogy ingest` (see `trilogy agent-info ingest`).
- **Query an existing model**: `trilogy explore <model>.preql` to discover concepts → write a `.preql` `select` → `trilogy run <file> <dialect>`.
- **ETL / directory runs**: `trilogy run jobs/ <dialect> -p N`.
- **Test before deploy**: `trilogy unit .` (mocked) and `trilogy integration . <dialect> <conn>` (real connection).

## Output Format

Commands emit human formatting (rich if installed, plain text otherwise) by default.
Use the --format flag to control; agentic access will default to --format json.
. Pass `--format rich` for explicit human formatting.

## Debug Mode

Add `--debug` flag to any command for verbose output:
```bash
trilogy --debug run query.preql duckdb
```

## Extended References (on demand)

Reference sections live behind `trilogy agent-info <topic>` subcommands. 
Call for more info.

- `trilogy agent-info report` — `trilogy render` command flags AND the
  markdown report format. Use to produce a rendered report or readout as a file.
- `trilogy agent-info datasources` — all datasource authoring forms.
  For authoring or editing datasource.
- `trilogy agent-info ingest` — `trilogy ingest` full reference.
  For bootstrapping a model from scratch.
- `trilogy agent-info config` — `trilogy.toml` schema (`[engine]`, `[setup]`,
  `[agent]`) and en-vars Only needed when
  editing the workspace config.
- `trilogy agent-info serve` — `trilogy public list/fetch` (browse and pull
  from trilogy-public-models) and `trilogy serve` (interactive debugging UI). 
"""


DATASOURCES_DOC = """# Trilogy Datasource Authoring - AI Agent Reference

When you must declare a NEW datasource (most agent tasks instead query an
existing one in `raw/`), this reference covers every form Trilogy supports:
the `root` keyword, file-based (Parquet / CSV / Python+Arrow), and the
`partial` / `complete` forms for unioning partitioned subsets.

## Root Datasources

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

## File-Based Datasources (Parquet, CSV)

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

## Complete and Partial Datasources

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

## Python Script Datasources (Arrow)

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
"""


SERVE_DOC = """# Trilogy Distribution & Hosting - AI Agent Reference

`trilogy public` browses and pulls models from the trilogy-public-models
registry. `trilogy serve` exposes a model directory over HTTP. Neither is
needed for query authoring — invoke this reference only when distributing or
hosting models.

## trilogy public <subcommand> [options]

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

## trilogy serve <directory> [engine] [options]

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
"""


CONFIG_DOC = """# trilogy.toml Configuration - AI Agent Reference

Every Trilogy workspace has a `trilogy.toml` at its root. 
The first one found recursively from the working directory is used.

## Example

```toml
[engine]
# Default dialect for execution
dialect = "duckdb"

# Max parallelism for multi-script execution
parallelism = 3

[setup]
# Startup scripts to run in a database on connection
trilogy = ["setup.preql"]
sql = ["init.sql"]

[agent]
# Default LLM provider for AI features
# Valid values: openai, anthropic, google, openrouter
provider = "anthropic"

# Default model for the chosen provider
model = "claude-sonnet-4-6"

[report]
# Default visual theme for `trilogy render` and chart `copy into` output
# Built-ins: inter (default), inter-dark, editorial, editorial-dark
theme = "inter"
```

## Sections

- `[engine]` — execution dialect and parallelism defaults. Most workspaces
  override only `dialect` (`duckdb`, `postgres`, ...). `parallelism` caps the
  worker count for multi-script execution.
- `[engine.config]` — dialect-specific connection params. For DuckDB the
  common key is `db_location = "<path>.duckdb"`; for warehouses, a connection
  string is supplied at the CLI instead.
- `[setup]` — scripts to run before any user script. `trilogy = [...]` runs
  `.preql` declarations to seed the environment; `sql = [...]` runs raw SQL
  for tables/extensions.
- `[agent]` — defaults for `trilogy agent` and AI-assisted features. `provider`
  + `model` are the LLM defaults; `api_key_env` overrides which env var the
  API key is read from (defaults below).
- `[report]` — rendering defaults. `theme` names the visual theme applied by
  `trilogy render` and chart `copy into` exports; overridable per invocation
  with `--theme` / `copy (theme='...')`.

## API keys

`[agent]` reads keys from environment variables — never from `trilogy.toml`:
- `OPENAI_API_KEY` for OpenAI
- `ANTHROPIC_API_KEY` for Anthropic
- `GOOGLE_API_KEY` for Google
- `OPENROUTER_API_KEY` for OpenRouter

OpenRouter gives access to models from many providers through a single API
and key.

## Supported Dialects

- `duckdb` / `duck_db` - DuckDB (default for unit tests)
- `sqlite` / `sqlite3` - SQLite
- `postgres` / `postgresql` - PostgreSQL
- `bigquery` - Google BigQuery
- `snowflake` - Snowflake
- `redshift` - Amazon Redshift
- `trino` - Trino/Presto
- `sql_server` - Microsoft SQL Server
"""


INGEST_DOC = """# trilogy ingest - AI Agent Reference

Bootstrap datasources from existing warehouse tables OR from data files
(local paths and remote URLs). Connects to a database, introspects schemas,
and generates Trilogy datasource definitions under `raw/`.

Most agent tasks query an EXISTING model — only invoke this when a fresh
model needs to be generated.

## Usage

`trilogy ingest <sources> [dialect] [options] [conn_args...]`

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

## Examples

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

**Arguments:**
- `input` (required): Path to a markdown (`.md`) report file.

**Options:**
- `--to {png|html}`: Output format (default: `png`).
- `--theme {inter|inter-dark|editorial|editorial-dark}`: Visual theme — font and
  colors. Defaults to `trilogy.toml` `[report].theme`, else `inter`.
- `--out PATH`, `-o PATH`: Output path (default: input path with the format's extension).

```bash
trilogy render report.md                     # -> report.png (default)
trilogy render report.md --to html           # -> report.html (interactive charts)
trilogy render report.md --theme editorial   # font + color theme
trilogy render report.md --theme inter-dark  # dark variant
trilogy render report.md -o out/q3.png       # explicit output path
```

Set a workspace-wide default in `trilogy.toml`:

```toml
[report]
theme = "inter-dark"
```

Requires the `report` extra (`pip install pytrilogy[report]`); PNG output also
needs `playwright install chromium`.

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

### Chart statement reference

```trilogy
chart
  set show_title            -- title from the value-axis label
  set scale_y: log          -- linear|log|sqrt; applies to continuous value axes
  layer bar (
    x_axis <- region,
    y_axis <- sum(revenue) as total,   -- computed bindings REQUIRE `as <name>`
    color <- channel,                  -- one series per color, with legend
    group <- channel,                  -- grouped (side-by-side) bars; no legend
    annotation <- note                 -- per-mark text label
  )
  from select region, channel, sum(revenue) as total, note
  order by total desc                  -- ORDER BY drives bar order
  place hline at 1000 as target;       -- reference rule with optional label
```

- **Chart types**: `bar`, `barh` (horizontal), `line`, `point`, `area`,
  `headline` (big KPI number; binds `x_axis` only).
- **Roles**: `x_axis`, `y_axis`, `color`, `size` (point size), `group`
  (side-by-side bars, or per-series split on line/point/area), `x_trellis` /
  `y_trellis` (small-multiple columns/rows), `annotation` (text label per
  mark). `geo` is reserved and not yet implemented.
- **`from select ...`** per layer is optional; without it the bindings become
  an implicit select. A bar chart's category order follows the select's
  `ORDER BY`; without one it sorts ascending.
- **Explicit colors**: include a `string::hex` column (trait from
  `import std.color;`) in the layer's `from select` alongside a `color`
  binding and each color-field member maps to the hex code on its rows
  (rows missing a hex fall back to gray). Binding the hex column itself to
  `color` uses the codes directly.
- **Settings**: `set hide_legend`, `set show_title`,
  `set scale_x: linear|log|sqrt`, `set scale_y: ...`.
- **Placements**: `place hline at <value> [as <label>]` and
  `place vline at <value> [as <label>]` draw labeled reference rules.
- **Constraints**: trellis roles cannot combine with multiple layers,
  placements, or annotations (Vega-Lite forbids facets inside layered
  charts).

## Standalone chart images (`copy into`)

To emit a single, chrome-free chart image per statement — e.g. embeddable
per-section assets for a blog or doc — use `copy into` with a chart source in
a `.preql` file run via `trilogy run`. Supported image formats: `png`, `svg`,
`html` (interactive), `pdf`:

```trilogy
copy into png 'revenue_by_region.png' from chart
  layer bar ( x_axis <- region, y_axis <- revenue );
```

Options go in parentheses after the path: `width`/`height` (chart size in
pixels), `scale` and `ppi` for raster output, `theme` (a quoted theme name),
and `background` (a CSS color; output is transparent by default so the host
page owns the surround):

```trilogy
copy into png 'revenue.png' (width=640, height=360, scale=2) from chart ...;
copy into png 'revenue.png' (theme='inter-dark', background='#161514') from chart ...;
```

Exports are themed like reports: per-statement `theme=` wins, else
`trilogy.toml` `[report].theme`, else the `inter` default.

The `from` clause takes a bare statement — `from chart ...` or
`from select ...` (no parentheses). `copy into csv|json|parquet ... from
select ...` exports query data the same way. Prefer `copy into` for
individual image assets; prefer `trilogy render report.md` when you want one
combined artifact.

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
        intro="## Trilogy Language Reference\nTrilogy is a SQL-inspired language with a built-in semantic layer, written as .preql files.",
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


@agent_info.command("datasources")
def agent_info_datasources() -> None:
    """Print the datasource authoring reference (root, file, partial, Python)."""
    print(DATASOURCES_DOC)


@agent_info.command("ingest")
def agent_info_ingest() -> None:
    """Print the `trilogy ingest` command reference."""
    print(INGEST_DOC)


@agent_info.command("config")
def agent_info_config() -> None:
    """Print the trilogy.toml configuration schema + API-key conventions."""
    print(CONFIG_DOC)


@agent_info.command("serve")
def agent_info_serve() -> None:
    """Print the distribution/hosting reference (`trilogy public`, `trilogy serve`)."""
    print(SERVE_DOC)


@agent_info.group("syntax", invoke_without_command=True)
@click.pass_context
def agent_info_syntax(ctx: click.Context) -> None:
    """Trilogy syntax examples for common patterns.

    With no subcommand, lists the available examples; fetch one with
    `trilogy agent-info syntax example <name>`.
    """
    if ctx.invoked_subcommand is None:
        print(example_index())


@agent_info_syntax.command("example")
@click.argument("name", required=False)
def agent_info_syntax_example(name: str | None) -> None:
    """Print a complete syntax example (omit NAME to list the available ones)."""
    if name is None:
        print(example_index())
        return
    body = render_example(name)
    if body is None:
        print(f"Unknown syntax example: {name!r}\n")
        print(example_index())
        raise SystemExit(2)
    print(body)
