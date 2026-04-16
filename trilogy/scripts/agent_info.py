"""Agent info command - outputs AGENTS.md-style usage guide for AI agents."""

from click import pass_context

from trilogy.ai.prompts import get_trilogy_prompt

AGENT_INFO_OUTPUT = """# Trilogy CLI - AI Agent Usage Guide

## Overview

Trilogy is a semantic ETL and reporting tool providing a SQL-like language with
optimizations. This CLI enables workspace management, script execution, testing,
and data ingestion.

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

### trilogy ingest <tables> [dialect] [options] [conn_args...]

Bootstrap datasources from existing warehouse tables. Connects to a database,
introspects table schemas, and generates Trilogy datasource definitions.

**Arguments:**
- `tables` (required): Comma-separated list of table names
- `dialect` (optional): Database dialect
- `conn_args` (optional): Connection arguments

**Options:**
- `--output PATH`, `-o PATH`: Output directory for generated files
- `--schema NAME`, `-s NAME`: Schema/database to ingest from
- `--config PATH`: Path to trilogy.toml
- `--fks SPEC`: Foreign key relationships (format: table.col:ref_table.col)

**Examples:**
```bash
# Ingest tables from DuckDB
trilogy ingest "users,orders,products" duckdb "path/to/db.duckdb"

# Ingest with schema and output directory
trilogy ingest "customers" postgres -s public -o raw/ "postgresql://localhost/db"

# Ingest with foreign key relationships
trilogy ingest "orders,customers" duckdb --fks "orders.customer_id:customers.id"
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

**`trilogy public fetch <model>` options:**
- `--path DIR`, `-p DIR`: Target directory (default `./<model>`).
- `--no-examples`: Skip example scripts/dashboards.
- `--force`, `-f`: Overwrite an existing non-empty target directory.

Writes all components, a README.md from the model description, and a
`trilogy.toml` with the engine dialect and any setup SQL preconfigured, so the
directory is immediately usable with `trilogy refresh` / `trilogy serve`.

**Example:**
```bash
trilogy public list --engine duckdb
trilogy public fetch bike_data --path ./bike-demo
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


def get_agent_info_output() -> str:
    """Build the complete agent info output with CLI docs and syntax reference."""
    syntax_section = get_trilogy_prompt(
        intro="## Trilogy Language Syntax\n\nTrilogy is a SQL-inspired language with a built-in semantic layer. Use the following syntax reference when writing .preql files.",
    )
    return AGENT_INFO_OUTPUT + "\n" + syntax_section


@pass_context
def agent_info(ctx):
    """Output comprehensive CLI documentation for AI agents.

    Prints an AGENTS.md-style guide with all commands, options,
    and usage examples optimized for AI agent consumption.
    """
    print(get_agent_info_output())
