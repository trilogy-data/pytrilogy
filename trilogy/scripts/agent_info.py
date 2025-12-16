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
- `raw/` - Directory for raw data models
- `derived/` - Directory for derived data models
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
```

## Supported Dialects

- `duckdb` / `duck_db` - DuckDB (default for unit tests)
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
    -s analytics -o raw/ "postgresql://localhost/warehouse"
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
