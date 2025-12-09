"""Ingest command for Trilogy CLI - bootstraps datasources from warehouse tables."""
from click import UNPROCESSED, Path, argument, option, pass_context


@argument("source", type=str)
@argument("dialect", type=str, required=False)
@option("--output", "-o", type=Path(), help="Output path for generated scripts")
@option(
    "--tables", "-t", multiple=True, help="Specific tables to ingest (default: all)"
)
@option("--schema", "-s", type=str, help="Schema/database to ingest from")
@argument("conn_args", nargs=-1, type=UNPROCESSED)
@pass_context
def ingest(
    ctx,
    source: str,
    dialect: str | None,
    output: str | None,
    tables: tuple[str, ...],
    schema: str | None,
    conn_args,
):
    """Bootstrap one or more datasources from tables in your warehouse.

    Connects to a warehouse and generates Trilogy datasource definitions
    from existing tables.

    Args:
        source: Source connection string or identifier
        dialect: Database dialect (e.g., duckdb, postgres, snowflake)
        output: Output path for generated scripts
        tables: Specific tables to ingest (if not specified, ingests all)
        schema: Schema/database to ingest from
        conn_args: Additional connection arguments
    """
    raise NotImplementedError("The 'ingest' command is not yet implemented")
