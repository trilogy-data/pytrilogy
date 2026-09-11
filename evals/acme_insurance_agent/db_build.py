"""Build and cache the ACME Insurance DuckDB from the vendored CSVs.

The tables are the thirteen in ``ACME_small.ddl`` (the DDL dbt's text-to-SQL
leg was given), loaded from data.world's cwd-benchmark-data CSVs under
``data/``. Types follow the DDL where DuckDB's sniffer would disagree: policy
and claim numbers are identifiers, not integers, and money is DOUBLE so the
tolerant row comparison treats it as numeric.

The data is static (no scale factor); the db is built once into ``.cache/``.
"""

from __future__ import annotations

from pathlib import Path

EVAL_DIR = Path(__file__).resolve().parent
DATA_DIR = EVAL_DIR / "data"
CACHE_DB = EVAL_DIR / ".cache" / "acme.duckdb"
DB_FILENAME = "acme.duckdb"

TABLES = (
    "Agreement_Party_Role",
    "Catastrophe",
    "Claim",
    "Claim_Amount",
    "Claim_Coverage",
    "Expense_Payment",
    "Expense_Reserve",
    "Loss_Payment",
    "Loss_Reserve",
    "Policy",
    "Policy_Amount",
    "Policy_Coverage_Detail",
    "Premium",
)
COLUMN_TYPES = {
    "Policy_Number": "VARCHAR",
    "Company_Claim_Number": "VARCHAR",
    "Company_Subclaim_Number": "VARCHAR",
    "Claim_Amount": "DOUBLE",
    "Policy_Amount": "DOUBLE",
}


def _type_overrides(csv: Path) -> str:
    """DuckDB rejects overrides for columns a file lacks, so filter by header."""
    header = csv.read_text(encoding="utf-8").splitlines()[0].split(",")
    types = ", ".join(
        f"'{col}': '{typ}'" for col, typ in COLUMN_TYPES.items() if col in header
    )
    return f", types={{{types}}}" if types else ""


def build_database() -> Path:
    """Return the cached DuckDB, building it from data/ on first use."""
    import duckdb

    if CACHE_DB.exists():
        return CACHE_DB
    CACHE_DB.parent.mkdir(parents=True, exist_ok=True)
    tmp = CACHE_DB.with_suffix(".building")
    tmp.unlink(missing_ok=True)
    con = duckdb.connect(str(tmp))
    try:
        for table in TABLES:
            csv = DATA_DIR / f"{table}.csv"
            con.execute(
                f"CREATE TABLE \"{table}\" AS SELECT * FROM read_csv('{csv.as_posix()}', "
                f"header=true{_type_overrides(csv)})"
            )
        con.execute("CHECKPOINT;")
    finally:
        con.close()
    tmp.with_name(tmp.name + ".wal").unlink(missing_ok=True)
    tmp.replace(CACHE_DB)
    return CACHE_DB
