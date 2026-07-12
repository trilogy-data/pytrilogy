"""Build and cache the DABstep DuckDB from the downloaded context files.

Tables mirror the context file names so the agent can map the docs
(``manual.md`` / ``payments-readme.md``) onto the model:

    payments                  — the transaction fact (payments.csv)
    fees                      — fee rules, scalar match columns (fees.json)
    fee_account_types         — fee_id -> account_type (unnested)
    fee_merchant_category_codes — fee_id -> mcc (unnested)
    fee_acis                  — fee_id -> aci (unnested)
    merchant_data             — merchant attributes (merchant_data.json)
    merchant_acquirers        — merchant -> acquirer (unnested)
    acquirer_countries        — acquirer -> country code (acquirer_countries.csv)
    merchant_category_codes   — MCC -> description (merchant_category_codes.csv)

The source JSON's list-valued matching fields (``account_type``, ``aci``,
``merchant_category_code``, ``acquirer``) are unnested into child tables; an
empty list upstream means "applies to all", i.e. a fee with no child rows for a
dimension is unconstrained on it (see ``schema_notes.md``).

The data is static (no scale factor); the db is built once into ``.cache/``.
"""

from __future__ import annotations

import shutil
from pathlib import Path

EVAL_DIR = Path(__file__).resolve().parent
CONTEXT_DIR = EVAL_DIR / "data" / "context"
CACHE_DB = EVAL_DIR / ".cache" / "dabstep.duckdb"

DB_FILENAME = "dabstep.duckdb"
# Docs every agent workspace gets: the two benchmark docs from data/context
# plus our note mapping the unnested tables back onto the manual's field names.
DOC_FILES = (
    CONTEXT_DIR / "manual.md",
    CONTEXT_DIR / "payments-readme.md",
    EVAL_DIR / "schema_notes.md",
)


def build_database() -> Path:
    """Return the cached DuckDB, building it from data/context on first use."""
    import duckdb

    if CACHE_DB.exists():
        return CACHE_DB
    missing = [f.name for f in _context_files() if not f.exists()]
    if missing:
        raise FileNotFoundError(
            f"context files missing: {missing} — run download_data.py first."
        )

    CACHE_DB.parent.mkdir(parents=True, exist_ok=True)
    tmp = CACHE_DB.with_suffix(".building")
    tmp.unlink(missing_ok=True)
    con = duckdb.connect(str(tmp))
    ctx = CONTEXT_DIR.as_posix()
    try:
        con.execute(
            f"CREATE TABLE payments AS SELECT * FROM read_csv('{ctx}/payments.csv', header=true)"
        )
        con.execute(
            f"CREATE TEMP TABLE fees_raw AS SELECT * FROM read_json('{ctx}/fees.json')"
        )
        con.execute(
            "CREATE TABLE fees AS SELECT ID, card_scheme, capture_delay, "
            "monthly_fraud_level, monthly_volume, is_credit, fixed_amount, rate, "
            "CAST(intracountry AS BOOLEAN) AS intracountry FROM fees_raw"
        )
        con.execute(
            "CREATE TABLE fee_account_types AS SELECT ID AS fee_id, "
            "unnest(account_type) AS account_type FROM fees_raw"
        )
        con.execute(
            "CREATE TABLE fee_merchant_category_codes AS SELECT ID AS fee_id, "
            "unnest(merchant_category_code) AS mcc FROM fees_raw"
        )
        con.execute(
            "CREATE TABLE fee_acis AS SELECT ID AS fee_id, unnest(aci) AS aci FROM fees_raw"
        )
        con.execute(
            f"CREATE TEMP TABLE merchant_raw AS SELECT * FROM read_json('{ctx}/merchant_data.json')"
        )
        con.execute(
            "CREATE TABLE merchant_data AS SELECT merchant, capture_delay, "
            "merchant_category_code, account_type FROM merchant_raw"
        )
        con.execute(
            "CREATE TABLE merchant_acquirers AS SELECT merchant, "
            "unnest(acquirer) AS acquirer FROM merchant_raw"
        )
        # First CSV column is an unnamed pandas index — drop it.
        con.execute(
            "CREATE TABLE acquirer_countries AS SELECT acquirer, country_code "
            f"FROM read_csv('{ctx}/acquirer_countries.csv', header=true)"
        )
        # mcc arrives as VARCHAR (auto-sniff); cast so it joins the BIGINT mcc
        # columns on fee_merchant_category_codes / merchant_data directly.
        con.execute(
            "CREATE TABLE merchant_category_codes AS SELECT CAST(mcc AS BIGINT) AS mcc, "
            f"description FROM read_csv('{ctx}/merchant_category_codes.csv', header=true)"
        )
        con.execute("CHECKPOINT;")
    finally:
        con.close()
    tmp.with_name(tmp.name + ".wal").unlink(missing_ok=True)
    tmp.replace(CACHE_DB)
    return CACHE_DB


def _context_files() -> list[Path]:
    return [
        CONTEXT_DIR / n
        for n in (
            "payments.csv",
            "fees.json",
            "merchant_data.json",
            "acquirer_countries.csv",
            "merchant_category_codes.csv",
            "manual.md",
            "payments-readme.md",
        )
    ]


def copy_docs(workspace: Path) -> None:
    """The manual is load-bearing context (fee semantics live there, not in the
    schema) — every agent workspace gets a copy it can `trilogy file read`."""
    for doc in DOC_FILES:
        shutil.copy2(doc, workspace / doc.name)
