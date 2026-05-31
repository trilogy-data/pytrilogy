"""Ad-hoc spike: print the grain `trilogy ingest` would pick per TPC-DS table.

Bypasses the LLM agent — builds an Executor on the cached DuckDB and runs the
same `create_datasource_from_table` path the CLI uses, so we can eyeball grain
assignments after tweaking candidate ranking.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from trilogy import Dialects  # noqa: E402
from trilogy.core.models.environment import Environment  # noqa: E402
from trilogy.dialect.config import DuckDBConfig  # noqa: E402
from trilogy.scripts.ingest import create_datasource_from_table  # noqa: E402

DB = Path(__file__).resolve().parent / ".cache" / "tpcds_sf1.duckdb"


def main() -> int:
    exec = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=DB.parent),
        conf=DuckDBConfig(path=str(DB)),
    )
    tables = [r[0] for r in exec.generator.list_tables(exec, None)]
    for table in sorted(tables):
        try:
            ds, _concepts, _imports = create_datasource_from_table(
                exec, table, None, root=True
            )
            grain = sorted(ds.grain.components) or ["<none>"]
            print(f"{table:24s} -> {', '.join(grain)}")
        except Exception as e:  # noqa: BLE001
            print(f"{table:24s} !! {type(e).__name__}: {e}")
            exec.connection.rollback()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
