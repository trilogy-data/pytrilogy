"""Measure REAL rendered SQL length (engine fixture, DB imported -> inlining applies)
for the size-tracked tpc_ds queries under v4. Trust this over v4_size_compare."""

from pathlib import Path

from trilogy import Dialects, Executor
from trilogy.constants import CONFIG
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import DuckDBConfig

CONFIG.use_v4_discovery = True

working_path = Path(__file__).parent.parent / "tests" / "modeling" / "tpc_ds_duckdb"
import_path = working_path / "memory"

# (label, preql_file, ceiling)
QUERIES = [
    ("10", "query10.preql", 7000),
    ("2.1", "query02-one.preql", 7500),
    ("2.2", "query02-two.preql", 7500),
    ("30.alt", "query30-alt.preql", 12000),
    ("73", "query73.preql", 3000),
    ("81", "query81.preql", 8000),
    ("23", "query23.preql", 8500),
    ("94", "query94.preql", 5000),
]


def make_engine() -> Executor:
    env = Environment(working_path=working_path)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env, conf=DuckDBConfig()
    )
    engine.execute_raw_sql(f"IMPORT DATABASE '{import_path}';")
    return engine


def _measure(engine: Executor, fname: str) -> int | str:
    engine.environment = Environment(working_path=working_path)
    text = (working_path / fname).read_text()
    try:
        return len(engine.generate_sql(text)[-1])
    except Exception as e:  # noqa: BLE001
        return f"ERR {type(e).__name__}"


def main() -> None:
    engine = make_engine()
    print(f"{'q':>7} {'ceil':>6} {'v3':>6} {'v4':>6} {'over?':>6}")
    for label, fname, ceiling in QUERIES:
        CONFIG.use_v4_discovery = False
        v3 = _measure(engine, fname)
        CONFIG.use_v4_discovery = True
        v4 = _measure(engine, fname)
        flag = "OVER" if isinstance(v4, int) and v4 >= ceiling else ""
        print(f"{label:>7} {ceiling:>6} {str(v3):>6} {str(v4):>6} {flag:>6}")


if __name__ == "__main__":
    main()
