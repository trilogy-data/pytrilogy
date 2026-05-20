from pathlib import Path

from tests.modeling._benchmark_reporting import BenchmarkConfig
from tests.modeling._benchmark_reporting import analyze as _analyze
from tests.modeling.tpc_ds_duckdb.query_size import query_size

_ALT_PREQL_NAMES: dict[str, str] = {
    "2.1": "query02-one.preql",
    "2.2": "query02-two.preql",
    "97.1": "query97-one.preql",
    "97.2": "query97-two.preql",
}

CONFIG = BenchmarkConfig(
    name="TPC-DS",
    file_prefix="tcp-ds",
    root=Path(__file__).parent,
    alt_preql_names=_ALT_PREQL_NAMES,
    query_size_fn=query_size,
)


def analyze(show: bool = False) -> None:
    _analyze(CONFIG, show=show)


if __name__ == "__main__":
    analyze()
