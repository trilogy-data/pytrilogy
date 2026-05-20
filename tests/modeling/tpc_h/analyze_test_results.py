from pathlib import Path

from tests.modeling._benchmark_reporting import BenchmarkConfig
from tests.modeling._benchmark_reporting import analyze as _analyze
from tests.modeling.tpc_h.query_size import query_size

CONFIG = BenchmarkConfig(
    name="TPC-H",
    file_prefix="tcp-h",
    root=Path(__file__).parent,
    query_size_fn=query_size,
)


def analyze(show: bool = False) -> None:
    _analyze(CONFIG, show=show)


if __name__ == "__main__":
    analyze()
