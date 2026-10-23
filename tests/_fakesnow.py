import duckdb
import pytest


def skip_if_fakesnow_unsupported() -> None:
    # fakesnow calls fetch_arrow_table and defines `x ->` lambda macros, both
    # removed in DuckDB 2.0.
    if int(duckdb.__version__.split(".")[0]) >= 2:
        pytest.skip("fakesnow does not support DuckDB 2.0 yet")
