"""Backwards-compatible shim: `analyze` lives in
`tests.modeling._benchmark_reporting`. Importing from here still works."""

from tests.modeling._benchmark_reporting import build_summary, write_summary

__all__ = ["build_summary", "write_summary"]


if __name__ == "__main__":
    from tests.modeling.tpc_ds_duckdb.analyze_test_results import analyze

    analyze()
