"""VGI tests skip without haybarn + vgi-python, except in the CI job built for them.

That job (.github/workflows/vgi.yml) sets TRILOGY_REQUIRE_VGI so a missing
dependency or an unbuilt feature fails loudly instead of skipping the very
tests it exists to run.
"""

import os

import pytest

REQUIRED = bool(os.environ.get("TRILOGY_REQUIRE_VGI"))


def require_vgi() -> None:
    if REQUIRED:
        import haybarn  # noqa: F401
        import vgi  # noqa: F401

        return
    pytest.importorskip("haybarn")
    pytest.importorskip("vgi")


def skip_or_fail(reason: str) -> None:
    if REQUIRED:
        pytest.fail(reason)
    pytest.skip(reason)


@pytest.fixture(autouse=True, scope="module")
def flush_vgi_workers():
    """Stop the pooled workers once a module is done with them.

    They outlive their connections (the pool is per process), so left alone
    they would be stopped after pytest-cov has combined its data and their
    coverage would be lost; see .coveragerc.vgi.
    """
    yield
    try:
        import haybarn
    except ImportError:
        return
    con = haybarn.connect()
    try:
        con.execute("LOAD vgi")
        con.execute("SELECT * FROM vgi_worker_pool_flush()").fetchall()
    finally:
        con.close()
