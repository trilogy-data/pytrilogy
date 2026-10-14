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
