import pytest

from tests.modeling.tree_reporting.generate_fixtures import (
    write_build_fixtures,
    write_dashboard_fixtures,
)


@pytest.fixture(scope="session", autouse=True)
def tree_reporting_fixtures() -> None:
    """The parquet fixtures are gitignored; they are generated deterministically."""
    write_dashboard_fixtures()
    write_build_fixtures()
