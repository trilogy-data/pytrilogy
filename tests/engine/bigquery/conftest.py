"""BigQuery test fixtures."""

import pytest

from trilogy import Dialects


@pytest.fixture(scope="module")
def bigquery_executor():
    """Create BigQuery executor if credentials are available."""
    try:
        executor = Dialects.BIGQUERY.default_executor()
        yield executor
        executor.close()
    except Exception as e:
        pytest.skip(f"BigQuery not available: {e}")
