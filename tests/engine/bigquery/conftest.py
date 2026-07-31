"""BigQuery test fixtures."""

import os
from pathlib import Path

import pytest

from trilogy import Dialects

REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture(scope="module")
def bigquery_executor():
    """Create BigQuery executor if credentials are available."""
    try:
        executor = Dialects.BIGQUERY.default_executor()
        yield executor
        executor.close()
    except Exception as e:
        pytest.skip(f"BigQuery not available: {e}")


def _seed_dotenv() -> None:
    """Seed the environment from the repo dotenv files, without overriding what
    is already set. Lazy rather than at import time so collection of deselected
    bigquery tests does not leak into other suites."""
    for candidate in (".env.secrets", ".env"):
        path = REPO_ROOT / candidate
        if not path.is_file():
            continue
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            value = value.strip().strip('"').strip("'")
            if value:
                os.environ.setdefault(key.strip(), value)


def bq_env(name: str) -> str | None:
    _seed_dotenv()
    return os.environ.get(name) or None


@pytest.fixture(scope="module")
def bq_write_dataset() -> str:
    """A dataset the test credentials may create and drop tables in."""
    dataset = bq_env("TRILOGY_BIGQUERY_TEST_DATASET") or bq_env(
        "TRILOGY_BIGQUERY_STAGING_DATASET"
    )
    if not dataset:
        pytest.skip("TRILOGY_BIGQUERY_TEST_DATASET not set")
    return dataset
