import os
from pathlib import Path

import pytest

from trilogy import Dialects, Environment
from trilogy.ai import Provider, text_to_query


def load_secret(key: str) -> str | None:
    """Load a secret from .env.secrets file in the current working directory."""
    secrets_path = Path.cwd() / ".env.secrets"

    if secrets_path.exists():
        with open(secrets_path, "r") as f:
            for line in f:
                line = line.strip()
                if line.startswith(f"{key}="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")

    # Fallback to environment variable
    return os.getenv(key)


# load a model
def test_llm_execution():
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )
    api_key = load_secret("OPENAI_API_KEY")

    if not api_key:
        pytest.skip("OPENAI_API_KEY not found in .env.secrets or environment variables")
    # load a model
    executor.parse_file("flight.preql")
    executor.execute_file("setup.sql")
    # generate a query
    query = text_to_query(
        executor.environment,
        "number of flights by month in 2005",
        Provider.OPENAI,
        "gpt-5-chat-latest",
        api_key,
    )

    # print the generated trilogy query
    results = executor.execute_text(query)[-1].fetchall()
    assert len(results) == 12

    for row in results:
        # all monthly flights are between 5000 and 7000
        assert row[1] > 5000 and row[1] < 7000, row
