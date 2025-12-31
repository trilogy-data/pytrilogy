from pathlib import Path

import pytest

from tests.conftest import load_secret
from trilogy import Dialects, Environment
from trilogy.ai import Provider, text_to_query


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
