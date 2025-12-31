from pathlib import Path

import pytest

from tests.conftest import load_secret
from trilogy import Environment
from trilogy.ai import Provider, text_to_query

env_path = Path(__file__).parent.parent / "modeling" / "faa"

GOOGLE_LATEST_MODEL = "gemini-2.5-flash"
OPENAI_LATEST_MODEL = "gpt-5-chat-latest"
ANTHROPIC_LATEST_MODEL = "claude-sonnet-4-5-20250929"


def validate_response(response: str):
    assert (
        "dep_time.year = 2020" in response
        or "date_part(local.dep_time, year) = 2020" in response
    ), response
    assert (
        "count(id2) as" in response
        or "count(local.id2) as" in response
        or "local.id2.count" in response
    ), response


def test_basic_openai_completion():
    # Load API key from .env.secrets file
    api_key = load_secret("OPENAI_API_KEY")

    if not api_key:
        pytest.skip("OPENAI_API_KEY not found in .env.secrets or environment variables")

    environment, _ = Environment(working_path=env_path).parse("""import flight;""")
    response = text_to_query(
        environment,
        "number of flights by month in 2020",
        Provider.OPENAI,
        OPENAI_LATEST_MODEL,
        api_key,
    )

    validate_response(response)


def test_basic_anthropic_completion():
    # Load API key from .env.secrets file
    api_key = load_secret("ANTHROPIC_API_KEY")
    if not api_key:
        pytest.skip(
            "ANTHROPIC_API_KEY not found in .env.secrets or environment variables"
        )
    environment, _ = Environment(working_path=env_path).parse("""import flight;""")
    response = text_to_query(
        environment,
        "number of flights by month in 2020",
        Provider.ANTHROPIC,
        ANTHROPIC_LATEST_MODEL,
        api_key,
    )
    validate_response(response)


def test_basic_google_completion():
    # Load API key from .env.secrets file
    api_key = load_secret("GOOGLE_API_KEY")
    if not api_key:
        pytest.skip("GOOGLE_API_KEY not found in .env.secrets or environment variables")
    environment, _ = Environment(working_path=env_path).parse("""import flight;""")
    response = text_to_query(
        environment,
        "number of flights by month in 2020",
        Provider.GOOGLE,
        GOOGLE_LATEST_MODEL,
        api_key,
    )
    validate_response(response)
