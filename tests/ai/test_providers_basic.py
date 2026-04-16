from pathlib import Path

import pytest

from tests.conftest import load_secret
from trilogy import Environment
from trilogy.ai import Provider, text_to_query
from trilogy.authoring import (
    AggregateWrapper,
    FunctionType,
    SelectStatement,
)

env_path = Path(__file__).parent.parent / "modeling" / "faa"

GOOGLE_LATEST_MODEL = "gemini-2.5-flash"
OPENAI_LATEST_MODEL = "gpt-5-chat-latest"
ANTHROPIC_LATEST_MODEL = "claude-sonnet-4-6"
OPENROUTER_LATEST_MODEL = "anthropic/claude-sonnet-4-6"


def validate_response(response: str, parsed: SelectStatement, env: Environment):
    assert (
        "dep_time.year = 2020" in response
        or "year(local.dep_time) = 2020" in response
        or "date_part(local.dep_time, year) = 2020" in response
    ), response

    found_count = False
    for x in parsed.output_components:
        full = env.concepts[x]
        if not isinstance(full.lineage, AggregateWrapper):
            continue
        flin = full.lineage.function
        if (
            flin.operator == FunctionType.COUNT
            and flin.arguments[0] == env.concepts["local.id2"].reference
        ):
            found_count = True
            break
    assert (
        found_count
    ), f"Expected to find a COUNT(local.id2) in the parsed output components, got {response}"


def run_provider_test(secret_name: str, provider: Provider, model: str):
    api_key = load_secret(secret_name)
    if not api_key:
        pytest.skip(f"{secret_name} not found in .env.secrets or environment variables")

    environment, _ = Environment(working_path=env_path).parse("""import flight;""")
    response = text_to_query(
        environment,
        "number of flights by month in 2020",
        provider,
        model,
        api_key,
    )
    _, parsed = environment.parse(response)
    validate_response(response, parsed[-1], environment)


def test_basic_openai_completion():
    run_provider_test("OPENAI_API_KEY", Provider.OPENAI, OPENAI_LATEST_MODEL)


def test_basic_anthropic_completion():
    run_provider_test("ANTHROPIC_API_KEY", Provider.ANTHROPIC, ANTHROPIC_LATEST_MODEL)


def test_basic_openrouter_completion():
    run_provider_test(
        "OPENROUTER_API_KEY", Provider.OPENROUTER, OPENROUTER_LATEST_MODEL
    )


def test_basic_google_completion():
    try:
        run_provider_test("GOOGLE_API_KEY", Provider.GOOGLE, GOOGLE_LATEST_MODEL)
    except Exception as exc:
        message = str(exc)
        if "429" in message and (
            "RESOURCE_EXHAUSTED" in message
            or "quota" in message.lower()
            or "rate limit" in message.lower()
        ):
            pytest.skip(
                f"Skipping Google integration test due to quota/rate limit: {exc}"
            )
        raise
