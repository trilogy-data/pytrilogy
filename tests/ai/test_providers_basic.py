from pathlib import Path

import pytest

from tests.conftest import load_secret
from trilogy import Environment
from trilogy.ai import Provider, text_to_query
from trilogy.authoring import (
    AggregateWrapper,
    Comparison,
    ComparisonOperator,
    ConceptRef,
    Conditional,
    Function,
    FunctionType,
    Parenthetical,
    SelectStatement,
    WhereClause,
)

env_path = Path(__file__).parent.parent / "modeling" / "faa"

GOOGLE_LATEST_MODEL = "gemini-2.5-flash"
OPENAI_LATEST_MODEL = "gpt-5-chat-latest"
ANTHROPIC_LATEST_MODEL = "claude-sonnet-4-6"
OPENROUTER_LATEST_MODEL = "anthropic/claude-sonnet-4-6"


def _iter_comparisons(node):
    if isinstance(node, WhereClause):
        yield from _iter_comparisons(node.conditional)
    elif isinstance(node, Comparison):
        yield node
        yield from _iter_comparisons(node.left)
        yield from _iter_comparisons(node.right)
    elif isinstance(node, Conditional):
        yield from _iter_comparisons(node.left)
        yield from _iter_comparisons(node.right)
    elif isinstance(node, Parenthetical):
        yield from _iter_comparisons(node.content)


def _extracts_year_of(node, env: Environment, address: str) -> bool:
    """True if node computes year(address), inline or via an auto-derived concept."""
    if isinstance(node, ConceptRef):
        concept = env.concepts.get(node.address)
        if concept is not None and concept.lineage is not None:
            return _extracts_year_of(concept.lineage, env, address)
        return False
    if isinstance(node, Function):
        if node.operator == FunctionType.YEAR:
            return node.arguments[0] == address
        if node.operator == FunctionType.DATE_PART and len(node.arguments) >= 2:
            part = getattr(node.arguments[1], "value", str(node.arguments[1])).lower()
            return part == "year" and node.arguments[0] == address
    return False


def _has_year_equals(
    where: WhereClause, env: Environment, address: str, value: int
) -> bool:
    """Logical-equivalence check: a `year(address) = value` comparison exists."""
    for comp in _iter_comparisons(where):
        if comp.operator != ComparisonOperator.EQ:
            continue
        for operand, other in ((comp.left, comp.right), (comp.right, comp.left)):
            if (
                isinstance(other, int)
                and not isinstance(other, bool)
                and other == value
                and _extracts_year_of(operand, env, address)
            ):
                return True
    return False


def validate_response(response: str, parsed: SelectStatement, env: Environment):
    assert parsed.where_clause is not None, f"Expected a where clause, got {response}"
    assert _has_year_equals(
        parsed.where_clause, env, env.concepts["local.dep_time"].address, 2020
    ), f"Expected a year(local.dep_time) = 2020 filter, got {response}"

    found_count = False
    for x in parsed.output_components:
        full = env.concepts[x.address]
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
