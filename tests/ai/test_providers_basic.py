from datetime import date, datetime
from pathlib import Path

import pytest

from tests.conftest import load_secret
from trilogy import Environment
from trilogy.ai import Provider
from trilogy.ai.conversation import Conversation
from trilogy.ai.execute import build_provider
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
OPENAI_LATEST_MODEL = "gpt-5.5"
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


def _matches_address_expr(node, env: Environment, address: str) -> bool:
    """True if node resolves to `address`, allowing casts/coercions/wrappers
    (e.g. `dep_time::datetime`) and concept-reference lineage around it."""
    if node == address:
        return True
    if isinstance(node, ConceptRef):
        if node.address == address:
            return True
        concept = env.concepts.get(node.address)
        if concept is not None and concept.lineage is not None:
            return _matches_address_expr(concept.lineage, env, address)
        return False
    if isinstance(node, Function):
        return any(_matches_address_expr(arg, env, address) for arg in node.arguments)
    return False


def _extracts_year_of(node, env: Environment, address: str) -> bool:
    """True if node computes year(address), inline or via an auto-derived concept."""
    if isinstance(node, ConceptRef):
        concept = env.concepts.get(node.address)
        if concept is not None and concept.lineage is not None:
            return _extracts_year_of(concept.lineage, env, address)
        return False
    if isinstance(node, Function):
        if node.operator == FunctionType.YEAR:
            return _matches_address_expr(node.arguments[0], env, address)
        if node.operator == FunctionType.DATE_PART and len(node.arguments) >= 2:
            part = getattr(node.arguments[1], "value", str(node.arguments[1])).lower()
            return part == "year" and _matches_address_expr(
                node.arguments[0], env, address
            )
    return False


def _computes_flight_count(node, env: Environment) -> bool:
    """True if node counts flights: COUNT(local.id2) directly, or via the model's
    `count` metric (`local.count <- count(id2)`), including a re-aggregating
    `sum(local.count)`. Both are valid answers to "number of flights"."""
    if isinstance(node, ConceptRef):
        concept = env.concepts.get(node.address)
        if concept is not None and concept.lineage is not None:
            return _computes_flight_count(concept.lineage, env)
        return False
    if isinstance(node, AggregateWrapper):
        return _computes_flight_count(node.function, env)
    if isinstance(node, Function):
        if node.operator == FunctionType.COUNT:
            return node.arguments[0] == env.concepts["local.id2"].reference
        if node.operator == FunctionType.SUM:
            return _computes_flight_count(node.arguments[0], env)
    return False


def _date_value(value: object) -> date | None:
    if (
        isinstance(value, Function)
        and value.operator == FunctionType.CONSTANT
        and value.arguments
    ):
        value = value.arguments[0]
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def _mirror_operator(operator: ComparisonOperator) -> ComparisonOperator:
    if operator == ComparisonOperator.GT:
        return ComparisonOperator.LT
    if operator == ComparisonOperator.GTE:
        return ComparisonOperator.LTE
    if operator == ComparisonOperator.LT:
        return ComparisonOperator.GT
    if operator == ComparisonOperator.LTE:
        return ComparisonOperator.GTE
    return operator


def _has_year_filter(
    where: WhereClause, env: Environment, address: str, value: int
) -> bool:
    """True for a year equality or an equivalent half-open calendar-year range."""
    lower_bound = False
    upper_bound = False
    start = date(value, 1, 1)
    end = date(value + 1, 1, 1)

    for comp in _iter_comparisons(where):
        for operand, other in ((comp.left, comp.right), (comp.right, comp.left)):
            if (
                comp.operator == ComparisonOperator.EQ
                and isinstance(other, int)
                and not isinstance(other, bool)
                and other == value
                and _extracts_year_of(operand, env, address)
            ):
                return True

        if _matches_address_expr(comp.left, env, address):
            operator, other = comp.operator, comp.right
        elif _matches_address_expr(comp.right, env, address):
            operator, other = _mirror_operator(comp.operator), comp.left
        else:
            continue

        bound = _date_value(other)
        lower_bound |= operator == ComparisonOperator.GTE and bound == start
        upper_bound |= operator == ComparisonOperator.LT and bound == end

    return lower_bound and upper_bound


def _counts_flights(concept, env: Environment, flight_key: str) -> bool:
    """True if concept is a flight count, regardless of phrasing.

    Equivalent shapes: COUNT(id2), COUNT(<property keyed only on id2>) such as
    flight_num, SUM over a concept that itself counts flights (e.g. the auto
    `count` measure rolled up via sum(count)), or a rename of any of those
    (`count as number_of_flights`).
    """
    lineage = concept.lineage
    if isinstance(lineage, Function) and lineage.operator == FunctionType.ALIAS:
        arg = lineage.arguments[0]
        aliased = env.concepts.get(arg.address) if isinstance(arg, ConceptRef) else None
        return aliased is not None and _counts_flights(aliased, env, flight_key)
    if not isinstance(lineage, AggregateWrapper):
        return False
    fn = lineage.function
    arg = fn.arguments[0]
    arg_concept = env.concepts.get(arg.address) if isinstance(arg, ConceptRef) else None
    if arg_concept is None:
        return False
    if fn.operator == FunctionType.COUNT:
        return arg_concept.address == flight_key or arg_concept.keys == {flight_key}
    if fn.operator == FunctionType.SUM:
        return _counts_flights(arg_concept, env, flight_key)
    return False


def _format_conversation(conversation: Conversation) -> str:
    lines = []
    for message in conversation.messages:
        lines.append(f"--- {message.role} ---")
        lines.append(message.content)
        if message.model_info:
            lines.append(f"[model_info: {message.model_info}]")
    return "\n".join(lines)


def validate_response(
    response: str,
    parsed: SelectStatement,
    env: Environment,
    transcript: str,
):
    def detail(reason: str) -> str:
        return f"{reason}, got {response}\n\nLLM conversation:\n{transcript}"

    assert parsed.where_clause is not None, detail("Expected a where clause")
    assert _has_year_filter(
        parsed.where_clause, env, env.concepts["local.dep_time"].address, 2020
    ), detail("Expected a calendar-year 2020 filter on local.dep_time")

    flight_key = env.concepts["local.id2"].address
    found_count = any(
        _counts_flights(env.concepts[x.address], env, flight_key)
        for x in parsed.output_components
    )
    assert found_count, detail(
        "Expected a flight-count aggregate in the output components"
    )


def run_provider_test(secret_name: str, provider: Provider, model: str):
    api_key = load_secret(secret_name)
    if not api_key:
        pytest.skip(f"{secret_name} not found in .env.secrets or environment variables")

    environment, _ = Environment(working_path=env_path).parse("""import flight;""")
    conversation = Conversation.create(
        provider=build_provider(provider, model, api_key)
    )
    response = conversation.generate_query(
        user_input="number of flights by month (departure date) in 2020",
        environment=environment,
    )
    _, parsed = environment.parse(response)
    validate_response(
        response, parsed[-1], environment, _format_conversation(conversation)
    )


@pytest.mark.parametrize(
    ("where", "expected"),
    [
        ("year(dep_time) = 2020", True),
        (
            (
                "dep_time >= '2020-01-01'::datetime and dep_time < "
                "'2021-01-01'::datetime"
            ),
            True,
        ),
        (
            (
                "'2020-01-01'::datetime <= dep_time and "
                "'2021-01-01'::datetime > dep_time"
            ),
            True,
        ),
        ("dep_time >= '2020-01-01'::datetime", False),
    ],
)
def test_has_year_filter(where: str, expected: bool) -> None:
    environment, _ = Environment(working_path=env_path).parse("import flight;")
    _, statements = environment.parse(f"where {where} select dep_time;")
    statement = statements[-1]

    assert statement.where_clause is not None
    assert (
        _has_year_filter(
            statement.where_clause,
            environment,
            environment.concepts["local.dep_time"].address,
            2020,
        )
        is expected
    )


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
