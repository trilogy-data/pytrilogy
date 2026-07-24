"""Both grammar backends (Lark, Pest) should surface a friendly Syntax [227]
when a user-defined (`def`) function is invoked without its required `@` prefix.

Real failure mode (TPC-DS enriched q66 trajectory): the agent writes helper
functions and calls one as `mon_sales(m)` instead of `@mon_sales(m)`, getting a
cryptic operator-expectation list pointed at the `(` instead of an actionable
correction. The name must resolve to a `def` declared earlier so unknown names
and built-ins (`sum(...)`) keep their normal diagnostics."""

from __future__ import annotations

import pytest

from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.parse_engine_v2 import parse_text
from trilogy.parsing.v2.errors import detect_named_function_missing_at
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

_BAD = [
    "def identity(x) -> x;\ndef doubled(x) -> identity(x) * 2;",
    # nested named-function calls
    "def identity(x) -> x;\ndef wrap(x) -> identity(identity(x));",
    # the original q66 shape
    (
        "def mon_sales(m) -> sum(store_sales.ext_sales_price);\n"
        "def mon_sales_sqft(m) -> mon_sales(m) / warehouse.square_feet;"
    ),
    # `def table` named functions are also invoked with `@`
    "def table gen(n) -> select val order by val desc limit 2;\ndef use(n) -> gen(n) * 2;",
]

# Must keep parsing (correct `@`, built-ins, and unrelated bare identifiers).
_GOOD = [
    "def identity(x) -> x;\ndef doubled(x) -> @identity(x) * 2;",
    "select sum(store_sales.ext_sales_price) as v;",
    "def identity(x) -> x;\nselect @identity(1) as v;",
    "select coalesce(store_sales.ext_sales_price, 0) as v;",
]


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("body", _BAD)
def test_named_function_missing_at_friendly_error(backend, body):
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(body)
    msg = str(exc.value)
    assert "Syntax [227]" in msg, msg
    assert "must be invoked with a leading `@`" in msg, msg


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("body", _GOOD)
def test_valid_calls_parse(backend, body):
    backend(body)


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_unknown_name_is_not_reported_as_named_function(backend):
    with pytest.raises(InvalidSyntaxException) as exc:
        backend("def doubled(x) -> missing(x) * 2;")
    assert "Syntax [227]" not in str(exc.value), str(exc.value)


def test_error_names_the_function_and_points_at_it():
    src = "def identity(x) -> x;\ndef doubled(x) -> identity(x) * 2;"
    with pytest.raises(InvalidSyntaxException) as exc:
        parse_pest(src)
    msg = str(exc.value)
    assert "`@identity(...)`" in msg, msg
    # The caret marker sits immediately before the function name, not the `(`.
    assert "??? identity(x)" in msg, msg


def test_end_to_end_parse_text_surfaces_227():
    src = "def identity(x) -> x;\ndef doubled(x) -> identity(x) * 2;"
    with pytest.raises(InvalidSyntaxException) as exc:
        parse_text(src)
    assert "Syntax [227]" in str(exc.value), str(exc.value)


def _call_paren(text: str, call: str) -> int:
    """Position of the `(` at the last `NAME(` call site — where both backends
    report the failure."""
    return text.index("(", text.rindex(call))


def test_detector_finds_missing_at():
    text = "def identity(x) -> x;\ndef doubled(x) -> identity(x) * 2;"
    found = detect_named_function_missing_at(text, _call_paren(text, "identity("))
    assert found is not None
    assert text[found:].startswith("identity(")


def test_detector_ignores_prefixed_call():
    text = "def identity(x) -> x;\ndef doubled(x) -> @identity(x) * 2;"
    assert (
        detect_named_function_missing_at(text, _call_paren(text, "identity(")) is None
    )


def test_detector_ignores_unknown_name():
    text = "def doubled(x) -> missing(x) * 2;"
    assert detect_named_function_missing_at(text, _call_paren(text, "missing(")) is None


def test_detector_ignores_builtin_call():
    text = "select sum(store_sales.ext_sales_price) as v;"
    assert detect_named_function_missing_at(text, _call_paren(text, "sum(")) is None
