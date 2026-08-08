import pyarrow as pa
import pytest

from trilogy.io.adapters import to_reader
from trilogy.io.contract import (
    Filter,
    SourceRequest,
    apply,
    bind,
    effective_pushdown,
    pushdown_parameters,
)
from trilogy.io.errors import ContractError

ROWS = [{"i": i, "state": "CA" if i % 2 else "NY"} for i in range(10)]


def result(request, pushed=()):
    return apply(to_reader(ROWS), request, pushed).read_all()


def test_filter_parses_operators_and_json_values():
    assert Filter.parse("i >= 5") == Filter("i", ">=", 5)
    assert Filter.parse('state = "CA"') == Filter("state", "=", "CA")
    assert Filter.parse('state in ["CA","NY"]') == Filter("state", "in", ["CA", "NY"])
    assert Filter.parse("state NOT IN [1]").op == "not in"
    assert Filter.parse("state like n%") == Filter("state", "like", "n%")


def test_filter_bare_word_value_stays_a_string():
    assert Filter.parse("state = CA").value == "CA"


def test_filter_rejects_garbage():
    with pytest.raises(ContractError, match="Could not parse filter"):
        Filter.parse("this is not a filter")


def test_filter_renders_back_to_a_parseable_string():
    original = Filter.parse('state in ["CA","NY"]')
    assert Filter.parse(original.render()) == original


def test_limit_truncates():
    assert result(SourceRequest(limit=3)).num_rows == 3


def test_limit_stops_pulling_from_the_source():
    pulled = []

    def batches():
        for i in range(5):
            pulled.append(i)
            yield pa.Table.from_pylist(ROWS)

    out = apply(to_reader(batches()), SourceRequest(limit=12), ()).read_all()
    assert out.num_rows == 12
    assert len(pulled) == 2


def test_columns_project_and_narrow_the_schema():
    reader = apply(to_reader(ROWS), SourceRequest(columns=("state",)), ())
    assert reader.schema.names == ["state"]
    assert reader.read_all().column_names == ["state"]


def test_filters_apply():
    out = result(SourceRequest(filters=(Filter("state", "=", "CA"),)))
    assert set(out.column("state").to_pylist()) == {"CA"}


def test_every_operator_runs():
    for predicate in (
        Filter("i", ">", 4),
        Filter("i", ">=", 4),
        Filter("i", "<", 4),
        Filter("i", "<=", 4),
        Filter("i", "!=", 4),
        Filter("i", "=", 4),
        Filter("state", "in", ["CA"]),
        Filter("state", "not in", ["CA"]),
        Filter("state", "like", "C%"),
    ):
        result(SourceRequest(filters=(predicate,)))


def test_combined_request():
    out = result(
        SourceRequest(limit=2, filters=(Filter("state", "=", "CA"),), columns=("i",))
    )
    assert out.num_rows == 2
    assert out.column_names == ["i"]


def test_unknown_column_is_a_clear_error():
    with pytest.raises(ContractError, match="Available columns"):
        result(SourceRequest(columns=("nope",)))
    with pytest.raises(ContractError, match="Available columns"):
        result(SourceRequest(filters=(Filter("nope", "=", 1),)))


def test_no_request_returns_the_reader_unchanged():
    reader = to_reader(ROWS)
    assert apply(reader, SourceRequest(), ()) is reader


# --- pushdown ----------------------------------------------------------------


def plain():
    return ROWS


def partial(limit=None, since=None):
    return ROWS


def whole(request: SourceRequest):
    return ROWS


def named_request(request):
    return ROWS


def test_pushdown_detection():
    assert pushdown_parameters(plain) == ()
    assert pushdown_parameters(partial) == ("limit", "since")
    assert pushdown_parameters(whole) == pushdown_parameters(named_request)
    assert "filters" in pushdown_parameters(whole)


def test_pushdown_suppresses_the_fallback():
    assert result(SourceRequest(limit=3), pushed=("limit",)).num_rows == 10


def test_fallback_still_applies_to_fields_not_pushed_down():
    out = result(
        SourceRequest(limit=3, filters=(Filter("state", "=", "CA"),)), pushed=("limit",)
    )
    assert set(out.column("state").to_pylist()) == {"CA"}
    assert out.num_rows == 5


def test_limit_pushdown_is_withdrawn_when_filters_stay_local():
    request = SourceRequest(limit=4, filters=(Filter("state", "=", "CA"),))
    assert effective_pushdown(("limit",), request) == ()
    assert effective_pushdown(("limit", "filters"), request) == ("limit", "filters")
    assert effective_pushdown(("limit",), SourceRequest(limit=4)) == ("limit",)


def test_limit_composes_with_a_local_filter():
    """A source that truncated first would return one row, not four."""
    request = SourceRequest(limit=4, filters=(Filter("state", "=", "CA"),))
    pushdown = effective_pushdown(pushdown_parameters(partial), request)

    def source(limit=None):
        return ROWS[: limit or len(ROWS)]

    out = apply(to_reader(source(**bind(source, request, pushdown))), request, pushdown)
    assert out.read_all().num_rows == 4


def test_withheld_fields_are_blanked_for_a_whole_request_function():
    request = SourceRequest(limit=4, filters=(Filter("state", "=", "CA"),))
    handed = bind(whole, request, ("filters",))["request"]
    assert handed.limit is None
    assert handed.filters == request.filters


def test_bind_omits_unset_fields_so_defaults_apply():
    assert bind(partial, SourceRequest(limit=4)) == {"limit": 4}
    assert bind(partial, SourceRequest()) == {}
    assert bind(plain, SourceRequest(limit=4)) == {}


def test_bind_hands_the_whole_request_over():
    request = SourceRequest(limit=4)
    assert bind(whole, request) == {"request": request}
