"""Parse, render, and validation behavior of `then where` staged filters.

`where A then where B select ...` is a staged filter chain: stage N's
aggregates/windows compute over only the rows passing stages 1..N-1, and the
final row gate is the AND of all stages. Execution semantics are covered in
test_then_where_execution.py.
"""

from collections.abc import Iterator
from contextlib import contextmanager

import pytest

from trilogy import parse
from trilogy.constants import CONFIG, ParserBackend
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.statements.author import SelectStatement
from trilogy.parsing.render import Renderer

BACKENDS = [ParserBackend.LARK, ParserBackend.PEST]

MODEL = """key id int;
property id.cat string;
property id.val int;

datasource d ( id, cat, val ) grain (id)
query '''select 1 as id, 'a' as cat, 5 as val''';
"""


@contextmanager
def _using_backend(backend: ParserBackend) -> Iterator[None]:
    prev = CONFIG.parser_backend
    CONFIG.parser_backend = backend
    try:
        yield
    finally:
        CONFIG.parser_backend = prev


def _select(text: str) -> SelectStatement:
    _, statements = parse(text)
    return next(s for s in statements if isinstance(s, SelectStatement))


@pytest.mark.parametrize("backend", BACKENDS)
def test_staged_where_parses_to_ordered_stages(backend: ParserBackend) -> None:
    with _using_backend(backend):
        select = _select(MODEL + """
where cat = 'a'
then where sum(val) > 10
select id;
""")
    assert len(select.where_clauses) == 2
    assert "cat" in str(select.where_clauses[0])
    assert "sum" in str(select.where_clauses[1])
    assert select.where_clause is not None
    # combined clause is the AND fold of the stages
    assert str(select.where_clauses[0]) in str(select.where_clause)
    assert str(select.where_clauses[1]) in str(select.where_clause)


@pytest.mark.parametrize("backend", BACKENDS)
def test_flat_where_is_single_stage(backend: ParserBackend) -> None:
    with _using_backend(backend):
        select = _select(MODEL + "\nwhere cat = 'a' select id;")
    assert len(select.where_clauses) == 1
    assert str(select.where_clauses[0]) == str(select.where_clause)


@pytest.mark.parametrize("backend", BACKENDS)
def test_three_stage_chain(backend: ParserBackend) -> None:
    with _using_backend(backend):
        select = _select(MODEL + """
where cat = 'a'
then where val > 1
then where sum(val) by cat > 10
select id;
""")
    assert len(select.where_clauses) == 3


@pytest.mark.parametrize("backend", BACKENDS)
def test_case_then_inside_stage(backend: ParserBackend) -> None:
    # CASE ... THEN ... END inside a stage must not consume the stage separator
    with _using_backend(backend):
        select = _select(MODEL + """
where case when cat = 'a' then 1 else 0 end = 1
then where sum(val) by cat > 10
select id;
""")
    assert len(select.where_clauses) == 2


@pytest.mark.parametrize("backend", BACKENDS)
def test_identifier_containing_then_prefix(backend: ParserBackend) -> None:
    # a concept whose name starts with `then` must not lex as the separator
    model = MODEL + "property id.then_flag int;\n"
    with _using_backend(backend):
        select = _select(model + "\nwhere then_flag = 1 select id;")
    assert len(select.where_clauses) == 1


@pytest.mark.parametrize("backend", BACKENDS)
def test_second_positional_where_slot_still_ands(backend: ParserBackend) -> None:
    # pre-`select` and post-select-list wheres AND into one stage (existing
    # behavior)
    with _using_backend(backend):
        select = _select(
            MODEL + "\nwhere cat = 'a' select id where val > 1 order by id asc;"
        )
    assert len(select.where_clauses) == 1
    combined = str(select.where_clause)
    assert "cat" in combined and "val" in combined


@pytest.mark.parametrize("backend", BACKENDS)
def test_staged_chain_conflicts_with_second_slot(backend: ParserBackend) -> None:
    with _using_backend(backend), pytest.raises(
        Exception, match="consolidate into one staged where"
    ):
        _select(MODEL + """
where cat = 'a' then where sum(val) > 10
select id
where val > 1;
""")


@pytest.mark.parametrize("backend", BACKENDS)
def test_render_round_trip_preserves_stages(backend: ParserBackend) -> None:
    query = """
where cat = 'a'
then where val > 1
then where sum(val) by cat > 10
select
    id,
;"""
    with _using_backend(backend):
        env, statements = parse(MODEL + query)
        select = next(s for s in statements if isinstance(s, SelectStatement))
        rendered = Renderer(environment=env).to_string(select)
        assert "then where" in rendered
        _, reparsed_statements = parse(MODEL + "\n" + rendered)
        reparsed = next(
            s for s in reparsed_statements if isinstance(s, SelectStatement)
        )
    assert len(reparsed.where_clauses) == 3
    assert [str(w) for w in reparsed.where_clauses] == [
        str(w) for w in select.where_clauses
    ]


@pytest.mark.parametrize("backend", BACKENDS)
def test_cross_row_earlier_stage_before_cross_row_stage_rejected(
    backend: ParserBackend,
) -> None:
    with _using_backend(backend), pytest.raises(
        InvalidSyntaxException, match="not yet supported"
    ):
        _select(MODEL + """
where sum(val) by cat > 5
then where count(id) by cat > 0
select id;
""")


@pytest.mark.parametrize("backend", BACKENDS)
def test_cross_row_earlier_stage_with_scalar_later_stage_allowed(
    backend: ParserBackend,
) -> None:
    with _using_backend(backend):
        select = _select(MODEL + """
where sum(val) by cat > 5
then where cat = 'a'
select id;
""")
    assert len(select.where_clauses) == 2
