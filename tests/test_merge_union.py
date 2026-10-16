"""`merge subset|union|equal a = b`: the relation spelling of a global merge,
shared with query-scoped joins. Planner parity with `union join` lives in
tests/join_matrix/test_merge_union_parity.py; this file pins the syntax, the
stored tuple, rendering, contradiction linting and persistence."""

from collections.abc import Iterator
from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.constants import CONFIG, ParserBackend
from trilogy.core.domain_graph import (
    DomainRelation,
    EdgeScope,
    declared_edge_from_join,
)
from trilogy.core.enums import JoinType
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.fingerprint import build_environment_fingerprint
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import MergeStatementV2
from trilogy.parsing.render import Renderer
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxToken

KEYS = """
key a int;
key b int;
key c int;
key d int;
"""


@pytest.fixture(params=[ParserBackend.PEST, ParserBackend.LARK])
def backend(request) -> Iterator[ParserBackend]:
    prior = CONFIG.parser_backend
    CONFIG.parser_backend = request.param
    try:
        yield request.param
    finally:
        CONFIG.parser_backend = prior


def _parse(text: str) -> tuple[Environment, list]:
    env = Environment()
    _, statements = env.parse(KEYS + text)
    return env, statements


def _merges(text: str) -> list[tuple[str, str, JoinType]]:
    return _parse(text)[0].merges


@pytest.mark.parametrize(
    "text,expected",
    [
        ("merge union a = b;", ("local.a", "local.b", JoinType.FULL)),
        ("MERGE UNION a = b;", ("local.a", "local.b", JoinType.FULL)),
        ("merge union a=b;", ("local.a", "local.b", JoinType.FULL)),
        ("merge equal a = b;", ("local.a", "local.b", JoinType.EQUAL)),
        ("merge a into b;", ("local.a", "local.b", JoinType.EQUAL)),
        ("merge subset a = b;", ("local.b", "local.a", JoinType.LEFT_OUTER)),
        ("merge a into ~b;", ("local.b", "local.a", JoinType.LEFT_OUTER)),
    ],
)
def test_merge_relation_tuple(backend: ParserBackend, text: str, expected: tuple):
    assert _merges(text) == [expected]


def test_merge_union_statement(backend: ParserBackend):
    _, statements = _parse("merge union a = b;")
    merge = statements[-1]
    assert isinstance(merge, MergeStatementV2)
    assert merge.relation is JoinType.UNION
    assert merge.join_relation is JoinType.UNION
    assert [s.address for s in merge.sources] == ["local.a"]
    assert merge.targets["local.a"].address == "local.b"


def test_into_form_relation_defaults(backend: ParserBackend):
    _, statements = _parse("merge a into b;\nmerge b into ~c;")
    plain, partial = statements[-2], statements[-1]
    assert plain.relation is None and plain.join_relation is JoinType.EQUAL
    assert partial.relation is None and partial.join_relation is JoinType.SUBSET


def test_relation_keyword_is_still_a_name_before_into(backend: ParserBackend):
    env = Environment()
    env.parse(
        "key union int;\nkey subset_id int;\nkey b int;\n"
        "merge union into b;\nmerge subset_id into ~b;"
    )
    assert env.merges == [
        ("local.union", "local.b", JoinType.EQUAL),
        ("local.b", "local.subset_id", JoinType.LEFT_OUTER),
    ]


def test_merge_union_wildcard(backend: ParserBackend, tmp_path: Path):
    (tmp_path / "side.preql").write_text("key id int;\nkey code int;\n")
    env = Environment(working_path=tmp_path)
    env.parse("import side as x;\nimport side as y;\nmerge union x.* = y.*;")
    assert sorted(m for m in env.merges if m[0].startswith("x.")) == [
        ("x.code", "y.code", JoinType.FULL),
        ("x.id", "y.id", JoinType.FULL),
    ]


@pytest.mark.parametrize("op", [">", "!=", "<="])
def test_merge_relation_requires_equals(backend: ParserBackend, op: str):
    with pytest.raises(Exception, match="with `=`, not"):
        _parse(f"merge union a {op} b;")


@pytest.mark.parametrize("parse", [parse_lark, parse_pest])
@pytest.mark.parametrize("relation", ["left", "full", "inner", "cross"])
def test_join_only_types_are_not_merge_relations(parse, relation: str):
    with pytest.raises(InvalidSyntaxException):
        parse(KEYS + f"merge {relation} a = b;")


@pytest.mark.parametrize("parse", [parse_lark, parse_pest])
def test_both_backends_emit_the_merge_relation_token(parse):
    document = parse(KEYS + "merge union a = b;")
    merges = [n for n in _walk_nodes(document.tree) if n.name == "merge_statement"]
    assert len(merges) == 1
    tokens = [
        (t.name, t.value) for t in merges[0].children if isinstance(t, SyntaxToken)
    ]
    assert tokens == [
        ("MERGE_RELATION", "union"),
        ("WILDCARD_IDENTIFIER", "a"),
        ("COMPARISON_OPERATOR", "="),
        ("WILDCARD_IDENTIFIER", "b"),
    ]


def _walk_nodes(node: SyntaxNode) -> Iterator[SyntaxNode]:
    yield node
    for child in node.children:
        if isinstance(child, SyntaxNode):
            yield from _walk_nodes(child)


def test_undefined_merge_union_source(backend: ParserBackend):
    with pytest.raises(Exception, match="missing"):
        _parse("merge union missing = b;")


@pytest.mark.parametrize(
    "first,second",
    [
        ("merge union a = b;", "merge a into b;"),
        ("merge a into b;", "merge union a = b;"),
        ("merge union a = b;", "merge subset a = b;"),
        ("merge subset a = b;", "merge union a = b;"),
        ("merge union a = b;", "merge b into ~a;"),
        ("merge union a = b;\nmerge b into c;", "merge subset a = c;"),
    ],
)
def test_union_contradicts_a_containment(backend: ParserBackend, first, second):
    with pytest.raises(InvalidSyntaxException, match="incomparable"):
        _parse(f"{first}\n{second}")


def test_union_beside_an_identity_in_one_group_is_consistent(backend: ParserBackend):
    assert _merges("merge a into b;\nmerge union b = c;") == [
        ("local.a", "local.b", JoinType.EQUAL),
        ("local.b", "local.c", JoinType.FULL),
    ]


def test_repeated_union_is_deduplicated(backend: ParserBackend):
    assert _merges("merge union a = b;\nmerge union a = b;") == [
        ("local.a", "local.b", JoinType.FULL)
    ]


def test_render_merge_union_statement(backend: ParserBackend):
    _, statements = _parse("merge union a = b;\nmerge subset b = c;")
    renderer = Renderer()
    assert renderer.to_string(statements[-2]) == "merge union a = b;"
    assert renderer.to_string(statements[-1]) == "merge subset b = c;"


def test_render_merge_union_wildcard():
    rendered = Renderer().to_string(
        MergeStatementV2(
            sources=[],
            targets={},
            source_wildcard="x",
            target_wildcard="y",
            relation=JoinType.UNION,
        )
    )
    assert rendered == "merge union x.* = y.*;"


def test_environment_render_round_trips_merges(backend: ParserBackend):
    env, _ = _parse("merge union a = b;\nmerge b into c;\nmerge subset a = d;")
    rendered = Renderer().to_string(env)
    assert "merge union a = b;" in rendered
    assert "merge b into c;" in rendered
    assert "merge a into ~d;" in rendered
    reparsed = Environment()
    reparsed.parse(rendered)
    assert reparsed.merges == env.merges


def test_cache_round_trip_keeps_union(tmp_path: Path):
    env, _ = _parse("merge union a = b;\nmerge b into c;")
    env.working_path = tmp_path
    restored = Environment.from_cache(env.to_cache())
    assert restored is not None
    assert restored.merges == env.merges


def test_fingerprint_separates_union_from_identity():
    union, _ = _parse("merge union a = b;")
    equal, _ = _parse("merge a into b;")
    assert (
        build_environment_fingerprint(union).extras
        != build_environment_fingerprint(equal).extras
    )


def test_union_tuple_declares_incomparable_at_every_scope():
    for scope in EdgeScope:
        edge = declared_edge_from_join("a", "b", JoinType.FULL, scope)
        assert edge is not None
        assert edge.relation is DomainRelation.INCOMPARABLE
        assert edge.null_extends is (scope is not EdgeScope.ROWSET)
        assert not edge.is_identity_merge


@pytest.mark.parametrize("join_type", [JoinType.EQUAL, JoinType.LEFT_OUTER])
def test_global_identity_merges_do_not_null_extend(join_type: JoinType):
    edge = declared_edge_from_join("a", "b", join_type, EdgeScope.GLOBAL)
    assert edge is not None
    assert edge.is_identity_merge
    assert not edge.null_extends


def test_import_carries_union_merge(backend: ParserBackend, tmp_path: Path):
    (tmp_path / "model.preql").write_text(KEYS + "merge union a = b;\n")
    env = Environment(working_path=tmp_path)
    env.parse("import model as m;")
    assert ("m.a", "m.b", JoinType.FULL) in env.merges


def test_executing_merge_union_registers_it():
    env, statements = _parse("merge union a = b;")
    statement = statements[-1]
    assert isinstance(statement, MergeStatementV2)
    env.merges.clear()
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    executor.execute_query(statement)
    assert executor.environment.merges == [("local.a", "local.b", JoinType.FULL)]
