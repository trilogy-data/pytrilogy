from pytest import raises

from trilogy.core.models.environment import Environment
from trilogy.parsing.parse_engine import parse_text as parse_text_v1
from trilogy.parsing.parse_engine_v2 import SyntaxNode, parse_syntax, parse_text
from trilogy.parsing.v2.hydration import UnsupportedSyntaxError


def test_parse_syntax_only_returns_syntax() -> None:
    document = parse_syntax(
        """
const a <- 1;
select missing_concept;
"""
    )

    assert isinstance(document.tree, SyntaxNode)
    assert document.tree.name == "start"
    assert all(isinstance(form, SyntaxNode) for form in document.forms)


def test_parse_text_v2_matches_v1_for_native_concept_statements() -> None:
    text = """
key id int;
property id.name string;
const name_count <- 1;
auto adjusted_count <- name_count + 1;
show concepts;
"""

    env_v1, output_v1 = parse_text_v1(text, Environment())
    env_v2, output_v2 = parse_text(text, Environment())

    assert [type(item) for item in output_v2] == [type(item) for item in output_v1]
    assert set(env_v2.concepts.keys()) == set(env_v1.concepts.keys())
    assert env_v2.concepts["local.adjusted_count"].lineage


def test_parse_text_v2_marks_unported_statements_explicitly() -> None:
    with raises(UnsupportedSyntaxError, match="select_item"):
        parse_text("select missing_concept;", Environment())
