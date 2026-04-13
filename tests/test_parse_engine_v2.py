from collections.abc import Iterator
from pathlib import Path

import pytest

from trilogy.core.enums import DatasourceState
from trilogy.core.exceptions import UndefinedConceptException
from trilogy.core.models.environment import Environment
from trilogy.parsing.parse_engine import parse_text as parse_text_v1
from trilogy.parsing.parse_engine_v2 import SyntaxNode, parse_syntax, parse_text
from trilogy.parsing.v2.syntax import SyntaxElement, SyntaxNodeKind, SyntaxTokenKind

V2_PATH = Path(__file__).parents[1] / "trilogy" / "parsing" / "v2"


def walk_nodes(element: SyntaxElement) -> Iterator[SyntaxNode]:
    if isinstance(element, SyntaxNode):
        yield element
        for child in element.children:
            yield from walk_nodes(child)


def test_parse_syntax_only_returns_syntax() -> None:
    document = parse_syntax(
        """
const a <- 1;
select missing_concept;
"""
    )

    assert isinstance(document.tree, SyntaxNode)
    assert document.tree.name == "start"
    assert document.tree.kind == SyntaxNodeKind.DOCUMENT
    assert all(isinstance(form, SyntaxNode) for form in document.forms)
    assert document.forms[0].kind == SyntaxNodeKind.BLOCK
    assert document.forms[1].kind == SyntaxNodeKind.BLOCK


def test_parse_syntax_translates_lark_token_names() -> None:
    document = parse_syntax("# hello\nconst a <- 1;")

    comment = document.forms[0]
    assert comment.kind == SyntaxTokenKind.COMMENT
    const_value = next(
        node
        for node in walk_nodes(document.forms[1])
        if node.kind == SyntaxNodeKind.INT_LITERAL
    )
    assert const_value.kind == SyntaxNodeKind.INT_LITERAL
    assert const_value.children[0].kind == SyntaxTokenKind.INT_LITERAL_PART


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


def test_parse_text_v2_supports_type_declaration() -> None:
    env, output = parse_text("type test int;", Environment())
    assert "test" in env.data_types
    assert len(output) == 1


def test_parse_text_v2_type_declaration_propagates_to_concepts() -> None:
    env, _ = parse_text(
        "type money float;\nkey revenue float::money;",
        Environment(),
    )
    assert "money" in env.data_types
    concept = env.concepts["local.revenue"]
    datatype = concept.datatype
    # TraitDataType wraps the base type with the money trait
    assert getattr(datatype, "traits", None) == ["money"]


def test_parse_text_v2_type_declaration_rolls_back_on_failure() -> None:
    env = Environment()
    with pytest.raises(Exception):
        parse_text(
            "type money float;\nkey revenue float::missing_trait;",
            env,
        )
    assert "money" not in env.data_types
    assert "missing_trait" not in env.data_types


def test_v2_architecture_avoids_lark_or_v1_shims() -> None:
    combined = "\n".join(path.read_text() for path in V2_PATH.glob("*.py"))

    assert "to_lark" not in combined
    assert "Transformer" not in combined
    for line in combined.splitlines():
        if "trilogy.parsing.parse_engine" in line:
            assert (
                "parse_engine_v2" in line
            ), f"v2 code references v1 parse_engine: {line.strip()}"


def test_v2_node_hydrators_use_syntax_nodes_not_hydrated_args() -> None:
    combined = "\n".join(path.read_text() for path in V2_PATH.glob("*.py"))

    assert "NodeHydrator = Callable[[SyntaxNode" in combined


def test_v2_environment_update_names_record_and_apply_semantics() -> None:
    combined = "\n".join(path.read_text() for path in V2_PATH.glob("*.py"))

    assert "SemanticState" in combined
    assert "RecordingEnvironmentUpdate" not in combined
    assert "EnvironmentUpdate" not in combined


@pytest.mark.parametrize(
    "remote_path",
    [
        "gs://bucket/data.parquet",
        "gcs://bucket/data.parquet",
        "s3://bucket/data.parquet",
        "abfs://container/data.parquet",
        "abfss://container/data.parquet",
        "https://example.com/data.parquet",
    ],
)
def test_parse_text_v2_datasource_preserves_remote_file_address(
    remote_path: str,
) -> None:
    env, _ = parse_text(
        f"""
key id int;
datasource remote (
    id: id,
)
grain (id)
file `{remote_path}`;
""",
        Environment(),
    )
    ds = env.datasources["local.remote"]
    assert ds.address.location == remote_path


def test_parse_text_v2_datasource_status_unpublished() -> None:
    env, _ = parse_text(
        """
key id int;
datasource hidden (
    id: id,
)
grain (id)
address some_table
state unpublished;
""",
        Environment(),
    )
    ds = env.datasources["local.hidden"]
    assert ds.status == DatasourceState.UNPUBLISHED
    build_env = env.materialize_for_select()
    assert "local.hidden" not in build_env.datasources


def test_parse_text_v2_rejects_property_on_missing_parent() -> None:
    with pytest.raises(UndefinedConceptException):
        parse_text("property missing_parent.foo string;", Environment())


def test_parse_text_v2_rejects_auto_with_missing_dependency() -> None:
    with pytest.raises(UndefinedConceptException):
        parse_text("auto bad <- missing_ref + 1;", Environment())


def test_parse_text_v2_allows_function_parameter_dotted_access() -> None:
    env, output = parse_text("def get_a(x)-> x.a;", Environment())
    assert "get_a" in env.functions


def test_parse_text_v2_allows_forward_reference_between_top_level_concepts() -> None:
    env, _ = parse_text(
        """
auto b <- a + 1;
key a int;
""",
        Environment(),
    )
    assert "local.a" in env.concepts
    assert "local.b" in env.concepts
    assert env.concepts["local.b"].lineage is not None
