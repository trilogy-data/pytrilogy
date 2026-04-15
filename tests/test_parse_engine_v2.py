from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from trilogy.constants import CONFIG, ParserBackend
from trilogy.core.enums import DatasourceState
from trilogy.core.exceptions import InvalidSyntaxException, UndefinedConceptException
from trilogy.core.models.environment import Environment
from trilogy.parsing.parse_engine_v2 import SyntaxNode, parse_syntax, parse_text
from trilogy.parsing.v2.syntax import SyntaxElement, SyntaxNodeKind, SyntaxTokenKind


@contextmanager
def _using_backend(backend: ParserBackend) -> Iterator[None]:
    prev = CONFIG.parser_backend
    CONFIG.parser_backend = backend
    try:
        yield
    finally:
        CONFIG.parser_backend = prev


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
    from trilogy.constants import CONFIG, ParserBackend

    prev = CONFIG.parser_backend
    CONFIG.parser_backend = ParserBackend.LARK
    try:
        document = parse_syntax("# hello\nconst a <- 1;")
    finally:
        CONFIG.parser_backend = prev

    comment = document.forms[0]
    assert comment.kind == SyntaxTokenKind.COMMENT
    const_value = next(
        node
        for node in walk_nodes(document.forms[1])
        if node.kind == SyntaxNodeKind.INT_LITERAL
    )
    assert const_value.kind == SyntaxNodeKind.INT_LITERAL
    assert const_value.children[0].kind == SyntaxTokenKind.INT_LITERAL_PART


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


def test_v2_architecture_avoids_lark_shims() -> None:
    combined = "\n".join(path.read_text() for path in V2_PATH.glob("*.py"))

    assert "to_lark" not in combined
    assert "Transformer" not in combined


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


def test_parse_text_v2_select_transform_shadowing_does_not_recurse() -> None:
    # Regression: a select transform whose output shadows a non-SELECT
    # concept must not stage a pending entry — the overlay would then
    # make set_select_grain see the new concept's lineage as self-referential.
    env, _ = parse_text(
        """
type money float;
key revenue float::money;
def revenue_times_2(revenue) -> revenue * 2;

with scaled as
SELECT
    @revenue_times_2(revenue) -> revenue
;
""",
        Environment(),
    )
    original_revenue = env.concepts["local.revenue"]
    assert original_revenue.lineage is None
    scaled_revenue = env.concepts["scaled.revenue"]
    assert scaled_revenue.lineage is not None


def test_parse_text_v2_select_transform_inline_shadowing() -> None:
    env, _ = parse_text(
        """
key revenue float;
SELECT revenue * 2 -> revenue;
""",
        Environment(),
    )
    assert env.concepts["local.revenue"].lineage is None


def test_parse_text_v2_select_as_definition_new_alias_commits() -> None:
    env, _ = parse_text(
        """
key revenue float;
SELECT revenue * 2 -> doubled;
""",
        Environment(),
    )
    assert "local.doubled" in env.concepts
    assert env.concepts["local.doubled"].lineage is not None


def test_parse_text_v2_duplicate_select_outputs_raise() -> None:
    with pytest.raises(InvalidSyntaxException, match="Duplicate select output"):
        parse_text(
            """
key revenue float;
SELECT
    revenue -> x,
    revenue * 2 -> x
;
""",
            Environment(),
        )


def test_parse_text_v2_rowset_output_forward_reference() -> None:
    env, _ = parse_text(
        """
key order_id int;
property order_id.store_id int;
property order_id.revenue float;

datasource orders (
    order_id: order_id,
    store_id: store_id,
    revenue: revenue,
)
grain (order_id)
address orders;

rowset even_orders <- select order_id, store_id, revenue where (order_id % 2) = 0;
auto even_order_store_revenue <- sum(even_orders.revenue);
""",
        Environment(),
    )
    assert "even_orders.revenue" in env.concepts
    assert "even_orders.local.revenue" not in env.concepts
    assert "local.even_order_store_revenue" in env.concepts


def test_parse_text_v2_non_ascii_source_roundtrips() -> None:
    # Pest reports byte offsets; the converter must translate them to char indices
    # or any file containing multibyte UTF-8 blows up with IndexError on comment
    # injection and produces wrong line/column for every later token.
    env, output = parse_text(
        "# comment with non-ascii: café ☕ π\nkey revenue float;\nauto doubled <- revenue * 2;\n",
        Environment(),
    )
    assert "local.revenue" in env.concepts
    assert "local.doubled" in env.concepts
    assert env.concepts["local.doubled"].lineage is not None
    assert len(output) >= 2


def test_pest_parse_error_raises_invalid_syntax_exception() -> None:
    with _using_backend(ParserBackend.PEST):
        with pytest.raises(InvalidSyntaxException):
            parse_text("this is not valid trilogy @#$", Environment())


def test_lark_parse_error_keeps_rich_error_codes() -> None:
    # The lark backend should still produce the numbered syntax hints even
    # after the pest decoupling. Regression guard: if parse_text starts
    # swallowing lark's UnexpectedToken again, these codes disappear.
    with _using_backend(ParserBackend.LARK):
        with pytest.raises(InvalidSyntaxException, match=r"Syntax \[201\]"):
            parse_text("key revenue float;\nSELECT revenue + 1;", Environment())


def _corpus_files() -> list[Path]:
    root = Path(__file__).parents[1]
    files: list[Path] = []
    files.extend(sorted(root.glob("trilogy/std/*.preql")))
    files.extend(sorted(root.glob("tests/modeling/**/*.preql")))
    return files


def test_lark_pest_corpus_parity() -> None:
    # Guards against pest regressions. If lark fails (pre-existing grammar gaps,
    # files that can't be parsed standalone, etc.) we can't compare — skip.
    # Only complain when pest drifts from or underperforms a successful lark run.
    files = _corpus_files()
    assert files, "no corpus files discovered — glob patterns drifted"

    mismatches: list[str] = []
    compared = 0
    for path in files:
        try:
            text = path.read_text()
        except OSError:
            continue
        try:
            with _using_backend(ParserBackend.LARK):
                env_lark, _ = parse_text(text, Environment(working_path=path.parent))
        except Exception:
            continue
        try:
            with _using_backend(ParserBackend.PEST):
                env_pest, _ = parse_text(text, Environment(working_path=path.parent))
        except Exception as e:
            mismatches.append(f"{path}: pest failed while lark succeeded: {e}")
            continue
        compared += 1
        lark_concepts = set(env_lark.concepts.keys())
        pest_concepts = set(env_pest.concepts.keys())
        if lark_concepts != pest_concepts:
            only_lark = sorted(lark_concepts - pest_concepts)[:5]
            only_pest = sorted(pest_concepts - lark_concepts)[:5]
            mismatches.append(
                f"{path}: concept drift lark_only={only_lark} pest_only={only_pest}"
            )
        lark_ds = set(env_lark.datasources.keys())
        pest_ds = set(env_pest.datasources.keys())
        if lark_ds != pest_ds:
            mismatches.append(
                f"{path}: datasource drift lark={sorted(lark_ds)} pest={sorted(pest_ds)}"
            )
    assert compared > 0, "corpus parity compared zero files"
    assert not mismatches, "\n".join(mismatches)
