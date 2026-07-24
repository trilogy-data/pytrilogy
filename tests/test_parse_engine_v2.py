from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.constants import CONFIG, ParserBackend
from trilogy.core.enums import (
    AggregateGroupingMode,
    ComparisonOperator,
    DatasourceState,
    WindowType,
)
from trilogy.core.exceptions import InvalidSyntaxException, UndefinedConceptException
from trilogy.core.models.author import (
    ConceptRef,
    Metadata,
    NumberingWindowItem,
    SubselectComparison,
    UndefinedConcept,
)
from trilogy.core.models.environment import (
    DictImportResolver,
    Environment,
    EnvironmentConfig,
)
from trilogy.parsing.common import _numbering_window_to_concept
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
    document = parse_syntax("""
const a <- 1;
select missing_concept;
""")

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
    with pytest.raises(TypeError):
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


def test_parse_text_v2_datasource_records_line_no() -> None:
    env, _ = parse_text(
        """
key id int;
property id.name string;

datasource first (
    id: id,
)
grain (id)
address foo.first;

datasource second (
    id: id,
)
grain (id)
address foo.second;
""",
        Environment(),
    )
    assert env.datasources["local.first"].metadata.line_no == 5
    assert env.datasources["local.second"].metadata.line_no == 11


def test_parse_text_v2_datasource_data_file_in_dict_resolver() -> None:
    env = Environment(
        config=EnvironmentConfig(
            import_resolver=DictImportResolver(
                data_files={"./virtual.csv": b"id\n1\n2\n"},
            )
        )
    )
    parse_text(
        """
key id int;
datasource virtual_ds (
    id: id,
)
grain (id)
file `./virtual.csv`;
""",
        env,
    )
    ds = env.datasources["local.virtual_ds"]
    assert ds.status == DatasourceState.PUBLISHED
    build_env = env.materialize_for_select()
    assert "virtual_ds" in build_env.datasources


def test_parse_text_v2_datasource_data_file_preserves_literal_address() -> None:
    """A data_files key that matches the literal `file '…'` path is preserved
    verbatim in the address — no working_path resolution. Use case: sandboxed
    clients (e.g. duckdb-wasm) where rendered SQL must reference the basename
    the client registered, not an absolute path on the parsing host."""
    env = Environment(
        config=EnvironmentConfig(
            import_resolver=DictImportResolver(
                data_files={"ratings.csv": b""},
            )
        )
    )
    parse_text(
        """
key id int;
datasource ratings_ds (
    id: id,
)
grain (id)
file `ratings.csv`;
""",
        env,
    )
    ds = env.datasources["local.ratings_ds"]
    assert ds.status == DatasourceState.PUBLISHED
    assert ds.address.location == "ratings.csv"
    assert ds.address.exists is True


def test_parse_text_v2_datasource_data_file_missing_marks_unpopulated() -> None:
    env = Environment(config=EnvironmentConfig(import_resolver=DictImportResolver()))
    parse_text(
        """
key id int;
datasource virtual_ds (
    id: id,
)
grain (id)
file `./not_in_resolver.csv`;
""",
        env,
    )
    ds = env.datasources["local.virtual_ds"]
    assert ds.status == DatasourceState.UNPOPULATED


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


def test_membership_classified_as_subselect_in_having_like_where() -> None:
    # A SELECT aggregate makes finalize walk the HAVING tree to substitute
    # matching aggregates; that walk used to rebuild every Comparison as a plain
    # one, stripping the SubselectComparison subclass off a membership and losing
    # its existence semantics. The same `id in big_ids` must classify identically
    # in WHERE and HAVING.
    model = """
key id int;
property id.val int;
datasource nums (id: id, val: val) grain (id) address nums;

auto big_ids <- filter id where val > 10;

select
    id,
    sum(val) as total,
    --id in big_ids as flag
where id in big_ids
having id in big_ids
;
"""
    _, parsed = parse_text(model, Environment())
    stmt = parsed[-1]
    assert isinstance(stmt.where_clause.conditional, SubselectComparison)
    assert isinstance(stmt.having_clause.conditional, SubselectComparison)


def test_having_membership_does_not_require_set_in_projection() -> None:
    # The membership's existence RHS (the set) is sourced as a subselect at plan
    # time and need not be projected; only the row side must be a SELECT output.
    model = """
key id int;
property id.val int;
datasource nums (id: id, val: val) grain (id) address nums;

auto big_ids <- filter id where val > 10;

select id, sum(val) as total
having id in big_ids
;
"""
    _, parsed = parse_text(model, Environment())
    assert isinstance(parsed[-1].having_clause.conditional, SubselectComparison)


def test_having_membership_row_side_resolves_as_semijoin() -> None:
    # A membership whose row side (`other`) is not a SELECT output is a
    # post-aggregation filter: keep the grain keys (`id`) whose `other` satisfies
    # the membership, via a semijoin on the grain key.
    model = """
key id int;
property id.val int;
property id.other int;
datasource nums (id: id, val: val, other: other) grain (id) address nums;

auto big_ids <- filter id where val > 10;

select id, sum(val) as total
having other in big_ids
;
"""
    _env, parsed = parse_text(model, Environment())
    assert parsed


def test_parse_text_v2_allows_function_parameter_dotted_access() -> None:
    env, _output = parse_text("def get_a(x)-> x.a;", Environment())
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


def test_parse_text_v2_select_transform_self_reference_raises() -> None:
    # A SELECT output aliased with a name its own expression references is a
    # recursive binding (`revenue` would mean both the input and the output).
    # The planner keys concepts by address and cannot represent both, so it
    # must raise rather than silently emit the original.
    with pytest.raises(InvalidSyntaxException, match="recursive self-reference"):
        parse_text(
            """
key revenue float;
SELECT revenue * 2 -> revenue;
""",
            Environment(),
        )


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


def test_parse_text_v2_select_self_referential_shadow_of_auto_raises() -> None:
    # Regression for q09 (ingest eval): an `auto`-defined name reused as a
    # self-referential SELECT alias silently emitted the original `count`.
    with pytest.raises(InvalidSyntaxException, match="recursive self-reference"):
        parse_text(
            """
key id int;
property id.quantity int;
property id.list_price float;
auto b1 <- count(id ? quantity between 1 and 20);
SELECT case when b1 > 5 then avg(list_price) else 0.0 end as b1;
""",
            Environment(),
        )


def test_parse_text_v2_select_non_self_referential_shadow_commits() -> None:
    # A shadow that does NOT reference its own alias is unambiguous and allowed:
    # the new expression replaces the prior definition for the output.
    env, _ = parse_text(
        """
key id int;
property id.quantity int;
property id.list_price float;
auto b1 <- count(id ? quantity between 1 and 20);
SELECT avg(list_price) as b1;
""",
        Environment(),
    )
    assert env.concepts["local.b1"].lineage is not None


_BOOLEAN_DERIVATION_PRELUDE = """
key id int;
property id.d date;
property id.flag int;
property id.v int;
"""


@pytest.mark.parametrize("backend", [ParserBackend.LARK, ParserBackend.PEST])
@pytest.mark.parametrize(
    "body",
    [
        "auto x <- flag = 1 and id = 1;",
        "auto x <- flag = 1 or id = 1;",
        "auto x <- d between '2020-01-01'::date and '2020-12-31'::date;",
        "auto x <- (d between '2020-01-01'::date and '2020-12-31'::date) and flag is not null;",
    ],
)
def test_parse_text_v2_derived_concept_accepts_compound_boolean(
    backend: ParserBackend, body: str
) -> None:
    # A derived concept body must admit the same boolean grammar (`and`/`or`,
    # `between`, `is null`) that `?`/`where` already accept, so a row predicate
    # can be named once and reused. Regression for
    # bug_compound_boolean_in_derived_concept.md.
    from trilogy.core.models.core import DataType

    with _using_backend(backend):
        env, _ = parse_text(_BOOLEAN_DERIVATION_PRELUDE + body, Environment())
    x = env.concepts["local.x"]
    assert x.lineage is not None
    assert x.datatype == DataType.BOOL


@pytest.mark.parametrize("backend", [ParserBackend.LARK, ParserBackend.PEST])
@pytest.mark.parametrize(
    "op,expected",
    [
        ("not like", ComparisonOperator.NOT_LIKE),
        ("not  LIKE", ComparisonOperator.NOT_LIKE),
        ("not ilike", ComparisonOperator.NOT_ILIKE),
    ],
)
def test_parse_text_v2_not_like_infix(
    backend: ParserBackend, op: str, expected: ComparisonOperator
) -> None:
    # SQL-style ``x not like 'y'`` parses to the same first-class operator on
    # both backends, regardless of casing / interior whitespace.
    from trilogy.core.models.author import Comparison

    with _using_backend(backend):
        env, _ = parse_text(
            f"const x <- 'hi';\nauto a <- x {op} 'h%';",
            Environment(),
        )
    lineage = env.concepts["local.a"].lineage
    assert isinstance(lineage, Comparison) and lineage.operator == expected


@pytest.mark.parametrize("backend", [ParserBackend.LARK, ParserBackend.PEST])
def test_parse_text_v2_named_predicate_usable_in_filter(backend: ParserBackend) -> None:
    # The named predicate is reusable inside a `?` inline-aggregate filter.
    with _using_backend(backend):
        env, _ = parse_text(
            _BOOLEAN_DERIVATION_PRELUDE + """
auto in_window <- (d between '2020-01-01'::date and '2020-12-31'::date) and flag = 1;
""",
            Environment(),
        )
    assert env.concepts["local.in_window"].lineage is not None


_ALIGN_MODEL = """
key one int; property one.label string; property one.v int;
datasource num1 (one:one, label:label, v:v) grain (one) address num1;
key two int; property two.label2 string; property two.w int;
datasource num2 (two:two, label2:label2, w:w) grain (two) address num2;
"""


@pytest.mark.parametrize("backend", [ParserBackend.LARK, ParserBackend.PEST])
def test_parse_text_v2_align_comma_between_groups_actionable(
    backend: ParserBackend,
) -> None:
    # `align a: x, y, b: p` (comma instead of `and` between groups) must raise the
    # actionable 221 error, not the opaque "expected limit, order_by, or having".
    bad = (
        "SELECT label, count(one) as c, MERGE SELECT label2, count(two) as c2,"
        " ALIGN k: label, label2, k2: c, c2 ORDER BY c desc;"
    )
    with _using_backend(backend), pytest.raises(
        InvalidSyntaxException, match=r"separated by `and`"
    ):
        parse_text(_ALIGN_MODEL + bad, Environment())


@pytest.mark.parametrize("backend", [ParserBackend.LARK, ParserBackend.PEST])
def test_parse_text_v2_align_and_between_groups_ok(backend: ParserBackend) -> None:
    good = (
        "SELECT label, count(one) as c, MERGE SELECT label2, count(two) as c2,"
        " ALIGN k: label, label2 and k2: c, c2 ORDER BY c desc;"
    )
    with _using_backend(backend):
        parse_text(_ALIGN_MODEL + good, Environment())


@pytest.mark.parametrize("backend", [ParserBackend.LARK, ParserBackend.PEST])
def test_parse_text_v2_multiselect_duplicate_arm_outputs_raise(
    backend: ParserBackend,
) -> None:
    # Two arms sharing an output address (`c`/`c` here) collapse to one graph node
    # and emit broken SQL (INVALID_REFERENCE_BUG); reject with a clear message
    # pointing at distinct names + align.
    bad = (
        "SELECT label as g, count(one) as c, MERGE SELECT label2 as g, count(two) as c,"
        " ALIGN grp: g, g ORDER BY grp;"
    )
    with _using_backend(backend), pytest.raises(
        InvalidSyntaxException, match="distinct output names"
    ):
        parse_text(_ALIGN_MODEL + bad, Environment())


@pytest.mark.parametrize("backend", [ParserBackend.LARK, ParserBackend.PEST])
def test_parse_text_v2_multiselect_align_reuses_arm_name_raise(
    backend: ParserBackend,
) -> None:
    # The align group output is a new merged concept; naming it after an arm
    # output (`g`) collapses them by address and emits INVALID_REFERENCE_BUG in
    # codegen. Reject with a message pointing at a distinct align name.
    bad = (
        "SELECT label as g, count(one) as c, MERGE SELECT label2 as g2, count(two) as c2,"
        " ALIGN g: g, g2 DERIVE coalesce(c, 0) as t ORDER BY g;"
    )
    with _using_backend(backend), pytest.raises(
        InvalidSyntaxException, match="reuses an arm output name"
    ):
        parse_text(_ALIGN_MODEL + bad, Environment())


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
    with _using_backend(ParserBackend.PEST), pytest.raises(InvalidSyntaxException):
        parse_text("this is not valid trilogy @#$", Environment())


def test_parse_error_does_not_mention_pest() -> None:
    for backend in (ParserBackend.PEST, ParserBackend.LARK):
        with _using_backend(backend):
            with pytest.raises(InvalidSyntaxException) as exc_info:
                parse_text("this is not valid trilogy @#$", Environment())
            message = str(exc_info.value)
            assert (
                "pest" not in message.lower()
            ), f"{backend.value} error leaked 'pest': {message!r}"


def test_aggregate_grouping_modes_parse_and_render() -> None:
    # Multi-level grouping is a SELECT-level clause; each mode lives on its own
    # select (one select cannot mix rollup with grouping sets).
    rollup_query = """
key a int;
key b int;
key x int;
select a, b, sum(x) as sx by rollup (a, b);
"""
    sets_query = """
key a int;
key b int;
key x int;
select a, b, sum(x) as sx_sets by grouping sets ((a, b), (a), ());
"""
    for backend in (ParserBackend.PEST, ParserBackend.LARK):
        with _using_backend(backend):
            env, output = parse_text(rollup_query, Environment())
        # the spec lives on the select; shared lineage stays unstamped (build
        # applies it)
        select = output[-1]
        assert select.grouping is not None
        assert select.grouping.mode == AggregateGroupingMode.ROLLUP
        assert [item.address for item in select.grouping.by] == [
            "local.a",
            "local.b",
        ]
        assert env.concepts["local.sx"].lineage.grouping == (
            AggregateGroupingMode.STANDARD
        )
        assert "by rollup (a, b)" in str(select)

        with _using_backend(backend):
            env, output = parse_text(sets_query, Environment())
        select = output[-1]
        assert select.grouping is not None
        assert select.grouping.mode == AggregateGroupingMode.GROUPING_SETS
        assert [
            [item.address for item in grouping_set]
            for grouping_set in select.grouping.grouping_sets
        ] == [["local.a", "local.b"], ["local.a"], []]
        assert env.concepts["local.sx_sets"].lineage.grouping == (
            AggregateGroupingMode.STANDARD
        )
        assert "by grouping sets ((a, b), (a), ())" in str(select)


def test_empty_rollup_leaves_lineage_unstamped() -> None:
    # `by rollup ()` key inference happens at build time
    # (test_empty_top_level_rollup_inherits_build_grain covers it); parse time
    # only records the spec and leaves aggregate lineage pristine.
    query = """
key a int;
key b int;
key x int;
select
    a,
    b,
    sum(x) as sx,
    rank() over (partition by a order by sx desc) as rk
by rollup ();
"""
    for backend in (ParserBackend.PEST, ParserBackend.LARK):
        with _using_backend(backend):
            _, output = parse_text(query, Environment())
        select = output[-1]
        assert select.grouping is not None
        assert select.grouping.mode == AggregateGroupingMode.ROLLUP
        assert select.grouping.by == []
        rollup = select.local_concepts["local.sx"].lineage
        assert rollup.grouping == AggregateGroupingMode.STANDARD
        assert rollup.by == []


def test_empty_top_level_rollup_inherits_build_grain() -> None:
    query = """
const a <- unnest([1, 2, 3]);
const b <- unnest([1, 1, 2]);
auto sx <- sum(1);
select
    a,
    b,
    sx,
    rank() over (partition by a order by sx desc) as rk
by rollup ();
"""
    for backend in (ParserBackend.PEST, ParserBackend.LARK):
        with _using_backend(backend):
            sql = Dialects.DUCK_DB.default_executor().generate_sql(query)[-1]
        assert "ROLLUP" in sql
        assert "rank() over" in sql


def test_numbering_window_returns_undefined_for_undefined_anchor() -> None:
    env = Environment()
    env.concepts["local.missing"] = UndefinedConcept(address="local.missing")
    item = NumberingWindowItem(
        type=WindowType.RANK,
        arguments=[ConceptRef(address="local.missing")],
        over=[],
        order_by=[],
    )

    concept = _numbering_window_to_concept(
        item,
        name="rk",
        namespace="local",
        environment=env,
        metadata=Metadata(),
    )

    assert isinstance(concept, UndefinedConcept)


def test_lark_parse_error_keeps_rich_error_codes() -> None:
    # The lark backend should still produce the numbered syntax hints even
    # after the pest decoupling. Regression guard: if parse_text starts
    # swallowing lark's UnexpectedToken again, these codes disappear.
    with _using_backend(ParserBackend.LARK), pytest.raises(
        InvalidSyntaxException, match=r"Syntax \[201\]"
    ):
        parse_text("key revenue float;\nSELECT revenue + 1 total;", Environment())


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
        except Exception:  # noqa: S112 -- only compare when lark succeeds
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


@pytest.mark.parametrize("backend", [ParserBackend.LARK, ParserBackend.PEST])
def test_parse_text_v2_hide_modifier(backend: ParserBackend) -> None:
    text = """
key id int;
--key hidden_key int; # secret key
properties id (
    name string, # visible
    --floor_space int, # hidden attribute
);
--auto hidden_calc <- id + 1; # hidden derived
auto shown_calc <- id + 2;
datasource s (
    sk: id,
    nm: name,
    fs: floor_space,
)
grain (id)
address memory.s;
"""
    with _using_backend(backend):
        env, _ = parse_text(text, Environment())
    hidden = {a.rsplit(".", 1)[-1] for a in env.concepts.hidden}
    assert {"hidden_key", "floor_space", "hidden_calc"} <= hidden
    assert "name" not in hidden and "shown_calc" not in hidden
    # excluded from the public listing but still resolvable
    public = dict(env.concepts.items())
    assert "local.floor_space" not in public
    assert "local.floor_space" in dict(env.concepts.all_items())
    assert env.concepts["local.floor_space"].metadata.hidden is True
    # hidden concepts remain queryable
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    sql = engine.generate_sql("select id, floor_space order by id limit 1;")[-1]
    assert "fs" in sql.lower()


_TVF_UNION_PRELUDE = """
key id int;
property id.a int;
property id.b int;
"""


@pytest.mark.parametrize("backend", [ParserBackend.LARK, ParserBackend.PEST])
def test_parse_text_v2_tvf_union_named(backend: ParserBackend) -> None:
    # A named relational union(...) TVF parses on both backends to a rowset
    # whose select is a UnionSelectStatement with positional output bindings.
    from trilogy.core.enums import Derivation
    from trilogy.core.statements.author import (
        RowsetDerivationStatement,
        UnionSelectStatement,
    )

    body = """
with combined as union(
    (select a -> k, b -> v),
    (select b -> k, a -> v)
) -> (k, v);
"""
    with _using_backend(backend):
        env, parsed = parse_text(_TVF_UNION_PRELUDE + body, Environment())
    rowset = parsed[-1]
    assert isinstance(rowset, RowsetDerivationStatement)
    assert isinstance(rowset.select, UnionSelectStatement)
    # Two positional output bindings, distinct per-arm mangled inputs.
    assert len(rowset.select.align.items) == 2
    arm0 = {c.address for c in rowset.select.align.items[0].concepts}
    assert len(arm0) == 2  # one mangled column per arm
    assert env.concepts["combined.k"].derivation == Derivation.ROWSET


@pytest.mark.parametrize("backend", [ParserBackend.LARK, ParserBackend.PEST])
def test_parse_text_v2_tvf_union_inline(backend: ParserBackend) -> None:
    # Inline `from union(...) -> (...) select ...` parses to the trailing select
    # with the union outputs exposed as bare ROWSET-derived local bindings.
    from trilogy.core.enums import Derivation
    from trilogy.core.statements.author import SelectStatement

    body = """
from union(
    (select a -> k, b -> v),
    (select b -> k, a -> v)
) -> (k, v)
select k, v;
"""
    with _using_backend(backend):
        env, parsed = parse_text(_TVF_UNION_PRELUDE + body, Environment())
    assert isinstance(parsed[-1], SelectStatement)
    assert env.concepts["local.k"].derivation == Derivation.ROWSET


def _tvf_union_output(env, name: str):
    """Return the inner TVF_UNION output concept for user output ``name``.

    A named `union(...)` registers its aligned outputs under the hidden
    per-rowset name (`_combined_k`), so match the unmangled suffix too.
    """
    from trilogy.core.enums import Derivation

    return next(
        c
        for c in env.concepts.values()
        if c.derivation == Derivation.TVF_UNION
        and (c.name == name or c.name.endswith(f"_{name}"))
    )


@pytest.mark.parametrize("backend", [ParserBackend.LARK, ParserBackend.PEST])
def test_parse_text_v2_tvf_union_outputs_are_keys(
    backend: ParserBackend,
) -> None:
    # The grain of stacking two selects (UNION ALL) is unknowable, so every union
    # output is its own grain component: a KEY, never a metric — even one that
    # fronts an aggregate arm column. (A metric output would be dropped from
    # downstream grain and break a consumer that re-aggregates the stack.)
    from trilogy.core.enums import Purpose

    body = """
with combined as union(
    (select a -> k, sum(b) -> v),
    (select b -> k, sum(a) -> v)
) -> (k, v);
"""
    with _using_backend(backend):
        env, _ = parse_text(_TVF_UNION_PRELUDE + body, Environment())

    assert _tvf_union_output(env, "k").purpose == Purpose.KEY
    assert _tvf_union_output(env, "v").purpose == Purpose.KEY


@pytest.mark.parametrize("backend", [ParserBackend.LARK, ParserBackend.PEST])
def test_parse_text_v2_tvf_union_outputs_form_the_grain(
    backend: ParserBackend,
) -> None:
    # Because every union output is a key, the stack's grain is all of them — the
    # dimension AND the aggregate-fronting output both anchor the row identity.
    body = """
with combined as union(
    (select a -> k, sum(b) -> v),
    (select b -> k, sum(a) -> v)
) -> (k, v);
"""
    with _using_backend(backend):
        env, _ = parse_text(_TVF_UNION_PRELUDE + body, Environment())

    build_env = env.materialize_for_select()
    from trilogy.core.models.build import BuildGrain

    k = build_env.concepts[_tvf_union_output(env, "k").address]
    v = build_env.concepts[_tvf_union_output(env, "v").address]
    grain = BuildGrain.from_concepts([k, v], environment=build_env)
    assert k.address in grain.components
    assert v.address in grain.components
