import sys
from datetime import date, datetime
from pathlib import Path, PurePosixPath, PureWindowsPath

import pytest

from trilogy.constants import DEFAULT_NAMESPACE, VIRTUAL_CONCEPT_PREFIX
from trilogy.core.enums import (
    AddressType,
    BooleanOperator,
    ComparisonOperator,
    DatasourceState,
    FunctionType,
    IOType,
    Modifier,
    Ordering,
    PersistMode,
    Purpose,
)
from trilogy.core.models.author import (
    AlignClause,
    AlignItem,
    CaseElse,
    CaseWhen,
    Comment,
    Comparison,
    Concept,
    ConceptRef,
    Conditional,
    Function,
    Grain,
    MultiSelectLineage,
    OrderBy,
    OrderItem,
    RowsetItem,
    WhereClause,
)
from trilogy.core.models.core import (
    ArrayType,
    DataType,
    ListWrapper,
    NumericType,
    TupleWrapper,
)
from trilogy.core.models.datasource import Address, ColumnAssignment, Datasource
from trilogy.core.models.environment import Environment, Import
from trilogy.core.statements.author import (
    ConceptDeclarationStatement,
    CopyStatement,
    ImportStatement,
    KeyMergeStatement,
    MergeStatementV2,
    MultiSelectStatement,
    PersistStatement,
    RawSQLStatement,
    RowsetDerivationStatement,
    SelectItem,
    SelectStatement,
)
from trilogy.parsing.render import Renderer, render_environment, render_query


def test_basic_query(test_environment):
    query = SelectStatement(
        selection=[test_environment.concepts["order_id"]],
        where_clause=None,
        order_by=OrderBy(
            items=[
                OrderItem(
                    expr=test_environment.concepts["order_id"].reference,
                    order=Ordering.ASCENDING,
                )
            ]
        ),
    )

    string_query = render_query(query)
    assert string_query == """select
    order_id,
order by
    order_id asc
;"""


def test_window_over(test_environment):
    env = Environment()
    _, parsed = env.parse("""
key order_id int;

datasource orders (
    order_id: order_id
)
grain (order_id)
address memory.orders;

select max(order_id) by order_id -> test;

""")
    string_query = Renderer().to_string(parsed[-1])
    assert string_query == """select
    max(order_id) by order_id as test,
;"""


def test_multi_select(test_environment):
    query = MultiSelectStatement(
        namespace=DEFAULT_NAMESPACE,
        selects=[
            SelectStatement(
                selection=[test_environment.concepts["order_id"]],
                where_clause=None,
            ),
            SelectStatement(
                selection=[test_environment.concepts["order_id"]],
                where_clause=None,
            ),
        ],
        derived_concepts=[test_environment.concepts["order_id"]],
        align=AlignClause(
            items=[
                AlignItem(
                    alias="merge", concepts=[test_environment.concepts["order_id"]]
                )
            ]
        ),
        order_by=OrderBy(
            items=[
                OrderItem(
                    expr=test_environment.concepts["order_id"],
                    order=Ordering.ASCENDING,
                )
            ]
        ),
    )

    string_query = render_query(query)
    assert string_query == """select
    order_id,
merge
select
    order_id,
align
    merge: order_id
order by
    order_id asc
;""", string_query


def test_full_query(test_environment):
    query = SelectStatement(
        selection=[test_environment.concepts["order_id"]],
        where_clause=WhereClause(
            conditional=Conditional(
                left=Comparison(
                    left=test_environment.concepts["order_id"],
                    right=123,
                    operator=ComparisonOperator.EQ,
                ),
                right=Comparison(
                    left=test_environment.concepts["order_id"],
                    right=456,
                    operator=ComparisonOperator.EQ,
                ),
                operator=BooleanOperator.OR,
            ),
        ),
        order_by=OrderBy(
            items=[
                OrderItem(
                    expr=test_environment.concepts["order_id"],
                    order=Ordering.ASCENDING,
                )
            ]
        ),
    )

    string_query = render_query(query)
    assert string_query == """where
    order_id = 123 or order_id = 456
select
    order_id,
order by
    order_id asc
;"""


def test_environment_rendering(test_environment):
    rendered = render_environment(test_environment)

    assert "address tblRevenue" in rendered


def test_persist(test_environment: Environment):
    select = SelectStatement(
        selection=[test_environment.concepts["order_id"]],
        where_clause=None,
        order_by=OrderBy(
            items=[
                OrderItem(
                    expr=test_environment.concepts["order_id"],
                    order=Ordering.ASCENDING,
                )
            ]
        ),
    )
    query = PersistStatement(
        select=select,
        datasource=select.to_datasource(
            namespace=test_environment.namespace,
            name="test",
            address=Address(location="tbl_test"),
            environment=test_environment,
        ),
    )

    string_query = render_query(query)
    assert string_query == """overwrite test into tbl_test from select
    order_id,
order by
    order_id asc
;"""

    query = PersistStatement(
        select=select,
        datasource=select.to_datasource(
            namespace=test_environment.namespace,
            name="test",
            address=Address(location="tbl_test"),
            environment=test_environment,
        ),
        partition_by=[test_environment.concepts["order_id"].reference],
    )

    string_query = render_query(query)
    assert string_query == """overwrite test into tbl_test by order_id from select
    order_id,
order by
    order_id asc
;"""

    query = PersistStatement(
        select=select,
        datasource=select.to_datasource(
            namespace=test_environment.namespace,
            name="test",
            address=Address(location="tbl_test"),
            environment=test_environment,
        ),
        partition_by=[test_environment.concepts["order_id"].reference],
        persist_mode=PersistMode.APPEND,
    )

    string_query = render_query(query)
    assert string_query == """append test into tbl_test by order_id from select
    order_id,
order by
    order_id asc
;"""


def test_render_select_item(test_environment: Environment):
    test = Renderer().to_string(
        SelectItem(
            content=test_environment.concepts["order_id"], modifiers=[Modifier.HIDDEN]
        )
    )

    assert test == "--order_id"


def test_render_concept_declaration(test_environment: Environment):
    test = Renderer().to_string(
        ConceptDeclarationStatement(concept=test_environment.concepts["order_id"])
    )

    assert test == "key order_id int;"
    env_two = Environment(namespace="test")
    env_two.parse("""key order_id int;""")
    test = Renderer().to_string(
        ConceptDeclarationStatement(concept=env_two.concepts["test.order_id"])
    )

    assert test == "key test.order_id int;"


def test_render_list_wrapper(test_environment: Environment):
    test = Renderer().to_string(ListWrapper([1, 2, 3, 4], type=DataType.INTEGER))

    assert test == "[1, 2, 3, 4]"


def test_render_constant(test_environment: Environment):
    test = Renderer().to_string(
        Function(
            arguments=[[1, 2, 3, 4]],
            operator=FunctionType.CONSTANT,
            output_purpose=Purpose.CONSTANT,
            output_datatype=DataType.ARRAY,
        )
    )

    assert test == "[1, 2, 3, 4]"


def test_render_rowset(test_environment: Environment):
    query = SelectStatement(
        selection=[test_environment.concepts["order_id"]],
        where_clause=None,
        order_by=OrderBy(
            items=[
                OrderItem(
                    expr=test_environment.concepts["order_id"],
                    order=Ordering.ASCENDING,
                )
            ]
        ),
    )
    test = Renderer().to_string(
        RowsetDerivationStatement(
            select=query, name="test", namespace=DEFAULT_NAMESPACE
        )
    )

    assert test == """rowset test <- select
    order_id,
order by
    order_id asc
;"""


def test_render_rowset_item():
    # A rowset-output concept's lineage is a RowsetItem. Rendering its
    # declaration (e.g. via `trilogy explore`) must surface the underlying
    # derivation plus a `# from rowset ... where ...` note for the filter the
    # bare expression drops; passthrough outputs with no derivation fall back
    # to the `<rowset>.<field>` reference.
    env, _ = Environment().parse("""
key order_id int;
property order_id.value float;

rowset totals <- where value > 0 select
    order_id,
    sum(value) -> total_value,
;
""")
    renderer = Renderer(environment=env)
    rendered = {
        addr: renderer.to_string(ConceptDeclarationStatement(concept=concept))
        for addr, concept in env.concepts.items()
        if isinstance(concept.lineage, RowsetItem)
    }
    assert rendered["totals.total_value"] == (
        "auto totals.total_value <- sum(value);\n# from rowset totals where value > 0"
    )
    assert rendered["totals.order_id"] == (
        "auto totals.order_id <- totals.order_id;\n# from rowset totals where value > 0"
    )


def test_render_subselect_item():
    # A `subselect(...)` concept's lineage is a SubselectItem; it round-trips.
    env, _ = Environment().parse("""
key id int;
property id.val float;
datasource nums (id: id, val: val) grain (id) address memory.nums;
auto top2 <- subselect(val order by val desc limit 2);
""")
    rendered = Renderer(environment=env).to_string(
        ConceptDeclarationStatement(concept=env.concepts["local.top2"])
    )
    assert rendered == "auto top2 <- subselect(val order by val desc limit 2);"


def test_render_multiselect_lineage_per_output():
    # A merge output's lineage is a MultiSelectLineage shared by every output;
    # the declaration renders each output concept-specifically — the derive
    # expression, or just the aligned arm columns it merges.
    env, _ = Environment().parse("""key a int;
key b int;
select a
merge
select b
align c: a, b
derive c + 1 -> d
;""")
    renderer = Renderer(environment=env)
    rendered = {
        addr: renderer.to_string(ConceptDeclarationStatement(concept=concept))
        for addr, concept in env.concepts.items()
        if isinstance(concept.lineage, MultiSelectLineage)
    }
    assert rendered["local.c"] == "auto c <- merge(a, b);"
    assert rendered["local.d"] == "auto d <- c + 1;"


def test_render_case(test_environment: Environment):
    case_else = CaseElse(
        expr=test_environment.concepts["order_id"],
    )

    test = Renderer().to_string(case_else)
    assert test == "else order_id"

    test = Renderer().to_string(case_else)
    assert test == "else order_id"
    case_when = CaseWhen(
        expr=test_environment.concepts["order_id"],
        comparison=Comparison(
            left=test_environment.concepts["order_id"],
            operator=ComparisonOperator.EQ,
            right=123,
        ),
    )

    test = Renderer().to_string(case_when)
    assert test == "when order_id = 123 then order_id"

    env, parsed = Environment().parse("""

key x int;
auto y <- case when x = 1 then 1 else 2 end;""")

    test = Renderer().to_string(parsed[-1])
    assert test == """auto y <- case
    when x = 1 then 1
    else 2
end;""", test

    #  property test_like <- CASE WHEN category_name like '%abc%' then True else False END;

    env, parsed = Environment().parse("""

key category_name string;
auto y <- CASE 
    WHEN category_name like '%abc%' then True else False END;""")

    test = Renderer().to_string(parsed[-1])
    # ``X like 'lit'`` is parsed as a ``Comparison`` (not a ``Function``-wrapped
    # ``= True``), so the round-tripped text reflects the cleaner infix form.
    assert test == """auto y <- case
    when category_name like '%abc%' then True
    else False
end;""", test


def test_render_math():
    # addition
    test = Renderer().to_string(
        Function(
            arguments=[1, 2],
            operator=FunctionType.ADD,
            output_purpose=Purpose.CONSTANT,
            output_datatype=DataType.INTEGER,
            arg_count=2,
        )
    )

    assert test == "1 + 2"

    # subtraction
    test = Renderer().to_string(
        Function(
            arguments=[1, 2],
            operator=FunctionType.SUBTRACT,
            output_purpose=Purpose.CONSTANT,
            output_datatype=DataType.INTEGER,
            arg_count=2,
        )
    )

    assert test == "1 - 2"

    # multiplication
    test = Renderer().to_string(
        Function(
            arguments=[1, 2],
            operator=FunctionType.MULTIPLY,
            output_purpose=Purpose.CONSTANT,
            output_datatype=DataType.INTEGER,
            arg_count=2,
        )
    )

    assert test == "1 * 2"

    # division
    test = Renderer().to_string(
        Function(
            arguments=[1, 2],
            operator=FunctionType.DIVIDE,
            output_purpose=Purpose.CONSTANT,
            output_datatype=DataType.INTEGER,
            arg_count=2,
        )
    )

    assert test == "1 / 2"

    test = Renderer().to_string(
        Function(
            arguments=[1, 2, 3],
            operator=FunctionType.DIVIDE,
            output_purpose=Purpose.CONSTANT,
            output_datatype=DataType.INTEGER,
            arg_count=-1,
        )
    )

    assert test == "1 / 2 / 3"


def test_render_anon(test_environment: Environment):
    test = Renderer().to_string(
        Concept(
            name="materialized",
            purpose=Purpose.CONSTANT,
            datatype=DataType.INTEGER,
            lineage=Function(
                arguments=[[1, 2, 3, 4]],
                operator=FunctionType.CONSTANT,
                output_purpose=Purpose.CONSTANT,
                output_datatype=DataType.ARRAY,
            ),
        )
    )

    assert test == "materialized"

    test = Renderer().to_string(
        Concept(
            name=f"{VIRTUAL_CONCEPT_PREFIX}_test",
            purpose=Purpose.CONSTANT,
            datatype=DataType.INTEGER,
            lineage=Function(
                arguments=[[1, 2, 3, 4]],
                operator=FunctionType.CONSTANT,
                output_purpose=Purpose.CONSTANT,
                output_datatype=DataType.ARRAY,
            ),
        )
    )

    assert test == "[1, 2, 3, 4]"


def test_render_dates():

    now = datetime.now()
    test = Renderer().to_string(
        Concept(
            name=f"{VIRTUAL_CONCEPT_PREFIX}_materialized",
            purpose=Purpose.CONSTANT,
            datatype=DataType.INTEGER,
            lineage=Function(
                arguments=[now],
                operator=FunctionType.CONSTANT,
                output_purpose=Purpose.CONSTANT,
                output_datatype=DataType.DATETIME,
            ),
        )
    )

    assert test == f"'{now.isoformat()}'::datetime"

    today = date.today()
    test = Renderer().to_string(
        Concept(
            name=f"{VIRTUAL_CONCEPT_PREFIX}_materialized",
            purpose=Purpose.CONSTANT,
            datatype=DataType.INTEGER,
            lineage=Function(
                arguments=[today],
                operator=FunctionType.CONSTANT,
                output_purpose=Purpose.CONSTANT,
                output_datatype=DataType.DATE,
            ),
        )
    )

    assert test == f"'{today.isoformat()}'::date"


def test_render_merge():
    test = Renderer().to_string(
        MergeStatementV2(
            sources=[
                Concept(
                    name="materialized",
                    purpose=Purpose.CONSTANT,
                    datatype=DataType.INTEGER,
                    lineage=Function(
                        arguments=[[1, 2, 3, 4]],
                        operator=FunctionType.CONSTANT,
                        output_purpose=Purpose.CONSTANT,
                        output_datatype=DataType.ARRAY,
                    ),
                )
            ],
            targets={
                "local.materialized": Concept(
                    name="materialized",
                    purpose=Purpose.CONSTANT,
                    namespace="test",
                    datatype=DataType.INTEGER,
                    lineage=Function(
                        arguments=[[1, 2, 3, 4]],
                        operator=FunctionType.CONSTANT,
                        output_purpose=Purpose.CONSTANT,
                        output_datatype=DataType.ARRAY,
                    ),
                )
            },
            modifiers=[Modifier.PARTIAL],
        )
    )
    assert test == "merge materialized into ~test.materialized;"


def test_render_merge_property():
    test = Renderer().to_string(
        KeyMergeStatement(keys=set(["abc", "def"]), target=ConceptRef(address="abc.id"))
    )
    assert test == "merge property <abc, def> from abc.id;"


def test_render_persist_to_source():
    pass


def test_render_raw_sqlpersist_to_source():
    test = Renderer().to_string(
        RawSQLStatement(
            text="SELECT * FROM test",
        )
    )

    assert test == "raw_sql('''SELECT * FROM test''');"


def test_render_numeric():
    test = Renderer().to_string(NumericType(precision=12, scale=3))

    assert test == "numeric(12,3)"


def test_render_index_access():
    test = Renderer().to_string(
        Function(
            arguments=[
                Concept(
                    name="user_id",
                    purpose=Purpose.KEY,
                    datatype=DataType.INTEGER,
                    lineage=None,
                ),
                1,
            ],
            operator=FunctionType.INDEX_ACCESS,
            output_purpose=Purpose.CONSTANT,
            output_datatype=DataType.ARRAY,
            arg_count=2,
        )
    )

    assert test == "user_id[1]"


def test_render_parenthetical():
    test = Renderer().to_string(
        Function(
            arguments=[
                Concept(
                    name="user_id",
                    purpose=Purpose.KEY,
                    datatype=DataType.INTEGER,
                    lineage=None,
                ),
            ],
            operator=FunctionType.PARENTHETICAL,
            output_purpose=Purpose.CONSTANT,
            output_datatype=DataType.ARRAY,
            arg_count=2,
        )
    )

    assert test == "(user_id)"


@pytest.mark.skipif(
    sys.version_info < (3, 12),
    reason="PurePosixPath(WindowsPath(...)) behavior differs on 3.11 (python/cpython#103631)",
)
def test_render_import():
    for obj in [ImportStatement, Import]:
        base = Path("/path/to/file.preql")
        test = Renderer().to_string(
            obj(alias="user_id", path=str(PureWindowsPath(base)), input_path="customer")
        )

        assert test == "import path.to.file as user_id;"

        base = Path("/path/to/file.preql")
        test = Renderer().to_string(
            obj(alias="user_id", path=str(PurePosixPath(base)), input_path="customer")
        )

        assert test == "import path.to.file as user_id;"

        base = Path("/path/to/file.preql")
        test = Renderer().to_string(
            obj(alias="", path=str(PurePosixPath(base)), input_path="customer")
        )

        assert test == "import path.to.file;"


def test_render_import_preserves_leading_dots():
    """``import ..store_sales as ss`` must round-trip; only ImportStatement
    carries the leading_dots count (Import is the import-record, not a stmt)."""
    base = Path("store_sales.preql")
    out = Renderer().to_string(
        ImportStatement(
            alias="ss",
            path=str(PurePosixPath(base)),
            input_path="store_sales",
            leading_dots=2,
        )
    )
    assert out == "import ..store_sales as ss;"

    out_one_dot = Renderer().to_string(
        ImportStatement(
            alias="ss",
            path=str(PurePosixPath(base)),
            input_path="store_sales",
            leading_dots=1,
        )
    )
    assert out_one_dot == "import .store_sales as ss;"


def test_parse_render_roundtrip_relative_import(tmp_path):
    """End-to-end: parser captures leading dots and renderer emits them."""
    from trilogy.parsing.parse_engine_v2 import parse_text

    (tmp_path / "store_sales.preql").write_text("key id int;")
    sub = tmp_path / "sub"
    sub.mkdir()
    env = Environment(working_path=sub)
    text = "import ..store_sales as ss;"
    _, parsed = parse_text(text, env)
    assert parsed[0].leading_dots == 2
    rendered = Renderer().to_string(parsed[0])
    assert rendered == text


def test_parse_render_roundtrip_aggregate_rollup_empty():
    """``sum(x) by rollup() as sx`` must round-trip — the empty rollup form
    is not the same as a plain ``sum(x)`` and the formatter must not drop it.
    Regression for q67 where formatter dropped/added rollup inconsistently."""
    from trilogy.parsing.parse_engine_v2 import parse_text

    env = Environment()
    text = "key x int;\nselect sum(x) by rollup() as sx;"
    _, parsed = parse_text(text, env)
    rendered = str(parsed[-1])
    assert "sum(x) by rollup() as sx" in rendered, rendered


def test_parse_render_roundtrip_aggregate_no_grouping():
    """Plain ``sum(x)`` must NOT gain a ``by rollup()`` on render."""
    from trilogy.parsing.parse_engine_v2 import parse_text

    env = Environment()
    text = "key x int;\nselect sum(x) as sx;"
    _, parsed = parse_text(text, env)
    rendered = str(parsed[-1])
    assert "by rollup" not in rendered, rendered
    assert "sum(x) as sx" in rendered, rendered


def test_render_datasource():
    user_id = Concept(
        name="user_id",
        purpose=Purpose.KEY,
        datatype=DataType.INTEGER,
        lineage=None,
    )

    ds = Datasource(
        name="useful_data",
        columns=[
            ColumnAssignment(
                alias="user_id", concept=user_id, modifiers=[Modifier.PARTIAL]
            )
        ],
        address="customers.dim_customers",
        grain=Grain(components=[user_id]),
        where=WhereClause(
            conditional=Conditional(
                left=Comparison(
                    left=user_id,
                    right=123,
                    operator=ComparisonOperator.EQ,
                ),
                right=Comparison(
                    left=user_id,
                    right=456,
                    operator=ComparisonOperator.EQ,
                ),
                operator=BooleanOperator.OR,
            ),
        ),
        non_partial_for=WhereClause(
            conditional=Comparison(
                left=user_id,
                right=123,
                operator=ComparisonOperator.EQ,
            )
        ),
    )

    test = Renderer().to_string(ds)
    assert test == """datasource useful_data (
    user_id: ~user_id,
)
grain (user_id)
complete where user_id = 123
address customers.dim_customers
where user_id = 123 or user_id = 456;"""
    ds = Datasource(
        name="useful_data",
        columns=[ColumnAssignment(alias="user_id", concept=user_id)],
        address=Address(location="SELECT * FROM test", type=AddressType.QUERY),
        grain=Grain(components=[user_id]),
        where=WhereClause(
            conditional=Conditional(
                left=Comparison(
                    left=user_id,
                    right=123,
                    operator=ComparisonOperator.EQ,
                ),
                right=Comparison(
                    left=user_id,
                    right=456,
                    operator=ComparisonOperator.EQ,
                ),
                operator=BooleanOperator.OR,
            ),
        ),
    )

    test = Renderer().to_string(ds)
    assert test == """datasource useful_data (
    user_id,
)
grain (user_id)
query '''SELECT * FROM test'''
where user_id = 123 or user_id = 456;"""

    basic = Environment()
    basic.parse("""key id int;
property id.date_string string;
property id.date date;
property id.year int;
property id.day_of_week int;
property id.week_seq int;
property id.month_of_year int;
property id.quarter int;
property id.d_week_seq1 int;

datasource date (
    D_DATE_SK: id,
    D_DATE_ID: date_string,
    D_DATE: date,
    D_DOW: day_of_week,
    D_WEEK_SEQ: week_seq,
    D_MOY: month_of_year,
    D_QOY: quarter,
    D_WEEK_SEQ1: d_week_seq1,
    raw('''cast("D_YEAR" as int)'''): year
)
grain (id)
address memory.date_dim;""")

    test = Renderer().to_string(basic.datasources["date"])
    assert test == """datasource date (
    D_DATE_SK: id,
    D_DATE_ID: date_string,
    D_DATE: date,
    D_DOW: day_of_week,
    D_WEEK_SEQ: week_seq,
    D_MOY: month_of_year,
    D_QOY: quarter,
    D_WEEK_SEQ1: d_week_seq1,
    raw('''cast("D_YEAR" as int)'''): year,
)
grain (id)
address memory.date_dim;""", test
    ds = Datasource(
        name="useful_data",
        columns=[ColumnAssignment(alias="user_id", concept=user_id)],
        address=Address(location="SELECT * FROM test", type=AddressType.QUERY),
        grain=Grain(components=set()),
        where=WhereClause(
            conditional=Conditional(
                left=Comparison(
                    left=user_id,
                    right=123,
                    operator=ComparisonOperator.EQ,
                ),
                right=Comparison(
                    left=user_id,
                    right=456,
                    operator=ComparisonOperator.EQ,
                ),
                operator=BooleanOperator.OR,
            ),
        ),
    )
    ds.grain = Grain()
    test2 = Renderer().to_string(ds)
    assert test2 == """datasource useful_data (
    user_id,
)

query '''SELECT * FROM test'''
where user_id = 123 or user_id = 456;"""

    ds = Datasource(
        name="useful_data",
        columns=[ColumnAssignment(alias="user_id", concept=user_id)],
        address=Address(location="SELECT * FROM test", type=AddressType.QUERY),
        grain=Grain(components=set()),
        incremental_by=[ConceptRef(address="local.user_id")],
        partition_by=[ConceptRef(address="local.user_id")],
        where=WhereClause(
            conditional=Conditional(
                left=Comparison(
                    left=user_id,
                    right=123,
                    operator=ComparisonOperator.EQ,
                ),
                right=Comparison(
                    left=user_id,
                    right=456,
                    operator=ComparisonOperator.EQ,
                ),
                operator=BooleanOperator.OR,
            ),
        ),
        status=DatasourceState.UNPUBLISHED,
    )
    ds.grain = Grain()
    test2 = Renderer().to_string(ds)
    assert test2 == """datasource useful_data (
    user_id,
)

query '''SELECT * FROM test'''
where user_id = 123 or user_id = 456
incremental by user_id
partition by user_id
state unpublished;"""

    # validate round trip
    basic.parse("key user_id int;")
    basic.parse(test2)


def test_render_file_address_single_path():
    """Single-file `file `path`` form preserves the literal location."""
    from trilogy.core.models.datasource import (
        Address,
        ColumnAssignment,
        Datasource,
    )

    user_id = Concept(
        name="user_id",
        datatype=DataType.INTEGER,
        purpose=Purpose.KEY,
    )
    ds = Datasource(
        name="raw_users",
        columns=[ColumnAssignment(alias="user_id", concept=user_id)],
        address=Address(location="/abs/data/users.csv", type=AddressType.CSV),
        grain=Grain(components=[user_id]),
    )
    rendered = Renderer().to_string(ds)
    assert "file `/abs/data/users.csv`" in rendered, rendered
    # No spurious `state unpopulated` (which the grammar doesn't accept).
    assert "state unpopulated" not in rendered


def test_render_file_address_array_form():
    """Multi-file `file [`a`, `b`]` form round-trips."""
    from trilogy.core.models.datasource import (
        Address,
        ColumnAssignment,
        Datasource,
    )

    user_id = Concept(
        name="user_id",
        datatype=DataType.INTEGER,
        purpose=Purpose.KEY,
    )
    ds = Datasource(
        name="multi",
        columns=[ColumnAssignment(alias="user_id", concept=user_id)],
        address=Address(
            location="/a.parquet",
            type=AddressType.PARQUET,
            additional_locations=["/b.parquet", "/c.parquet"],
        ),
        grain=Grain(components=[user_id]),
    )
    rendered = Renderer().to_string(ds)
    assert "file [`/a.parquet`, `/b.parquet`, `/c.parquet`]" in rendered, rendered


def test_render_file_address_write_location():
    """`file `read`:`write`` form is preserved."""
    from trilogy.core.models.datasource import (
        Address,
        ColumnAssignment,
        Datasource,
    )

    user_id = Concept(
        name="user_id",
        datatype=DataType.INTEGER,
        purpose=Purpose.KEY,
    )
    ds = Datasource(
        name="rw",
        columns=[ColumnAssignment(alias="user_id", concept=user_id)],
        address=Address(
            location="/in.csv",
            write_location="/out.csv",
            type=AddressType.CSV,
        ),
        grain=Grain(components=[user_id]),
    )
    rendered = Renderer().to_string(ds)
    assert "file `/in.csv`:`/out.csv`" in rendered, rendered


def test_circular_rendering():
    basic = Environment()

    _, commands = basic.parse(
        """
key id int;

persist test into test from
select id
where id in (1,2,3);
""",
    )
    assert isinstance(
        commands[-1].select.where_clause.conditional.right, TupleWrapper
    ), type(commands[-1].select.where_clause.conditional.right)
    rendered = Renderer().to_string(commands[-1])

    assert rendered == """overwrite test into test from where
    id in (1, 2, 3)
select
    id,
;""", rendered
    # validate round trip
    basic.parse(rendered)


def test_render_list_type():
    basic = Environment()

    env, commands = basic.parse(
        """
key id list<int>;
""",
    )
    rendered = Renderer().to_string(commands[0])

    assert env.concepts["id"].datatype == ArrayType(type=DataType.INTEGER)
    assert isinstance(commands[0], ConceptDeclarationStatement)
    assert rendered == """key id list<int>;""", rendered


def test_render_copy_statement(test_environment):
    select = SelectStatement(
        selection=[test_environment.concepts["order_id"]],
        where_clause=None,
        order_by=OrderBy(
            items=[
                OrderItem(
                    expr=test_environment.concepts["order_id"],
                    order=Ordering.ASCENDING,
                )
            ]
        ),
    )
    query = CopyStatement(select=select, target_type=IOType.CSV, target="test.csv")
    string_query = render_query(query)
    assert string_query == """copy into csv 'test.csv' from select
    order_id,
order by
    order_id asc
;""", string_query


def test_render_substring_filter():
    basic = Environment()

    env, commands = basic.parse(
        """
key id list<int>;

auto string_array <- ['abc', 'def', 'gef'];
const p_cust_zip <- 'abcdef';

auto zips <- unnest(string_array);

auto final_zips <-substring(filter zips where zips in substring(p_cust_zip,1,5),1,2);

select
final_zips;
""",
    )

    final_zips: ConceptDeclarationStatement = commands[-2]

    rendered = Renderer(environment=basic).to_string(final_zips)

    assert (
        rendered
        == "auto final_zips <- substring(zips ? zips in substring(p_cust_zip, 1, 5), 1, 2);"
    )


def test_render_environment():
    x = Environment(working_path=Path(__file__).parent)
    x.parse("""import a;
        import b;
        
    select a, b;""")

    rendered = Renderer().to_string(x)

    assert rendered == "import a;\nimport b;", rendered


def test_render_property(test_environment: Environment):

    env = Environment.from_string("""
key x int;
key y int;

property x.x_name string;
property <x,y>.correlation float;
property y.y_name string;
""")
    test = Renderer().to_string(
        ConceptDeclarationStatement(concept=env.concepts["x_name"])
    )

    assert test == "property x.x_name string;"

    test = Renderer().to_string(
        ConceptDeclarationStatement(concept=env.concepts["correlation"])
    )

    assert test == "property <x, y>.correlation float;"

    test = Renderer().to_string(
        ConceptDeclarationStatement(concept=env.concepts["y_name"])
    )

    assert test == "property y.y_name string;"


def test_render_property_imported_key_round_trip(tmp_path):
    """`<abc.def>.some_name` declares a local property keyed off an imported
    concept. The `<>` are load-bearing — without them, `abc.def.some_name`
    parses as a property in namespace `abc`, a different concept.
    """
    (tmp_path / "abc.preql").write_text("key def int;")
    env = Environment(working_path=tmp_path)
    env.parse("import abc as abc;\nproperty <abc.def>.some_name string;")

    concept = env.concepts["some_name"]
    assert concept.namespace == DEFAULT_NAMESPACE
    assert {addr for addr in concept.keys} == {"abc.def"}

    rendered = Renderer(environment=env).to_string(
        ConceptDeclarationStatement(concept=concept)
    )
    assert rendered == "property <abc.def>.some_name string;", rendered

    env2 = Environment(working_path=tmp_path)
    env2.parse(f"import abc as abc;\n{rendered}")
    concept2 = env2.concepts["some_name"]
    assert concept2.namespace == concept.namespace
    assert {a for a in concept2.keys} == {a for a in concept.keys}


def test_render_properties_grouped_round_trip():
    src = """key order_number int;
key item_id int;

properties <order_number,item_id> (
    quantity int,
    sales_price float,
    net_profit float,
);"""
    env = Environment.from_string(src)
    rendered = Renderer().to_string(env)

    assert (
        "properties <item_id, order_number> (\n    quantity int,\n    sales_price float,\n    net_profit float,\n);"
        in rendered
    )

    # round-trip: re-parse the rendered environment
    env2 = Environment.from_string(rendered)
    for name in ("quantity", "sales_price", "net_profit"):
        c1 = env.concepts[name]
        c2 = env2.concepts[name]
        assert c1.datatype == c2.datatype
        assert c1.keys == c2.keys


def test_render_properties_grouped_description_round_trip():
    """Property descriptions ride as trailing `# ...` comments and round-trip."""
    src = """key order_number int;
key item_id int;

properties <order_number,item_id> (
    quantity int, #units sold
    sales_price float,
);"""
    env = Environment.from_string(src)
    rendered = Renderer().to_string(env)
    assert "quantity int, #units sold" in rendered

    env2 = Environment.from_string(rendered)
    assert env2.concepts["quantity"].metadata.description == "units sold"


def test_render_window_aggregate_over_round_trip():
    """`sum(x) over (partition by y order by z)` covers both the
    window_item_sql_aggregate parser rule and the NavigationWindowItem
    render branch."""
    env = Environment()
    env.parse("""
key x int;
key y int;
key z int;
auto wsum <- sum(x) over (partition by y order by z);
""")
    rendered = Renderer(environment=env).to_string(
        ConceptDeclarationStatement(concept=env.concepts["wsum"])
    )
    assert "sum(x)" in rendered, rendered
    assert "over" in rendered, rendered
    assert "partition by y" in rendered, rendered
    assert "order by z" in rendered, rendered

    env2 = Environment()
    env2.parse(f"key x int;\nkey y int;\nkey z int;\n{rendered}")
    assert "wsum" in env2.concepts


def test_render_window_aggregate_over_expression():
    """`sum(x + 1) over (...)` hits the arbitrary-expression promotion path
    (virtual concept) inside window_item_sql_aggregate."""
    env = Environment()
    env.parse("""
key x int;
key y int;
key z int;
auto wsum <- sum(x + 1) over (partition by y order by z);
""")
    rendered = Renderer(environment=env).to_string(
        ConceptDeclarationStatement(concept=env.concepts["wsum"])
    )
    assert "over" in rendered, rendered
    assert "partition by y" in rendered, rendered


def test_render_window_aggregate_over_with_by_rejected():
    """Mixing `by` aggregate grouping with `over (...)` is a parse error."""
    env = Environment()
    with pytest.raises(Exception):
        env.parse("""
key x int;
key y int;
key z int;
auto wsum <- sum(x) by y over (partition by y order by z);
""")


def test_render_window_aggregate_over_unmappable_aggregate_rejected():
    """An aggregate with no window equivalent (array_agg) can't take `over`."""
    env = Environment()
    with pytest.raises(Exception):
        env.parse("""
key x int;
key y int;
key z int;
auto wagg <- array_agg(x) over (partition by y order by z);
""")


def test_render_simple_case():
    """`case x when 1 then ... when 2 then ... else ... end` exercises the
    SIMPLE_CASE render branch."""
    env = Environment()
    _, parsed = env.parse("""
key x int;
auto y <- case x when 1 then 'a' when 2 then 'b' else 'c' end;
""")
    rendered = Renderer().to_string(parsed[-1])
    assert "case x" in rendered, rendered
    assert "when 1 then" in rendered, rendered
    assert "when 2 then" in rendered, rendered
    assert "else" in rendered, rendered
    assert "end" in rendered, rendered


def test_render_show_concepts():
    env = Environment()
    _, parsed = env.parse("key x int;\nshow concepts;")
    rendered = Renderer().to_string(parsed[-1])
    assert rendered == "show concepts;", rendered


def test_render_show_select():
    env = Environment()
    _, parsed = env.parse("key x int;\nshow select x;")
    rendered = Renderer().to_string(parsed[-1])
    assert rendered.startswith("show "), rendered
    assert rendered.endswith(";"), rendered
    assert "select" in rendered


def test_render_validate_all():
    env = Environment()
    _, parsed = env.parse("key x int;\nvalidate all;")
    rendered = Renderer().to_string(parsed[-1])
    assert rendered == "validate all;", rendered


def test_render_validate_scoped():
    env = Environment()
    _, parsed = env.parse("key x int;\nvalidate concepts x;")
    rendered = Renderer().to_string(parsed[-1])
    assert rendered == "validate concepts x;", rendered


def test_properties_block_trailing_description():
    """A `# ...` on the line that closes a `properties (...)` block attaches
    as the description of the first concept in the block."""
    src = """key a int;
key b int;
properties <a, b> (
    quantity int,
); #block description
"""
    env = Environment.from_string(src)
    assert env.concepts["quantity"].metadata.description == "block description"


def test_render_enum_type():
    """EnumType renders as enum<base>[..members..]."""
    from trilogy.core.models.core import EnumType

    assert (
        Renderer().to_string(EnumType(type=DataType.STRING, values=["a", "b"]))
        == "enum<string>['a', 'b']"
    )


def test_render_properties_grouped_mixed():
    """Single-key properties render individually; multi-key groups use grouped format."""
    env = Environment.from_string("""
key x int;
key y int;

property x.x_name string;
properties <x,y> (
    a int,
    b float,
);
property y.y_name string;
""")
    rendered = Renderer().to_string(env)

    # single-key properties render individually
    assert "property x.x_name string;" in rendered
    assert "property y.y_name string;" in rendered
    # multi-key group renders as grouped block
    assert "properties <x, y> (\n    a int,\n    b float,\n);" in rendered

    # round-trip
    env2 = Environment.from_string(rendered)
    for name in ("x_name", "y_name", "a", "b"):
        c1 = env.concepts[name]
        c2 = env2.concepts[name]
        assert c1.datatype == c2.datatype
        assert c1.keys == c2.keys


def test_render_trait_type():
    basic = Environment()

    env, commands = basic.parse(
        """
    type email string;

    key id string::email;

    """,
    )
    expectation = ["type email string;", "key id string::email;"]
    for idx, cmd in enumerate(commands):
        rendered = Renderer().to_string(cmd)
        assert rendered == expectation[idx], rendered


def test_render_function():
    basic = Environment()

    env, commands = basic.parse(
        """

def add_thrice(x)-> x +x + x;

select
    @add_thrice(1) as test;
""",
    )
    expected = [
        """def add_thrice(x) -> x + x + x;""",
        """select
    @add_thrice(1) as test,
;""",
    ]
    for idx, cmd in enumerate(commands):
        rendered = Renderer().to_string(cmd)
        assert rendered == expected[idx], rendered

    env, commands = basic.parse("""
    select round(@add_thrice(1),2) as test_sum;
                """)
    expected = [
        """select
    round(@add_thrice(1), 2) as test_sum,
;""",
    ]
    for idx, cmd in enumerate(commands):
        rendered = Renderer().to_string(cmd)
        assert rendered == expected[idx], rendered


def test_parse_preserves_leading_comments():
    """Standalone comments preceding a statement must survive parse.

    Regression: the pest gobbler parks leading-comment tokens on the
    *previous* block, and ``trailing_description`` only attaches comments
    on the statement's end line. Anything past the first line break used
    to be silently dropped during hydration, causing ``trilogy fmt`` to
    strip leading comments. The planner now peels detached comments off
    each block and emits them as standalone CommentStatementPlans.
    """
    src = """
key id int;
# leading comment for next field
# second line of that comment
property id.x int;

# new section
property id.y int;
"""
    env, queries = Environment().parse(src)
    kinds = [type(q).__name__ for q in queries]
    assert kinds == [
        "ConceptDeclarationStatement",
        "Comment",
        "Comment",
        "ConceptDeclarationStatement",
        "Comment",
        "ConceptDeclarationStatement",
    ], kinds
    comments = [q.text.strip() for q in queries if isinstance(q, Comment)]
    assert comments == [
        "# leading comment for next field",
        "# second line of that comment",
        "# new section",
    ]
    # Round-trip: rendering and re-parsing must yield the same shape.
    rendered = Renderer().render_statement_string(queries)
    _, queries2 = Environment().parse(rendered)
    assert [type(q).__name__ for q in queries2] == kinds


def test_render_function_trailing_comment():
    basic = Environment()

    env, commands = basic.parse("""
def add_thrice(x) -> x + x + x; # multiply x by three via addition
""")
    rendered = Renderer().to_string(commands[0])
    assert (
        rendered == "def add_thrice(x) -> x + x + x; # multiply x by three via addition"
    ), rendered

    # round-trip: re-parsing preserves the description
    env2 = Environment()
    _, commands2 = env2.parse(rendered)
    assert commands2[0].meta.description == " multiply x by three via addition"


def test_render_map():
    basic = Environment()

    env, commands = basic.parse("""
   const num_map <- {1: 10, 2: 20};
   """)
    expected = [
        """const num_map <- {1: 10, 2: 20};""",
    ]
    for idx, cmd in enumerate(commands):
        rendered = Renderer().to_string(cmd)
        assert rendered == expected[idx], rendered


def test_render_struct():
    basic = Environment()

    env, commands = basic.parse("""
   const x <- 1;
   const y <- 2;

   select
   struct(x-> label, y-> field) as num_struct;
   """)
    expected = [
        """const x <- 1;""",
        """const y <- 2;""",
        """select
    struct(
            x-> label,
            y-> field
        ) as num_struct,
;""",
    ]
    for idx, cmd in enumerate(commands):
        rendered = Renderer().to_string(cmd)
        assert rendered == expected[idx], rendered


def test_render_unnest_array():
    basic = Environment()

    env, commands = basic.parse("""

const zips_pre <- unnest(['24128',
                                     '35576'
        ]);
        """)
    expected = [
        """const zips_pre <- unnest(['24128', '35576']);""",
    ]
    for idx, cmd in enumerate(commands):
        rendered = Renderer(environment=env).to_string(cmd)
        assert rendered == expected[idx], rendered


def test_render_group_by():
    basic = Environment()

    env, commands = basic.parse("""

key x int;

select group(1) by x as test, group(1) by * as all_rows;
""")
    expected = [
        """key x int;""",
        """select
    group(1) by x as test,
    group(1) by * as all_rows,
;""",
    ]
    for idx, cmd in enumerate(commands):
        rendered = Renderer(environment=env).to_string(cmd)
        assert rendered == expected[idx], rendered


def test_render_alias():
    basic = Environment()

    env, commands = basic.parse("""

key x int;

select x as x2;
""")
    expected = [
        """key x int;""",
        """select
    x as x2,
;""",
    ]
    for idx, cmd in enumerate(commands):
        rendered = Renderer(environment=env).to_string(cmd)
        assert rendered == expected[idx], rendered


def test_render_cast():
    basic = Environment()

    env, commands = basic.parse("""

key x int;

select x::float as x2, cast(x as float) as x3;
""")
    expected = [
        """key x int;""",
        """select
    x::float as x2,
    x::float as x3,
;""",
    ]
    for idx, cmd in enumerate(commands):
        rendered = Renderer(environment=env).to_string(cmd)
        assert rendered == expected[idx], rendered


def test_column_assignment_shorthand():
    """Test that ColumnAssignment renders with shorthand when alias matches concept."""
    user_id = Concept(
        name="user_id",
        purpose=Purpose.KEY,
        datatype=DataType.INTEGER,
    )

    # Test shorthand: when alias matches concept name and no modifiers
    assignment_shorthand = ColumnAssignment(
        alias="user_id",
        concept=user_id.reference,
        modifiers=[],
    )
    rendered = Renderer().to_string(assignment_shorthand)
    assert rendered == "user_id", f"Expected 'user_id', got '{rendered}'"

    # Test full syntax: when there are modifiers
    assignment_with_modifier = ColumnAssignment(
        alias="user_id",
        concept=user_id.reference,
        modifiers=[Modifier.NULLABLE],
    )
    rendered = Renderer().to_string(assignment_with_modifier)
    assert (
        rendered == "user_id: ?user_id"
    ), f"Expected 'user_id: ?user_id', got '{rendered}'"

    # Test full syntax: when alias differs from concept
    assignment_different = ColumnAssignment(
        alias="id",
        concept=user_id.reference,
        modifiers=[],
    )
    rendered = Renderer().to_string(assignment_different)
    assert rendered == "id: user_id", f"Expected 'id: user_id', got '{rendered}'"


def test_column_assignment_in_datasource():
    """Test that shorthand works correctly in a full datasource definition."""
    user_id = Concept(
        name="user_id",
        purpose=Purpose.KEY,
        datatype=DataType.INTEGER,
    )

    user_name = Concept(
        name="user_name",
        purpose=Purpose.PROPERTY,
        datatype=DataType.STRING,
    )

    # Datasource with shorthand and full syntax mixed
    ds = Datasource(
        name="users",
        columns=[
            ColumnAssignment(alias="user_id", concept=user_id.reference, modifiers=[]),
            ColumnAssignment(
                alias="user_name", concept=user_name.reference, modifiers=[]
            ),
            ColumnAssignment(alias="id_copy", concept=user_id.reference, modifiers=[]),
        ],
        address="dim_users",
        grain=Grain(components=[user_id]),
    )

    rendered = Renderer().to_string(ds)
    expected = """datasource users (
    user_id,
    user_name,
    id_copy: user_id,
)
grain (user_id)
address dim_users;"""
    assert rendered == expected, f"Got:\n{rendered}\n\nExpected:\n{expected}"


def test_render_nullable_typed_declaration_round_trip():
    """`fmt` must keep the trailing `?` on typed key/property declarations —
    dropping it silently flips a concept from nullable to non-nullable."""
    src = """key kid int?;
property kid.name string?;
"""
    env = Environment(working_path=".")
    _, queries = env.parse(src)
    rendered = Renderer(environment=env).render_statement_string(queries)
    assert "key kid int?;" in rendered, rendered
    assert "property kid.name string?;" in rendered, rendered

    env2 = Environment(working_path=".")
    env2.parse(rendered)
    for addr in ("local.kid", "local.name"):
        assert (Modifier.NULLABLE in env.concepts[addr].modifiers) == (
            Modifier.NULLABLE in env2.concepts[addr].modifiers
        )


def test_render_date_trunc_round_trip():
    """The `date_truncate` enum value isn't the parseable keyword; render the
    canonical `date_trunc` so output round-trips."""
    env = Environment(working_path=".")
    _, queries = env.parse("auto t <- date_trunc('2020-01-01'::date, month);\n")
    rendered = Renderer(environment=env).render_statement_string(queries)
    assert "date_trunc(" in rendered, rendered
    assert "date_truncate(" not in rendered, rendered
    Environment(working_path=".").parse(rendered)


def test_render_hash_round_trip():
    """The hash algorithm is a bare keyword (md5/sha256/...), not a string."""
    env = Environment(working_path=".")
    _, queries = env.parse("const s <- 'x';\nauto h <- hash(s, sha256);\n")
    rendered = Renderer(environment=env).render_statement_string(queries)
    assert "hash(s, sha256)" in rendered, rendered
    Environment(working_path=".").parse(rendered)
