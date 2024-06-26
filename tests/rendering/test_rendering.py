from trilogy.parsing.render import render_query, render_environment, Renderer
from trilogy.core.models import (
    OrderBy,
    Ordering,
    OrderItem,
    SelectStatement,
    WhereClause,
    Conditional,
    Comparison,
    PersistStatement,
    Address,
    SelectItem,
    ConceptDeclarationStatement,
    ListWrapper,
    Function,
    Purpose,
    DataType,
    RowsetDerivationStatement,
    CaseElse,
    CaseWhen,
    Concept,
    MergeStatement,
    MultiSelectStatement,
    AlignClause,
    AlignItem,
)
from trilogy import Environment
from trilogy.core.enums import ComparisonOperator, BooleanOperator, Modifier, FunctionType
from trilogy.constants import VIRTUAL_CONCEPT_PREFIX, DEFAULT_NAMESPACE


def test_basic_query(test_environment):
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

    string_query = render_query(query)
    assert (
        string_query
        == """SELECT
    order_id,
ORDER BY
    order_id asc
;"""
    )


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
    assert (
        string_query
        == """SELECT
    order_id,
MERGE
SELECT
    order_id,
ALIGN
\tmerge:order_id
ORDER BY
\torder_id asc
;"""
    ), string_query


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
    assert (
        string_query
        == """SELECT
    order_id,
WHERE
    (order_id = 123 or order_id = 456)
ORDER BY
    order_id asc
;"""
    )


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
            identifier="test",
            address=Address(location="tbl_test"),
        ),
    )

    string_query = render_query(query)
    assert (
        string_query
        == """PERSIST test INTO tbl_test FROM SELECT
    order_id,
ORDER BY
    order_id asc
;"""
    )


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

    assert test == "key local.order_id int;"


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

    assert (
        test
        == """rowset test <- SELECT
    order_id,
ORDER BY
    order_id asc
;"""
    )


def test_render_case(test_environment: Environment):

    case_else = CaseElse(
        expr=test_environment.concepts["order_id"],
    )

    test = Renderer().to_string(case_else)
    assert test == "ELSE order_id"

    test = Renderer().to_string(case_else)
    assert test == "ELSE order_id"
    case_when = CaseWhen(
        expr=test_environment.concepts["order_id"],
        comparison=Comparison(
            left=test_environment.concepts["order_id"],
            operator=ComparisonOperator.EQ,
            right=123,
        ),
    )

    test = Renderer().to_string(case_when)
    assert test == "WHEN order_id = 123 THEN order_id"


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


def test_render_merge():
    test = Renderer().to_string(
        MergeStatement(
            datatype=DataType.INTEGER,
            concepts=[
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
                ),
                Concept(
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
                ),
            ],
        )
    )
    assert test == "merge materialized, test.materialized;"


def test_render_persist_to_source():
    pass
