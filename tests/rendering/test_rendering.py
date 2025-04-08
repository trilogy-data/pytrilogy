from datetime import date, datetime
from pathlib import Path, PurePosixPath, PureWindowsPath

from trilogy.constants import DEFAULT_NAMESPACE, VIRTUAL_CONCEPT_PREFIX
from trilogy.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    FunctionType,
    IOType,
    Modifier,
    Ordering,
    Purpose,
)
from trilogy.core.models.author import (
    AlignClause,
    AlignItem,
    CaseElse,
    CaseWhen,
    Comparison,
    Concept,
    Conditional,
    Function,
    Grain,
    OrderBy,
    OrderItem,
    WhereClause,
)
from trilogy.core.models.core import (
    DataType,
    ListType,
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
    assert (
        string_query
        == """SELECT
    order_id,
ORDER BY
    order_id asc
;"""
    )


def test_window_over(test_environment):
    env = Environment()
    _, parsed = env.parse(
        """
key order_id int;

datasource orders (
    order_id: order_id
)
grain (order_id)
address memory.orders;

select max(order_id) by order_id -> test;

"""
    )
    string_query = Renderer().to_string(parsed[-1])
    assert (
        string_query
        == """SELECT
    max(order_id) by order_id -> test,
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
        == """WHERE
    order_id = 123 or order_id = 456
SELECT
    order_id,
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
            name="test",
            address=Address(location="tbl_test"),
            environment=test_environment,
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

    env, parsed = Environment().parse(
        """

key x int;
auto y <- case when x = 1 then 1 else 2 end;"""
    )

    test = Renderer().to_string(parsed[-1])
    assert (
        test
        == """property y <- CASE WHEN x = 1 THEN 1
ELSE 2
END;"""
    ), test

    #  property test_like <- CASE WHEN category_name like '%abc%' then True else False END;

    env, parsed = Environment().parse(
        """

key category_name string;
auto y <- CASE WHEN category_name like '%abc%' then True else False END;"""
    )

    test = Renderer().to_string(parsed[-1])
    assert (
        test
        == """property y <- CASE WHEN like(category_name,'%abc%') = True THEN True
ELSE False
END;"""
    ), test


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
    assert test == "MERGE materialized into ~test.materialized;"


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

    assert test == "Numeric(12,3)"


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
    assert (
        test
        == """datasource useful_data (
    user_id: ~user_id
    )
grain (user_id)
complete where user_id = 123
address customers.dim_customers
where user_id = 123 or user_id = 456;"""
    )
    ds = Datasource(
        name="useful_data",
        columns=[ColumnAssignment(alias="user_id", concept=user_id)],
        address=Address(is_query=True, location="SELECT * FROM test"),
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
    assert (
        test
        == """datasource useful_data (
    user_id: user_id
    )
grain (user_id)
query '''SELECT * FROM test'''
where user_id = 123 or user_id = 456;"""
    )

    basic = Environment()
    basic.parse(
        """key id int;
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
address memory.date_dim;"""
    )

    test = Renderer().to_string(basic.datasources["date"])
    assert (
        test
        == """datasource date (
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
address memory.date_dim;"""
    ), test
    ds = Datasource(
        name="useful_data",
        columns=[ColumnAssignment(alias="user_id", concept=user_id)],
        address=Address(is_query=True, location="SELECT * FROM test"),
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
    assert (
        test2
        == """datasource useful_data (
    user_id: user_id
    )

query '''SELECT * FROM test'''
where user_id = 123 or user_id = 456;"""
    )


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

    assert (
        rendered
        == """PERSIST test INTO test FROM WHERE
    id in (1, 2, 3)
SELECT
    id,
;"""
    ), rendered


def test_render_list_type():
    basic = Environment()

    env, commands = basic.parse(
        """
key id list<int>;
""",
    )
    rendered = Renderer().to_string(commands[0])

    assert env.concepts["id"].datatype == ListType(type=DataType.INTEGER)
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
    assert (
        string_query
        == """COPY INTO CSV 'test.csv' FROM SELECT
    order_id,
ORDER BY
    order_id asc
;"""
    ), string_query


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
        == "property final_zips <- substring(filter zips where zips in substring(p_cust_zip,1,5),1,2);"
    )


def test_render_environment():
    x = Environment(working_path=Path(__file__).parent)
    x.parse(
        """import a;
        import b;
        
    select a, b;"""
    )

    rendered = Renderer().to_string(x)

    assert rendered == "import a;\nimport b;", rendered


def test_render_property(test_environment: Environment):

    env = Environment.from_string(
        """
key x int;
key y int;

property x.x_name string;
property <x,y>.correlation float;
property y.y_name string;
"""
    )
    test = Renderer().to_string(
        ConceptDeclarationStatement(concept=env.concepts["x_name"])
    )

    assert test == "property x.x_name string;"

    test = Renderer().to_string(
        ConceptDeclarationStatement(concept=env.concepts["correlation"])
    )

    assert test == "property <x,y>.correlation float;"

    test = Renderer().to_string(
        ConceptDeclarationStatement(concept=env.concepts["y_name"])
    )

    assert test == "property y.y_name string;"


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
        """SELECT
    @add_thrice(1) -> test,
;""",
    ]
    for idx, cmd in enumerate(commands):
        rendered = Renderer().to_string(cmd)
        assert rendered == expected[idx], rendered

    env, commands = basic.parse(
        """
    select round(@add_thrice(1),2) as test_sum;
                """
    )
    expected = [
        """SELECT
    round(@add_thrice(1),2) -> test_sum,
;""",
    ]
    for idx, cmd in enumerate(commands):
        rendered = Renderer().to_string(cmd)
        assert rendered == expected[idx], rendered
