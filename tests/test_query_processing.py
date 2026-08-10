from trilogy.core.query_processor import get_query_datasources, process_query
from trilogy.core.statements.author import SelectStatement


def test_query_aggregation(test_environment, test_environment_graph):
    select = SelectStatement(selection=[test_environment.concepts["total_revenue"]])
    datasource = get_query_datasources(environment=test_environment, statement=select)

    assert {datasource.identifier} == {
        "revenue_at_local_order_id_grouped_by__at_abstract"
    }
    check = datasource
    # Only the aggregate's argument is projected; the (unused) order_id grain
    # key is not carried.
    input_names = {c.name for c in check.input_concepts}
    assert input_names == {"revenue"}
    assert len(check.output_concepts) == 1
    assert check.output_concepts[0].name == "total_revenue"


def test_query_datasources(test_environment, test_environment_graph):
    select = SelectStatement(
        selection=[
            test_environment.concepts["category_id"],
            test_environment.concepts["category_name"],
            test_environment.concepts["total_revenue"],
        ]
    )
    get_query_datasources(environment=test_environment, statement=select)


def test_full_query(test_environment, test_environment_graph):
    select = SelectStatement(
        selection=[
            test_environment.concepts["category_id"],
            test_environment.concepts["category_name"],
            test_environment.concepts["total_revenue"],
        ]
    )

    processed = process_query(statement=select, environment=test_environment)

    assert {c.name for c in processed.output_columns} == {
        "category_id",
        "category_name",
        "total_revenue",
    }
