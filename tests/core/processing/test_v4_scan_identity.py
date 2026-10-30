from trilogy import Dialects, Environment
from trilogy.core.env_processor import generate_graph
from trilogy.core.processing.v4_helper.history import V4History
from trilogy.core.processing.v4_helper.source_planning import (
    SourceRequest,
    scan_regroups,
)

MODEL = """
key order_id int;
key line_no int;
key supplier_id int;
property <order_id, line_no>.ship_date date;
property supplier_id.supplier_name string;
key undeclared_id int;
property undeclared_id.label string;

datasource lines (
    order_id: order_id,
    line_no: line_no,
    supplier_id: supplier_id,
    ship_date: ship_date,
)
grain (order_id, line_no)
query '''
select 1 order_id, 1 line_no, 7 supplier_id, DATE '2024-01-02' ship_date
union all select 1, 2, 7, DATE '2024-01-02'
union all select 2, 1, 8, DATE '2024-01-05'
''';

datasource suppliers (
    supplier_id: supplier_id,
    supplier_name: supplier_name,
)
grain (supplier_id)
query '''
select 7 supplier_id, 'a' supplier_name
union all select 8, 'b'
''';

datasource labels (
    undeclared_id: undeclared_id,
    label: label,
)
query '''
select 1 undeclared_id, 'x' label
''';
"""


def _scan(*outputs: str) -> bool:
    env = Environment()
    env.parse(MODEL)
    build = env.materialize_for_select()
    return scan_regroups(
        SourceRequest(
            outputs=[build.concepts[f"local.{a}"] for a in outputs],
            environment=build,
            graph=generate_graph(build),
            history=V4History(base_environment=env),
        )
    )


def test_a_scan_without_its_source_grain_regroups():
    assert _scan("order_id", "supplier_id")


def test_a_scan_at_its_source_grain_holds_its_rows():
    assert not _scan("order_id", "line_no", "ship_date")
    assert not _scan("supplier_id", "supplier_name")


def test_a_source_joined_for_fewer_columns_than_its_grain_regroups():
    assert _scan("order_id", "supplier_name")


def test_a_source_with_no_grain_regroups():
    assert _scan("undeclared_id", "label")


def test_filter_over_a_deduped_scan_folds_into_its_aggregate():
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(MODEL)
    query = """
    auto late <- filter supplier_id where ship_date > '2024-01-03'::date;
    select order_id, count(late) -> late_suppliers order by order_id;
    """
    rows = executor.execute_text(query)[-1].fetchall()
    assert [tuple(r) for r in rows] == [(1, 0), (2, 1)]


def test_a_scan_reads_a_class_as_requested_after_an_alias_names_it():
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(MODEL)
    executor.execute_text("select supplier_id as sid, supplier_name;")
    build = executor.environment.materialize_for_select()
    assert not scan_regroups(
        SourceRequest(
            outputs=[
                build.concepts["local.supplier_id"],
                build.concepts["local.supplier_name"],
            ],
            environment=build,
            graph=generate_graph(build),
            history=V4History(base_environment=executor.environment),
        )
    )
