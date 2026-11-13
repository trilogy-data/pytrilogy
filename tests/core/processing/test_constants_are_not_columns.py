"""A constant is its expression on every row, so it is rendered where it is
read and nothing pairs on it. Read as a parent's column it is NULL on each row
an outer join pads that parent onto; the all-rows marker (the grain of a `by *`
value) was carried and paired on that way, and the pairing dropped the padded
rows (#947)."""

from pathlib import Path

from tests.helpers.planning import recorded
from tests.helpers.rows import executor_for, fetch_rows
from trilogy.core.constants import ALL_ROWS_ADDRESS
from trilogy.core.processing.v4_helper.models import pairing_keys

GRAND_TOTAL_GATE = """
key tree_id string;
key city string;
key genus string;
key fit_class string;
property tree_id.cls string;
property genus.genus_passes bool;
property fit_class.class_passes bool;

datasource trees (tree_id, city, cls)
grain (tree_id)
query '''select 't1' tree_id, 'SF' city, 'all' cls
union all select 't2', 'SF', 'none' union all select 't3', 'SF', 'all' ''';

datasource predictions (tree_id: ~tree_id, city, ?genus)
grain (tree_id)
query '''select 't1' tree_id, 'SF' city, 'acer' genus union all select 't9', 'SF', 'ulmus' ''';

datasource genus_fits (genus, ?genus_passes)
grain (genus)
query '''select 'acer' genus, true genus_passes''';

datasource class_fits (fit_class, ?class_passes)
grain (fit_class)
query '''select 'all' fit_class, true class_passes''';

auto class_total <- max(class_passes ? fit_class = 'all') by *;
auto class_gate <- case when cls = 'all' then class_total end;
auto genus_gate <- case when genus is null then false else coalesce(genus_passes, false) end;
auto fit_level <- case when genus_gate then 'genus' when class_gate then 'class' else 'none' end;
const k <- 7;
"""

PARTIAL_KEY_MODEL = Path(__file__).with_name("constants_are_not_columns.preql")


def _carry_demands(node: dict) -> set[str]:
    demands = {*node["input_grain"], *node["projection_grain"]}
    for contract in node["input_contracts"]:
        demands |= {*contract["required_grain"], *contract["relation_keys"]}
    return demands


def test_constant_is_not_read_off_a_padded_parent() -> None:
    executor = executor_for(GRAND_TOTAL_GATE)
    query = "select genus, genus_passes, tree_id, k order by tree_id asc;"
    assert fetch_rows(executor, query) == [
        ("acer", True, "t1", 7),
        (None, None, "t2", 7),
        (None, None, "t3", 7),
    ]


def test_pairing_keys_drops_the_marker() -> None:
    assert pairing_keys({ALL_ROWS_ADDRESS, "local.cls"}) == {"local.cls"}
    assert pairing_keys({ALL_ROWS_ADDRESS}) == frozenset()


def test_no_group_is_asked_to_carry_the_marker() -> None:
    executor = executor_for(GRAND_TOTAL_GATE)
    query = "select tree_id, city, fit_level order by tree_id asc;"
    trace = recorded(executor, query)
    graphs = [s.data.graph for s in trace.steps if s.phase == "group_graph"]
    grains = {
        ALL_ROWS_ADDRESS in node["grain_components"]
        for node in graphs[-1].nodes.values()
    }
    assert grains == {True, False}
    for graph in graphs:
        for gid, node in graph.nodes.items():
            assert ALL_ROWS_ADDRESS not in _carry_demands(node), gid
    assert "__preql_internal_all_rows" not in executor.generate_sql(query)[-1]
    assert fetch_rows(executor, query) == [
        ("t1", "SF", "genus"),
        ("t2", "SF", "none"),
        ("t3", "SF", "class"),
    ]


def test_grand_total_over_partial_key_source_keeps_padded_rows() -> None:
    executor = executor_for(PARTIAL_KEY_MODEL.read_text(encoding="utf-8"))
    trees = ["pred-1", "sf-1", "sf-4"]
    plain = fetch_rows(executor, "select tree_id, city order by tree_id asc;")
    assert [r[0] for r in plain] == trees
    query = "select tree_id, city, accepted_height_m order by tree_id asc;"
    assert "__preql_internal_all_rows" not in executor.generate_sql(query)[-1]
    assert [r[0] for r in fetch_rows(executor, query)] == trees
