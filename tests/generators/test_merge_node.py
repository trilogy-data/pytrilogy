from trilogy.core.processing.nodes import MergeNode
from trilogy.core.processing.nodes import NodeJoin, ConstantNode
from trilogy.core.models import Environment
from trilogy.core.enums import JoinType
from trilogy.core.processing.graph_utils import extract_required_subgraphs
from collections import defaultdict


def test_same_join_fails(test_environment: Environment, test_environment_graph):
    x = ConstantNode(
        input_concepts=[],
        output_concepts=[test_environment.concepts["constant_one"]],
        environment=test_environment,
        g=test_environment_graph,
        parents=[],
        depth=0,
    )
    try:
        n = MergeNode(
            input_concepts=[],
            output_concepts=[],
            environment=None,
            g=None,
            parents=[],
            node_joins=[
                NodeJoin(
                    left_node=x,
                    right_node=x,
                    concepts=[test_environment.concepts["constant_one"]],
                    join_type=JoinType.INNER,
                )
            ],
        )
        len(n.node_joins) == 1
    except Exception as e:
        assert isinstance(e, SyntaxError)


def test_graph_resolution():
    assocs = defaultdict(list)
    extract_required_subgraphs(
        assocs,
        [
            "ds~rich_info.rich_info",
            "c~rich_info.full_name@Grain<rich_info.full_name>",
            "c~rich_info.last_name@Grain<Abstract>",
            "ds~local.__virtual_bridge_passenger_last_name_rich_info_last_name",
            "c~passenger.last_name@Grain<Abstract>",
            "c~passenger.last_name@Grain<passenger.id>",
        ],
    )
    assert assocs["ds~rich_info.rich_info"] == [
        "c~rich_info.full_name@Grain<rich_info.full_name>",
        "c~rich_info.last_name@Grain<Abstract>",
    ]
    assert assocs[
        "ds~local.__virtual_bridge_passenger_last_name_rich_info_last_name"
    ] == [
        "c~rich_info.last_name@Grain<Abstract>",
        "c~passenger.last_name@Grain<Abstract>",
        "c~passenger.last_name@Grain<passenger.id>",
    ]


def test_graph_rez_bridge():
    assocs = defaultdict(list)
    for k, v in {
        "c~rich_info.full_name@Grain<rich_info.full_name>": [
            "ds~rich_info.rich_info",
            "c~rich_info.full_name@Grain<rich_info.full_name>",
        ],
        "c~passenger.name@Grain<passenger.id>": [
            "ds~rich_info.rich_info",
            "c~rich_info.full_name@Grain<rich_info.full_name>",
            "c~local.__merge_passenger_last_name_rich_info_last_name@Grain<Abstract>",
            "c~passenger.last_name@Grain<passenger.id>",
            "c~passenger.last_name@Grain<Abstract>",
            "ds~local.dim_passenger",
            "c~passenger.name@Grain<Abstract>",
            "c~passenger.name@Grain<passenger.id>",
        ],
        "c~rich_info.net_worth_1918_dollars@Grain<rich_info.full_name>": [
            "ds~rich_info.rich_info",
            "c~rich_info.net_worth_1918_dollars@Grain<Abstract>",
            "c~rich_info.net_worth_1918_dollars@Grain<rich_info.full_name>",
        ],
    }.items():

        extract_required_subgraphs(assocs, v)

    assert set(assocs["ds~rich_info.rich_info"]) == set(
        [
            "c~rich_info.full_name@Grain<rich_info.full_name>",
            "c~local.__merge_passenger_last_name_rich_info_last_name@Grain<Abstract>",
            "c~passenger.last_name@Grain<passenger.id>",
            "c~passenger.last_name@Grain<Abstract>",
            "c~rich_info.net_worth_1918_dollars@Grain<Abstract>",
            "c~rich_info.net_worth_1918_dollars@Grain<rich_info.full_name>",
        ]
    )
