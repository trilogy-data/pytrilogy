from preql.core.processing.nodes import MergeNode
from preql.core.processing.nodes import NodeJoin, ConstantNode
from preql.core.models import Environment
from preql.core.enums import JoinType
from preql.core.processing.graph_utils import extract_required_subgraphs
from typing import List

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
        _ = MergeNode(
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
        assert 1 == 0, "test should fail"
    except Exception as e:
        assert isinstance(e, SyntaxError)


def test_graph_resolution():

    assert extract_required_subgraphs([
            "ds~rich_info.rich_info",
            "c~rich_info.full_name@Grain<rich_info.full_name>",
            "c~rich_info.last_name@Grain<Abstract>",
            "ds~local.__virtual_bridge_passenger_last_name_rich_info_last_name",
            "c~passenger.last_name@Grain<Abstract>",
            "c~passenger.last_name@Grain<passenger.id>",
        ]) == [["c~rich_info.full_name@Grain<rich_info.full_name>", "c~rich_info.last_name@Grain<Abstract>"],
               ["c~rich_info.last_name@Grain<Abstract>", "c~passenger.last_name@Grain<Abstract>", "c~passenger.last_name@Grain<passenger.id>"]]
    

    assert extract_required_subgraphs(
        ['ds~rich_info.rich_info', 'c~rich_info.net_worth_1918_dollars@Grain<Abstract>', 'c~rich_info.net_worth_1918_dollars@Grain<rich_info.full_name>']
    ) == ['c~rich_info.net_worth_1918_dollars@Grain<Abstract>', 'c~rich_info.net_worth_1918_dollars@Grain<rich_info.full_name>']