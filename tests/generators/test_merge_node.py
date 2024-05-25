from preql.core.processing.nodes import MergeNode
from preql.core.processing.nodes import NodeJoin, ConstantNode
from preql.core.models import Environment
from preql.core.enums import JoinType


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
