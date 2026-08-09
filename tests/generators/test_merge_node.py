from trilogy.core.enums import JoinType
from trilogy.core.models.environment import Environment
from trilogy.core.processing.nodes import ConstantNode, MergeNode, NodeJoin


def test_same_join_fails(test_environment: Environment, test_environment_graph):
    test_environment = test_environment.materialize_for_select()
    x = ConstantNode(
        input_concepts=[],
        output_concepts=[test_environment.concepts["constant_one"]],
        environment=test_environment,
        parents=[],
        depth=0,
    )
    try:
        n = MergeNode(
            input_concepts=[],
            output_concepts=[],
            environment=None,
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
        assert len(n.node_joins) == 1
    except Exception as e:
        assert isinstance(e, SyntaxError)
