from trilogy.core.enums import JoinType
from trilogy.core.models.environment import Environment
from trilogy.core.processing.nodes import ConstantNode, MergeNode, NodeJoin
from trilogy.core.processing.nodes.merge_node import tree_in_play_spans


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


def test_tree_in_play_spans_reads_the_plans_nested_below(
    test_environment: Environment,
):
    environment = test_environment.materialize_for_select()
    environment.in_play_spans = frozenset({"local.inner_span"})
    inner = MergeNode(
        input_concepts=[], output_concepts=[], environment=environment, parents=[]
    )
    environment.in_play_spans = frozenset({"local.outer_span"})
    outer = MergeNode(
        input_concepts=[], output_concepts=[], environment=environment, parents=[inner]
    )
    assert outer.in_play_spans == frozenset({"local.outer_span"})
    assert outer.copy().in_play_spans == outer.in_play_spans
    assert tree_in_play_spans(outer, set()) == frozenset(
        {"local.inner_span", "local.outer_span"}
    )
