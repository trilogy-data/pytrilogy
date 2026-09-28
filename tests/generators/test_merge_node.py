from trilogy.core.enums import JoinType
from trilogy.core.models.build_environment import SpanScope
from trilogy.core.models.environment import Environment
from trilogy.core.models.keyspace import Keyspace, Region
from trilogy.core.processing.nodes import ConstantNode, MergeNode, NodeJoin


def _scope(span: str, **witnessed: str) -> SpanScope:
    return SpanScope(
        keyspace=Keyspace(
            regions=(Region(present=frozenset(), spans=frozenset({span})),),
            witnessed=witnessed,
        )
    )


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


def test_merge_captures_the_scope_it_is_built_under(test_environment: Environment):
    """A rowset body's merge can resolve after the outer plan's scope is back
    on the environment, so each merge keeps the scope of its own plan."""
    environment = test_environment.materialize_for_select()
    environment.span_scope = _scope("local.inner_span")
    inner = MergeNode(
        input_concepts=[], output_concepts=[], environment=environment, parents=[]
    )
    environment.span_scope = _scope(
        "local.outer_span", **{"local.inner_span": "local.outer_span"}
    )
    outer = MergeNode(
        input_concepts=[], output_concepts=[], environment=environment, parents=[inner]
    )
    assert inner.span_scope.keyspace.in_play_spans == frozenset({"local.inner_span"})
    assert outer.span_scope.keyspace.in_play_spans == frozenset({"local.outer_span"})
    assert outer.copy().span_scope == outer.span_scope
