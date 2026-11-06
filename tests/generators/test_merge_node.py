from trilogy.core.enums import JoinType
from trilogy.core.models.build_environment import SpanScope
from trilogy.core.models.environment import Environment
from trilogy.core.models.keyspace import Keyspace, Region
from trilogy.core.processing.join_resolution import PlannedPairs
from trilogy.core.processing.nodes import ConstantNode, MergeNode, NodeJoin
from trilogy.core.processing.v4_helper.join_plan import JoinPlan, Pairing, PairingKind
from trilogy.core.processing.v4_helper.strategy_builder import (
    _deep_copy_node,
    _plan_moved_contributor,
)


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
                    left=x,
                    right=x,
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


def _constants(environment, count: int) -> list[ConstantNode]:
    return [
        ConstantNode(
            input_concepts=[],
            output_concepts=[environment.concepts["constant_one"]],
            environment=environment,
            parents=[],
        )
        for _ in range(count)
    ]


def test_replaced_parents_carry_the_joins_and_the_plan(
    test_environment: Environment,
):
    environment = test_environment.materialize_for_select()
    a, b, c = _constants(environment, 3)
    merge = MergeNode(
        input_concepts=[],
        output_concepts=[],
        environment=environment,
        parents=[a, b],
        planned_pairs=PlannedPairs(
            {id(a): frozenset({"ga"}), id(b): frozenset({"gb"})}, {}
        ),
    )
    merge.node_joins = [
        NodeJoin(
            left=a,
            right=b,
            concepts=[environment.concepts["constant_one"]],
            join_type=JoinType.INNER,
        )
    ]
    merge.replace_parents([c, b])
    assert merge.node_joins[0].left is c
    assert merge.planned_pairs.sides == {id(c): {"ga"}, id(b): {"gb"}}
    clone = _deep_copy_node(merge)
    assert set(clone.planned_pairs.sides) == {id(p) for p in clone.parents}


def test_moved_contributor_joins_the_plan_only_when_planned(
    test_environment: Environment,
):
    environment = test_environment.materialize_for_select()
    row, stream, other, stray = _constants(environment, 4)
    plan = JoinPlan(
        contributors=("s", "o"),
        pins={},
        identity={},
        carry_demands={},
        pairings=(Pairing("s", "o", frozenset({"k"}), PairingKind.LOOKUP),),
        carries={"s": frozenset(), "o": frozenset()},
    )
    origin = {id(stream): "s", id(other): "o"}
    unplanned = MergeNode(
        input_concepts=[], output_concepts=[], environment=environment, parents=[row]
    )
    _plan_moved_contributor(unplanned, other, stream, origin, plan)
    assert unplanned.planned_pairs is None
    merge = MergeNode(
        input_concepts=[],
        output_concepts=[],
        environment=environment,
        parents=[row],
        planned_pairs=PlannedPairs({id(row): frozenset({"r"})}, {}),
    )
    _plan_moved_contributor(merge, stray, stream, origin, plan)
    assert id(stray) not in merge.planned_pairs.sides
    _plan_moved_contributor(merge, other, stream, origin, plan)
    assert merge.planned_pairs.sides[id(other)] == {"o"}
    assert merge.planned_pairs.between(frozenset({"o"}), frozenset({"r"})) == {"k"}
