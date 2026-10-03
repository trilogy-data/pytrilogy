from trilogy import parse
from trilogy.core.enums import ComparisonOperator
from trilogy.core.models.build import BuildComparison, BuildWhereClause
from trilogy.core.models.build_environment import SpanScope
from trilogy.core.processing.nodes import StrategyNode
from trilogy.core.processing.v4_helper.strategy_builder import (
    RootRequest,
    _existence_parents_for,
)


def test_two_sets_off_one_built_group_each_get_a_slice():
    env, _ = parse("""
key a int;
key b int;
datasource ab (a:a, b:b) grain (a, b) address ab;
""")
    build_env = env.materialize_for_select()
    a, b = build_env.concepts["a"], build_env.concepts["b"]
    provider = StrategyNode(
        input_concepts=[a, b], output_concepts=[a, b], environment=build_env
    )
    feeders = _existence_parents_for([(a,), (b,)], {"g": provider})
    assert [[c.address for c in f.output_concepts] for f in feeders] == [
        [a.address],
        [b.address],
    ]


def test_root_request_is_not_answered_under_other_inherited_filters():
    env, _ = parse("""
key a int;
datasource s (a:a) grain (a) address s;
""")
    build_env = env.materialize_for_select()
    a = build_env.concepts["a"]
    node = StrategyNode(input_concepts=[a], output_concepts=[a], environment=build_env)
    where = BuildWhereClause(
        conditional=BuildComparison(left=a, right=1, operator=ComparisonOperator.GT)
    )
    bare = RootRequest(frozenset({a.address}), None, SpanScope())
    inherited = RootRequest(
        frozenset({a.address}), None, SpanScope(), preexisting=where
    )
    assert bare != inherited
    assert not inherited.answered_by(node, bare)
    assert bare.answered_by(node, bare)
