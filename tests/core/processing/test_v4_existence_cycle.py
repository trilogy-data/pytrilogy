"""Regression: existence-source attachment must not wire a row-stream cycle.

`_existence_parents_for` attaches the built node that supplies an existence
concept as an extra (subselect side-channel) parent. `StrategyNode.copy()`
shallow-shares `parents`, so if the supplying node's subtree already contains
the host node, a naive copy creates `host -> candidate -> ... -> host`, which
recurses forever in `resolve()` (the q10 / q2.1 RecursionError). The fix
deep-copies such a candidate so the shared host becomes an independent
duplicate.
"""

from trilogy.core.models.environment import Environment
from trilogy.core.processing.nodes.base_node import StrategyNode
from trilogy.core.processing.v4_helper.strategy_builder import (
    _existence_parents_for,
    _strategy_nodes,
)


def _env() -> Environment:
    env = Environment()
    env.parse("""
key order_id int;
property order_id.buyers int;
""")
    return env.materialize_for_select()


def test_existence_parent_deep_copies_cyclic_candidate():
    env = _env()
    order_id = env.concepts["order_id"]
    buyers = env.concepts["buyers"]

    # host -> ... and candidate's subtree includes host: candidate's parent IS host.
    host = StrategyNode(input_concepts=[], output_concepts=[order_id], environment=env)
    candidate = StrategyNode(
        input_concepts=[order_id],
        output_concepts=[buyers],
        environment=env,
        parents=[host],
    )
    built = {"host": host, "candidate": candidate}

    parents = _existence_parents_for([buyers], built, skip=host)

    assert len(parents) == 1
    result = parents[0]
    # still supplies the existence concept
    assert any(o.address == buyers.address for o in result.output_concepts)
    # cycle broken: the original host object is absent from the returned subtree
    assert all(n is not host for n in _strategy_nodes(result))


def test_existence_parent_shallow_copy_when_acyclic():
    env = _env()
    order_id = env.concepts["order_id"]
    buyers = env.concepts["buyers"]

    host = StrategyNode(input_concepts=[], output_concepts=[order_id], environment=env)
    leaf = StrategyNode(input_concepts=[], output_concepts=[order_id], environment=env)
    candidate = StrategyNode(
        input_concepts=[order_id],
        output_concepts=[buyers],
        environment=env,
        parents=[leaf],
    )
    built = {"host": host, "candidate": candidate}

    parents = _existence_parents_for([buyers], built, skip=host)

    assert len(parents) == 1
    # acyclic candidate keeps its original parent objects (shallow copy)
    assert any(n is leaf for n in _strategy_nodes(parents[0]))
