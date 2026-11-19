from trilogy.core.enums import Derivation
from trilogy.core.models.build import BuildConcept
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.environment import Environment
from trilogy.core.processing.nodes import RowsetNode, SelectNode
from trilogy.core.processing.v4_helper.projection import (
    concept_satisfiable,
    literal_producible,
    renders_at,
    row_lineage_arguments,
    satisfiable_outputs,
)

MODEL = """
key id int;
property id.amount float;
datasource rows (id: id, amount: amount) grain (id) address rows;
rowset agg <- select id, amount + 1 -> bumped;
rowset other <- select id;
auto twice <- agg.bumped * 2;
"""


def _environment() -> BuildEnvironment:
    env = Environment()
    env.parse(MODEL)
    return env.materialize_for_select()


def _boundary(env: BuildEnvironment, *handles: BuildConcept) -> RowsetNode:
    contents = [h.lineage.content for h in handles]
    body = SelectNode(
        input_concepts=contents, output_concepts=contents, environment=env
    )
    return RowsetNode(
        input_concepts=contents,
        output_concepts=list(handles),
        parents=[body],
        environment=env,
    )


def _body_columns(*handles: BuildConcept) -> set[str]:
    return {h.lineage.content.address for h in handles}


def test_a_handle_is_a_leaf():
    env = _environment()
    handle = env.concepts["agg.bumped"]
    assert handle.derivation == Derivation.ROWSET
    assert row_lineage_arguments(handle) == []
    assert not concept_satisfiable(handle, _body_columns(handle))
    assert not literal_producible(handle)


def test_only_its_own_boundary_computes_a_handle():
    env = _environment()
    bumped, agg_id = env.concepts["agg.bumped"], env.concepts["agg.id"]
    other_id = env.concepts["other.id"]
    available = _body_columns(agg_id, bumped)
    boundary = _boundary(env, agg_id)
    assert renders_at(boundary, bumped, available)
    assert renders_at(boundary, env.concepts["twice"], available)
    assert not renders_at(_boundary(env, other_id), bumped, available)
    scan = SelectNode(input_concepts=[], output_concepts=[], environment=env)
    assert not renders_at(scan, bumped, available)


def test_a_rowset_group_builds_its_handles_off_their_content():
    env = _environment()
    handles = [env.concepts["agg.id"], env.concepts["agg.bumped"]]
    contents = [h.lineage.content for h in handles]
    body = SelectNode(
        input_concepts=contents, output_concepts=contents, environment=env
    )
    assert satisfiable_outputs(handles, [body]) == []
    assert satisfiable_outputs(handles, [body], boundary=True) == handles
