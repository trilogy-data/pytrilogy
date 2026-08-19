"""Round-trip tests for `render_environment`.

`render_environment` writes an environment back out as Trilogy source. Reparsing
that source has to land on the same model: the same concepts, the same
datasources, the same merge edges. An import is represented by its `import`
line, so the rule is that a symbol is declared exactly once — by this file or by
the import that contributed it, never both and never neither.
"""

from pathlib import Path

import pytest

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.models.environment import Environment
from trilogy.parsing.render import Renderer, render_environment

MODELING = Path(__file__).resolve().parents[1] / "modeling"
TPCDS_DIR = MODELING / "tpc_ds_duckdb"


def round_trip(source: str, working_path: Path) -> tuple[Environment, Environment]:
    env = Environment(working_path=working_path)
    env.parse(source)
    reparsed = Environment(working_path=working_path)
    reparsed.parse(render_environment(env))
    return env, reparsed


def public_concepts(env: Environment) -> set[str]:
    return {k for k in env.concepts.data if "__preql_internal" not in k}


def assert_equivalent(env: Environment, reparsed: Environment) -> None:
    assert public_concepts(env) == public_concepts(reparsed)
    assert set(env.datasources.keys()) == set(reparsed.datasources.keys())
    assert set(env.merges) == set(reparsed.merges)


BARE_IMPORT_MODEL = """
import std.geography;

key id int;
property id.state string::us_state_short;

datasource things (
    ID: id,
    STATE: state,
)
grain (id)
address memory.things;
"""


def test_bare_import_keeps_local_declarations():
    env, reparsed = round_trip(BARE_IMPORT_MODEL, TPCDS_DIR)
    rendered = render_environment(env)
    assert "import std.geography;" in rendered
    assert "key id int;" in rendered
    assert_equivalent(env, reparsed)


BARE_IMPORT_OF_MODEL = """
import address;

key order_id int;

datasource orders (
    ORDER_ID: order_id,
    ADDRESS_SK: sk,
)
grain (order_id)
address memory.orders;
"""


def test_bare_import_of_a_model_is_not_re_declared():
    env, reparsed = round_trip(BARE_IMPORT_OF_MODEL, TPCDS_DIR)
    rendered = render_environment(env)
    assert "import address;" in rendered
    assert "key sk int;" not in rendered
    assert "datasource customer_address" not in rendered
    assert_equivalent(env, reparsed)


LOCAL_TYPE_AND_FUNCTION = """
type dollar float;

def double(x) -> x * 2;

key id int;
property id.amount float::dollar;
--property id.secret string;

datasource things (
    ID: id,
    AMOUNT: amount,
    SECRET: secret,
)
grain (id)
address memory.things;
"""


def test_local_types_functions_and_hidden_properties_render():
    env, reparsed = round_trip(LOCAL_TYPE_AND_FUNCTION, TPCDS_DIR)
    rendered = render_environment(env)
    assert "type dollar float;" in rendered
    assert "def double(x) -> x * 2;" in rendered
    assert "--secret string" in rendered
    assert set(env.data_types) == set(reparsed.data_types)
    assert set(env.functions) == set(reparsed.functions)
    assert_equivalent(env, reparsed)


def test_multi_namespace_model_round_trips():
    source = (TPCDS_DIR / "all_sales.preql").read_text()
    env, reparsed = round_trip(source, TPCDS_DIR)
    rendered = render_environment(env)
    assert "import customer as billing_customer;" in rendered
    assert "billing_customer.current_address" not in rendered
    assert_equivalent(env, reparsed)


def test_composite_key_property_survives():
    source = (TPCDS_DIR / "all_sales.preql").read_text()
    env, reparsed = round_trip(source, TPCDS_DIR)
    quantity = reparsed.concepts["quantity"]
    assert quantity.keys == env.concepts["quantity"].keys


def test_namespaced_datasource_keeps_its_namespace():
    env = Environment(working_path=TPCDS_DIR)
    env.parse((TPCDS_DIR / "all_sales.preql").read_text())
    namespaced = [
        d for d in env.datasources.values() if d.namespace != DEFAULT_NAMESPACE
    ]
    assert len({d.name for d in namespaced}) < len(namespaced)
    renderer = Renderer(environment=env)
    assert len({renderer.to_string(d).split("(")[0] for d in namespaced}) == len(
        namespaced
    )


@pytest.mark.parametrize(
    "model", ["stocks/entrypoint.preql", "hackernews/hackernews.preql"]
)
def test_merges_survive_round_trip(model: str):
    path = MODELING / model
    env, reparsed = round_trip(path.read_text(), path.parent)
    assert env.merges
    assert_equivalent(env, reparsed)


def test_relative_import_keeps_leading_dots():
    path = TPCDS_DIR / "aggregates" / "opt_three.preql"
    env = Environment(working_path=path.parent)
    env.parse(path.read_text())
    assert "import ..store_sales as store_sales;" in render_environment(env)


def test_rowset_members_are_not_declared():
    path = TPCDS_DIR / "query35.preql"
    env = Environment(working_path=TPCDS_DIR)
    env.parse(path.read_text())
    rendered = render_environment(env)
    assert env.concepts.rowset_namespaces
    assert "store_buyers.store_cust_id" not in rendered
