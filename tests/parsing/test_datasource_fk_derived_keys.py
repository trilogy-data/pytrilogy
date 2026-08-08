"""Foreign keys implied by a datasource declaration.

A datasource that binds a ``Purpose.KEY`` concept OUTSIDE its own grain asserts
that the grain determines that key. The assertion belongs to the datasource, so
it is derived from ``Environment.fk_derived_keys`` wherever ``keys`` is read as
a functional dependency, rather than written back onto the concept at parse
time.
"""

from pathlib import Path

from trilogy import Environment, parse
from trilogy.core.models.build import BuildGrain

FACT_MODEL = (
    "key order_id int;\n"
    "key line_no int;\n"
    "key customer_id int;\n"
    "property <order_id,line_no>.amount float;\n"
    "datasource lines (\n"
    "    oid: order_id,\n"
    "    lno: line_no,\n"
    "    cid: customer_id,\n"
    "    amt: amount,\n"
    ")\n"
    "grain (order_id, line_no)\n"
    "address lines_tbl;\n"
)


def _env(tmp_path: Path) -> Environment:
    (tmp_path / "model.preql").write_text(FACT_MODEL)
    env = Environment(working_path=str(tmp_path))
    parse("import model;", env)
    return env


def test_non_grain_key_column_derives_the_grain_as_its_keys(tmp_path: Path):
    env = _env(tmp_path)
    assert env.fk_derived_keys()["local.customer_id"] == frozenset(
        {"local.order_id", "local.line_no"}
    )
    assert not env.concepts["local.customer_id"].keys
    assert env.concepts["local.customer_id"].effective_keys(env) == {
        "local.order_id",
        "local.line_no",
    }


def test_grain_components_are_not_given_keys(tmp_path: Path):
    env = _env(tmp_path)
    derived = env.fk_derived_keys()
    assert "local.order_id" not in derived
    assert "local.line_no" not in derived
    # a property is not a key, and keeps its declared keys
    assert "local.amount" not in derived


def test_derived_fk_reduces_out_of_a_build_grain(tmp_path: Path):
    env = _env(tmp_path)
    build_env = env.materialize_for_select()
    grain = BuildGrain.from_concepts(
        ["local.order_id", "local.line_no", "local.customer_id"],
        environment=build_env,
    )
    assert "local.customer_id" not in grain.components


def test_derivation_tracks_a_later_datasource(tmp_path: Path):
    env = _env(tmp_path)
    parse(
        "datasource customers (cid: customer_id, oid: order_id)"
        " grain (customer_id) address customers_tbl;",
        env,
    )
    derived = env.fk_derived_keys()
    # the new table is grained on customer_id and binds order_id outside it
    assert derived["local.order_id"] == frozenset({"local.customer_id"})
    # and the fact the fact table declared still stands
    assert derived["local.customer_id"] == frozenset(
        {"local.order_id", "local.line_no"}
    )
