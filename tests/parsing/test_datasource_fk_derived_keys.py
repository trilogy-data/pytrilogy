"""Foreign keys implied by a datasource declaration.

A datasource binding a column at its grain declares the column a function of
its rows (`Environment.binding_fds`); a KEY bound outside the grain is the
classic foreign key. The dependency lives in the FD closure, never on the
concept's `keys`, and every table that binds a key contributes its own.
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


def test_non_grain_key_column_is_determined_by_the_grain(tmp_path: Path):
    env = _env(tmp_path)
    assert env.binding_determines(
        {"local.order_id", "local.line_no"}, "local.customer_id"
    )
    assert not env.binding_determines({"local.order_id"}, "local.customer_id")
    assert not env.concepts["local.customer_id"].keys


def test_grain_components_are_not_determined_by_the_bound_key(tmp_path: Path):
    env = _env(tmp_path)
    assert not env.binding_determines({"local.customer_id"}, "local.order_id")


def test_derived_fk_reduces_out_of_a_build_grain(tmp_path: Path):
    env = _env(tmp_path)
    build_env = env.materialize_for_select()
    grain = BuildGrain.from_concepts(
        ["local.order_id", "local.line_no", "local.customer_id"],
        environment=build_env,
    )
    assert "local.customer_id" not in grain.components


def test_every_binding_contributes_its_own_dependency(tmp_path: Path):
    env = _env(tmp_path)
    parse(
        "datasource customers (cid: customer_id, oid: order_id)"
        " grain (customer_id) address customers_tbl;",
        env,
    )
    assert env.binding_determines({"local.customer_id"}, "local.order_id")
    # the fact table's dependency still stands beside the new one
    assert env.binding_determines(
        {"local.order_id", "local.line_no"}, "local.customer_id"
    )


def test_an_arm_determines_its_columns_only_with_its_discriminator(tmp_path: Path):
    env = Environment(working_path=str(tmp_path))
    parse(
        "key species string;\n"
        "key city enum<string>['A', 'B'];\n"
        "partial datasource arm_a (species, city) grain (species)"
        " complete where city = 'A' address arm_a;",
        env,
    )
    assert not env.binding_determines({"local.species"}, "local.city")
