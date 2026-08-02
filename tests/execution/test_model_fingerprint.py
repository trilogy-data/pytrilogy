from pathlib import Path

from trilogy import parse
from trilogy.core.fingerprint import ChangeKind, build_environment_fingerprint
from trilogy.execution.envs import (
    EnvActivation,
    EnvironmentManager,
    apply_env_prefix,
    apply_env_to_environment,
    strip_env_prefix,
)
from trilogy.execution.model_fingerprint import (
    build_project_fingerprint,
    diff_project_fingerprints,
    load_project_fingerprint,
    update_project_fingerprint,
)

MODEL_A = """
key order_id int;
property order_id.amount float;
key customer_id int;

datasource orders (
    order_id: order_id,
    amount: amount,
    customer_id: customer_id
)
grain (order_id)
address orders_tbl;

auto total_amount <- sum(amount) by customer_id;

datasource customer_totals (
    customer_id: customer_id,
    total_amount: total_amount
)
grain (customer_id)
address customer_totals_tbl;
"""

MODEL_B = """
key product_id int;
property product_id.price float;

datasource products (
    product_id: product_id,
    price: price
)
grain (product_id)
address products_tbl;
"""

PERSIST_MODEL = """
key order_id int;
property order_id.amount int;

datasource raw_orders (
    order_id: order_id,
    amount: amount
)
grain (order_id)
address raw_orders;

persist orders_summary into orders_summary from
select
    order_id,
    amount
;
"""


def test_strip_env_prefix_round_trips():
    for location in ("orders", "schema.orders", "db.schema.orders"):
        assert strip_env_prefix(apply_env_prefix(location, "dev"), "dev") == location
    for location in ("gs://bucket/path/f.parquet", "data/out.parquet", "noext"):
        applied = apply_env_prefix(location, "dev", is_file=True)
        assert strip_env_prefix(applied, "dev", is_file=True) == location


def test_strip_env_prefix_leaves_unprefixed_alone():
    assert strip_env_prefix("orders", "dev") == "orders"
    assert strip_env_prefix("data/out.parquet", "dev", is_file=True) == (
        "data/out.parquet"
    )


def test_fingerprint_is_env_invariant(tmp_path: Path):
    plain_env, _ = parse(MODEL_A)
    plain = build_environment_fingerprint(plain_env)

    scoped_env, _ = parse(MODEL_A)
    manager = EnvironmentManager("proj", home=tmp_path)
    activation = EnvActivation(name="dev", manager=manager)
    apply_env_to_environment(scoped_env, activation)
    scoped = build_environment_fingerprint(scoped_env)

    assert scoped_env.datasources["orders"].safe_address == "dev_orders_tbl"
    assert plain.root == scoped.root
    assert plain.model_dump() == scoped.model_dump()


def test_project_fingerprint_diff_and_storage(tmp_path: Path):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "a.preql").write_text(MODEL_A, encoding="utf-8")
    (scripts / "b.preql").write_text(MODEL_B, encoding="utf-8")

    original = build_project_fingerprint(
        [scripts / "a.preql", scripts / "b.preql"], tmp_path
    )
    assert set(original.scripts) == {"scripts/a.preql", "scripts/b.preql"}

    manager = EnvironmentManager("proj", home=tmp_path / "home")
    update_project_fingerprint(manager, "dev", original)
    recorded = load_project_fingerprint(manager, "dev")
    assert recorded is not None and recorded.root == original.root
    assert diff_project_fingerprints(recorded, original).identical

    (scripts / "a.preql").write_text(
        MODEL_A.replace("sum(amount)", "sum(amount * 2)"), encoding="utf-8"
    )
    current = build_project_fingerprint(
        [scripts / "a.preql", scripts / "b.preql"], tmp_path
    )
    diff = diff_project_fingerprints(recorded, current)
    assert not diff.identical
    assert set(diff.changed_scripts) == {"scripts/a.preql"}
    assert diff.invalidated_locations == ["customer_totals_tbl"]
    entry = diff.invalidated_datasources[0]
    assert entry.datasource_id == "customer_totals"
    assert entry.kind == ChangeKind.UPSTREAM


def test_partial_update_preserves_other_scripts(tmp_path: Path):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "a.preql").write_text(MODEL_A, encoding="utf-8")
    (scripts / "b.preql").write_text(MODEL_B, encoding="utf-8")
    manager = EnvironmentManager("proj", home=tmp_path / "home")

    both = build_project_fingerprint(
        [scripts / "a.preql", scripts / "b.preql"], tmp_path
    )
    update_project_fingerprint(manager, "dev", both)

    (scripts / "a.preql").write_text(
        MODEL_A.replace("sum(amount)", "sum(amount * 3)"), encoding="utf-8"
    )
    only_a = build_project_fingerprint([scripts / "a.preql"], tmp_path)
    merged = update_project_fingerprint(manager, "dev", only_a)
    assert set(merged.scripts) == {"scripts/a.preql", "scripts/b.preql"}
    assert merged.scripts["scripts/b.preql"] == both.scripts["scripts/b.preql"]
    assert merged.scripts["scripts/a.preql"] != both.scripts["scripts/a.preql"]


def test_persist_targets_are_fingerprinted(tmp_path: Path):
    (tmp_path / "model.preql").write_text(PERSIST_MODEL, encoding="utf-8")
    original = build_project_fingerprint([tmp_path / "model.preql"], tmp_path)
    assert "orders_summary" in original.scripts["model.preql"].datasources

    (tmp_path / "model.preql").write_text(
        PERSIST_MODEL.replace("    amount\n", "    amount * 2 -> amount_doubled\n"),
        encoding="utf-8",
    )
    current = build_project_fingerprint([tmp_path / "model.preql"], tmp_path)
    diff = diff_project_fingerprints(original, current)
    assert not diff.identical
    assert "orders_summary" in {d.datasource_id for d in diff.invalidated_datasources}
