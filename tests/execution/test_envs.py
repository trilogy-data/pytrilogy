from pathlib import Path

from trilogy.core.models.datasource import Address, AddressType, Datasource
from trilogy.execution.envs import (
    EnvActivation,
    EnvironmentManager,
    active_env,
    apply_env_prefix,
    env_activation_scope,
    env_backup_address,
    transform_datasource,
)


def make_ds(address: Address | str, is_root: bool = False) -> Datasource:
    return Datasource(name="orders", columns=[], address=address, is_root=is_root)


def test_apply_env_prefix_tables():
    assert apply_env_prefix("orders", "dev") == "dev_orders"
    assert apply_env_prefix("schema.orders", "dev") == "schema.dev_orders"
    assert apply_env_prefix("db.schema.orders", "dev") == "db.schema.dev_orders"


def test_apply_env_prefix_files():
    assert (
        apply_env_prefix("gs://bucket/path/f.parquet", "dev", is_file=True)
        == "gs://bucket/path/f_dev.parquet"
    )
    assert (
        apply_env_prefix("data/out.parquet", "dev", is_file=True)
        == "data/out_dev.parquet"
    )
    assert apply_env_prefix("noext", "dev", is_file=True) == "noext_dev"


def test_env_backup_address():
    assert env_backup_address("schema.orders") == "schema.orders__pub_backup"


def test_transform_rewrites_managed_table():
    ds = make_ds(Address(location="schema.orders"))
    rewritten = transform_datasource(ds, "dev")
    assert rewritten is not None and rewritten.location == "schema.dev_orders"
    assert ds.safe_address == "schema.dev_orders"
    assert isinstance(ds.address, Address) and ds.address.env_label == "dev"


def test_transform_is_idempotent():
    ds = make_ds(Address(location="orders"))
    transform_datasource(ds, "dev")
    assert transform_datasource(ds, "dev") is None
    assert ds.safe_address == "dev_orders"


def test_transform_skips_roots_and_queries():
    root = make_ds(Address(location="orders"), is_root=True)
    assert transform_datasource(root, "dev") is None
    assert root.safe_address == "orders"

    query = make_ds(Address(location="select 1", type=AddressType.QUERY))
    assert transform_datasource(query, "dev") is None
    assert query.safe_address == "select 1"


def test_transform_rewrites_file_locations():
    ds = make_ds(
        Address(
            location="out/a.parquet",
            write_location="out/a.parquet",
            type=AddressType.PARQUET,
        )
    )
    transform_datasource(ds, "dev")
    assert isinstance(ds.address, Address)
    assert ds.address.location == "out/a_dev.parquet"
    assert ds.address.write_location == "out/a_dev.parquet"


def test_manager_registry_lifecycle(tmp_path: Path):
    manager = EnvironmentManager("proj", home=tmp_path)
    assert manager.list_envs() == []
    assert manager.get_active() is None

    meta = manager.create("dev")
    assert meta.name == "dev"
    assert manager.exists("dev")
    assert [m.name for m in manager.list_envs()] == ["dev"]

    manager.activate("dev")
    assert manager.get_active() == "dev"
    manager.deactivate()
    assert manager.get_active() is None

    manager.track_assets("dev", ["dev_orders", "dev_items"])
    manager.track_assets("dev", ["dev_orders"])
    assert manager.get_meta("dev").tracked_assets == ["dev_orders", "dev_items"]

    manager.clear_tracked_assets("dev")
    assert manager.get_meta("dev").tracked_assets == []

    manager.delete("dev")
    assert not manager.exists("dev")


def test_manager_create_duplicate_and_missing_errors(tmp_path: Path):
    manager = EnvironmentManager("proj", home=tmp_path)
    manager.create("dev")
    try:
        manager.create("dev")
        raise AssertionError("expected ValueError")
    except ValueError:
        pass
    try:
        manager.activate("nope")
        raise AssertionError("expected ValueError")
    except ValueError:
        pass


def test_delete_active_env_deactivates(tmp_path: Path):
    manager = EnvironmentManager("proj", home=tmp_path)
    manager.create("dev")
    manager.activate("dev")
    manager.delete("dev")
    assert manager.get_active() is None


def test_activation_scope_tracks_and_flushes(tmp_path: Path):
    manager = EnvironmentManager("proj", home=tmp_path)
    activation = EnvActivation(name="dev", manager=manager)
    assert active_env() is None
    with env_activation_scope(activation):
        assert active_env() is activation
        ds = make_ds(Address(location="orders"))
        activation.transform(ds)
        assert ds.safe_address == "dev_orders"
    assert active_env() is None
    assert manager.get_meta("dev").tracked_assets == ["table:dev_orders"]


def test_activation_scope_none_is_noop():
    with env_activation_scope(None):
        assert active_env() is None


def test_transform_rewrites_additional_locations():
    ds = make_ds(
        Address(
            location="out/a.parquet",
            additional_locations=["out/b.parquet"],
            type=AddressType.PARQUET,
        )
    )
    transform_datasource(ds, "dev")
    assert isinstance(ds.address, Address)
    assert ds.address.additional_locations == ["out/b_dev.parquet"]
