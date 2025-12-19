from trilogy import Environment, Executor
from trilogy.execution.state.state_store import BaseStateStore


def get_default_root_assets(env: Environment) -> set[str]:
    # TODO: find all assets that are not updated by persist statements in the entire workspace
    return set()


def get_assets_to_update(
    env: Environment,
    store: BaseStateStore,
    executor: Executor,
    root_assets: set[str] | None,
) -> None:
    root_assets = root_assets or get_default_root_assets(env)

    for root in root_assets:
        datasource = env.datasources[root]
        store.watermark_asset(datasource, executor)

    keys_to_check = [x for x in env.datasources.keys() if x not in root_assets]

    for key in keys_to_check:
        store.check_datasource_state(env.datasources[key])
