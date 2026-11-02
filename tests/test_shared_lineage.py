import time

from trilogy import Dialects, Environment

DEPTH = 40


def shared_chain(depth: int) -> Environment:
    lines = [
        "key id int;",
        "property id.x float;",
        "datasource rows (id: id, x: x) grain (id) address rows;",
        "auto c0 <- x;",
    ]
    lines += [f"auto c{i} <- c{i - 1} + c{i - 1};" for i in range(1, depth + 1)]
    env = Environment()
    env.parse("\n".join(lines))
    return env


def test_sources_lists_each_parent_once():
    build_env = shared_chain(3).materialize_for_select()
    sources = build_env.concepts["c3"].sources
    assert [c.address for c in sources] == [
        "local.c2",
        "local.c1",
        "local.c0",
        "local.x",
    ]


def test_a_deep_shared_lineage_plans_in_linear_time():
    executor = Dialects.DUCK_DB.default_executor(environment=shared_chain(DEPTH))
    start = time.monotonic()
    executor.generate_sql("select id, x;")
    assert time.monotonic() - start < 10
