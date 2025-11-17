from trilogy import Environment


def test_canonical():
    env, _ = Environment().parse(
        """
key x int;
property x.val float;

auto total_x <- sum(val) by *;
auto total_val_2 <- sum(val);
"""
    )

    build_env = env.materialize_for_select()
    total_x = build_env.concepts["local.total_x"]
    total_val_2 = build_env.concepts["local.total_val_2"]
    assert total_x.canonical_address == total_val_2.canonical_address
