def test_derivations(test_environment):
    assert (
        test_environment.concepts["order_timestamp"].address == "local.order_timestamp"
    )
    assert (
        test_environment.concepts["order_timestamp.date"].address
        == "local.order_timestamp.date"
    )
