from pytest import raises

from trilogy import Dialects


def test_error_not_provided():
    for val in [Dialects.PRESTO, Dialects.TRINO, Dialects.SNOWFLAKE]:
        with raises(ValueError, match="Config must be provided"):
            val.default_engine()
