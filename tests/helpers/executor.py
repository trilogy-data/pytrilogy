from trilogy.dialect.enums import DialectConfig


def mock_factory(conf: DialectConfig, config_type, **kwargs):
    from sqlalchemy import create_engine

    if not isinstance(conf, config_type):
        raise TypeError(
            f"Invalid dialect configuration for type {type(config_type).__name__}"
        )
    assert conf.connection_string()
    return create_engine("duckdb:///:memory:", future=True)
