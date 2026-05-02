from trilogy import Dialects, parse
from trilogy.constants import Rendering
from trilogy.core.models.environment import Environment
from trilogy.dialect.clickhouse import ClickhouseDialect
from trilogy.dialect.config import ClickhouseConfig
from trilogy.render import get_dialect_generator


def test_dialect_registered():
    assert Dialects("clickhouse") is Dialects.CLICKHOUSE
    assert Dialects("chdb") is Dialects.CLICKHOUSE


def test_render_const_query():
    generator = get_dialect_generator(Dialects.CLICKHOUSE)
    assert isinstance(generator, ClickhouseDialect)
    assert generator.QUOTE_CHARACTER == "`"

    env = Environment()
    env, _ = parse("const pi <- 3.14;", environment=env)
    executor = Dialects.CLICKHOUSE.default_executor(
        environment=env,
        conf=ClickhouseConfig(mode="chdb"),
        rendering=Rendering(parameters=False),
    )
    sql = executor.generate_sql("select pi;")[0]
    assert "3.14" in sql
    assert "`pi`" in sql
