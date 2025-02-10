from trilogy import Dialects, Environment
from trilogy.core.query_processor import process_query
from trilogy.render import get_dialect_generator


def test_sql_generators():
    env = Environment()
    _, statements = env.parse("""const a <- 1; select a;""")
    processed = process_query(env, statements[-1])
    for dialect in Dialects:
        generator = get_dialect_generator(dialect)
        compiled = generator.compile_statement(processed)
        assert compiled.startswith("""SELECT\n    :a"""), f"{dialect} compiled"
