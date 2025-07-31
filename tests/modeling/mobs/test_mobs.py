from pathlib import Path

from trilogy import Dialects, Environment
from trilogy.hooks import DebuggingHook


def test_mobs_query_discovery():

    env = Environment(working_path=Path(__file__).parent)
    dialect = Dialects.DUCK_DB.default_executor(environment=env)
    DebuggingHook()

    query = """import data_all;
    
    where genus.image is not null and log_length_bin='100-1000 (Dolphin or tuna)'
    SELECT
        genus.description, --genus.name,
        -- avg(length_cm) as avg_length
    order by
        avg_length desc
    limit 1;"""

    query = dialect.generate_sql(query)
