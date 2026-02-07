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


def test_mobs_query_duplication():
    from logging import INFO

    from trilogy.hooks import DebuggingHook

    DebuggingHook(level=INFO)
    env = Environment(working_path=Path(__file__).parent)
    dialect = Dialects.DUCK_DB.default_executor(environment=env)
    query = """import data_all;


where class = 'Nuda'
and length_cm is not null
select
    class,
    genus.name,
    log_genus_size,
    --round(genus_size_bucket, 1) as size_spread,
    --row_number genus.name over class as class_sample,
    --row_number genus.name over size_spread order by class asc, class_sample asc  as genus_rank,
    case 
        when genus_rank % 2 = 1 then (genus_rank* 1000 + 1) / 2
        else -1 *genus_rank* 1000 / 2
    end as y_scatter,
having
    class_sample <50
;
"""

    query = dialect.generate_sql(query)[0]
    assert '''GROUP BY 
    "data_all_112224"."class",
    "data_all_112224"."genus",
    "data_all_112224"."length_cm"''' not in query, query
