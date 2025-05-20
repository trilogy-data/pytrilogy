from pathlib import Path

from trilogy import Dialects, Environment
from trilogy.hooks import DebuggingHook


def test_query_gen():
    """Make sure we inject another group by when conditions forced an evaluation with an early grain"""
    DebuggingHook()
    x = Environment(working_path=Path(__file__).parent)

    x = Dialects.DUCK_DB.default_executor(environment=x)

    sql = x.generate_sql(
        """import flight;

where local.dep_time.month_start between '2001-12-31'::date and '2002-03-31'::date  
select 
    count(carrier.name) as carrier_count;
    """
    )[-1]
    # if we don't have this group by, we will get the wrong result
    assert (
        '''SELECT
    "cheerful"."carrier_code" as "carrier_code",
    "cheerful"."carrier_name" as "carrier_name"
FROM
    "cheerful"
GROUP BY 
    "cheerful"."carrier_code",
    "cheerful"."carrier_name"'''
        in sql
    )
