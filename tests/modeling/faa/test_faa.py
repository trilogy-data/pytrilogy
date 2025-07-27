from pathlib import Path

from build.lib.trilogy.core.exceptions import InvalidSyntaxException
from trilogy import Dialects, Environment
from trilogy.hooks import DebuggingHook
from pytest import raises

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


def test_helpful_error():
    """Make sure we raise a helpful error when we have a join with no grain"""
    DebuggingHook()
    x = Environment(working_path=Path(__file__).parent)

    x = Dialects.DUCK_DB.default_executor(environment=x)
    with raises(InvalidSyntaxException) as e:
        sql = x.generate_sql(
        """import flight;
        
select
    max(dep_time.year_start) as max_year,
    min(dep_time.year_start) min_year;
    """

    )
    assert 'AS ' in str(e.value)