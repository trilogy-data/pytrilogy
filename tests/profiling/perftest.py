from cProfile import Profile
from pstats import SortKey, Stats
from trilogy.core.models import SelectStatement
from trilogy import parse

# from trilogy.compiler import compile

from trilogy.core.models import DataType
from trilogy.core.query_processor import process_query
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.dialect.sql_server import SqlServerDialect

from trilogy import Environment
from trilogy.core.enums import (
    Purpose,
    FunctionType,
    ComparisonOperator,
    WindowType,
)
from trilogy.core.functions import Count, CountDistinct, Max, Min
from trilogy.core.models import (
    Concept,
    Datasource,
    ColumnAssignment,
    Function,
    Grain,
    WindowItem,
    FilterItem,
    OrderItem,
    WhereClause,
    Comparison,
)
from trilogy import Environment, Dialects, Executor
from trilogy.hooks.query_debugger import DebuggingHook
from pathlib import Path
from trilogy.parsing.parse_engine import parse_text, parse_text_raw

from datetime import datetime

def parsetest():
    env = Environment(working_path=Path(__file__).parent)
    working_path =Path(__file__).parent    
        
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env)
    with open(working_path / f"query12.preql") as f:
        text = f.read()
    start = datetime.now()
    parse_text_raw(text)
    print(datetime.now()-start)
    start = datetime.now()
    parse_text(text, env)
    print(datetime.now()-start)
#513958/513273
#513840/513155
#446226/445526
if __name__ == "__main__":
    with Profile() as profile:
        parsetest()
        (Stats(profile).strip_dirs().sort_stats(SortKey.TIME).print_stats(25))
