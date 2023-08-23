
from preql import Environment, Executor, Dialects
from preql.core.models import Concept, Datasource, DataType, Purpose, Function, FunctionType
from sqlalchemy import create_engine
from preql.constants import logger
from logging import StreamHandler, INFO, DEBUG
logger.addHandler(StreamHandler())
logger.setLevel(DEBUG)
duckdb = Environment()

lineitem = Datasource(
   identifier='lineitem',
   address = r"'C:\Users\ethan\coding_projects\pypreql\examples\lineitem.parquet'",
    columns = []
)

order_key = duckdb.add_concept(
    Concept(
        name='l_orderkey',
        datatype = DataType.INTEGER,
        purpose = Purpose.KEY,

    )
)

text= duckdb.add_concept(
    Concept(
        name='l_comment',
        datatype = DataType.STRING,
        purpose = Purpose.PROPERTY,

    )
)

text_length = duckdb.add_concept(
    Concept(
        name='l_comment.length',
        datatype = DataType.INTEGER,
        purpose = Purpose.PROPERTY,
        lineage = Function(operator=FunctionType.LENGTH, arguments=[text],
                           output_datatype=DataType.INTEGER,
                           output_purpose = Purpose.PROPERTY)
    )
)

lineitem .add_column(order_key, 'l_orderkey')
lineitem .add_column(text, 'l_comment')

duckdb.add_datasource(lineitem )


executor = Executor(
    dialect=Dialects.DUCK_DB,
    engine = create_engine("duckdb:///:memory:"),
    environment=duckdb 
)
cmds = [
    'INSTALL httpfs',
    'LOAD httpfs'

]
for cmd in cmds:
    # you need to modify PREQL to have this property
    # does not exist yet
    executor.connection.execute(cmd)

# con.sql(f"""SELECT horoscope, 
#     count(*), 
#     AVG(LENGTH(text)) AS avg_blog_length 
#     FROM '{url}' 
#     GROUP BY horoscope 
#     ORDER BY avg_blog_length 
#     DESC LIMIT(5)"""
# )
output = executor.execute_text(


    '''
persist into test
select 
avg(l_comment.length) -> avg_comment_length
 limit 5;


'''


)
from preql.core.processing.nodes.select_node import SelectNode
from preql.core.env_processor import generate_graph
test = SelectNode(
                [executor.environment.concepts['avg_comment_length']],
                [],
                executor.environment,
                generate_graph(executor.environment),
                parents=[
                ],
            )
            
resolved = test.resolve_direct_select()
print(resolved)

for ds in resolved.datasources:
    print(ds)
    print(ds.address)
# if resolved:
#     logger.info(f'{LOGGER_PREFIX} found direct select node {resolved} has {resolved.output_concepts} against {mandatory_concepts}!')
#     return test
output = executor.execute_text(
    '''select avg_comment_length;'''

)


for  rs in output:
    
    for row in rs:
        print(row)
