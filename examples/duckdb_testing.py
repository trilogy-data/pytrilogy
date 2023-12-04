
import pandas as pd
from preql import Executor, Dialects
from preql.core.models import Environment
from sqlalchemy import create_engine, text
from preql.core.models import Datasource, Concept, ColumnAssignment, ConceptDeclaration
from preql.core.enums import DataType, Purpose
from os.path import dirname
from pathlib import PurePath
from preql import Executor
from preql.parsing.render import render_query, render_environment, Renderer
from preql.constants import logger
from preql.hooks.query_debugger import DebuggingHook
from logging import StreamHandler

logger.addHandler(StreamHandler())
def setup_engine()->Executor:
    engine = create_engine(r"duckdb:///:memory:", future=True)
    csv = PurePath(dirname(__file__)) / 'train.csv'
    df = pd.read_csv(csv)
    output = Executor(engine=engine, dialect=Dialects.DUCK_DB, hooks=[])

    output.execute_raw_sql("CREATE TABLE raw_titanic AS SELECT * FROM df")
    return output


def setup_titanic(env:Environment):
    namespace = 'passenger'
    id = Concept(name='id', namespace=namespace, datatype=DataType.INTEGER,
                            purpose=Purpose.KEY)
    age = Concept(name='age', namespace=namespace, datatype=DataType.INTEGER, purpose=Purpose.PROPERTY)

    name = Concept(name='name', namespace=namespace, datatype=DataType.STRING, purpose=Purpose.PROPERTY)

    pclass = Concept(name='passenger_class', namespace=namespace, purpose = Purpose.PROPERTY,
                            datatype=DataType.INTEGER
                            )
    survived = Concept(name='survived', namespace=namespace, purpose = Purpose.PROPERTY,
                            datatype=DataType.BOOL
                                                                                                                           )
    fare = Concept(name='fare', namespace=namespace, purpose = Purpose.PROPERTY, datatype=DataType.FLOAT )
    for x in [id, age, survived, name, pclass, fare]:
        env.add_concept(x)

    env.add_datasource(
        Datasource(
            identifier = 'raw_data',
            address = 'raw_titanic',
            columns= [ColumnAssignment(alias='passengerid', concept=id ),
                    ColumnAssignment(alias='age', concept=age),
                    ColumnAssignment(alias='survived', concept=survived),
                    ColumnAssignment(alias='pclass', concept=pclass),
                    ColumnAssignment(alias='name', concept=name),
                    ColumnAssignment(alias='fare', concept=fare)]

        ), 

    )
    return env


# if main gating for python
if __name__ == "__main__": 
    from preql import __version__
    print(__version__)
    executor = setup_engine()
    env = Environment()
    model = setup_titanic(env)
    renderer = Renderer()
    for c, conc in env.concepts.items():
        print(c)

        print(renderer.to_string(ConceptDeclaration(concept=conc)))

    executor.environment = env
    test = '''property passenger.id.family <- split(passenger.name, ',')[1]; 
auto surviving_passenger<- filter passenger.id where passenger.survived =1; 
select 
    passenger.family,
    passenger.id.count,
    count(surviving_passenger) -> surviving_size
order by
    passenger.id.count desc
limit 5;'''

    queries = executor.parse_text(test)
    candidate = queries[-1]
    # alias = candidate.base.get_alias(executor.environment.concepts['passenger.family'])
    # family_source = [c for c in  candidate.ctes if c.name == candidate.base.source_map['passenger.family']][0]

    # print(family_source.source.source_map.keys())

    results= executor. execute_text(test)
    for r in results[0]:
        print(r)
    # print('-------------')
    # print(render_environment(env))