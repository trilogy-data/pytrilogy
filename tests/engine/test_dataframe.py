from pathlib import Path

import pandas as pd

from trilogy import Dialects, Environment, Executor
from trilogy.dialect.config import DataFrameConfig
from trilogy.hooks import DebuggingHook


def test_dataframe_executor():
    DebuggingHook()
    df = pd.read_csv(Path(__file__).parent / "test.csv")

    env = Environment()
    env.parse(
        """
key id int;
property id.age int;

datasource data (
id: id,
age: age)              
grain (id)
address df
;   
"""
    )

    executor: Executor = Dialects.DATAFRAME.default_executor(
        environment=env, conf=DataFrameConfig(dataframes={"df": df})
    )
    results = executor.execute_query(
        """
select avg(age) -> avg_age;
        """
    )
    assert results.fetchall()[0].avg_age == 45

    df = pd.read_csv(Path(__file__).parent / "enrich.csv")
    env.parse(
        """
key id int;
property id.name string;

datasource name_data (
id: id,
name: name)              
grain (id)
address df2
;   
"""
    )

    executor.engine.add_dataframe("df2", df, executor.connection, env)

    results = executor.execute_query(
        """
select age ? name = "diane" -> diane_age;
        """
    )
    assert results.fetchall()[0].diane_age == 67
