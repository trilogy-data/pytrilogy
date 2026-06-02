"""Validate every `trilogy agent-info syntax example` against real models.

The examples are advertised as canonical, copy-pasteable syntax. This test bed
stands up tiny in-memory DuckDB models (iris + a university enrollments schema)
matching the concept names the examples use, then COMPILES and EXECUTES each
example so a syntax/semantic regression fails CI instead of misleading an agent.
"""

from __future__ import annotations

import pytest

from trilogy import Dialects
from trilogy.ai.syntax_examples import SYNTAX_EXAMPLES, available_names
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import DuckDBConfig

IRIS = """
key id int;
property id.species string;
property id.sepal_length float;
property id.sepal_width float;
property id.petal_length float;
property id.petal_width float;

datasource iris_data (
    id: id,
    species: species,
    sepal_length: sepal_length,
    sepal_width: sepal_width,
    petal_length: petal_length,
    petal_width: petal_width,
)
grain (id)
query '''
select 1 as id, 'setosa' as species, 5.1 as sepal_length, 3.5 as sepal_width, 1.4 as petal_length, 0.2 as petal_width
union all select 2, 'setosa', 4.9, 3.0, 1.4, 0.2
union all select 3, 'versicolor', 7.0, 3.2, 4.7, 1.4
union all select 4, 'versicolor', 6.4, 3.2, 4.5, 1.5
union all select 5, 'virginica', 6.3, 3.3, 6.0, 2.5
union all select 6, 'virginica', 5.8, 2.7, 5.1, 1.9
''';
"""

ENROLLMENTS = """
key id int;
property id.student_id int;
property id.course string;
property id.year int;
property id.completed bool;

datasource enrollments_data (
    id: id,
    student_id: student_id,
    course: course,
    year: year,
    completed: completed,
)
grain (id)
query '''
select 1 as id, 101 as student_id, 'Biology 101' as course, 2015 as year, true as completed
union all select 2, 102, 'Biology 101', 2016, false
union all select 3, 103, 'Chemistry 101', 2017, true
union all select 4, 101, 'Chemistry 101', 2018, true
union all select 5, 104, 'Biology 101', 2019, false
union all select 6, 105, 'Physics 101', 2020, true
union all select 7, 106, 'Biology 101', 2020, false
union all select 8, 107, 'Physics 101', 2021, true
''';
"""

STUDENTS = """
key id int;
property id.major string;

datasource students_data (
    id: id,
    major: major,
)
grain (id)
query '''
select 101 as id, 'Biology' as major
union all select 102, 'Biology'
union all select 103, 'Chemistry'
union all select 104, 'Physics'
union all select 105, 'Physics'
union all select 106, 'Biology'
union all select 107, 'Math'
union all select 108, 'Math'
''';
"""


@pytest.fixture(scope="module")
def model_dir(tmp_path_factory):
    d = tmp_path_factory.mktemp("syntax_example_models")
    (d / "iris.preql").write_text(IRIS, encoding="utf-8")
    (d / "enrollments.preql").write_text(ENROLLMENTS, encoding="utf-8")
    (d / "students.preql").write_text(STUDENTS, encoding="utf-8")
    return d


@pytest.fixture(scope="module")
def executor(model_dir):
    return Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=model_dir),
        conf=DuckDBConfig(),
    )


@pytest.mark.parametrize("name", available_names())
def test_syntax_example_compiles_and_runs(name, executor, model_dir):
    body = SYNTAX_EXAMPLES[name].body
    # Fresh environment per example so imports re-resolve and concepts from one
    # example don't leak into the next.
    executor.environment = Environment(working_path=model_dir)
    statements = executor.generate_sql(body)
    assert statements, f"{name}: produced no executable SQL"
    for sql in statements:
        executor.execute_raw_sql(sql)
