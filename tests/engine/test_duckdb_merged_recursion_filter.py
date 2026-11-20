from pathlib import Path

from trilogy import Dialects, Environment

POST = """
key id int;
property id.create_time timestamp;
property id.parent int;

datasource posts (id, create_time, ?parent) grain (id)
query '''select 1 as id, timestamp '2023-05-01' as create_time, null::int as parent
union all select 2, timestamp '2024-05-01', 1
union all select 3, timestamp '2024-06-01', 2
union all select 4, timestamp '2023-07-01', null''';
"""

THREADS = """
import post;
import post as root_parent;
import post as direct_parent;

auto recursive_parent <- recurse_edge(id, parent);
merge recursive_parent into root_parent.id;
merge parent into direct_parent.id;
"""


def test_where_holds_beside_a_merged_recursion(tmp_path: Path):
    (tmp_path / "post.preql").write_text(POST)
    (tmp_path / "threads.preql").write_text(THREADS)
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    executor.parse_text("import threads;")
    rows = executor.execute_text(
        "where create_time.year = 2024 select root_parent.id, create_time.year, id limit 10;"
    )[-1].fetchall()
    assert sorted(map(tuple, rows), key=repr) == [(1, 2024, 2), (1, 2024, 3)]
