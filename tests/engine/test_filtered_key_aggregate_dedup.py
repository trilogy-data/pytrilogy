"""`count(key ? cond)` dedups its input to the key's grain; the filter mask has
to be computed BELOW that dedup or the condition columns are grouped away."""

from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import Environment

LOCAL_KEYS = """
key tick int;
key isk int;
properties <tick, isk> (
    qty int,
);
datasource t (
    a: tick,
    b: isk,
    q: qty,
)
grain (isk, tick)
query '''
select 1 as a, 100 as b, 5 as q
union all select -1 as a, 100 as b, 2 as q
union all select 2 as a, 200 as b, 3 as q
union all select -5 as a, 300 as b, 1 as q
''';
"""

IMPORTED_DIM_KEY_ITEM = """
key sk int;
property sk.name string;
datasource item (i_sk: sk, i_name: name)
grain (sk)
query ''' select 10 as i_sk, 'x' as i_name union all select 11, 'y' union all select 12, 'z' ''';
"""

IMPORTED_DIM_KEY_FACT = """
import item as item;

key tick int;
properties <tick, item.sk> (
    qty int,
);
datasource t (
    a: tick,
    b: item.sk,
    q: qty,
)
grain (item.sk, tick)
query '''
select 1 as a, 10 as b, 5 as q
union all select -1 as a, 10 as b, 2 as q
union all select 2 as a, 11 as b, 3 as q
union all select -5 as a, 12 as b, 1 as q
''';
"""

UNION_ITEM = IMPORTED_DIM_KEY_ITEM

UNION_SALES = """
import item as item;

key order_id int;
key chan enum<string>['A', 'B', 'C'];
property <order_id, chan, item.sk>.quantity int?;

partial datasource sale_a (
    raw(''' 'A' '''): chan,
    oid: order_id,
    iid: item.sk,
    q: quantity,
)
grain (order_id, chan, item.sk)
complete where chan = 'A'
query ''' select 1 as oid, 10 as iid, 5 as q union all select 2 as oid, 11 as iid, null as q ''';

partial datasource sale_b (
    raw(''' 'B' '''): chan,
    oid: order_id,
    iid: item.sk,
    q: quantity,
)
grain (order_id, chan, item.sk)
complete where chan = 'B'
query ''' select 3 as oid, 10 as iid, 7 as q union all select 4 as oid, 12 as iid, null as q ''';

partial datasource sale_c (
    raw(''' 'C' '''): chan,
    oid: order_id,
    iid: item.sk,
    q: quantity,
)
grain (order_id, chan, item.sk)
complete where chan = 'C'
query ''' select 5 as oid, 11 as iid, 3 as q ''';
"""


def _executor(tmp_path: Path, files: dict[str, str]):
    for name, text in files.items():
        (tmp_path / name).write_text(text)
    return Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )


def test_filtered_key_count_condition_on_cograin_key(tmp_path):
    executor = _executor(tmp_path, {})
    executor.execute_text(LOCAL_KEYS)
    rows = executor.execute_text("select count(isk ? tick > 0) as c;")[0].fetchall()
    assert [tuple(r) for r in rows] == [(2,)]


def test_filtered_key_count_beside_other_aggregate(tmp_path):
    executor = _executor(tmp_path, {})
    executor.execute_text(LOCAL_KEYS)
    rows = executor.execute_text(
        "select count(isk ? tick > 0) as c, sum(qty) as total;"
    )[0].fetchall()
    assert [tuple(r) for r in rows] == [(2, 11)]


def test_filtered_imported_dim_key_count_condition_on_cograin_key(tmp_path):
    executor = _executor(
        tmp_path,
        {"item.preql": IMPORTED_DIM_KEY_ITEM, "fact.preql": IMPORTED_DIM_KEY_FACT},
    )
    executor.execute_text("import fact as f;")
    rows = executor.execute_text("select count(f.item.sk ? f.tick > 0) as c;")[
        0
    ].fetchall()
    assert [tuple(r) for r in rows] == [(2,)]


def test_filtered_imported_dim_key_count_beside_other_aggregate(tmp_path):
    executor = _executor(
        tmp_path,
        {"item.preql": IMPORTED_DIM_KEY_ITEM, "fact.preql": IMPORTED_DIM_KEY_FACT},
    )
    executor.execute_text("import fact as f;")
    rows = executor.execute_text(
        "select count(f.item.sk ? f.qty > 2) as c, count(f.qty) as n;"
    )[0].fetchall()
    assert [tuple(r) for r in rows] == [(2, 4)]


def test_filtered_key_count_over_union_datasource(tmp_path):
    executor = _executor(
        tmp_path, {"item.preql": UNION_ITEM, "all_sales.preql": UNION_SALES}
    )
    executor.execute_text("import all_sales as s;")
    rows = executor.execute_text(
        "select count(s.item.sk ? s.quantity is not null) as c;"
    )[0].fetchall()
    assert [tuple(r) for r in rows] == [(2,)]
