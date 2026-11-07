from datetime import datetime

from trilogy import Dialects

SETUP = """
key sid string;
property sid.sgenus string;
property sid.sclass string;
property sid.sx float;
property <*>.wm datetime;

root datasource sample (sid: sid, genus: ?sgenus, fit_class: sclass, x: sx)
grain (sid)
query '''
select 'a' as sid, 'g1' as genus, 'c1' as fit_class, 1.0 as x
union all select 'b', null, 'c1', 2.0
''';

auto genus_n <- count(sx) by sgenus;
auto class_n <- count(sx) by sclass;

datasource genus_fits (genus: sgenus, n: ?genus_n, wm: wm)
grain (sgenus)
query '''select 'g1' as genus, 1 as n, TIMESTAMP '2026-01-01' as wm'''
where sgenus is not null and genus_n > 0;
"""

CLASS_TARGET = """
datasource class_fits (fit_class: sclass, n: ?class_n, wm: wm)
grain (sclass)
address class_fits_tbl
where sclass is not null
freshness by wm;
"""


def test_abstract_property_read_off_aggregate_filtered_datasource():
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(SETUP)
    sql = executor.generate_sql("select wm;")[-1]
    assert '"genus_fits"."n" > 0' in sql, sql
    rows = executor.execute_text("select wm;")[-1].fetchall()
    assert [r.wm for r in rows] == [datetime(2026, 1, 1)]


def test_refresh_sibling_target_with_shared_watermark():
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(SETUP + CLASS_TARGET)
    sql = executor.update_datasource(
        executor.environment.datasources["class_fits"], dry_run=True
    )
    assert sql and "INVALID_REFERENCE_BUG" not in sql
