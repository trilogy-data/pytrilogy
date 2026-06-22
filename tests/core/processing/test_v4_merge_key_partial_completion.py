"""Regression lock for the v4 merge-key partial completion
(`_complete_partial_requested` in `v4_helper/source_planning.py`).

A fact (`launches`) binds two non-authoritative merge keys (`~name`,
`~pcode`). A query that filters on `name` while also pulling a `pcode`
dimension forces the bridge to anchor on `launches`, which can only carry the
requested `name` as a *partial* column -- the final-output guard rejects it and
the query is `UnresolvableQueryException`. The fix completes the partial key
against its authoritative dimension (`vehicles`) and outer-joins, carrying the
`name like '%Falcon%'` filter onto that dimension. Mirrors v3's
`lv_info LEFT JOIN launch_info` shape (gcat `test_join`).

This is v4-only (`CONFIG.use_v4_discovery`), so it does not run in the default
v3 coverage pass; the fixture enables it here so the completion body is
exercised every run. Without the completion v4 crashes, so the asserted rows
themselves are the discriminating evidence."""

from trilogy import Dialects, Environment
from trilogy.constants import CONFIG

_MODEL = """
key name string;
property name.vclass string;
datasource vehicles ( nm: name, cls: vclass )
grain (name)
query '''select 'Falcon 9' as nm, 'medium' as cls
         union all select 'Atlas V' as nm, 'heavy' as cls''';

key pcode string;
property pcode.pname string;
datasource platforms ( pc: pcode, pn: pname )
grain (pcode)
query '''select 'CC' as pc, 'Cape' as pn
         union all select 'VB' as pc, 'Vandenberg' as pn''';

key launch_tag string;
datasource launches ( tag: launch_tag, lvt: ~name, plat: ~pcode )
grain (launch_tag)
query '''select 'L1' as tag, 'Falcon 9' as lvt, 'CC' as plat
         union all select 'L2' as tag, 'Atlas V' as lvt, 'VB' as plat
         union all select 'L3' as tag, 'Falcon 9' as lvt, 'VB' as plat''';
"""

_QUERY = """
where name like '%Falcon%'
select pname, name, count(launch_tag) as launches
order by pname asc, name asc;
"""


def test_partial_merge_key_completed_against_dimension():
    env = Environment()
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    executor.parse_text(_MODEL)
    prior = CONFIG.use_v4_discovery
    CONFIG.use_v4_discovery = True
    try:
        rows = executor.execute_text(_QUERY)[-1].fetchall()
    finally:
        CONFIG.use_v4_discovery = prior
    assert [tuple(r) for r in rows] == [
        ("Cape", "Falcon 9", 1),
        ("Vandenberg", "Falcon 9", 1),
    ]
