"""Lock: a bound derived column stays readable beside a merged-away key.

`organization.preql` binds `short_e_name` -- a coalesce over `_short_e_name`,
which no table binds -- directly as a column of its raw table. Any request that
names `short_e_name` must therefore read the column; decomposing it walks into
`_short_e_name` and dead-ends ("No datasource exists for root concept").

`launch.preql` imports that file twice and merges `org.state_code`, a property
of `org.code`, into `state.code`, a key. The merged property is now spelled by
a KEY, so a request naming it beside `org.code` sits at `Grain<org.code,
state.code>` while the organization table declares `Grain<org.code>`. The
exact-grain gate that lets a bound derived column short-circuit its lineage
compared the two grains for equality and refused, and the request fell through
to the lineage walk.

The scan still has one row per (org.code, state.code): its grain is its unique
key, and `state.code` is a column it binds. That is what the gate now proves.
The `state` spelling of the same query never had the problem (its key is the
surviving side of the merge) and is the control row. This is the shape
space_reporting's `launch.org.organizations` refresh failed in from
2026-09-14 (pytrilogy 0.3.358 -- and every release before it, masked by an
earlier failure on the same tick).
"""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment

_ORGANIZATION = """
key code string;
property code.state_code string;
property code.short_name string;
property code._short_e_name string;
auto short_e_name <- coalesce(
    CASE WHEN _short_e_name = '-' THEN NULL ELSE _short_e_name END,
    short_name
);

datasource organizations_raw (
    Code: code,
    StateCode: state_code,
    ShortName: short_name,
    ShortEName: short_e_name,
)
grain (code)
query '''
select 'NASA' as Code, 'US' as StateCode, 'NASA Hq' as ShortName, 'NASA' as ShortEName
union all
select 'CASC' as Code, 'CN' as StateCode, 'CASC-CN' as ShortName, 'CASC' as ShortEName
''';
"""

_LAUNCH = """
import organization as org;
import organization as state;

merge org.state_code into state.code;

key id string;
property id.agency string;

datasource launches (
    Launch_Tag: id,
    Agency: org.code,
)
grain (id)
query '''
select 'L1' as Launch_Tag, 'NASA' as Agency
union all select 'L2' as Launch_Tag, 'CASC' as Agency
''';
"""


@pytest.fixture
def model_dir(tmp_path: Path) -> Path:
    (tmp_path / "organization.preql").write_text(_ORGANIZATION)
    (tmp_path / "launch.preql").write_text(_LAUNCH)
    return tmp_path


def _engine(model_dir: Path, text: str):
    engine = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=model_dir)
    )
    engine.parse_text(text)
    return engine


# `org` is the demoted side of the merge (its property now reads as the key
# `state.code`); `state` is the surviving side and the control.
@pytest.mark.parametrize("side", ["org", "state"])
def test_bound_derived_reads_beside_merged_key(model_dir: Path, side: str):
    engine = _engine(model_dir, "import launch;")
    sql = engine.generate_sql(
        f"select {side}.code, {side}.state_code, {side}.short_e_name"
        f" order by {side}.code asc;"
    )[-1]
    assert '"ShortEName"' in sql, sql
    rows = engine.execute_text(
        f"select {side}.code, {side}.state_code, {side}.short_e_name"
        f" order by {side}.code asc;"
    )[-1].fetchall()
    assert [tuple(r) for r in rows] == [("CASC", "CN", "CASC"), ("NASA", "US", "NASA")]


# The failure surfaced through `refresh`, whose rebuild query names every
# column of the managed table -- key, merged property and bound derived alike.
# `launch` sorts before `local`, the namespace space_reporting reaches
# launch.preql under; `zorg` after, the control.
@pytest.mark.parametrize("alias", ["launch", "zorg"])
def test_managed_table_rebuild_reads_bound_derived(model_dir: Path, alias: str):
    (model_dir / "organization.preql").write_text(_ORGANIZATION + """
datasource organizations (
    code,
    state_code,
    short_name,
    short_e_name,
)
grain (code)
address organizations;
""")
    engine = _engine(model_dir, f"import launch as {alias};")
    for side in ("org", "state"):
        target = engine.environment.datasources[f"{alias}.{side}.organizations"]
        sql = engine.update_datasource(target, dry_run=True)
        assert sql is not None and '"ShortEName"' in sql, sql
