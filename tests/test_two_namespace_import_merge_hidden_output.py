"""One model imported under two namespaces with a merge between them: a
WHERE-gated root scan hides its fallback output (`org.code`), and the merge
above it must not demand that hidden column from its parents."""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment

FILES = {
    "etl.preql": "key data_updated_through datetime;\n",
    "vehicle.preql": """import etl;
datasource lv_info (
    ~data_updated_through
)
query '''select timestamp '2026-10-01' as data_updated_through''';
""",
    "sites.preql": """key key string;
property key.longitude float;
datasource sites (
    key,
    longitude
)
grain (key)
query '''select 'CC' as key, -80.6 as longitude''';
""",
    "organization.preql": """key code string;
property code.state_code string;
property code.e_name string;
property code.color <- CASE WHEN state_code = 'NL' THEN '#FFB347' ELSE '#7FB7FF' END;
datasource organizations (
    code,
    state_code,
    e_name
)
grain (code)
query '''select 'NASA' as code, 'US' as state_code, 'NASA' as e_name
union all select 'US', 'US', 'United States' ''';
""",
    "launch.preql": """key id string;
import vehicle as vehicle;
import sites as site;
import organization as org;
import organization as state;
merge org.state_code into state.code;
key _launch_code string;
property id.success_flag <- CASE
    WHEN len(_launch_code) > 1 THEN substring(_launch_code, 2, len(_launch_code) - 1)
    ELSE null
END;
datasource launch_info (
    id,
    site.key,
    org.code,
    _launch_code
)
grain (id)
query '''select '1957-001' as id, 'CC' as site_key, 'NASA' as org_code, 'OS' as _launch_code''';
""",
}


@pytest.fixture
def models(tmp_path: Path) -> Path:
    for name, text in FILES.items():
        (tmp_path / name).write_text(text, encoding="utf-8")
    return tmp_path


@pytest.mark.parametrize(
    "outputs",
    [
        "id as launch_tag, success_flag, site.longitude, org.e_name as launch_org, state.color",
        "id, success_flag, site.longitude, org.e_name as launch_org, state.color",
        "id as launch_tag, success_flag, site.longitude, org.e_name, state.color",
        "id, success_flag, site.longitude, org.e_name, state.color",
    ],
)
def test_two_namespace_import_merge_where_plans(models: Path, outputs: str):
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=models)
    )
    rows = executor.execute_text(
        f"import launch;\nwhere success_flag is not null\nselect {outputs};"
    )[-1].fetchall()
    assert [(r[0], r[1], float(r[2]), r[3], r[4]) for r in rows] == [
        ("1957-001", "S", -80.6, "NASA", "#7FB7FF")
    ]
