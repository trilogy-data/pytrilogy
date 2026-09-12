"""Lock: a merged DERIVED key resolves under any import namespace.

`launch.preql` splits a launch's agency string into `first_org` and merges that
into the imported `org.code`. Only the launch scan can say which org launched a
given rocket, and only by computing the split inline -- the organization table
holds the set of codes, not the launch-to-org bridge.

The bridge assembly carried the variant a chosen scan actually reads only when
the equivalence class representative was the `_virt_` canonical. The
representative is the lexicographic minimum of the class, so importing the model
under a namespace sorting before `local` (`launch.`) made the derivation
invisible and the scan had no source for `launch.org.code` (SyntaxError:
"Missing source map entry"). Under `zorg.` the same model planned fine, which is
why this is parametrized on the alias alone.
"""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment

_ORGS = """
key code string;
property code.label string;

datasource organizations (
    code: code,
    label: label,
)
grain (code)
query '''
select 'NASA' as code, 'NASA' as label
union all select 'SPACEX' as code, 'SpaceX' as label
''';
"""

_LAUNCH = """
import orgs as org;

key id string;
property id.agency string;
property id.first_org <- split(agency, '/')[1];

merge first_org into org.code;

datasource launches (
    id: id,
    agency: agency,
)
grain (id)
query '''
select 'L1' as id, 'NASA/ESA' as agency
union all select 'L2' as id, 'SPACEX' as agency
''';
"""


@pytest.fixture
def model_dir(tmp_path: Path) -> Path:
    (tmp_path / "orgs.preql").write_text(_ORGS)
    (tmp_path / "launch.preql").write_text(_LAUNCH)
    return tmp_path


# `launch` sorts before `local`, `zorg` after: the control row.
@pytest.mark.parametrize("alias", ["launch", "zorg"])
def test_merged_derived_key_under_import_namespace(model_dir: Path, alias: str):
    engine = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=model_dir)
    )
    engine.parse_text(f"import launch as {alias};")
    rows = engine.execute_text(
        f"select {alias}.id, {alias}.org.code order by {alias}.id asc;"
    )[-1].fetchall()
    assert [(r[0], r[1]) for r in rows] == [("L1", "NASA"), ("L2", "SPACEX")]


@pytest.mark.parametrize("alias", ["launch", "zorg"])
def test_merged_derived_key_joins_org_attribute(model_dir: Path, alias: str):
    engine = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=model_dir)
    )
    engine.parse_text(f"import launch as {alias};")
    rows = engine.execute_text(
        f"select {alias}.id, {alias}.org.label order by {alias}.id asc;"
    )[-1].fetchall()
    assert [(r[0], r[1]) for r in rows] == [("L1", "NASA"), ("L2", "SpaceX")]
