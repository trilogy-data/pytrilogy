"""Lock: every spelling of a merged axis plans the same query.

`merge first_org into org.code` makes the two names one axis. Which name a query
uses -- in the projection, in an aggregate's `by`, or both -- is a spelling
choice, not a semantic one. All four combinations must return the same rows, and
swapping only the `by` spelling must leave the SQL byte-identical.

Projecting the DEMOTED side (`first_org`) alongside an aggregate failed to plan
at all ("Could not render the query: Missing source reference to
local.first_org") while the `org.code` spelling of the same query planned fine.
The `by` spelling was never the trigger -- the projection was.
"""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment

_ORGS = """
key code string;
property code.label string;

datasource organizations (code: code, label: label)
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

datasource launches (id: id, agency: agency)
grain (id)
query '''
select 'L1' as id, 'NASA/ESA' as agency
union all select 'L2' as id, 'NASA' as agency
union all select 'L3' as id, 'SPACEX' as agency
''';
"""

_SPELLINGS = ["org.code", "first_org"]
_EXPECTED = [("NASA", 2), ("SPACEX", 1)]


@pytest.fixture
def model_dir(tmp_path: Path) -> Path:
    (tmp_path / "orgs.preql").write_text(_ORGS)
    (tmp_path / "launch.preql").write_text(_LAUNCH)
    return tmp_path


def _engine(model_dir: Path):
    engine = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=model_dir)
    )
    engine.parse_text(_LAUNCH)
    return engine


@pytest.mark.parametrize("projected", _SPELLINGS)
@pytest.mark.parametrize("grouped", _SPELLINGS)
def test_merged_axis_spellings_agree_on_rows(
    model_dir: Path, projected: str, grouped: str
):
    rows = (
        _engine(model_dir)
        .execute_text(
            f"select {projected}, count(id) by {grouped} as n order by {projected} asc;"
        )[-1]
        .fetchall()
    )
    assert sorted((r[0], r[1]) for r in rows) == _EXPECTED


@pytest.mark.parametrize("projected", _SPELLINGS)
def test_by_spelling_does_not_change_the_sql(model_dir: Path, projected: str):
    generated = [
        _engine(model_dir).generate_sql(
            f"select {projected}, count(id) by {grouped} as n order by {projected} asc;"
        )[-1]
        for grouped in _SPELLINGS
    ]
    assert generated[0] == generated[1]
