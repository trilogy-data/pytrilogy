"""A `merge` declares one value space; it says nothing of a NULL. A launch
whose agency is NULL (`?` binding) has no org code, and the lookup keeps it."""

from trilogy import Dialects

MODEL = """
key code string;
property code.label string;
datasource organizations (code: code, label: label)
grain (code)
query '''
select 'NASA' as code, 'NASA' as label
union all select 'SPACEX', 'SpaceX'
''';

key id string;
property id.agency string?;
property id.first_org <- split(agency, '/')[1];
merge first_org into code;
datasource launches (id: id, agency: agency)
grain (id)
query '''
select 'L1' as id, 'NASA/ESA' as agency
union all select 'L2', 'SPACEX'
union all select 'L3', cast(null as varchar)
''';
"""


def test_a_null_binding_beside_an_equal_merge_keeps_its_row():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(MODEL)
    rows = executor.execute_text("select id, label order by id asc;")[-1].fetchall()
    assert [tuple(r) for r in rows] == [
        ("L1", "NASA"),
        ("L2", "SpaceX"),
        ("L3", None),
    ]
