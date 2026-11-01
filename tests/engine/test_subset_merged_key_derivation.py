"""A derivation over a key that a derived concept is subset-merged into reads
the same rows as the key itself. `merged_group` (computed off `raw_groups`) is
merged into `~group_id`, so `group_id * 10` may be sourced through it; the rows
must not depend on that choice. Group 3 has no events: when `events` binds
`~group_id` the key is completed with it, padded, under both spellings. (A
model binding `group_id` complete on `events` while a group has no event makes
a claim the data breaks, which completeness validation reports; there the two
spellings read different, equally licensed, sources.)"""

import pytest

from trilogy import Dialects

MODEL = """
key group_id int;
key event_id int;
property group_id.group_name string;
datasource events (eid: event_id, gid: ~group_id) grain (event_id)
query '''select 1 eid, 1 gid union all select 2, 2''';
datasource groups (gid: group_id, name: group_name) grain (group_id)
query '''select 1 gid, 'a' as name union all select 2, 'b' union all select 3, 'c' ''';
property event_id.raw_group int;
auto merged_group <- coalesce(raw_group, -1);
merge merged_group into ~group_id;
datasource raw_groups (eid: event_id, gid: raw_group) grain (event_id)
query '''select 1 eid, 1 gid union all select 2, 2''';
"""

PADDED = [(1, 1, "a"), (2, 2, "b"), (3, None, "c")]


@pytest.mark.parametrize(
    "query, scale",
    [
        ("select group_id, event_id, group_name;", 1),
        ("select group_id * 10 as g, event_id, group_name;", 10),
    ],
)
def test_derivation_reads_the_key_rows(query: str, scale: int):
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(MODEL)
    rows = sorted(
        (tuple(r) for r in executor.execute_text(query)[-1].fetchall()),
        key=str,
    )
    assert rows == sorted(((g * scale, e, n) for g, e, n in PADDED), key=str)
