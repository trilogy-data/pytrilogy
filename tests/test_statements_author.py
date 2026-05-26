"""Coverage for trilogy.core.statements.author validation and representation."""

from __future__ import annotations

import pytest

from trilogy import parse
from trilogy.core.exceptions import InvalidSyntaxException


def test_order_by_references_unprojected_concept_raises_clean_error():
    src = """key id int;
property id.x int;
property id.y int;
datasource src (id:id, x:x, y:y) grain(id) address src;
select id, x order by y asc;
"""
    with pytest.raises(InvalidSyntaxException, match="ORDER BY references"):
        parse(src)


def test_duplicate_select_output_concept_ref_raises():
    src = """key id int;
property id.x int;
datasource src (id:id, x:x) grain(id) address src;
select x, x;
"""
    raised = None
    try:
        parse(src)
    except Exception as e:
        raised = e
    assert raised is not None, "expected duplicate-select error"
    assert "Duplicate select output" in str(raised)


def test_duplicate_select_output_with_transform_raises():
    src = """key id int;
property id.x int;
datasource src (id:id, x:x) grain(id) address src;
select x+1 as y, x+2 as y;
"""
    raised = None
    try:
        parse(src)
    except Exception as e:
        raised = e
    assert raised is not None, "expected duplicate-transform error"
    assert "Duplicate select output" in str(raised)


def test_rowset_derivation_repr_via_parse():
    """Drives RowsetDerivationStatement.__repr__ by parsing a rowset definition."""
    src = """key id int;
property id.x int;
datasource src (id:id, x:x) grain(id) address src;
rowset top_ten <- SELECT id, x ORDER BY x desc LIMIT 10;
SELECT top_ten.id, top_ten.x;
"""
    env, _ = parse(src)
    assert "top_ten.id" in env.concepts
