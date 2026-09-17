"""`plan_source` raises when a requested ROOT concept is bound nowhere, and
does NOT raise merely because the concept has no column of its own.

Removing the pre-v4 cover search took `validate_query_is_resolvable` off the
decline path with it, so `plan_source` now calls it on a decline. The risk in
moving it is over-raising: the check runs on EVERY decline, including
sub-requests whose caller used to recover from a `None`, so a concept that is
reachable only through a pseudonym must still plan.
"""

import pytest

from trilogy import Dialects
from trilogy.core.exceptions import (
    DisconnectedConceptsException,
    NoDatasourceException,
)

# `floating_key` owns no column; the merge makes `bound_key`'s column answer
# for it, which is a resolvable pseudonym and not an unsourceable root.
MERGED_INTO_BOUND = """
key bound_key int;
property bound_key.label string;
key floating_key int;

datasource src (k: bound_key, l: label)
grain (bound_key)
query '''select 1 as k, 'a' as l''';

merge floating_key into bound_key;
"""

# Both sides of the merge are unbound, so the class has no column anywhere.
MERGED_INTO_UNBOUND = """
key bound_key int;
key floating_key int;
key other_key int;

datasource src (k: bound_key)
grain (bound_key)
query '''select 1 as k''';

merge floating_key into other_key;
"""


def _executor(model: str):
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(model)
    return executor


def test_pseudonym_of_a_bound_key_is_not_unsourceable():
    executor = _executor(MERGED_INTO_BOUND)
    sql = executor.generate_sql("select floating_key;")[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    assert [tuple(r) for r in executor.execute_raw_sql(sql).fetchall()] == [(1,)]


def test_pseudonym_carries_its_partner_s_properties():
    executor = _executor(MERGED_INTO_BOUND)
    sql = executor.generate_sql("select floating_key, label;")[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    assert [tuple(r) for r in executor.execute_raw_sql(sql).fetchall()] == [(1, "a")]


def test_root_bound_nowhere_raises():
    executor = _executor(MERGED_INTO_UNBOUND)
    with pytest.raises(NoDatasourceException):
        executor.generate_sql("select floating_key;")


def test_root_bound_nowhere_beside_a_bound_concept_reports_the_disconnect():
    """Asked for beside a concept that IS bound, the same unsourceable key is
    reported as a disconnect instead: the two share no join component, and that
    proof runs ahead of the unsourceable-root check. Unchanged from before the
    fallback was removed -- pinned so the two error surfaces stay distinct."""
    executor = _executor(MERGED_INTO_UNBOUND)
    with pytest.raises(DisconnectedConceptsException):
        executor.generate_sql("select bound_key, floating_key;")
