"""A WHERE/comparison whose inputs live in disconnected subgraphs must surface a
clean `DisconnectedConceptsException` — never the internal `INCOMPLETE_CONDITION`
sentinel (`SyntaxError: Have {GroupNode<...>} and need ...`), which is a loud
"planner reached an invalid state" assertion, not a user-facing error.

Root cause: comparing an aggregate of one fact against an aggregate of a wholly
unrelated fact (`a_total > 2 * b_total`), selecting only the a-side key. The
planner sourced the connected a-side (`GroupNode<a_item, a_total>`), found its
outputs complete, then tripped the sentinel on the first INCOMPLETE_CONDITION
*before* it ever attempted to source `b_total`. It should keep searching: the
unsourced input is in a disconnected subgraph, so the loop converges to a clean
DISCONNECTED outcome (the same a SELECT of those concepts produces). Mirrors the
q64 cross-import aggregate-comparison form.
"""

import pytest

from trilogy import Dialects
from trilogy.core.exceptions import DisconnectedConceptsException
from trilogy.core.models.environment import Environment

# two fully independent facts (no shared key, no join/merge)
MODEL = """
key a_item int;
property a_item.a_val float;
datasource a_src (i: a_item, v: a_val) grain (a_item)
query '''select 1 i, 100.0 v union all select 2 i, 50.0 v''';

key b_item int;
property b_item.b_val float;
datasource b_src (i: b_item, v: b_val) grain (b_item)
query '''select 1 i, 10.0 v union all select 2 i, 5.0 v''';
"""

CROSS_GRAIN_COMPARISON = MODEL + """
auto a_total <- sum(a_val) by a_item;
auto b_total <- sum(b_val) by b_item;
select a_item where a_total > 2 * b_total;
"""

# the same query is resolvable once a derived threshold is sourced from the SAME
# fact (no disconnection) — guards against over-broadening the disconnected path.
RESOLVABLE = MODEL + """
auto a_threshold <- 0.5 * (max(a_val) by *);
select a_item where a_val > a_threshold order by a_item;
"""

INTERNAL_REPR_MARKERS = ["GroupNode<", "RowsetNode<", "@Grain<", "Have {"]


def test_disconnected_comparison_raises_clean_disconnected_error():
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    with pytest.raises(DisconnectedConceptsException) as exc:
        executor.generate_sql(CROSS_GRAIN_COMPARISON)
    message = str(exc.value)
    assert not isinstance(exc.value, SyntaxError)
    for marker in INTERNAL_REPR_MARKERS:
        assert marker not in message, f"leaked internal repr {marker!r}: {message}"


def test_resolvable_same_fact_filter_still_builds():
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    rows = [tuple(r) for r in executor.execute_text(RESOLVABLE)[0].fetchall()]
    # threshold = 0.5 * max(100, 50) = 50; only a_item=1 (100) beats it
    assert rows == [(1,)]
