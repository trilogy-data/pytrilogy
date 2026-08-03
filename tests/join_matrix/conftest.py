"""Run every join-matrix cell under BOTH planners (v3 default, v4 discovery).

The matrix is the oracle-checked contract for scoped-join / rowset semantics,
and most of the v4 ports of those mechanisms (presence probes, rowset-pair
key-carry, coalescing axis) are only reachable with `use_v4_discovery` on.
Parametrizing here keeps them exercised in the regular CI suite instead of
only under the manual `TRILOGY_V4_DISCOVERY=1` sweep.

Cells the v4 planner does not yet pass are non-strict xfails (same convention
as tests/v4_known_failing.py, which only applies under the env-var sweep);
prune entries as parity work lands. Keys are `<file>::<test name>` with the
planner suffix stripped, so parametrized cells are matched per-param.
"""

import pytest

from trilogy.constants import CONFIG

# 2026-08-02: EMPTY — the last three cells (one root cause: v4 never sourced a
# scoped-join key-group mate nothing else references) fixed by
# `_unsourced_relation_mates` + `_add_partial_completion_contributors`.
# Non-strict xfails rot silently: re-measure with `--runxfail` before trusting
# any future entry's count.
V4_FAILING: dict[str, str] = {}


def _cell_key(node: pytest.Item) -> str:
    name = node.name.replace("[v4]", "").replace("[v4-", "[")
    return f"{node.path.name}::{name}"


@pytest.fixture(autouse=True, params=["v3", "v4"])
def planner(request: pytest.FixtureRequest):
    if request.param == "v4":
        reason = V4_FAILING.get(_cell_key(request.node))
        if reason:
            request.applymarker(pytest.mark.xfail(reason=reason, strict=False))
    prior = CONFIG.use_v4_discovery
    CONFIG.use_v4_discovery = request.param == "v4"
    yield request.param
    CONFIG.use_v4_discovery = prior
