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

_AXIS_RECURSION = (
    "v4 coalescing-axis over derived (cast/concat) members: recursion gap "
    "(coalescing_presence Bug-1 family, see local_scripts/v4_audit.md)"
)
_COMPOSITE_FULL = "v4 composite-key FULL cells with a derived key member"
_FILTERED_ANCHOR = "v4 filtered-rowset-anchor directional narrowing not ported"
_STITCH = "v4 nullable-measure stitch re-sources one side through the other"
_PREDROP = "v4 predrop chain narrowing: aggregate-consumer cell"
_MULTI_PROBE = (
    "v4 multi-probe coalescing carry not ported: a second member presence probe "
    "re-derives off the fused key (v3 fix = retain_presence_probes, TPC-DS q35)"
)
_OFFSET_ROWSET = (
    "v4 aggregate-rowset offset join: subset-side projected member key resolves "
    "only from partial sources (q59 shape; v3 fix = _enrich_rowset_node)"
)

V4_FAILING: dict[str, str] = {
    "test_coalescing_presence_matrix.py::test_presence_union_cast_single": _AXIS_RECURSION,
    "test_coalescing_presence_matrix.py::test_presence_full_cast_single": _AXIS_RECURSION,
    "test_coalescing_presence_matrix.py::test_presence_union_concat_composite": _AXIS_RECURSION,
    "test_coalescing_presence_matrix.py::test_bare_member_projection_unions_domains": _AXIS_RECURSION,
    "test_composite_matrix.py::test_composite_key_join[plain_derived-full]": _COMPOSITE_FULL,
    "test_composite_matrix.py::test_composite_key_join[derived_derived-full]": _COMPOSITE_FULL,
    "test_composite_matrix.py::test_full_derived_key_as_left_operand_direction": _COMPOSITE_FULL,
    "test_composite_matrix.py::test_mixed_anchor_composite_composes_to_full": _COMPOSITE_FULL,
    "test_filtered_rowset_anchor.py::test_filtered_rowset_anchor_subset_join_narrows": _FILTERED_ANCHOR,
    "test_filtered_rowset_anchor.py::test_unfiltered_rowset_anchor_subset_join_narrows": _FILTERED_ANCHOR,
    "test_filtered_rowset_anchor.py::test_explicit_is_not_null_matches_directional": _FILTERED_ANCHOR,
    "test_nullable_measure_stitch_keys.py::test_nullable_measure_rows_survive_stitch": _STITCH,
    "test_predrop_chain_narrowing.py::test_predrop_chain_cell[aggregate_consumer]": _PREDROP,
    "test_multi_probe_coalescing.py::test_store_and_web_or_catalog_key_only": _MULTI_PROBE,
    "test_multi_probe_coalescing.py::test_store_and_web_or_catalog_join_order_swapped": _MULTI_PROBE,
    "test_multi_probe_coalescing.py::test_store_and_web_or_catalog_with_property_and_aggregate": _MULTI_PROBE,
    "test_aggregate_rowset_offset_join.py::test_offset_join_between_aggregate_rowsets_plans_on_authored_keys": _OFFSET_ROWSET,
}


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
