"""Unit lock for `_raise_if_keyless_row_bearing_join`: the join-resolution
guard that refuses to ship a silently-planned cartesian (rendered `ON 1=1`).

A keyless join is a planner bug exactly when the sides share a PROJECTABLE
join axis the planner failed to use (q30: the group parents' FD keys sat
hidden, but rendered, on the sibling scan). Legal keyless joins: axis-disjoint
row-bearing sides (an aggregate selected without its grouping key is an
authored fan-out), row-independent sides (constants, global-aggregate
scalars), and a grain key the source never actually projects.
"""

import pytest

from trilogy.core.enums import Granularity, JoinType, Purpose
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.models.build import (
    BuildColumnAssignment,
    BuildConcept,
    BuildDatasource,
    BuildGrain,
)
from trilogy.core.models.core import DataType
from trilogy.core.models.execute import QueryDatasource
from trilogy.core.processing.join_resolution import (
    JoinOrderOutput,
    _raise_if_keyless_row_bearing_join,
)


def _concept(
    name: str,
    granularity: Granularity = Granularity.MULTI_ROW,
    purpose: Purpose = Purpose.KEY,
    keys: set[str] | None = None,
) -> BuildConcept:
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=DataType.INTEGER,
        purpose=purpose,
        build_is_aggregate=False,
        granularity=granularity,
        keys=keys,
        grain=BuildGrain(),
    )


def _source(
    name: str, grain_addresses: set[str], outputs: list[BuildConcept]
) -> QueryDatasource:
    base = BuildDatasource(
        name=name,
        columns=[BuildColumnAssignment(alias=c.name, concept=c) for c in outputs],
        address=name,
        grain=BuildGrain(),
    )
    return QueryDatasource(
        input_concepts=outputs,
        output_concepts=outputs,
        datasources=[base],
        source_map={c.address: {base} for c in outputs},
        grain=BuildGrain(components=frozenset(grain_addresses)),
        joins=[],
    )


def _check(
    joins: list[JoinOrderOutput],
    sources: dict[str, QueryDatasource],
    rollup_padded: frozenset[str] = frozenset(),
) -> None:
    _raise_if_keyless_row_bearing_join(joins, sources, {}, rollup_padded, None)


def test_keyless_join_with_shared_axis_raises() -> None:
    """q30 shape: the right side's key is projected (hidden) by the tree side,
    so the planner could have joined on it and did not."""
    sources = {
        "ds~customers": _source(
            "customers",
            {"local.cust_id"},
            [_concept("cust_id"), _concept("date_id")],
        ),
        "ds~dates": _source("dates", {"local.date_id"}, [_concept("date_id")]),
    }
    joins = [
        JoinOrderOutput(
            right="ds~dates", type=JoinType.INNER, keys={}, left="ds~customers"
        )
    ]
    with pytest.raises(UnresolvableQueryException, match="keyless join"):
        _check(joins, sources)


def test_keyless_join_via_fd_key_raises() -> None:
    """The shared axis is reached through `keys`, not carried directly: a
    review dimension keyed by customer beside a scan projecting customer."""
    sources = {
        "ds~customers": _source(
            "customers", {"local.cust_id"}, [_concept("cust_id"), _concept("cname")]
        ),
        "ds~reviews": _source(
            "reviews",
            {"local.review_id"},
            [_concept("review_id", keys={"local.cust_id"})],
        ),
    }
    joins = [
        JoinOrderOutput(
            right="ds~reviews", type=JoinType.INNER, keys={}, left="ds~customers"
        )
    ]
    with pytest.raises(UnresolvableQueryException, match="keyless join"):
        _check(joins, sources)


def test_keyless_join_axis_disjoint_allowed() -> None:
    """Authored fan-out: an aggregate at a foreign grain selected without its
    key legitimately cross-joins (`by_item` beside `sid, by_store`)."""
    sources = {
        "ds~by_store": _source(
            "by_store",
            {"local.sid"},
            [_concept("sid"), _concept("by_store", purpose=Purpose.METRIC)],
        ),
        "ds~by_item": _source(
            "by_item",
            {"local.iid"},
            [_concept("by_item", purpose=Purpose.METRIC)],
        ),
    }
    joins = [
        JoinOrderOutput(
            right="ds~by_item", type=JoinType.FULL, keys={}, left="ds~by_store"
        )
    ]
    _check(joins, sources)


def test_unprojected_grain_component_is_not_an_axis() -> None:
    """A source at grain {name, value} projecting only {value, dim} has no
    `name` column to pair on, so sharing `name` with a sibling's grain is not
    a lost axis (the unnest-beside-name-filter shape)."""
    sources = {
        "ds~merged": _source(
            "merged",
            {"local.name", "local.value"},
            [_concept("value"), _concept("dim")],
        ),
        "ds~by_name": _source(
            "by_name",
            {"local.name"},
            [_concept("name"), _concept("births", purpose=Purpose.METRIC)],
        ),
    }
    joins = [
        JoinOrderOutput(
            right="ds~merged", type=JoinType.INNER, keys={}, left="ds~by_name"
        )
    ]
    _check(joins, sources)


def test_keyless_join_against_empty_grain_side_allowed() -> None:
    sources = {
        "ds~customers": _source("customers", {"local.cust_id"}, [_concept("cust_id")]),
        "ds~global_total": _source(
            "global_total", set(), [_concept("total", purpose=Purpose.METRIC)]
        ),
    }
    joins = [
        JoinOrderOutput(
            right="ds~global_total", type=JoinType.FULL, keys={}, left="ds~customers"
        )
    ]
    _check(joins, sources)


def test_keyless_join_against_single_row_side_allowed() -> None:
    """A constant node can carry a self-grain; single-row outputs make it safe
    to cross-join even when its axis address appears on the other side."""
    sources = {
        "ds~customers": _source(
            "customers",
            {"local.cust_id"},
            [_concept("cust_id"), _concept("value")],
        ),
        "ds~const": _source(
            "const",
            {"local.value"},
            [_concept("value", granularity=Granularity.SINGLE_ROW)],
        ),
    }
    joins = [
        JoinOrderOutput(
            right="ds~const", type=JoinType.FULL, keys={}, left="ds~customers"
        )
    ]
    _check(joins, sources)


def test_keyless_join_against_self_grained_metric_allowed() -> None:
    """A global-aggregate scalar carries grain = {the metric itself}."""
    sources = {
        "ds~customers": _source("customers", {"local.cust_id"}, [_concept("cust_id")]),
        "ds~agg": _source(
            "agg", {"local.total"}, [_concept("total", purpose=Purpose.METRIC)]
        ),
    }
    joins = [
        JoinOrderOutput(right="ds~agg", type=JoinType.FULL, keys={}, left="ds~customers")
    ]
    _check(joins, sources)


def test_keyed_metric_self_grain_is_not_row_independent() -> None:
    """A per-group aggregate whose grain is mislabelled as itself still has a
    real axis through its keys, so it stays subject to the guard."""
    sources = {
        "ds~facts": _source(
            "facts", {"local.id"}, [_concept("id"), _concept("grp_key")]
        ),
        "ds~agg": _source(
            "agg",
            {"local.total"},
            [_concept("total", purpose=Purpose.METRIC, keys={"local.grp_key"})],
        ),
    }
    joins = [
        JoinOrderOutput(right="ds~agg", type=JoinType.FULL, keys={}, left="ds~facts")
    ]
    with pytest.raises(UnresolvableQueryException, match="keyless join"):
        _check(joins, sources)


def test_keyed_join_between_grained_sources_allowed() -> None:
    sources = {
        "ds~customers": _source(
            "customers",
            {"local.cust_id"},
            [_concept("cust_id"), _concept("date_id")],
        ),
        "ds~dates": _source("dates", {"local.date_id"}, [_concept("date_id")]),
    }
    joins = [
        JoinOrderOutput(
            right="ds~dates",
            type=JoinType.INNER,
            keys={"ds~customers": {"c~local.date_id"}},
            left="ds~customers",
        )
    ]
    _check(joins, sources)


def test_keyless_join_later_in_tree_raises() -> None:
    sources = {
        "ds~customers": _source(
            "customers",
            {"local.cust_id"},
            [_concept("cust_id"), _concept("date_id")],
        ),
        "ds~totals": _source("totals", {"local.cust_id"}, [_concept("cust_id")]),
        "ds~dates": _source("dates", {"local.date_id"}, [_concept("date_id")]),
    }
    joins = [
        JoinOrderOutput(
            right="ds~totals",
            type=JoinType.INNER,
            keys={"ds~customers": {"c~local.cust_id"}},
            left="ds~customers",
        ),
        JoinOrderOutput(right="ds~dates", type=JoinType.INNER, keys={}, left=None),
    ]
    with pytest.raises(UnresolvableQueryException, match="keyless join"):
        _check(joins, sources)


def test_rollup_padded_axis_excluded() -> None:
    """A ROLLUP-padded key is not a usable axis (subtotal rows NULL it), so a
    keyless join sharing only that address stays legal."""
    sources = {
        "ds~rollup": _source("rollup", {"local.cat"}, [_concept("cat")]),
        "ds~dims": _source("dims", {"local.cat"}, [_concept("cat")]),
    }
    joins = [
        JoinOrderOutput(right="ds~dims", type=JoinType.FULL, keys={}, left="ds~rollup")
    ]
    _check(joins, sources, rollup_padded=frozenset({"local.cat"}))
