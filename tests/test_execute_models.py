from pathlib import Path

from pytest import raises

from trilogy import Environment
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.execute import (
    BuildConcept,
    BuildDatasource,
    BuildGrain,
    DataType,
    Purpose,
    raise_helpful_join_validation_error,
)


def test_raise_helpful_join_validation_error():

    with raises(InvalidSyntaxException):
        raise_helpful_join_validation_error(
            concepts=[
                BuildConcept(
                    name="test_concept",
                    canonical_name="test_concept",
                    datatype=DataType.INTEGER,
                    purpose=Purpose.KEY,
                    build_is_aggregate=False,
                    grain=BuildGrain(),
                )
            ],
            left_datasource=BuildDatasource(name="left_ds", columns=[], address="agsg"),
            right_datasource=BuildDatasource(
                name="right_ds", columns=[], address="agsg"
            ),
        )
    with raises(InvalidSyntaxException):
        raise_helpful_join_validation_error(
            concepts=[
                BuildConcept(
                    name="test_concept",
                    canonical_name="test_concept",
                    datatype=DataType.INTEGER,
                    purpose=Purpose.KEY,
                    build_is_aggregate=False,
                    grain=BuildGrain(),
                )
            ],
            left_datasource=None,
            right_datasource=BuildDatasource(
                name="right_ds", columns=[], address="agsg"
            ),
        )
    with raises(InvalidSyntaxException):
        raise_helpful_join_validation_error(
            concepts=[
                BuildConcept(
                    name="test_concept",
                    canonical_name="test_concept",
                    datatype=DataType.INTEGER,
                    purpose=Purpose.KEY,
                    build_is_aggregate=False,
                    grain=BuildGrain(),
                )
            ],
            left_datasource=BuildDatasource(name="left_ds", columns=[], address="agsg"),
            right_datasource=None,
        )


def test_build_datasource_source_resolution():
    env = Environment(working_path=Path(__file__).parent)
    env.parse("""
key x int;
property x.val float;
auto _total_val <- sum(val) by *;

datasource totals(
_total_val
    )
address abc_123l;

select
    sum(val) as total_val;
   """)
    build_env = env.materialize_for_select()
    built_ds = build_env.datasources["totals"]
    total_val = build_env.concepts["local.total_val"]
    built_ds.get_alias(total_val)


def _key_concept(name: str) -> BuildConcept:
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=DataType.INTEGER,
        purpose=Purpose.KEY,
        build_is_aggregate=False,
        grain=BuildGrain(),
    )


def _property_concept(name: str) -> BuildConcept:
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=DataType.INTEGER,
        purpose=Purpose.PROPERTY,
        build_is_aggregate=False,
        grain=BuildGrain(),
    )


def test_build_datasource_effective_grain_includes_key_outputs():
    """``effective_grain`` unions the declared grain with any KEY-purpose
    outputs that aren't already in the grain."""
    from trilogy.core.models.build import BuildColumnAssignment

    key = _key_concept("k")
    prop = _property_concept("p")
    ds = BuildDatasource(
        name="ds",
        columns=[
            BuildColumnAssignment(alias="k", concept=key),
            BuildColumnAssignment(alias="p", concept=prop),
        ],
        address="ds_addr",
        grain=BuildGrain(),
    )
    grain = ds.effective_grain
    assert key.address in grain.components
    assert prop.address not in grain.components


def test_query_datasource_effective_grain_skips_recursive_gating():
    """``QueryDatasource.effective_grain`` mirrors the BuildDatasource version
    but excludes the recursive-gating sentinel concept."""
    from trilogy.constants import RECURSIVE_GATING_CONCEPT
    from trilogy.core.models.execute import QueryDatasource

    key = _key_concept("k")
    gating = BuildConcept(
        name=RECURSIVE_GATING_CONCEPT,
        canonical_name=RECURSIVE_GATING_CONCEPT,
        datatype=DataType.INTEGER,
        purpose=Purpose.KEY,
        build_is_aggregate=False,
        grain=BuildGrain(),
    )
    qds = QueryDatasource(
        input_concepts=[key, gating],
        output_concepts=[key, gating],
        datasources=[],
        source_map={key.address: set(), gating.address: set()},
        grain=BuildGrain(),
        joins=[],
    )
    grain = qds.effective_grain
    assert key.address in grain.components
    assert gating.address not in grain.components


def test_join_get_name_falls_back_to_cte_base_when_no_inlined_datasource():
    """When ``inline_cte`` is called without an explicit datasource, ``get_name``
    and ``right_ref`` recover the BuildDatasource from the CTE's base."""
    from trilogy.core.enums import JoinType
    from trilogy.core.models.build import BuildColumnAssignment
    from trilogy.core.models.execute import CTE, Join

    key = _key_concept("k")
    base_ds = BuildDatasource(
        name="base",
        columns=[BuildColumnAssignment(alias="k", concept=key)],
        address="base_addr",
        grain=BuildGrain(),
    )
    cte = CTE.from_datasource(base_ds)
    join = Join(
        right_cte=cte,
        jointype=JoinType.INNER,
    )
    join.inline_cte(cte)  # no datasource argument → fallback path
    assert join.get_name(cte) == base_ds.safe_identifier
    assert base_ds.safe_identifier in join.right_ref


def test_query_datasource_raises_on_missing_source_map():
    """``QueryDatasource.__post_init__`` raises when an output concept has no
    representation in ``source_map`` (and isn't covered by canonical/pseudonym
    fallbacks)."""
    from trilogy.core.models.execute import QueryDatasource

    # Two unrelated concepts: one is in source_map, the other isn't and has no
    # canonical/pseudonym tie to it → triggers the SyntaxError raise.
    mapped = _key_concept("mapped")
    orphan = BuildConcept(
        name="orphan",
        canonical_name="orphan",
        datatype=DataType.INTEGER,
        purpose=Purpose.PROPERTY,
        build_is_aggregate=False,
        grain=BuildGrain(),
        namespace="test",
    )
    with raises(SyntaxError, match="Missing source map entry"):
        QueryDatasource(
            input_concepts=[mapped, orphan],
            output_concepts=[mapped, orphan],
            datasources=[],
            source_map={mapped.address: set()},
            grain=BuildGrain(),
            joins=[],
        )


def test_query_datasource_passes_when_address_in_source_map_but_canonical_differs():
    """If a concept's address is in source_map but its canonical_address isn't
    in any other concept's mapped_canonical set (e.g., name != canonical_name),
    the explicit address-in-source_map check at line 681 keeps it valid."""
    from trilogy.core.models.execute import QueryDatasource

    aliased = BuildConcept(
        name="alias_a",
        canonical_name="canonical_a",  # diverges from name → address != canonical_address
        datatype=DataType.INTEGER,
        purpose=Purpose.PROPERTY,
        build_is_aggregate=False,
        grain=BuildGrain(),
        namespace="test",
    )
    QueryDatasource(
        input_concepts=[aliased],
        output_concepts=[aliased],
        datasources=[],
        source_map={aliased.address: set()},
        grain=BuildGrain(),
        joins=[],
    )


def test_unnest_join_equality_and_hash():
    """``UnnestJoin`` equality/hash is keyed on ``safe_identifier`` (alias +
    concept addresses); non-UnnestJoin comparisons return ``NotImplemented``."""
    from trilogy.core.enums import FunctionType
    from trilogy.core.models.build import BuildFunction
    from trilogy.core.models.execute import UnnestJoin

    parent = BuildFunction(
        operator=FunctionType.UNNEST,
        arguments=[],
        output_data_type=DataType.INTEGER,
        output_purpose=Purpose.PROPERTY,
        arg_count=0,
    )
    a1 = UnnestJoin(concepts=[_key_concept("k")], parent=parent, alias="u")
    a2 = UnnestJoin(concepts=[_key_concept("k")], parent=parent, alias="u")
    diff_alias = UnnestJoin(concepts=[_key_concept("k")], parent=parent, alias="v")
    diff_concept = UnnestJoin(
        concepts=[_key_concept("other")], parent=parent, alias="u"
    )

    assert a1 == a2
    assert hash(a1) == hash(a2)
    assert a1 != diff_alias
    assert a1 != diff_concept
    assert a1.__eq__("not an unnest join") is NotImplemented
    assert (a1 == "not an unnest join") is False


def test_cte_inline_parent_rewrites_existence_source_map():
    """``inline_parent_datasource`` must rewrite existence_source_map entries
    that pointed to the parent QDS, mirroring the source_map loop."""
    from trilogy.core.models.build import BuildColumnAssignment
    from trilogy.core.models.execute import CTE

    key = _key_concept("k")
    parent_ds = BuildDatasource(
        name="parent_ds",
        columns=[BuildColumnAssignment(alias="k", concept=key)],
        address="parent_addr",
        grain=BuildGrain(),
    )
    parent = CTE.from_datasource(parent_ds)

    # Build a child CTE off a separate base, then point its source at the parent
    # CTE's QDS so inline_parent_datasource recognizes it. Populate
    # existence_source_map with an entry referencing the parent's safe_identifier
    # to force the rewrite loop into doing real work.
    child_base_ds = BuildDatasource(
        name="child_ds",
        columns=[BuildColumnAssignment(alias="k", concept=key)],
        address="child_addr",
        grain=BuildGrain(),
    )
    child = CTE.from_datasource(child_base_ds)
    # Splice the parent's QDS into the child's datasources list so the inline
    # logic actually finds it.
    child.source.datasources = [parent.source]
    child.existence_source_map = {key.address: [parent.safe_identifier]}

    assert child.inline_parent_datasource(parent) is True
    # After inline, the parent's safe_identifier should be replaced with the
    # parent's *base datasource*'s safe_identifier (since that's what was
    # inlined).
    assert child.existence_source_map[key.address] == [parent_ds.safe_identifier]
