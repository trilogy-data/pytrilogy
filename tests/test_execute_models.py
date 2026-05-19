from pathlib import Path

from pytest import raises

from trilogy import Environment
from trilogy.core.enums import JoinType
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.build import BuildColumnAssignment
from trilogy.core.models.execute import (
    CTE,
    BuildConcept,
    BuildDatasource,
    BuildGrain,
    CTEConceptPair,
    DatasourceCTE,
    DataType,
    Join,
    Purpose,
    QueryDatasource,
    UnionCTE,
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


def _datasource_cte(name: str, concept: BuildConcept) -> DatasourceCTE:
    ds = BuildDatasource(
        name=name,
        columns=[BuildColumnAssignment(alias=concept.name, concept=concept)],
        address=f"{name}_addr",
        grain=BuildGrain(),
    )
    cte = CTE.from_datasource(ds)
    assert isinstance(cte, DatasourceCTE)
    return cte


def _query_cte(
    name: str,
    concept: BuildConcept,
    parents: list[CTE | UnionCTE] | None = None,
) -> CTE:
    parents = parents or []
    return CTE(
        name=name,
        source=QueryDatasource(
            input_concepts=[concept],
            output_concepts=[concept],
            datasources=[parent.source for parent in parents],
            grain=BuildGrain(),
            joins=[],
            source_map={concept.address: {parent.source for parent in parents}},
        ),
        output_columns=[concept],
        parent_ctes=parents,
        grain=BuildGrain(),
        source_map={concept.address: [parent.name for parent in parents]},
        existence_source_map={},
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


def test_join_reference_for_inlined_datasource_renders_raw_table():
    """An inlined ``DatasourceCTE`` join target renders its raw table under
    the datasource alias; a materialized one is referenced by name."""
    from trilogy.core.enums import JoinType
    from trilogy.core.models.build import BuildColumnAssignment
    from trilogy.core.models.execute import CTE, Join, QueryDatasource

    key = _key_concept("k")
    base_ds = BuildDatasource(
        name="base",
        columns=[BuildColumnAssignment(alias="k", concept=key)],
        address="base_addr",
        grain=BuildGrain(),
    )
    dim = CTE.from_datasource(base_ds)
    consumer = CTE(
        name="consumer",
        source=QueryDatasource(
            input_concepts=[key],
            output_concepts=[key],
            datasources=[dim.source],
            grain=BuildGrain(),
            joins=[],
            source_map={key.address: {dim.source}},
        ),
        output_columns=[key],
        parent_ctes=[dim],
        grain=BuildGrain(),
        source_map={key.address: [dim.name]},
        existence_source_map={},
    )
    join = Join(right_cte=dim, jointype=JoinType.INNER)
    join.quote = '"'

    # normal parent: referenced by CTE name
    assert consumer.renders_inline(dim) is False
    assert join.name_for(consumer, dim) == dim.name
    assert join.reference_for(consumer, dim) == f'"{dim.name}"'

    # folded into the consumer (parked on inlined_parents): raw table under
    # the datasource alias
    consumer.parent_ctes = []
    consumer.inlined_parents = [dim]
    assert consumer.renders_inline(dim) is True
    assert join.name_for(consumer, dim) == base_ds.safe_identifier
    ref = join.reference_for(consumer, dim)
    assert base_ds.safe_location in ref
    assert f'as "{base_ds.safe_identifier}"' in ref


def test_source_bindings_include_emitted_and_inlined_sources():
    key = _key_concept("k")
    emitted = _datasource_cte("emitted_ds", key)
    folded = _datasource_cte("folded_ds", key)
    consumer = _query_cte("consumer", key, [emitted])
    consumer.add_inlined_datasource(folded)

    emitted_only = consumer.source_bindings(include_inlined=False)
    assert [binding.key for binding in emitted_only] == [emitted.name]
    assert emitted_only[0].node is emitted
    assert emitted_only[0].datasource is emitted.datasource
    assert emitted_only[0].emitted is True

    bindings = consumer.source_bindings()
    folded_binding = next(binding for binding in bindings if binding.inlined)
    assert folded_binding.node is folded
    assert folded_binding.datasource is folded.datasource
    assert folded_binding.key == folded.datasource.safe_identifier
    assert folded_binding.emitted is False
    assert consumer.source_key_for(folded) == folded.datasource.safe_identifier


def test_replace_dependency_updates_cte_references_and_source_tokens():
    key = _key_concept("k")
    old = _datasource_cte("old_ds", key)
    old.name = "old_cte"
    new = _datasource_cte("new_ds", key)
    new.name = "new_cte"
    consumer = _query_cte("consumer", key, [old])
    consumer.inlined_parents = [old]
    consumer.source_map = {key.address: [old.datasource.safe_identifier]}
    consumer.existence_source_map = {key.address: [old.datasource.safe_identifier]}
    consumer.base_alias_override = old.safe_identifier
    consumer.base_name_override = old.safe_identifier
    pair = CTEConceptPair(
        left=key,
        right=key,
        existing_datasource=old.source,
        cte=old,
    )
    join = Join(
        right_cte=old,
        left_cte=old,
        jointype=JoinType.INNER,
        joinkey_pairs=[pair],
    )
    consumer.joins = [join]

    consumer.replace_dependency(old, new)

    assert consumer.parent_ctes == [new]
    assert consumer.inlined_parents == [new]
    assert consumer.source_map[key.address] == [new.datasource.safe_identifier]
    assert consumer.existence_source_map[key.address] == [
        new.datasource.safe_identifier
    ]
    assert consumer.base_alias_override == new.safe_identifier
    assert consumer.base_name_override == new.safe_identifier
    assert join.right_cte is new
    assert join.left_cte is new
    assert pair.cte is new


def test_union_dependency_helpers_keep_branches_separate_from_parents():
    key = _key_concept("k")
    parent = _datasource_cte("parent_ds", key)
    branch = _query_cte("branch", key, [parent])
    union = UnionCTE(
        name="unioned",
        source=QueryDatasource(
            input_concepts=[key],
            output_concepts=[key],
            datasources=[branch.source],
            grain=BuildGrain(),
            joins=[],
            source_map={key.address: {branch.source}},
        ),
        parent_ctes=[parent],
        internal_ctes=[branch],
        output_columns=[key],
        grain=BuildGrain(),
    )

    assert union.dependency_nodes() == [parent]
    assert union.dependency_nodes(include_branches=True) == [parent, branch]
    branch_binding = next(
        binding for binding in union.source_bindings() if binding.branch
    )
    assert branch_binding.node is branch
    assert branch_binding.datasource is branch.source
    assert branch_binding.emitted is False

    replacement = _query_cte("replacement", key, [parent])
    union.replace_dependency(branch, replacement)
    assert union.parent_ctes == [parent]
    assert union.internal_ctes == [replacement]


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


def test_inline_parent_datasource_folds_leaf_into_consumer():
    """``inline_parent_datasource`` structurally folds the datasource leaf
    into the consumer (parent leaves ``parent_ctes``, source maps repoint to
    the datasource, the BD enters ``source.datasources``) and parks the
    CTE-shaped leaf on ``inlined_parents`` for uniform rendering."""
    from trilogy.core.models.build import BuildColumnAssignment
    from trilogy.core.models.execute import CTE, DatasourceCTE

    key = _key_concept("k")
    parent_ds = BuildDatasource(
        name="parent_ds",
        columns=[BuildColumnAssignment(alias="k", concept=key)],
        address="parent_addr",
        grain=BuildGrain(),
    )
    parent = CTE.from_datasource(parent_ds)
    assert isinstance(parent, DatasourceCTE)

    child_base_ds = BuildDatasource(
        name="child_ds",
        columns=[BuildColumnAssignment(alias="k", concept=key)],
        address="child_addr",
        grain=BuildGrain(),
    )
    child = CTE.from_datasource(child_base_ds)
    child.source.datasources = [parent.source]
    child.parent_ctes = [parent]
    child.source_map = {key.address: [parent.safe_identifier]}
    child.existence_source_map = {key.address: [parent.safe_identifier]}

    assert child.inline_parent_datasource(parent) is True
    # parent left the optimizer graph and is parked for rendering
    assert child.parent_ctes == []
    assert parent in child.inlined_parents
    assert child.renders_inline(parent) is True
    # source maps now resolve via the datasource alias
    assert child.source_map[key.address] == [parent_ds.safe_identifier]
    assert child.existence_source_map[key.address] == [parent_ds.safe_identifier]
    # the inlined datasource is now a direct source of the consumer's QDS
    assert any(
        getattr(d, "safe_location", None) == parent_ds.safe_location
        for d in child.source.datasources
    )


def test_inline_parent_datasource_alias_collision_keeps_existing_source():
    """When a folded datasource would reuse an existing raw alias, render it
    under the parent CTE name without dropping the existing source."""
    from trilogy.core.models.build import BuildColumnAssignment
    from trilogy.core.models.execute import CTE

    key = _key_concept("k")
    parent_ds = BuildDatasource(
        name="shared",
        columns=[BuildColumnAssignment(alias="parent_k", concept=key)],
        address="parent_addr",
        grain=BuildGrain(),
    )
    parent = CTE.from_datasource(parent_ds)
    parent.name = "dim_cte"

    existing_ds = BuildDatasource(
        name="shared",
        columns=[BuildColumnAssignment(alias="existing_k", concept=key)],
        address="existing_addr",
        grain=BuildGrain(),
    )
    child = CTE.from_datasource(existing_ds)
    child.source.datasources = [existing_ds, parent.source]
    child.parent_ctes = [parent]
    child.source_map = {key.address: [parent.safe_identifier]}
    child.existence_source_map = {key.address: [parent.safe_identifier]}

    assert child.inline_parent_datasource(parent) is True

    inlined = child.inlined_parent_for_source(parent.name)
    assert inlined is not None
    assert inlined.datasource.safe_identifier == parent.name
    assert child.source_map[key.address] == [parent.name]
    assert child.existence_source_map[key.address] == [parent.name]
    assert any(ds is existing_ds for ds in child.source.datasources)
