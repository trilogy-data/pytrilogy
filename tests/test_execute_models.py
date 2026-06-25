from pathlib import Path

from pytest import raises

from trilogy import Environment
from trilogy.core.enums import ComparisonOperator, FunctionType, JoinType
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.build import BuildColumnAssignment, BuildSubselectComparison
from trilogy.core.models.execute import (
    CTE,
    BuildConcept,
    BuildDatasource,
    BuildGrain,
    CTEConceptPair,
    DatasourceCTE,
    DataType,
    InstantiatedUnnestJoin,
    Join,
    Purpose,
    QueryDatasource,
    UnionCTE,
    raise_helpful_join_validation_error,
)
from trilogy.dialect.base import BaseDialect, safe_get_cte_value


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


def test_cte_rejects_multiple_join_derived_concepts():
    with raises(NotImplementedError):
        CTE(
            name="bad",
            source=QueryDatasource(
                input_concepts=[],
                output_concepts=[],
                datasources=[],
                grain=BuildGrain(),
                joins=[],
                source_map={},
            ),
            output_columns=[],
            source_map={},
            grain=BuildGrain(),
            join_derived_concepts=[_property_concept("a"), _property_concept("b")],
        )


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


def test_join_reference_for_emitted_datasource_ignores_scanned_raw_source():
    key = _key_concept("k")
    base_ds = BuildDatasource(
        name="base",
        columns=[BuildColumnAssignment(alias="k", concept=key)],
        address="base_addr",
        grain=BuildGrain(),
    )
    dim = CTE.from_datasource(base_ds)
    dim.name = "dim_cte"
    consumer = CTE(
        name="consumer",
        source=QueryDatasource(
            input_concepts=[key],
            output_concepts=[key],
            datasources=[base_ds, dim.source],
            grain=BuildGrain(),
            joins=[],
            source_map={key.address: {base_ds, dim.source}},
        ),
        output_columns=[key],
        parent_ctes=[dim],
        grain=BuildGrain(),
        source_map={key.address: [dim.name]},
        existence_source_map={},
    )
    join = Join(right_cte=dim, jointype=JoinType.INNER)
    join.quote = '"'

    assert consumer.renders_inline(dim) is False
    assert join.name_for(consumer, dim) == dim.name
    assert join.reference_for(consumer, dim) == f'"{dim.name}"'


def test_join_reference_for_union_consumer_uses_node_name():
    key = _key_concept("k")
    branch = _query_cte("branch", key)
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
        parent_ctes=[],
        internal_ctes=[branch],
        output_columns=[key],
        grain=BuildGrain(),
    )
    join = Join(right_cte=branch, jointype=JoinType.INNER)
    join.quote = '"'

    assert union.identifier == union.name
    assert union.safe_identifier == union.name
    assert union.group_to_grain is False
    assert union.group_concepts == []
    assert join.name_for(union, branch) == branch.name
    assert join.reference_for(union, branch) == f'"{branch.name}"'
    assert join.right_name == branch.name


def test_join_unique_id_includes_sorted_join_key_pairs():
    left = _key_concept("left_id")
    right = _key_concept("right_id")
    cte = _query_cte("source", left)
    pair = CTEConceptPair(
        left=left,
        right=right,
        existing_datasource=cte.source,
        cte=cte,
    )
    join = Join(
        right_cte=cte,
        jointype=JoinType.INNER,
        joinkey_pairs=[pair],
    )

    assert join.unique_id == "inner join source on source.local.left_id=local.right_id"


def test_join_unique_id_without_pairs_uses_string_form():
    key = _key_concept("k")
    cte = _query_cte("source", key)
    join = Join(right_cte=cte, jointype=JoinType.INNER)

    assert join.unique_id == str(join)


def test_cte_group_concepts_keeps_rollup_outputs():
    key = _key_concept("k")
    cte = _query_cte("cte", key)
    cte.rollup_concepts = [key]
    cte.group_to_grain = True

    assert cte.group_concepts == []


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


def test_source_key_for_resolves_bound_datasources_by_identity_and_identifier():
    key = _key_concept("k")
    emitted = _datasource_cte("emitted_ds", key)
    folded = _datasource_cte("folded_ds", key)
    query_parent = _query_cte("query_parent", key)
    consumer = _query_cte("consumer", key, [emitted, query_parent])
    consumer.add_inlined_datasource(folded)

    same_identifier = BuildDatasource(
        name=folded.datasource.name,
        columns=[],
        address="other_addr",
        grain=BuildGrain(),
    )
    unbound = BuildDatasource(
        name="unbound",
        columns=[],
        address="unbound_addr",
        grain=BuildGrain(),
    )
    unbound_qds = QueryDatasource(
        input_concepts=[],
        output_concepts=[],
        datasources=[],
        grain=BuildGrain(),
        joins=[],
        source_map={},
    )

    assert consumer.source_key_for(folded.datasource) == folded.datasource.name
    assert consumer.source_key_for(same_identifier) == folded.datasource.name
    assert consumer.source_key_for(unbound) == unbound.safe_identifier
    assert consumer.source_key_for(query_parent.source) == query_parent.name
    assert consumer.source_key_for(unbound_qds) == unbound_qds.safe_identifier


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
        isinstance(d, BuildDatasource) and d.safe_location == parent_ds.safe_location
        for d in child.source.datasources
    )


def test_inline_parent_datasource_adds_unmapped_parent_outputs_and_force_group():
    key = _key_concept("k")
    extra = _property_concept("extra")
    parent_ds = BuildDatasource(
        name="parent_ds",
        columns=[
            BuildColumnAssignment(alias="k", concept=key),
            BuildColumnAssignment(alias="extra", concept=extra),
        ],
        address="parent_addr",
        grain=BuildGrain(),
    )
    parent = CTE.from_datasource(parent_ds)
    child = _query_cte("child", key, [parent])

    assert child.inline_parent_datasource(parent, force_group=True) is True
    assert child.source_map[extra.address] == [parent_ds.safe_identifier]
    assert child.group_to_grain is True


def test_cte_add_rejects_union_cte():
    key = _key_concept("k")
    cte = _query_cte("cte", key)
    union = UnionCTE(
        name="unioned",
        source=QueryDatasource(
            input_concepts=[key],
            output_concepts=[key],
            datasources=[cte.source],
            grain=BuildGrain(),
            joins=[],
            source_map={key.address: {cte.source}},
        ),
        parent_ctes=[],
        internal_ctes=[cte],
        output_columns=[key],
        grain=BuildGrain(),
    )

    with raises(ValueError, match="cannot merge CTE and union CTE"):
        cte + union


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


def test_inline_parent_datasource_rejects_non_datasource_parent():
    key = _key_concept("k")
    child = _query_cte("child", key)
    parent = _query_cte("parent", key)

    assert child.inline_parent_datasource(parent) is False


def test_replace_dependency_ignores_unnest_joins():
    key = _key_concept("k")
    old = _datasource_cte("old_ds", key)
    new = _datasource_cte("new_ds", key)
    consumer = _query_cte("consumer", key, [old])
    unnest = InstantiatedUnnestJoin(object_to_unnest=key)
    consumer.joins = [unnest]

    consumer.replace_dependency(old, new)

    assert consumer.joins == [unnest]
    assert consumer.parent_ctes == [new]


def test_inlined_parent_lookup_accepts_cte_name_and_raw_alias():
    key = _key_concept("k")
    dim = _datasource_cte("dim", key)
    consumer = _query_cte("consumer", key)
    consumer.add_inlined_datasource(dim)

    assert consumer.inlined_parent_for_source(dim.name) is dim
    assert consumer.inlined_parent_for_source(dim.datasource.safe_identifier) is dim


def test_inlined_parent_providing_returns_none_for_unknown_concept():
    key = _key_concept("k")
    dim = _datasource_cte("dim", key)
    consumer = _query_cte("consumer", key)
    consumer.add_inlined_datasource(dim)

    assert consumer.inlined_parent_providing(_property_concept("missing")) is None


def test_get_alias_skips_source_mismatch_and_reports_invalid():
    key = _key_concept("k")
    missing = _property_concept("missing")
    parent = _datasource_cte("parent", key)
    consumer = _query_cte("consumer", key, [parent])

    assert consumer.get_alias(key, source="other") == key.safe_address
    assert consumer.get_alias(missing).startswith("INVALID_ALIAS:")


def test_union_condition_setter_is_not_supported():
    key = _key_concept("k")
    branch = _query_cte("branch", key)
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
        parent_ctes=[],
        internal_ctes=[branch],
        output_columns=[key],
        grain=BuildGrain(),
    )

    with raises(NotImplementedError):
        union.condition = None


def test_inlined_datasource_renders_raw_column_missing_from_source_map():
    key = _key_concept("k")
    raw = _property_concept("raw_name")
    dim_ds = BuildDatasource(
        name="dim",
        columns=[
            BuildColumnAssignment(alias="k", concept=key),
            BuildColumnAssignment(alias="_raw_name", concept=raw),
        ],
        address="dim_addr",
        grain=BuildGrain(),
    )
    dim = CTE.from_datasource(dim_ds)
    consumer = _query_cte("consumer", key)
    consumer.add_inlined_datasource(dim)

    rendered = BaseDialect().render_concept_sql(raw, consumer, alias=False)

    assert rendered == "`dim`.`_raw_name`"


def test_safe_get_cte_value_returns_none_when_no_source_can_render():
    key = _key_concept("k")
    missing = _property_concept("missing")
    cte = _query_cte("consumer", key)

    rendered = safe_get_cte_value(
        BaseDialect().FUNCTION_MAP[FunctionType.COALESCE],
        cte,
        missing,
        "`",
        BaseDialect().render_expr,
        {},
    )

    assert rendered is None


def test_inlined_datasource_subselect_renders_raw_table_source():
    left = _key_concept("left_id")
    right = _key_concept("right_id")
    dim = _datasource_cte("dim", right)
    consumer = _query_cte("consumer", left)
    consumer.add_inlined_datasource(dim)
    consumer.existence_source_map = {right.address: [dim.datasource.safe_identifier]}

    rendered = BaseDialect().render_expr(
        BuildSubselectComparison(
            left=1,
            right=right,
            operator=ComparisonOperator.IN,
        ),
        cte=consumer,
    )

    assert "from dim_addr as dim" in rendered
    assert "dim.`right_id`" in rendered


def test_union_source_key_helpers_resolve_parents_branches_and_fallbacks():
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
        parent_ctes=[],
        internal_ctes=[branch],
        output_columns=[key],
        grain=BuildGrain(),
    )
    unbound = BuildDatasource(
        name="unbound",
        columns=[],
        address="unbound_addr",
        grain=BuildGrain(),
    )

    union.add_dependency(parent)

    assert union.source_key_for("literal") == "literal"
    assert union.source_key_for(parent) == parent.name
    assert union.source_key_for(parent.datasource) == parent.name
    assert union.source_key_for(branch.source) == branch.name
    assert union.source_key_for(unbound) == unbound.safe_identifier
    assert union.dependency_nodes() == [parent]
    assert union.dependency_nodes(include_branches=True) == [parent, branch]


def test_union_add_rejects_mismatched_union():
    key = _key_concept("k")
    branch = _query_cte("branch", key)
    left = UnionCTE(
        name="left_union",
        source=QueryDatasource(
            input_concepts=[key],
            output_concepts=[key],
            datasources=[branch.source],
            grain=BuildGrain(),
            joins=[],
            source_map={key.address: {branch.source}},
        ),
        parent_ctes=[],
        internal_ctes=[branch],
        output_columns=[key],
        grain=BuildGrain(),
    )
    right = UnionCTE(
        name="right_union",
        source=left.source,
        parent_ctes=[],
        internal_ctes=[branch],
        output_columns=[key],
        grain=BuildGrain(),
    )

    with raises(SyntaxError, match="Cannot merge union CTEs"):
        left + right
