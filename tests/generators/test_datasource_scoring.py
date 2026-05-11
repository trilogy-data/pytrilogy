from unittest.mock import MagicMock

from trilogy.core.enums import (
    AddressType,
    BooleanOperator,
    ComparisonOperator,
    Modifier,
    Purpose,
)
from trilogy.core.graph_models import (
    ReferenceGraph,
    SearchCriteria,
    concept_to_node,
)
from trilogy.core.models.build import (
    BuildColumnAssignment,
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildDatasource,
    BuildGrain,
    BuildParenthetical,
    BuildWhereClause,
)
from trilogy.core.models.core import DataType, EnumType
from trilogy.core.models.datasource import Address
from trilogy.core.processing.node_generators.select_helpers.datasource_injection import (
    _best_enum_union,
    _datasource_score,
    _extract_enum_value_for_key,
)
from trilogy.core.processing.node_generators.select_helpers.source_scoring import (
    get_graph_partial_nodes,
    get_materialization_score,
    resolve_subgraphs,
    score_datasource_node,
)


class TestGetMaterializationScore:
    def test_table_returns_0(self):
        assert get_materialization_score(AddressType.TABLE) == 0

    def test_csv_returns_1(self):
        assert get_materialization_score(AddressType.CSV) == 1

    def test_tsv_returns_1(self):
        assert get_materialization_score(AddressType.TSV) == 1

    def test_parquet_returns_1(self):
        assert get_materialization_score(AddressType.PARQUET) == 1

    def test_query_returns_2(self):
        assert get_materialization_score(AddressType.QUERY) == 2

    def test_sql_returns_2(self):
        assert get_materialization_score(AddressType.SQL) == 2

    def test_python_script_returns_3(self):
        assert get_materialization_score(AddressType.PYTHON_SCRIPT) == 3


class TestScoreDatasourceNode:
    def _make_mock_ds(self, address_type: AddressType) -> MagicMock:
        ds = BuildDatasource(
            name="dummy",
            columns=[],
            address=Address(location="test", type=address_type),
        )

        return ds

    def test_table_preferred_over_csv(self):
        table_ds = self._make_mock_ds(AddressType.TABLE)
        csv_ds = self._make_mock_ds(AddressType.CSV)

        datasources = {"ds~table": table_ds, "ds~csv": csv_ds}
        grain_map = {"ds~table": [], "ds~csv": []}
        concept_map = {"ds~table": [], "ds~csv": []}
        exact_map: set[str] = set()
        subgraphs = {"ds~table": [], "ds~csv": []}

        table_score = score_datasource_node(
            "ds~table", datasources, grain_map, concept_map, exact_map, subgraphs
        )
        csv_score = score_datasource_node(
            "ds~csv", datasources, grain_map, concept_map, exact_map, subgraphs
        )

        assert table_score < csv_score

    def test_csv_preferred_over_query(self):
        csv_ds = self._make_mock_ds(AddressType.CSV)
        query_ds = self._make_mock_ds(AddressType.QUERY)

        datasources = {"ds~csv": csv_ds, "ds~query": query_ds}
        grain_map = {"ds~csv": [], "ds~query": []}
        concept_map = {"ds~csv": [], "ds~query": []}
        exact_map: set[str] = set()
        subgraphs = {"ds~csv": [], "ds~query": []}

        csv_score = score_datasource_node(
            "ds~csv", datasources, grain_map, concept_map, exact_map, subgraphs
        )
        query_score = score_datasource_node(
            "ds~query", datasources, grain_map, concept_map, exact_map, subgraphs
        )

        assert csv_score < query_score

    def test_query_preferred_over_python_script(self):
        query_ds = self._make_mock_ds(AddressType.QUERY)
        script_ds = self._make_mock_ds(AddressType.PYTHON_SCRIPT)

        datasources = {"ds~query": query_ds, "ds~script": script_ds}
        grain_map = {"ds~query": [], "ds~script": []}
        concept_map = {"ds~query": [], "ds~script": []}
        exact_map: set[str] = set()
        subgraphs = {"ds~query": [], "ds~script": []}

        query_score = score_datasource_node(
            "ds~query", datasources, grain_map, concept_map, exact_map, subgraphs
        )
        script_score = score_datasource_node(
            "ds~script", datasources, grain_map, concept_map, exact_map, subgraphs
        )

        assert query_score < script_score

    def test_table_preferred_over_python_script(self):
        table_ds = self._make_mock_ds(AddressType.TABLE)
        script_ds = self._make_mock_ds(AddressType.PYTHON_SCRIPT)

        datasources = {"ds~table": table_ds, "ds~script": script_ds}
        grain_map = {"ds~table": [], "ds~script": []}
        concept_map = {"ds~table": [], "ds~script": []}
        exact_map: set[str] = set()
        subgraphs = {"ds~table": [], "ds~script": []}

        table_score = score_datasource_node(
            "ds~table", datasources, grain_map, concept_map, exact_map, subgraphs
        )
        script_score = score_datasource_node(
            "ds~script", datasources, grain_map, concept_map, exact_map, subgraphs
        )

        assert table_score < script_score

    def test_grain_score_still_affects_ordering(self):
        # Table with high grain should be worse than CSV with lower grain
        # when materialization difference doesn't override
        table_ds = self._make_mock_ds(AddressType.TABLE)
        other_table_ds = self._make_mock_ds(AddressType.TABLE)

        datasources = {"ds~high_grain": table_ds, "ds~low_grain": other_table_ds}
        grain_map = {"ds~high_grain": ["a", "b", "c"], "ds~low_grain": ["a"]}
        concept_map = {"ds~high_grain": [], "ds~low_grain": []}
        exact_map: set[str] = set()
        subgraphs = {"ds~high_grain": [], "ds~low_grain": []}

        high_grain_score = score_datasource_node(
            "ds~high_grain", datasources, grain_map, concept_map, exact_map, subgraphs
        )
        low_grain_score = score_datasource_node(
            "ds~low_grain", datasources, grain_map, concept_map, exact_map, subgraphs
        )

        # Same materialization, so grain should differentiate
        assert low_grain_score < high_grain_score

    def test_exact_match_affects_ordering(self):
        ds1 = self._make_mock_ds(AddressType.TABLE)
        ds2 = self._make_mock_ds(AddressType.TABLE)

        datasources = {"ds~exact": ds1, "ds~not_exact": ds2}
        grain_map = {"ds~exact": [], "ds~not_exact": []}
        concept_map = {"ds~exact": [], "ds~not_exact": []}
        exact_map = {"ds~exact"}
        subgraphs = {"ds~exact": [], "ds~not_exact": []}

        exact_score = score_datasource_node(
            "ds~exact", datasources, grain_map, concept_map, exact_map, subgraphs
        )
        not_exact_score = score_datasource_node(
            "ds~not_exact", datasources, grain_map, concept_map, exact_map, subgraphs
        )

        assert exact_score < not_exact_score

    def test_missing_datasource_defaults_to_query_score(self):
        grain_map = {"ds~missing": []}
        concept_map = {"ds~missing": []}
        exact_map: set[str] = set()
        subgraphs = {"ds~missing": []}

        score = score_datasource_node(
            "ds~missing", {}, grain_map, concept_map, exact_map, subgraphs
        )

        # Should default to materialization score of 2 (query level)
        assert score[0] == 2

    def test_score_tuple_ordering(self):
        # Verify the tuple ordering works correctly with all components
        table_ds = self._make_mock_ds(AddressType.TABLE)
        csv_ds = self._make_mock_ds(AddressType.CSV)

        datasources = {"ds~table": table_ds, "ds~csv": csv_ds}
        grain_map = {"ds~table": ["a", "b"], "ds~csv": []}
        concept_map = {"ds~table": [], "ds~csv": []}
        exact_map: set[str] = set()
        subgraphs = {"ds~table": [], "ds~csv": []}

        table_score = score_datasource_node(
            "ds~table", datasources, grain_map, concept_map, exact_map, subgraphs
        )
        csv_score = score_datasource_node(
            "ds~csv", datasources, grain_map, concept_map, exact_map, subgraphs
        )

        # Table should still win because materialization is more important
        assert table_score < csv_score

    def test_parquet_same_priority_as_csv(self):
        csv_ds = self._make_mock_ds(AddressType.CSV)
        parquet_ds = self._make_mock_ds(AddressType.PARQUET)

        datasources = {"ds~csv": csv_ds, "ds~parquet": parquet_ds}
        grain_map = {"ds~csv": [], "ds~parquet": []}
        concept_map = {"ds~csv": [], "ds~parquet": []}
        exact_map: set[str] = set()
        subgraphs = {"ds~csv": [], "ds~parquet": []}

        csv_score = score_datasource_node(
            "ds~csv", datasources, grain_map, concept_map, exact_map, subgraphs
        )
        parquet_score = score_datasource_node(
            "ds~parquet", datasources, grain_map, concept_map, exact_map, subgraphs
        )

        # Same materialization level
        assert csv_score[0] == parquet_score[0] == 1


def _make_concept(name: str, datatype=DataType.STRING) -> BuildConcept:
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=datatype,
        purpose=Purpose.KEY,
        build_is_aggregate=False,
    )


def _make_enum_ds_two_key(
    val1: str,
    key1: BuildConcept,
    val2: str,
    key2: BuildConcept,
    shared: BuildConcept,
    address_type: AddressType = AddressType.PYTHON_SCRIPT,
) -> BuildDatasource:
    condition = BuildWhereClause(
        conditional=BuildConditional(
            left=BuildComparison(left=key1, right=val1, operator=ComparisonOperator.EQ),
            right=BuildComparison(
                left=key2, right=val2, operator=ComparisonOperator.EQ
            ),
            operator=BooleanOperator.AND,
        )
    )
    return BuildDatasource(
        name=f"{val1}_{val2}_{address_type.value}",
        columns=[
            BuildColumnAssignment(alias="k1", concept=key1),
            BuildColumnAssignment(alias="k2", concept=key2),
            BuildColumnAssignment(alias="shared", concept=shared),
        ],
        address=Address(location=f"/data/{val1}_{val2}", type=address_type),
        non_partial_for=condition,
    )


def _make_enum_ds_or(
    val1: str,
    val2: str,
    key: BuildConcept,
    shared: BuildConcept,
) -> BuildDatasource:
    condition = BuildWhereClause(
        conditional=BuildConditional(
            left=BuildComparison(left=key, right=val1, operator=ComparisonOperator.EQ),
            right=BuildComparison(left=key, right=val2, operator=ComparisonOperator.EQ),
            operator=BooleanOperator.OR,
        )
    )
    return BuildDatasource(
        name=f"{val1}_or_{val2}",
        columns=[
            BuildColumnAssignment(alias="key", concept=key),
            BuildColumnAssignment(alias="shared", concept=shared),
        ],
        address=Address(
            location=f"/data/{val1}_or_{val2}.py", type=AddressType.PYTHON_SCRIPT
        ),
        non_partial_for=condition,
    )


def _make_enum_ds(
    city_val: str,
    address_type: AddressType,
    city_concept: BuildConcept,
    shared_concept: BuildConcept,
) -> BuildDatasource:
    condition = BuildWhereClause(
        conditional=BuildComparison(
            left=city_concept,
            right=city_val,
            operator=ComparisonOperator.EQ,
        )
    )
    return BuildDatasource(
        name=f"{city_val}_{address_type.value}",
        columns=[
            BuildColumnAssignment(alias="city", concept=city_concept),
            BuildColumnAssignment(alias="name", concept=shared_concept),
        ],
        address=Address(
            location=f"/data/{city_val}.{address_type.value}", type=address_type
        ),
        non_partial_for=condition,
    )


class TestDatasourceScore:
    def test_parquet_beats_python_script(self):
        ds_parquet = BuildDatasource(
            name="parquet_ds",
            columns=[],
            address=Address(location="/data/file.parquet", type=AddressType.PARQUET),
        )
        ds_script = BuildDatasource(
            name="script_ds",
            columns=[],
            address=Address(location="/data/script.py", type=AddressType.PYTHON_SCRIPT),
        )
        assert _datasource_score(ds_parquet) > _datasource_score(ds_script)

    def test_table_beats_parquet(self):
        ds_table = BuildDatasource(
            name="table_ds",
            columns=[],
            address=Address(location="my_table", type=AddressType.TABLE),
        )
        ds_parquet = BuildDatasource(
            name="parquet_ds",
            columns=[],
            address=Address(location="/data/file.parquet", type=AddressType.PARQUET),
        )
        assert _datasource_score(ds_table) > _datasource_score(ds_parquet)


class TestBestEnumUnion:
    def test_prefers_parquet_over_python_script(self):
        """When both parquet and script sources cover all enum values, pick parquet."""
        city_enum = EnumType(type=DataType.STRING, values=["USSFO", "USNYC"])
        city = _make_concept("city", datatype=city_enum)
        species = _make_concept("species")

        sf_parquet = _make_enum_ds("USSFO", AddressType.PARQUET, city, species)
        nyc_parquet = _make_enum_ds("USNYC", AddressType.PARQUET, city, species)
        sf_script = _make_enum_ds("USSFO", AddressType.PYTHON_SCRIPT, city, species)
        nyc_script = _make_enum_ds("USNYC", AddressType.PYTHON_SCRIPT, city, species)

        result = _best_enum_union(
            [sf_parquet, nyc_parquet, sf_script, nyc_script], city_enum, city
        )

        assert result is not None
        # Sources with identical concept signatures (parquet vs script both expose
        # {city, species}) collapse into a single signature group; the higher-
        # scoring (parquet) member wins.
        assert len(result) == 1
        result_types = {ds.address.type for ds in result[0]}
        assert result_types == {
            AddressType.PARQUET
        }, f"Expected parquet sources, got {result_types}"

    def test_single_value_enum_returns_none(self):
        """Single-value enum: both sources share the only value → no valid union."""
        city_enum = EnumType(type=DataType.STRING, values=["USBOS"])
        city = _make_concept("city", datatype=city_enum)
        shared = _make_concept("name")

        src_a = _make_enum_ds("USBOS", AddressType.PYTHON_SCRIPT, city, shared)
        src_b = _make_enum_ds("USBOS", AddressType.PYTHON_SCRIPT, city, shared)

        result = _best_enum_union([src_a, src_b], city_enum, city)
        assert result is None, f"Expected None for single-value enum, got {result}"

    def test_three_value_enum_three_sources(self):
        """Three-value enum: one source per value produces a valid 3-way union."""
        region_enum = EnumType(type=DataType.STRING, values=["NORTH", "SOUTH", "EAST"])
        region = _make_concept("region", datatype=region_enum)
        shared = _make_concept("value")

        north = _make_enum_ds("NORTH", AddressType.TABLE, region, shared)
        south = _make_enum_ds("SOUTH", AddressType.TABLE, region, shared)
        east = _make_enum_ds("EAST", AddressType.TABLE, region, shared)

        result = _best_enum_union([north, south, east], region_enum, region)
        assert result is not None
        assert len(result) == 1
        combo = result[0]
        assert len(combo) == 3
        assert {ds.name for ds in combo} == {
            "NORTH_table",
            "SOUTH_table",
            "EAST_table",
        }

    def test_two_key_condition_discriminates_on_correct_key(self):
        """When non_partial_for has two enum keys, only the discriminating key produces a union.

        city=['USBOS'] (single value) and source=['CITY','ARBORETUM'] (two values).
        Two sources: (city=USBOS, source=CITY) and (city=USBOS, source=ARBORETUM).
        The city key has all sources sharing the same value → None.
        The source key correctly discriminates → 2-way union.
        """
        city_enum = EnumType(type=DataType.STRING, values=["USBOS"])
        source_enum = EnumType(type=DataType.STRING, values=["CITY", "ARBORETUM"])
        city = _make_concept("city", datatype=city_enum)
        source = _make_concept("source", datatype=source_enum)
        shared = _make_concept("name")

        city_raw = _make_enum_ds_two_key("USBOS", city, "CITY", source, shared)
        arb_raw = _make_enum_ds_two_key("USBOS", city, "ARBORETUM", source, shared)

        city_result = _best_enum_union([city_raw, arb_raw], city_enum, city)
        assert city_result is None, f"city key should return None, got {city_result}"

        source_result = _best_enum_union([city_raw, arb_raw], source_enum, source)
        assert source_result is not None
        assert len(source_result) == 1
        combo = source_result[0]
        assert len(combo) == 2
        assert {ds.name for ds in combo} == {city_raw.name, arb_raw.name}

    def test_or_condition_not_used_as_slot(self):
        """A source whose non_partial_for uses OR must not occupy a single enum slot.

        Without the OR guard, the OR-source extracts the first branch value ('CITY')
        and gets placed in the 'CITY' slot, forming a union with the real 'ARBORETUM'
        source.  That union would duplicate all ARBORETUM rows since the OR-source
        already covers them.  With the fix, the OR-source is ignored and the union
        returns None (no complete 2-source cover remains).
        """
        source_enum = EnumType(type=DataType.STRING, values=["CITY", "ARBORETUM"])
        source = _make_concept("source", datatype=source_enum)
        shared = _make_concept("name")

        or_src = _make_enum_ds_or("CITY", "ARBORETUM", source, shared)
        arb_src = _make_enum_ds("ARBORETUM", AddressType.PYTHON_SCRIPT, source, shared)

        result = _best_enum_union([or_src, arb_src], source_enum, source)
        assert (
            result is None
        ), f"OR-condition source must not be used as a single-value slot; got {result}"


class TestExtractEnumValueForKey:
    def _concept(self, name: str) -> BuildConcept:
        return _make_concept(name)

    def test_parenthetical_wrapping_comparison(self):
        """Value is extracted when the comparison is wrapped in a BuildParenthetical."""
        key = self._concept("region")
        cmp = BuildComparison(left=key, right="NORTH", operator=ComparisonOperator.EQ)
        paren = BuildParenthetical(content=cmp)

        result = _extract_enum_value_for_key(paren, key.address)
        assert result == "NORTH"

    def test_parenthetical_wrapping_conditional(self):
        """Value is extracted when a parenthetical wraps a compound AND condition."""
        key = self._concept("region")
        other = self._concept("other")
        inner = BuildConditional(
            left=BuildComparison(
                left=key, right="SOUTH", operator=ComparisonOperator.EQ
            ),
            right=BuildComparison(
                left=other, right="X", operator=ComparisonOperator.EQ
            ),
            operator=BooleanOperator.AND,
        )
        paren = BuildParenthetical(content=inner)

        result = _extract_enum_value_for_key(paren, key.address)
        assert result == "SOUTH"

    def test_parenthetical_with_non_condition_content_returns_none(self):
        """A BuildParenthetical whose content is not a condition type returns None."""
        key = self._concept("region")
        paren = BuildParenthetical(content="not_a_condition")  # type: ignore[arg-type]

        result = _extract_enum_value_for_key(paren, key.address)
        assert result is None


def _scoped_concept(name: str) -> BuildConcept:
    """KEY concept at a single-component grain, with a populated namespace.

    concept_to_node needs grain.str_no_condition, and many planner paths key off
    canonical_address — both must be set explicitly.
    """
    addr = f"local.{name}"
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=DataType.INTEGER,
        purpose=Purpose.KEY,
        build_is_aggregate=False,
        namespace="local",
        grain=BuildGrain(components={addr}),
    )


def _build_two_ds_graph(
    ds_a: BuildDatasource,
    ds_b: BuildDatasource,
    concepts: list[BuildConcept],
) -> ReferenceGraph:
    g = ReferenceGraph()
    a_node = f"ds~{ds_a.name}"
    b_node = f"ds~{ds_b.name}"
    g.datasources = {a_node: ds_a, b_node: ds_b}
    g.add_node(a_node)
    g.add_node(b_node)
    g.concepts = {concept_to_node(c): c for c in concepts}
    for cnode in g.concepts:
        g.add_node(cnode)
    # Connect each DS to every concept it materially exposes.
    for ds_name, ds in g.datasources.items():
        for col in ds.columns:
            cnode = concept_to_node(col.concept)
            if cnode in g.concepts:
                g.add_edge(ds_name, cnode)
                g.add_edge(cnode, ds_name)
    return g


class TestResolveSubgraphsRelevanceFilter:
    """resolve_subgraphs should judge subset coverage using only relevant
    concepts. A DS that uniquely exposes a non-relevant concept must NOT be
    treated as "uniquely covering" anything — otherwise the planner drags in
    spurious datasources (e.g. a returns CTE for a query that only touches
    sales)."""

    def test_strict_subset_after_relevance_filter_drops_extra_ds(self):
        """ds_b adds a non-relevant extra concept; ds_a covers strictly more
        relevant concepts. ds_b must be pruned even though its raw output set
        (including the non-relevant concept) is not a subset of ds_a's."""
        a = _scoped_concept("a")
        b = _scoped_concept("b")
        c = _scoped_concept("c")
        extra = _scoped_concept("extra")  # NOT in relevant

        ds_a = BuildDatasource(
            name="ds_wide",
            columns=[
                BuildColumnAssignment(alias="a", concept=a),
                BuildColumnAssignment(alias="b", concept=b),
                BuildColumnAssignment(alias="c", concept=c),
            ],
            address=Address(location="ds_wide", type=AddressType.TABLE),
            grain=BuildGrain(components={a.canonical_address}),
        )
        ds_b = BuildDatasource(
            name="ds_narrow_with_extra",
            columns=[
                BuildColumnAssignment(alias="a", concept=a),
                BuildColumnAssignment(alias="b", concept=b),
                BuildColumnAssignment(alias="extra", concept=extra),
            ],
            address=Address(location="ds_narrow_with_extra", type=AddressType.TABLE),
            grain=BuildGrain(components={a.canonical_address}),
        )
        g = _build_two_ds_graph(ds_a, ds_b, [a, b, c, extra])

        result = resolve_subgraphs(
            g, relevant=[a, b, c], criteria=SearchCriteria.FULL_ONLY, conditions=None
        )

        # ds_b should be dropped: its relevant coverage {a, b} is a strict
        # subset of ds_a's relevant coverage {a, b, c}.
        assert "ds~ds_wide" in result
        assert "ds~ds_narrow_with_extra" not in result

    def test_uniquely_relevant_concept_blocks_subset_pruning(self):
        """When a DS uniquely covers a *relevant* concept, it must be kept
        even if it would otherwise look like a subset."""
        a = _scoped_concept("a")
        b = _scoped_concept("b")
        c = _scoped_concept("c")  # relevant, ONLY on ds_b

        ds_a = BuildDatasource(
            name="ds_ab",
            columns=[
                BuildColumnAssignment(alias="a", concept=a),
                BuildColumnAssignment(alias="b", concept=b),
            ],
            address=Address(location="ds_ab", type=AddressType.TABLE),
            grain=BuildGrain(components={a.canonical_address}),
        )
        ds_b = BuildDatasource(
            name="ds_ac",
            columns=[
                BuildColumnAssignment(alias="a", concept=a),
                BuildColumnAssignment(alias="c", concept=c),
            ],
            address=Address(location="ds_ac", type=AddressType.TABLE),
            grain=BuildGrain(components={a.canonical_address}),
        )
        g = _build_two_ds_graph(ds_a, ds_b, [a, b, c])

        result = resolve_subgraphs(
            g, relevant=[a, b, c], criteria=SearchCriteria.FULL_ONLY, conditions=None
        )

        # Neither is a strict subset on relevant concepts; both stay.
        assert "ds~ds_ab" in result
        assert "ds~ds_ac" in result


class TestGetGraphPartialNodesPreservesStructuralPartials:
    """Under a satisfied complete-where, the implicit table-level partial stamp
    goes away — but a ``~col`` (column-level) partial is structural and survives.
    A returns table that's `partial datasource ... complete where channel='STORE'`
    with `~order_id` is NOT fully complete on order_id even when the query
    filter satisfies the complete-where; only some sales have returns."""

    def test_column_level_partial_survives_condition_implies(self):
        channel = _scoped_concept("channel")
        order_id = _scoped_concept("order_id")
        amount = _scoped_concept("amount")

        complete_where = BuildWhereClause(
            conditional=BuildComparison(
                left=channel, right="STORE", operator=ComparisonOperator.EQ
            )
        )

        # `partial datasource` (is_partial=True) stamps Modifier.PARTIAL on
        # every column; the explicit ~order_id additionally lands in
        # column_level_partial_addresses (the structural set).
        ds = BuildDatasource(
            name="store_returns_like",
            columns=[
                BuildColumnAssignment(
                    alias="channel",
                    concept=channel,
                    modifiers=[Modifier.PARTIAL],
                ),
                BuildColumnAssignment(
                    alias="order_id",
                    concept=order_id,
                    modifiers=[Modifier.PARTIAL],
                ),
                BuildColumnAssignment(
                    alias="amount",
                    concept=amount,
                    modifiers=[Modifier.PARTIAL],
                ),
            ],
            address=Address(location="returns", type=AddressType.TABLE),
            grain=BuildGrain(
                components={channel.canonical_address, order_id.canonical_address}
            ),
            non_partial_for=complete_where,
            column_level_partial_addresses={order_id.canonical_address},
        )

        g = ReferenceGraph()
        node = f"ds~{ds.name}"
        g.datasources = {node: ds}
        g.add_node(node)

        query_conditions = BuildWhereClause(
            conditional=BuildComparison(
                left=channel, right="STORE", operator=ComparisonOperator.EQ
            )
        )

        partial_map = get_graph_partial_nodes(g, query_conditions)

        # The implicit stamp on `channel` and `amount` is wiped by
        # condition_implies, but the structural ~order_id survives.
        partials = partial_map[node]
        assert partials == [concept_to_node(order_id)], (
            f"Expected only the structural ~order_id partial to remain "
            f"under condition_implies; got {partials}"
        )

    def test_no_complete_where_means_all_partials_present(self):
        """Sanity: without a complete-where, every Modifier.PARTIAL column shows
        up in the partial map (no condition_implies path)."""
        x = _scoped_concept("x")
        y = _scoped_concept("y")

        ds = BuildDatasource(
            name="plain_partial",
            columns=[
                BuildColumnAssignment(
                    alias="x", concept=x, modifiers=[Modifier.PARTIAL]
                ),
                BuildColumnAssignment(
                    alias="y", concept=y, modifiers=[Modifier.PARTIAL]
                ),
            ],
            address=Address(location="plain_partial", type=AddressType.TABLE),
            grain=BuildGrain(components={x.canonical_address}),
        )
        g = ReferenceGraph()
        node = f"ds~{ds.name}"
        g.datasources = {node: ds}
        g.add_node(node)

        partial_map = get_graph_partial_nodes(g, None)
        assert set(partial_map[node]) == {concept_to_node(x), concept_to_node(y)}
