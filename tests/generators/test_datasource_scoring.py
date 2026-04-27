from unittest.mock import MagicMock

from trilogy.core.enums import AddressType, BooleanOperator, ComparisonOperator, Purpose
from trilogy.core.models.build import (
    BuildColumnAssignment,
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildDatasource,
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
    get_materialization_score,
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
        result_types = {ds.address.type for ds in result}
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
        assert len(result) == 3
        assert {ds.name for ds in result} == {
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
        assert len(source_result) == 2
        assert {ds.name for ds in source_result} == {city_raw.name, arb_raw.name}

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
