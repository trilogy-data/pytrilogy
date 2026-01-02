from unittest.mock import MagicMock

from trilogy.core.enums import AddressType
from trilogy.core.models.build import BuildDatasource
from trilogy.core.models.datasource import Address
from trilogy.core.processing.node_generators.select_merge_node import (
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
