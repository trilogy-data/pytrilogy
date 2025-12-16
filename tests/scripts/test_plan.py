"""Tests for the plan command."""

import json
import tempfile
from pathlib import Path

import pytest
from click.testing import CliRunner

from trilogy.scripts.plan import (
    format_plan_text,
    get_execution_levels,
    graph_to_json,
    safe_relative_path,
)
from trilogy.scripts.trilogy import cli


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def test_data_dir():
    return Path(__file__).parent / "test_data"


@pytest.fixture
def complex_chain_dir(test_data_dir):
    return test_data_dir / "complex_chain"


class TestPlanJsonOutput:
    """Tests for JSON output format of plan command."""

    def test_plan_json_structure(self, runner, complex_chain_dir):
        """Test that JSON output has correct structure."""
        result = runner.invoke(cli, ["plan", str(complex_chain_dir), "--json"])
        assert result.exit_code == 0, f"Command failed: {result.output}"

        data = json.loads(result.output)
        assert "nodes" in data
        assert "edges" in data
        assert "execution_order" in data
        assert isinstance(data["nodes"], list)
        assert isinstance(data["edges"], list)
        assert isinstance(data["execution_order"], list)

    def test_plan_json_nodes(self, runner, complex_chain_dir):
        """Test that JSON output includes all nodes."""
        result = runner.invoke(cli, ["plan", str(complex_chain_dir), "--json"])
        assert result.exit_code == 0

        data = json.loads(result.output)
        node_ids = {node["id"] for node in data["nodes"]}
        assert "base.preql" in node_ids
        assert "updater.preql" in node_ids
        assert "consumer.preql" in node_ids
        assert "main.preql" in node_ids

    def test_plan_json_edges(self, runner, complex_chain_dir):
        """Test that JSON output includes dependency edges."""
        result = runner.invoke(cli, ["plan", str(complex_chain_dir), "--json"])
        assert result.exit_code == 0

        data = json.loads(result.output)
        edges = {(e["from"], e["to"]) for e in data["edges"]}
        # updater persists to a table that base declares, so updater->base
        assert ("updater.preql", "base.preql") in edges

    def test_plan_json_execution_order(self, runner, complex_chain_dir):
        """Test that execution_order groups scripts by level."""
        result = runner.invoke(cli, ["plan", str(complex_chain_dir), "--json"])
        assert result.exit_code == 0

        data = json.loads(result.output)
        execution_order = data["execution_order"]
        assert len(execution_order) > 0

        # All scripts should appear exactly once across all levels
        all_scripts = []
        for level in execution_order:
            all_scripts.extend(level)

        assert len(all_scripts) == len(data["nodes"])
        assert len(set(all_scripts)) == len(all_scripts)  # No duplicates

    def test_plan_json_output_file(self, runner, complex_chain_dir):
        """Test writing JSON output to file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            result = runner.invoke(
                cli, ["plan", str(complex_chain_dir), "--json", "-o", tmp_path]
            )
            assert result.exit_code == 0
            assert "Plan written to" in result.output

            with open(tmp_path) as f:
                data = json.load(f)
            assert "nodes" in data
            assert "edges" in data
            assert "execution_order" in data
        finally:
            Path(tmp_path).unlink(missing_ok=True)


class TestPlanTextOutput:
    """Tests for text output format of plan command."""

    def test_plan_text_output(self, runner, complex_chain_dir):
        """Test text output format."""
        result = runner.invoke(cli, ["plan", str(complex_chain_dir)])
        assert result.exit_code == 0

    def test_plan_text_output_file(self, runner, complex_chain_dir):
        """Test writing text output to file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            result = runner.invoke(
                cli, ["plan", str(complex_chain_dir), "-o", tmp_path]
            )
            assert result.exit_code == 0
            assert "Plan written to" in result.output

            with open(tmp_path) as f:
                content = f.read()
            assert "Execution Plan" in content
            assert "Scripts:" in content
            assert "Dependencies:" in content
        finally:
            Path(tmp_path).unlink(missing_ok=True)


class TestPlanSingleFile:
    """Tests for plan command with single file input."""

    def test_plan_single_file(self, runner, test_data_dir):
        """Test plan for a single file."""
        single_file = test_data_dir / "base.preql"
        result = runner.invoke(cli, ["plan", str(single_file), "--json"])
        assert result.exit_code == 0

        data = json.loads(result.output)
        assert len(data["nodes"]) == 1
        assert len(data["edges"]) == 0


class TestPlanErrors:
    """Tests for error handling in plan command."""

    def test_plan_nonexistent_path(self, runner):
        """Test error handling for nonexistent path."""
        result = runner.invoke(cli, ["plan", "/nonexistent/path"])
        assert result.exit_code != 0


class TestGraphToJson:
    """Tests for graph_to_json helper function."""

    def test_graph_to_json_empty(self):
        """Test graph_to_json with empty graph."""
        import networkx as nx

        graph = nx.DiGraph()
        result = graph_to_json(graph, Path("/root"))
        assert result == {"nodes": [], "edges": []}


class TestGetExecutionLevels:
    """Tests for get_execution_levels helper function."""

    def test_get_execution_levels_empty(self):
        """Test get_execution_levels with empty graph."""
        import networkx as nx

        graph = nx.DiGraph()
        result = get_execution_levels(graph)
        assert result == []


class TestFormatPlanText:
    """Tests for format_plan_text helper function."""

    def test_format_plan_text_empty(self):
        """Test format_plan_text with empty graph."""
        import networkx as nx

        graph = nx.DiGraph()
        nodes, edges, execution_order = format_plan_text(graph, Path("/root"))
        assert nodes == []
        assert edges == []
        assert execution_order == []


class TestSafeRelativePath:
    """Tests for safe_relative_path helper function."""

    def test_path_inside_root(self):
        """Test with path that is inside the root directory."""
        root = Path("/project/etl")
        path = Path("/project/etl/scripts/main.preql")
        result = safe_relative_path(path, root)
        assert result == str(Path("scripts/main.preql"))

    def test_path_is_root(self):
        """Test with path that equals the root."""
        root = Path("/project/etl")
        path = Path("/project/etl")
        result = safe_relative_path(path, root)
        assert result == "."

    def test_path_outside_root(self):
        """Test with path outside the root directory (external import)."""
        root = Path("/project/etl")
        path = Path("/project/shared/utils.preql")
        result = safe_relative_path(path, root)
        assert result == str(Path("/project/shared/utils.preql"))

    def test_path_sibling_directory(self):
        """Test with path in sibling directory (common external import case)."""
        root = Path("/project/etl")
        path = Path("/project/common/base.preql")
        result = safe_relative_path(path, root)
        assert result == str(Path("/project/common/base.preql"))

    def test_path_parent_directory(self):
        """Test with path in parent directory."""
        root = Path("/project/etl/scripts")
        path = Path("/project/etl/config.preql")
        result = safe_relative_path(path, root)
        assert result == str(Path("/project/etl/config.preql"))

    def test_deeply_nested_inside(self):
        """Test with deeply nested path inside root."""
        root = Path("/project")
        path = Path("/project/etl/scripts/daily/main.preql")
        result = safe_relative_path(path, root)
        assert result == str(Path("etl/scripts/daily/main.preql"))

    def test_completely_different_tree(self):
        """Test with path in completely different directory tree."""
        root = Path("/project/etl")
        path = Path("/other/location/script.preql")
        result = safe_relative_path(path, root)
        assert result == str(Path("/other/location/script.preql"))


class TestPlanExternalImports:
    """Tests for plan command with external imports (files outside root dir)."""

    @pytest.fixture
    def external_import_dir(self, test_data_dir):
        return test_data_dir / "external_import_test"

    def test_plan_with_external_import_json(self, runner, external_import_dir):
        """Test that external imports are handled correctly in JSON output."""
        result = runner.invoke(cli, ["plan", str(external_import_dir), "--json"])
        assert result.exit_code == 0, f"Command failed: {result.output}"

        data = json.loads(result.output)
        node_ids = {node["id"] for node in data["nodes"]}

        # main.preql should be relative to root
        assert "main.preql" in node_ids

        # shared.preql is outside the root dir, should appear as absolute path
        external_nodes = [n for n in node_ids if "external_shared" in n]
        assert len(external_nodes) == 1, f"Expected external node, got: {node_ids}"

    def test_plan_with_external_import_text(self, runner, external_import_dir):
        """Test that external imports are handled correctly in text output."""
        result = runner.invoke(cli, ["plan", str(external_import_dir)])
        assert result.exit_code == 0, f"Command failed: {result.output}"

    def test_plan_external_import_edges(self, runner, external_import_dir):
        """Test that edges involving external imports are correctly represented."""
        result = runner.invoke(cli, ["plan", str(external_import_dir), "--json"])
        assert result.exit_code == 0

        data = json.loads(result.output)
        # Should have an edge from shared.preql to main.preql (import dependency)
        assert len(data["edges"]) >= 1
        edge_froms = [e["from"] for e in data["edges"]]
        assert any("external_shared" in f or "shared" in f for f in edge_froms)
