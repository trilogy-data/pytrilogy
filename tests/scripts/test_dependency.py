from pathlib import Path
import networkx as nx

from trilogy.scripts.dependency import (
    NoDependencyStrategy,
    ETLDependencyStrategy,
    ScriptNode,
    create_script_nodes,
    normalize_path_variants,
)

TEST_NODES = [
    ScriptNode(path=Path("/some/path")),
    ScriptNode(path=Path("/some_other/path")),
]


def test_no_dependency():
    """Test that NoDependencyStrategy creates no edges"""
    x = NoDependencyStrategy()

    built = x.build_graph(TEST_NODES)

    assert len(built.edges) == 0


def test_etl_dependency_with_imports():
    """Test ETL dependency resolution with import statements"""
    # Use persistent test data directory
    test_dir = Path(__file__).parent / "test_data"
    base_file = test_dir / "base.preql"
    consumer_file = test_dir / "consumer.preql"

    # Ensure test files exist
    assert base_file.exists(), f"Test file {base_file} not found"
    assert consumer_file.exists(), f"Test file {consumer_file} not found"

    # Create script nodes
    nodes = create_script_nodes([base_file, consumer_file])

    # Build dependency graph
    strategy = ETLDependencyStrategy()
    graph = strategy.build_folder_graph(test_dir)

    print(graph.nodes)

    # Verify that base comes before consumer
    base_node = next(n for n in nodes if n.path == base_file)
    consumer_node = next(n for n in nodes if n.path == consumer_file)

    # Check if there's an edge from base to consumer
    assert graph.has_edge(base_node, consumer_node), "base should run before consumer"


def test_etl_dependency_with_persist():
    """Test ETL dependency resolution with persist statements"""
    # Use persistent test data directory
    test_dir = Path(__file__).parent / "test_data" / "persist_scenario"
    updater_file = test_dir / "updater.preql"
    declarer_file = test_dir / "declarer.preql"
    main_file = test_dir / "main.preql"

    # Ensure test files exist
    assert updater_file.exists(), f"Test file {updater_file} not found"
    assert declarer_file.exists(), f"Test file {declarer_file} not found"
    assert main_file.exists(), f"Test file {main_file} not found"

    # Create script nodes
    nodes = create_script_nodes([updater_file, declarer_file, main_file])

    # Build dependency graph
    strategy = ETLDependencyStrategy()
    graph = strategy.build_graph(nodes)

    # Verify that updater comes before declarer
    updater_node = next(n for n in nodes if n.path == updater_file)
    declarer_node = next(n for n in nodes if n.path == declarer_file)

    # Check if there's a path from updater to declarer (may be indirect through main)
    assert nx.has_path(graph, updater_node, declarer_node), (
        "updater should run before declarer (persist-before-declare rule)"
    )


def test_etl_dependency_complex_chain():
    """Test ETL dependency resolution with complex dependency chain"""
    # Use persistent test data directory
    test_dir = Path(__file__).parent / "test_data" / "complex_chain"
    base_file = test_dir / "base.preql"
    updater_file = test_dir / "updater.preql"
    consumer_file = test_dir / "consumer.preql"
    main_file = test_dir / "main.preql"

    # Ensure test files exist
    assert base_file.exists(), f"Test file {base_file} not found"
    assert updater_file.exists(), f"Test file {updater_file} not found"
    assert consumer_file.exists(), f"Test file {consumer_file} not found"
    assert main_file.exists(), f"Test file {main_file} not found"

    # Create script nodes
    nodes = create_script_nodes([base_file, updater_file, consumer_file, main_file])

    # Build dependency graph
    strategy = ETLDependencyStrategy()
    graph = strategy.build_graph(nodes)

    # Get topological order
    try:
        order = list(nx.topological_sort(graph))
    except nx.NetworkXError:
        raise AssertionError("Graph has cycles - dependency resolution failed")

    # Find positions in execution order
    updater_node = next(n for n in nodes if n.path == updater_file)
    base_node = next(n for n in nodes if n.path == base_file)
    consumer_node = next(n for n in nodes if n.path == consumer_file)
    main_node = next(n for n in nodes if n.path == main_file)

    updater_pos = order.index(updater_node)
    base_pos = order.index(base_node)
    consumer_pos = order.index(consumer_node)
    main_pos = order.index(main_node)

    # Verify ordering constraints
    assert updater_pos < base_pos, "updater must run before base (persist-before-declare)"
    assert base_pos < consumer_pos, "base must run before consumer (declare-before-use)"
    assert updater_pos < main_pos and base_pos < main_pos and consumer_pos < main_pos, (
        "main should run after all dependencies"
    )
