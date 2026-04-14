use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::path::PathBuf;

use crate::graph::GraphCore;
use crate::parser::parse_file;
use crate::resolver::ImportResolver;
use crate::trilogy_parser::{
    parse_trilogy_syntax, parse_trilogy_syntax_count, parse_trilogy_syntax_tuple,
    PestNode, PestToken,
};

/// Python module for PreQL import resolution
#[pymodule]
fn _preql_import_resolver(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyImportResolver>()?;
    m.add_class::<PyGraphCore>()?;
    m.add_class::<PestNode>()?;
    m.add_class::<PestToken>()?;
    m.add_function(wrap_pyfunction!(parse_preql_file, m)?)?;
    m.add_function(wrap_pyfunction!(parse_trilogy_syntax, m)?)?;
    m.add_function(wrap_pyfunction!(parse_trilogy_syntax_tuple, m)?)?;
    m.add_function(wrap_pyfunction!(parse_trilogy_syntax_count, m)?)?;
    Ok(())
}

/// Parse a PreQL file and return imports, datasources, and persists
#[pyfunction]
fn parse_preql_file(py: Python<'_>, content: &str) -> PyResult<PyObject> {
    let parsed = parse_file(content)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Parse error: {}", e)))?;

    let result = PyDict::new_bound(py);

    // Add imports
    let imports = PyList::empty_bound(py);
    for import in parsed.imports {
        let import_dict = PyDict::new_bound(py);
        import_dict.set_item("raw_path", import.raw_path)?;
        import_dict.set_item("alias", import.alias)?;
        import_dict.set_item("is_stdlib", import.is_stdlib)?;
        import_dict.set_item("parent_dirs", import.parent_dirs)?;
        imports.append(import_dict)?;
    }
    result.set_item("imports", imports)?;

    // Add datasources
    let datasources = PyList::empty_bound(py);
    for ds in parsed.datasources {
        let ds_dict = PyDict::new_bound(py);
        ds_dict.set_item("name", ds.name)?;
        datasources.append(ds_dict)?;
    }
    result.set_item("datasources", datasources)?;

    // Add persists
    let persists = PyList::empty_bound(py);
    for persist in parsed.persists {
        let persist_dict = PyDict::new_bound(py);
        persist_dict.set_item("mode", persist.mode.to_string())?;
        persist_dict.set_item("target_datasource", persist.target_datasource)?;
        persists.append(persist_dict)?;
    }
    result.set_item("persists", persists)?;

    Ok(result.into())
}

#[pyclass]
#[derive(Clone)]
struct PyGraphCore {
    graph: GraphCore,
}

#[pymethods]
impl PyGraphCore {
    #[new]
    fn new(directed: bool) -> Self {
        Self {
            graph: GraphCore::new(directed),
        }
    }

    fn directed(&self) -> bool {
        self.graph.directed()
    }

    fn add_node(&mut self, node: &str) {
        self.graph.add_node(node);
    }

    fn has_node(&self, node: &str) -> bool {
        self.graph.has_node(node)
    }

    fn add_edge(&mut self, left: &str, right: &str) {
        self.graph.add_edge(left, right);
    }

    fn has_edge(&self, left: &str, right: &str) -> bool {
        self.graph.has_edge(left, right)
    }

    fn remove_node(&mut self, node: &str) {
        self.graph.remove_node(node);
    }

    fn remove_nodes(&mut self, nodes: Vec<String>) {
        self.graph.remove_nodes(nodes);
    }

    fn remove_edges(&mut self, edges: Vec<(String, String)>) {
        self.graph.remove_edges(edges);
    }

    fn nodes(&self) -> Vec<String> {
        self.graph.nodes()
    }

    fn edges(&self) -> Vec<(String, String)> {
        self.graph.edges()
    }

    fn neighbors(&self, node: &str) -> Vec<String> {
        self.graph.neighbors(node)
    }

    fn predecessors(&self, node: &str) -> Vec<String> {
        self.graph.predecessors(node)
    }

    fn successors(&self, node: &str) -> Vec<String> {
        self.graph.successors(node)
    }

    fn all_neighbors(&self, node: &str) -> Vec<String> {
        self.graph.all_neighbors(node)
    }

    fn in_degree(&self, node: &str) -> usize {
        self.graph.in_degree(node)
    }

    fn out_degree(&self, node: &str) -> usize {
        self.graph.out_degree(node)
    }

    fn clone_graph(&self) -> Self {
        self.clone()
    }

    fn induced_subgraph(&self, nodes: Vec<String>) -> Self {
        Self {
            graph: self.graph.induced_subgraph(nodes),
        }
    }

    fn to_undirected_graph(&self) -> Self {
        Self {
            graph: self.graph.to_undirected_graph(),
        }
    }

    fn connected_components(&self) -> Vec<Vec<String>> {
        self.graph.connected_components()
    }

    fn is_weakly_connected(&self) -> bool {
        self.graph.is_weakly_connected()
    }

    fn topological_sort(&self) -> PyResult<Vec<String>> {
        self.graph
            .topological_sort()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))
    }

    fn shortest_path(&self, source: &str, target: &str) -> Option<Vec<String>> {
        self.graph.shortest_path(source, target)
    }

    fn shortest_path_length(&self, source: &str, target: &str) -> Option<usize> {
        self.graph.shortest_path_length(source, target)
    }

    fn ego_graph_nodes(&self, center: &str, radius: usize) -> Vec<String> {
        self.graph.ego_graph_nodes(center, radius)
    }

    fn multi_source_dijkstra_path(
        &self,
        sources: Vec<String>,
        weights: Vec<(String, String, f64)>,
    ) -> PyResult<Vec<(String, Vec<String>)>> {
        self.graph
            .multi_source_dijkstra_path(sources, weights)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))
    }

    fn steiner_tree_nodes(
        &self,
        terminals: Vec<String>,
        weights: Vec<(String, String, f64)>,
    ) -> PyResult<Vec<String>> {
        self.graph
            .steiner_tree_nodes(terminals, weights)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))
    }
}

/// Python wrapper for ImportResolver
#[pyclass]
struct PyImportResolver {
    resolver: ImportResolver,
}

#[pymethods]
impl PyImportResolver {
    #[new]
    fn new() -> Self {
        PyImportResolver {
            resolver: ImportResolver::new(),
        }
    }

    /// Resolve dependencies for a file and return the dependency graph
    fn resolve(&mut self, py: Python<'_>, path: &str) -> PyResult<PyObject> {
        let path_buf = PathBuf::from(path);

        let graph = self
            .resolver
            .resolve(&path_buf)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Resolve error: {}", e)))?;

        let result = pyo3::types::PyDict::new_bound(py);

        // Add root
        result.set_item("root", graph.root.to_string_lossy().to_string())?;

        // Add order (list of file paths in execution order)
        let order = PyList::empty_bound(py);
        for file_path in &graph.order {
            order.append(file_path.to_string_lossy().to_string())?;
        }
        result.set_item("order", order)?;

        // Add files (detailed info about each file)
        let files = pyo3::types::PyDict::new_bound(py);
        for (path, node) in &graph.files {
            let node_dict = pyo3::types::PyDict::new_bound(py);

            node_dict.set_item("path", node.path.to_string_lossy().to_string())?;
            node_dict.set_item("relative_path", node.relative_path.to_string_lossy().to_string())?;

            // imports
            let imports = PyList::empty_bound(py);
            for import in &node.imports {
                let import_dict = pyo3::types::PyDict::new_bound(py);
                import_dict.set_item("raw_path", &import.raw_path)?;
                import_dict.set_item("alias", &import.alias)?;
                import_dict.set_item("is_stdlib", import.is_stdlib)?;
                if let Some(resolved_path) = &import.resolved_path {
                    import_dict.set_item("resolved_path", resolved_path.to_string_lossy().to_string())?;
                }
                imports.append(import_dict)?;
            }
            node_dict.set_item("imports", imports)?;

            // datasources
            let datasources = PyList::empty_bound(py);
            for ds in &node.datasources {
                let ds_dict = pyo3::types::PyDict::new_bound(py);
                ds_dict.set_item("name", &ds.name)?;
                datasources.append(ds_dict)?;
            }
            node_dict.set_item("datasources", datasources)?;

            // persists
            let persists = PyList::empty_bound(py);
            for persist in &node.persists {
                let persist_dict = pyo3::types::PyDict::new_bound(py);
                persist_dict.set_item("mode", &persist.mode)?;
                persist_dict.set_item("target_datasource", &persist.target_datasource)?;
                persists.append(persist_dict)?;
            }
            node_dict.set_item("persists", persists)?;

            // dependency lists
            let import_dependencies = PyList::empty_bound(py);
            for dep in &node.import_dependencies {
                import_dependencies.append(dep.to_string_lossy().to_string())?;
            }
            node_dict.set_item("import_dependencies", import_dependencies)?;

            let updates_datasources = PyList::empty_bound(py);
            for ds in &node.updates_datasources {
                updates_datasources.append(ds)?;
            }
            node_dict.set_item("updates_datasources", updates_datasources)?;

            let declares_datasources = PyList::empty_bound(py);
            for ds in &node.declares_datasources {
                declares_datasources.append(ds)?;
            }
            node_dict.set_item("declares_datasources", declares_datasources)?;

            let depends_on_datasources = PyList::empty_bound(py);
            for ds in &node.depends_on_datasources {
                depends_on_datasources.append(ds)?;
            }
            node_dict.set_item("depends_on_datasources", depends_on_datasources)?;

            files.set_item(path.to_string_lossy().to_string(), node_dict)?;
        }
        result.set_item("files", files)?;

        // Add datasource_declarations
        let declarations = pyo3::types::PyDict::new_bound(py);
        for (ds_name, declaring_path) in &graph.datasource_declarations {
            declarations.set_item(ds_name, declaring_path.to_string_lossy().to_string())?;
        }
        result.set_item("datasource_declarations", declarations)?;

        // Add datasource_updaters
        let updaters = pyo3::types::PyDict::new_bound(py);
        for (ds_name, updater_paths) in &graph.datasource_updaters {
            let paths_list = PyList::empty_bound(py);
            for updater_path in updater_paths {
                paths_list.append(updater_path.to_string_lossy().to_string())?;
            }
            updaters.set_item(ds_name, paths_list)?;
        }
        result.set_item("datasource_updaters", updaters)?;

        // Add warnings
        let warnings = PyList::empty_bound(py);
        for warning in &graph.warnings {
            warnings.append(warning)?;
        }
        result.set_item("warnings", warnings)?;

        Ok(result.into())
    }

    /// Get just the dependency order for a file
    fn resolve_order(&mut self, path: &str) -> PyResult<Vec<String>> {
        let path_buf = PathBuf::from(path);

        let graph = self
            .resolver
            .resolve(&path_buf)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Resolve error: {}", e)))?;

        Ok(graph
            .order
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect())
    }

    /// Resolve dependencies for all files in a directory
    fn resolve_directory(&mut self, py: Python<'_>, dir_path: &str, _recursive: bool) -> PyResult<PyObject> {
        use crate::directory_resolver::{process_directory_with_imports, build_edges, EdgeReason};
        use std::fs;

        let dir = PathBuf::from(dir_path);
        if !dir.is_dir() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("{} is not a directory", dir_path)
            ));
        }

        // Collect all .preql files in the top-level directory
        let mut initial_files = Vec::new();
        let read_dir = fs::read_dir(&dir)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read directory: {}", e)))?;

        for entry in read_dir {
            let entry = entry.map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read entry: {}", e)))?;
            let path = entry.path();
            if path.is_file() && path.extension().map_or(false, |ext| ext == "preql") {
                initial_files.push(path);
            }
        }

        if initial_files.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("No .preql files found in {}", dir_path)
            ));
        }

        // Process directory with transitive import discovery
        let graph = process_directory_with_imports(initial_files)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;

        // Build edges
        let edges = build_edges(&graph);

        // Convert to Python objects
        let result = PyDict::new_bound(py);
        let edges_list = PyList::empty_bound(py);
        let files_list = PyList::empty_bound(py);
        let warnings_list = PyList::empty_bound(py);

        // Add files with debug info
        let files_info_dict = PyDict::new_bound(py);
        for file_info in graph.files.values() {
            files_list.append(file_info.path.to_string_lossy().to_string())?;
            let info_dict = PyDict::new_bound(py);
            let ds_list = PyList::empty_bound(py);
            for ds in &file_info.datasources {
                ds_list.append(ds)?;
            }
            info_dict.set_item("datasources", ds_list)?;
            let persist_list = PyList::empty_bound(py);
            for p in &file_info.persists {
                persist_list.append(p)?;
            }
            info_dict.set_item("persists", persist_list)?;
            files_info_dict.set_item(file_info.path.to_string_lossy().to_string(), info_dict)?;
        }
        result.set_item("files_info", files_info_dict)?;

        // Add edges
        for edge in edges {
            let edge_dict = PyDict::new_bound(py);
            edge_dict.set_item("from", edge.from.to_string_lossy().to_string())?;
            edge_dict.set_item("to", edge.to.to_string_lossy().to_string())?;

            let reason_dict = PyDict::new_bound(py);
            match edge.reason {
                EdgeReason::Import => {
                    reason_dict.set_item("type", "import")?;
                }
                EdgeReason::PersistBeforeDeclare { datasource } => {
                    reason_dict.set_item("type", "persist_before_declare")?;
                    reason_dict.set_item("datasource", datasource)?;
                }
                EdgeReason::TransitivePersistOrder {
                    upstream_datasource,
                    downstream_datasource,
                } => {
                    reason_dict.set_item("type", "transitive_persist_order")?;
                    reason_dict.set_item("upstream_datasource", upstream_datasource)?;
                    reason_dict.set_item("downstream_datasource", downstream_datasource)?;
                }
            }
            edge_dict.set_item("reason", reason_dict)?;
            edges_list.append(edge_dict)?;
        }

        // Add warnings
        for warning in graph.warnings {
            warnings_list.append(warning)?;
        }

        result.set_item("directory", dir.to_string_lossy().to_string())?;
        result.set_item("files", files_list)?;
        result.set_item("edges", edges_list)?;
        result.set_item("warnings", warnings_list)?;

        Ok(result.into())
    }
}
