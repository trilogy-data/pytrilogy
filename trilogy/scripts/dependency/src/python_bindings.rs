use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::path::PathBuf;

use crate::parser::parse_file;
use crate::resolver::ImportResolver;

/// Python module for PreQL import resolution
#[pymodule]
fn _preql_import_resolver(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyImportResolver>()?;
    m.add_function(wrap_pyfunction!(parse_preql_file, m)?)?;
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

        // Add files
        for file_info in graph.files.values() {
            files_list.append(file_info.path.to_string_lossy().to_string())?;
        }

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
