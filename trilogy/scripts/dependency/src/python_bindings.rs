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
        use std::collections::{HashMap, HashSet};
        use crate::parser::parse_file;
        use std::fs;

        let dir = PathBuf::from(dir_path);
        if !dir.is_dir() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("{} is not a directory", dir_path)
            ));
        }

        // Collect all .preql files
        let mut files = Vec::new();
        let read_dir = fs::read_dir(&dir)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read directory: {}", e)))?;

        for entry in read_dir {
            let entry = entry.map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read entry: {}", e)))?;
            let path = entry.path();
            if path.is_file() && path.extension().map_or(false, |ext| ext == "preql") {
                files.push(path);
            }
        }

        if files.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("No .preql files found in {}", dir_path)
            ));
        }

        let result = PyDict::new_bound(py);
        let edges_list = PyList::empty_bound(py);
        let files_list = PyList::empty_bound(py);
        let warnings_list = PyList::empty_bound(py);

        let mut all_imports: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        let mut files_info: HashMap<PathBuf, (Vec<String>, Vec<String>)> = HashMap::new(); // (datasources, persists)

        // Parse all files and collect info
        for file in &files {
            let canonical = match fs::canonicalize(file) {
                Ok(c) => c,
                Err(e) => {
                    warnings_list.append(format!("Failed to canonicalize {}: {}", file.display(), e))?;
                    continue;
                }
            };

            files_list.append(canonical.to_string_lossy().to_string())?;

            let content = match fs::read_to_string(file) {
                Ok(c) => c,
                Err(e) => {
                    warnings_list.append(format!("Failed to read {}: {}", file.display(), e))?;
                    continue;
                }
            };

            let parsed = match parse_file(&content) {
                Ok(p) => p,
                Err(e) => {
                    warnings_list.append(format!("Failed to parse {}: {}", file.display(), e))?;
                    continue;
                }
            };

            let mut resolved_imports = Vec::new();
            let file_dir = file.parent().unwrap_or(std::path::Path::new("."));

            for import in &parsed.imports {
                if import.is_stdlib {
                    continue;
                }
                if let Some(resolved) = import.resolve(file_dir) {
                    if resolved.exists() {
                        if let Ok(resolved_canonical) = fs::canonicalize(&resolved) {
                            resolved_imports.push(resolved_canonical);
                        }
                    }
                }
            }

            let datasources: Vec<String> = parsed.datasources.iter().map(|d| d.name.clone()).collect();
            let persists: Vec<String> = parsed.persists.iter().map(|p| p.target_datasource.clone()).collect();

            all_imports.insert(canonical.clone(), resolved_imports);
            files_info.insert(canonical, (datasources, persists));
        }

        let known_files: HashSet<PathBuf> = files_info.keys().cloned().collect();

        // Build edges based on ETL rules
        // Rule 1: Import dependencies (imported files run before importing files)
        for (file, imports) in &all_imports {
            for resolved_path in imports {
                if !known_files.contains(resolved_path) {
                    continue;
                }

                // Add import dependency edge
                let edge_dict = PyDict::new_bound(py);
                edge_dict.set_item("from", resolved_path.to_string_lossy().to_string())?;
                edge_dict.set_item("to", file.to_string_lossy().to_string())?;

                let reason_dict = PyDict::new_bound(py);
                reason_dict.set_item("type", "import")?;
                edge_dict.set_item("reason", reason_dict)?;

                edges_list.append(edge_dict)?;
            }
        }

        // Rule 2: Persist-before-declare
        // For each file that declares a datasource, check if any other file persists to it
        for (declarer_path, (datasources, _)) in &files_info {
            for ds_name in datasources {
                // Find all files that persist to this datasource
                for (updater_path, (_, persists)) in &files_info {
                    if updater_path == declarer_path {
                        continue; // Skip self
                    }

                    if persists.contains(ds_name) {
                        // updater persists to a datasource that declarer declares
                        // Check if updater imports declarer (if so, skip this edge per priority rules)
                        let updater_imports_declarer = all_imports
                            .get(updater_path)
                            .map(|imports| imports.contains(declarer_path))
                            .unwrap_or(false);

                        if !updater_imports_declarer {
                            // Add edge: updater -> declarer (updater must run before declarer)
                            let edge_dict = PyDict::new_bound(py);
                            edge_dict.set_item("from", updater_path.to_string_lossy().to_string())?;
                            edge_dict.set_item("to", declarer_path.to_string_lossy().to_string())?;

                            let reason_dict = PyDict::new_bound(py);
                            reason_dict.set_item("type", "persist_before_declare")?;
                            reason_dict.set_item("datasource", ds_name)?;
                            edge_dict.set_item("reason", reason_dict)?;

                            edges_list.append(edge_dict)?;
                        }
                    }
                }
            }
        }

        result.set_item("directory", dir.to_string_lossy().to_string())?;
        result.set_item("files", files_list)?;
        result.set_item("edges", edges_list)?;
        result.set_item("warnings", warnings_list)?;

        Ok(result.into())
    }
}
