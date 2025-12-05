use crate::parser::{parse_file, DatasourceDeclaration, ImportStatement, ParseError, PersistStatement, ParsedFile};
use serde::Serialize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ResolveError {
    #[error("Failed to read file {path}: {source}")]
    IoError {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("Parse error in {path}: {source}")]
    ParseError { path: PathBuf, source: ParseError },

    #[error("Circular dependency detected: {cycle}")]
    CircularDependency { cycle: String },

    #[error("Import not found: {import_path} (resolved to {resolved_path})")]
    ImportNotFound {
        import_path: String,
        resolved_path: PathBuf,
    },
}

/// Information about a single import
#[derive(Debug, Clone, Serialize)]
pub struct ImportInfo {
    pub raw_path: String,
    pub alias: Option<String>,
    pub resolved_path: Option<PathBuf>,
    pub is_stdlib: bool,
}

impl From<&ImportStatement> for ImportInfo {
    fn from(stmt: &ImportStatement) -> Self {
        ImportInfo {
            raw_path: stmt.raw_path.clone(),
            alias: stmt.alias.clone(),
            resolved_path: None,
            is_stdlib: stmt.is_stdlib,
        }
    }
}

/// Information about a datasource declaration
#[derive(Debug, Clone, Serialize)]
pub struct DatasourceInfo {
    pub name: String,
}

impl From<&DatasourceDeclaration> for DatasourceInfo {
    fn from(ds: &DatasourceDeclaration) -> Self {
        DatasourceInfo {
            name: ds.name.clone(),
        }
    }
}

/// Information about a persist statement
#[derive(Debug, Clone, Serialize)]
pub struct PersistInfo {
    pub mode: String,
    pub target_datasource: String,
}

impl From<&PersistStatement> for PersistInfo {
    fn from(ps: &PersistStatement) -> Self {
        PersistInfo {
            mode: ps.mode.to_string(),
            target_datasource: ps.target_datasource.clone(),
        }
    }
}

/// A node in the dependency graph
#[derive(Debug, Clone, Serialize)]
pub struct FileNode {
    /// Absolute path to the file
    pub path: PathBuf,
    /// Relative path from the root
    pub relative_path: PathBuf,
    /// List of imports in this file
    pub imports: Vec<ImportInfo>,
    /// List of datasources declared in this file
    pub datasources: Vec<DatasourceInfo>,
    /// List of persist statements in this file
    pub persists: Vec<PersistInfo>,
    /// List of resolved import dependencies (paths to other files)
    pub import_dependencies: Vec<PathBuf>,
    /// Datasources this file updates (via persist)
    pub updates_datasources: Vec<String>,
    /// Datasources this file declares
    pub declares_datasources: Vec<String>,
    /// Datasources this file depends on (through imports)
    pub depends_on_datasources: Vec<String>,
}

/// Result of dependency resolution
#[derive(Debug, Clone, Serialize)]
pub struct DependencyGraph {
    /// Root file that was analyzed
    pub root: PathBuf,
    /// All files in dependency order (dependencies come before dependents)
    pub order: Vec<PathBuf>,
    /// Detailed information about each file
    pub files: HashMap<PathBuf, FileNode>,
    /// Mapping of datasource names to the file that declares them
    pub datasource_declarations: HashMap<String, PathBuf>,
    /// Mapping of datasource names to files that update them (via persist)
    pub datasource_updaters: HashMap<String, Vec<PathBuf>>,
    /// Any errors encountered (non-fatal)
    pub warnings: Vec<String>,
}

/// Resolver for PreQL import dependencies
pub struct ImportResolver {
    /// Cache of parsed files
    parsed_cache: HashMap<PathBuf, ParsedFile>,
    /// Track files currently being processed (for cycle detection)
    #[allow(dead_code)]
    processing: HashSet<PathBuf>,
    /// Track fully processed files
    #[allow(dead_code)]
    processed: HashSet<PathBuf>,
    /// Warnings accumulated during resolution
    warnings: Vec<String>,
}

impl ImportResolver {
    pub fn new() -> Self {
        Self {
            parsed_cache: HashMap::new(),
            processing: HashSet::new(),
            processed: HashSet::new(),
            warnings: Vec::new(),
        }
    }

    /// Resolve all dependencies starting from a root file
    pub fn resolve(&mut self, root_path: &Path) -> Result<DependencyGraph, ResolveError> {
        let root_path = fs::canonicalize(root_path).map_err(|e| ResolveError::IoError {
            path: root_path.to_path_buf(),
            source: e,
        })?;

        let root_dir = root_path.parent().unwrap_or(Path::new("."));
        let mut files: HashMap<PathBuf, FileNode> = HashMap::new();

        // BFS to collect all files first
        let mut queue: VecDeque<PathBuf> = VecDeque::new();
        let mut seen: HashSet<PathBuf> = HashSet::new();

        queue.push_back(root_path.clone());
        seen.insert(root_path.clone());

        while let Some(current_path) = queue.pop_front() {
            let parsed = self.parse_file(&current_path)?;
            let file_dir = current_path.parent().unwrap_or(Path::new("."));

            let mut import_infos: Vec<ImportInfo> = Vec::new();
            let mut import_dependencies: Vec<PathBuf> = Vec::new();

            for import in &parsed.imports {
                let mut info = ImportInfo::from(import);

                if import.is_stdlib {
                    import_infos.push(info);
                    continue;
                }

                if let Some(resolved) = import.resolve(file_dir) {
                    if resolved.exists() {
                        let canonical =
                            fs::canonicalize(&resolved).map_err(|e| ResolveError::IoError {
                                path: resolved.clone(),
                                source: e,
                            })?;

                        info.resolved_path = Some(canonical.clone());
                        import_dependencies.push(canonical.clone());

                        if !seen.contains(&canonical) {
                            seen.insert(canonical.clone());
                            queue.push_back(canonical);
                        }
                    } else {
                        self.warnings.push(format!(
                            "Import '{}' in {} resolved to non-existent file: {}",
                            import.raw_path,
                            current_path.display(),
                            resolved.display()
                        ));
                    }
                }

                import_infos.push(info);
            }

            let datasource_infos: Vec<DatasourceInfo> =
                parsed.datasources.iter().map(DatasourceInfo::from).collect();
            let persist_infos: Vec<PersistInfo> =
                parsed.persists.iter().map(PersistInfo::from).collect();

            let declares_datasources: Vec<String> =
                parsed.datasources.iter().map(|d| d.name.clone()).collect();
            let updates_datasources: Vec<String> = parsed
                .persists
                .iter()
                .map(|p| p.target_datasource.clone())
                .collect();

            let relative_path = pathdiff::diff_paths(&current_path, root_dir)
                .unwrap_or_else(|| current_path.clone());

            files.insert(
                current_path.clone(),
                FileNode {
                    path: current_path,
                    relative_path,
                    imports: import_infos,
                    datasources: datasource_infos,
                    persists: persist_infos,
                    import_dependencies,
                    updates_datasources,
                    declares_datasources,
                    depends_on_datasources: Vec::new(), // Will be computed later
                },
            );
        }

        // Build datasource mappings
        let mut datasource_declarations: HashMap<String, PathBuf> = HashMap::new();
        let mut datasource_updaters: HashMap<String, Vec<PathBuf>> = HashMap::new();

        for (path, node) in &files {
            for ds_name in &node.declares_datasources {
                if let Some(existing) = datasource_declarations.get(ds_name) {
                    self.warnings.push(format!(
                        "Datasource '{}' declared in multiple files: {} and {}",
                        ds_name,
                        existing.display(),
                        path.display()
                    ));
                } else {
                    datasource_declarations.insert(ds_name.clone(), path.clone());
                }
            }

            for ds_name in &node.updates_datasources {
                datasource_updaters
                    .entry(ds_name.clone())
                    .or_insert_with(Vec::new)
                    .push(path.clone());
            }
        }

        // Compute transitive datasource dependencies through imports
        self.compute_datasource_dependencies(&mut files, &datasource_declarations);

        // Topological sort with datasource-aware ordering
        let order =
            self.topological_sort_with_datasources(&files, &datasource_declarations, &datasource_updaters)?;

        Ok(DependencyGraph {
            root: root_path,
            order,
            files,
            datasource_declarations,
            datasource_updaters,
            warnings: self.warnings.clone(),
        })
    }

    fn parse_file(&mut self, path: &Path) -> Result<ParsedFile, ResolveError> {
        if let Some(cached) = self.parsed_cache.get(path) {
            return Ok(cached.clone());
        }

        let content = fs::read_to_string(path).map_err(|e| ResolveError::IoError {
            path: path.to_path_buf(),
            source: e,
        })?;

        let parsed = parse_file(&content).map_err(|e| ResolveError::ParseError {
            path: path.to_path_buf(),
            source: e,
        })?;

        self.parsed_cache.insert(path.to_path_buf(), parsed.clone());
        Ok(parsed)
    }

    /// Compute which datasources each file depends on through its import chain
    fn compute_datasource_dependencies(
        &self,
        files: &mut HashMap<PathBuf, FileNode>,
        _datasource_declarations: &HashMap<String, PathBuf>,
    ) {
        // For each file, find all datasources reachable through imports
        let paths: Vec<PathBuf> = files.keys().cloned().collect();

        for path in paths {
            let mut reachable_datasources: HashSet<String> = HashSet::new();
            let mut visited: HashSet<PathBuf> = HashSet::new();
            let mut stack: Vec<PathBuf> = vec![path.clone()];

            while let Some(current) = stack.pop() {
                if visited.contains(&current) {
                    continue;
                }
                visited.insert(current.clone());

                if let Some(node) = files.get(&current) {
                    // Add datasources declared in imported files (not the file itself for the starting file)
                    if current != path {
                        for ds in &node.declares_datasources {
                            reachable_datasources.insert(ds.clone());
                        }
                    }

                    // Follow imports
                    for dep in &node.import_dependencies {
                        if !visited.contains(dep) {
                            stack.push(dep.clone());
                        }
                    }
                }
            }

            if let Some(node) = files.get_mut(&path) {
                node.depends_on_datasources = reachable_datasources.into_iter().collect();
            }
        }
    }

    /// Topological sort with datasource-aware dependency edges
    ///
    /// The ordering rules are:
    /// 1. Standard import dependencies (imported files run before importing files) - HIGHEST PRIORITY
    /// 2. Files that UPDATE a datasource (via persist) must run BEFORE files that DECLARE that datasource
    ///    BUT ONLY if the updater doesn't import the declarer (import takes precedence)
    /// 3. Files that DECLARE a datasource must run BEFORE files that IMPORT something containing that datasource
    fn topological_sort_with_datasources(
        &self,
        files: &HashMap<PathBuf, FileNode>,
        datasource_declarations: &HashMap<String, PathBuf>,
        datasource_updaters: &HashMap<String, Vec<PathBuf>>,
    ) -> Result<Vec<PathBuf>, ResolveError> {
        // Build adjacency list with all dependency edges
        // Edge A -> B means A must be processed before B
        let mut edges: HashMap<PathBuf, HashSet<PathBuf>> = HashMap::new();

        // Initialize
        for path in files.keys() {
            edges.insert(path.clone(), HashSet::new());
        }

        // Add edges for each dependency type
        for (path, node) in files {
            // Rule 1: Import dependencies - imported file must run before importing file (HIGHEST PRIORITY)
            for dep in &node.import_dependencies {
                if files.contains_key(dep) {
                    edges.get_mut(dep).unwrap().insert(path.clone());
                }
            }

            // Rule 2: Files that UPDATE a datasource must run BEFORE files that DECLARE it
            // BUT ONLY if the updater doesn't import the declarer
            // If this file declares a datasource, all files that update it must run first
            // (unless they import this file, in which case import dependency takes precedence)
            for ds_name in &node.declares_datasources {
                if let Some(updaters) = datasource_updaters.get(ds_name) {
                    for updater_path in updaters {
                        if updater_path != path && files.contains_key(updater_path) {
                            // Check if updater imports this file (directly or transitively)
                            let updater_node = files.get(updater_path).unwrap();
                            let imports_declarer = updater_node.import_dependencies.contains(path);

                            // Only add persist-before-declare edge if there's no import dependency
                            if !imports_declarer {
                                // updater must run before declarer
                                edges.get_mut(updater_path).unwrap().insert(path.clone());
                            }
                        }
                    }
                }
            }

            // Rule 3: Files that DECLARE a datasource must run BEFORE files that depend on it (through imports)
            // If this file depends on a datasource (through imports), the declaring file must run first
            for ds_name in &node.depends_on_datasources {
                if let Some(declaring_path) = datasource_declarations.get(ds_name) {
                    if declaring_path != path && files.contains_key(declaring_path) {
                        // declarer must run before dependent
                        edges.get_mut(declaring_path).unwrap().insert(path.clone());
                    }
                }
            }
        }

        // Kahn's algorithm
        let mut in_degree: HashMap<PathBuf, usize> = HashMap::new();
        for path in files.keys() {
            in_degree.insert(path.clone(), 0);
        }

        for dependents in edges.values() {
            for dep in dependents {
                *in_degree.get_mut(dep).unwrap() += 1;
            }
        }

        let mut queue: VecDeque<PathBuf> = VecDeque::new();
        let mut result: Vec<PathBuf> = Vec::new();

        // Start with nodes that have no incoming edges (no dependencies)
        for (path, &degree) in &in_degree {
            if degree == 0 {
                queue.push_back(path.clone());
            }
        }

        while let Some(current) = queue.pop_front() {
            result.push(current.clone());

            if let Some(dependents) = edges.get(&current) {
                for dependent in dependents {
                    let degree = in_degree.get_mut(dependent).unwrap();
                    *degree -= 1;
                    if *degree == 0 {
                        queue.push_back(dependent.clone());
                    }
                }
            }
        }

        // Check for cycles
        if result.len() != files.len() {
            let remaining: Vec<_> = files
                .keys()
                .filter(|p| !result.contains(p))
                .map(|p| p.display().to_string())
                .collect();
            return Err(ResolveError::CircularDependency {
                cycle: remaining.join(" -> "),
            });
        }

        Ok(result)
    }
}

impl Default for ImportResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn create_test_file(dir: &Path, name: &str, content: &str) -> PathBuf {
        let path = dir.join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&path, content).unwrap();
        path
    }

    #[test]
    fn test_simple_resolution() {
        let temp = TempDir::new().unwrap();
        let root = temp.path();

        create_test_file(root, "a.preql", "import b;");
        create_test_file(root, "b.preql", "// no imports");

        let a_path = root.join("a.preql");
        let mut resolver = ImportResolver::new();
        let graph = resolver.resolve(&a_path).unwrap();

        assert_eq!(graph.order.len(), 2);
        // b should come before a
        let b_idx = graph
            .order
            .iter()
            .position(|p| p.ends_with("b.preql"))
            .unwrap();
        let a_idx = graph
            .order
            .iter()
            .position(|p| p.ends_with("a.preql"))
            .unwrap();
        assert!(b_idx < a_idx, "b should come before a");
    }

    #[test]
    fn test_datasource_declaration_ordering() {
        let temp = TempDir::new().unwrap();
        let root = temp.path();

        // a.preql imports b, which declares datasource "orders"
        // c.preql persists to "orders"
        // Order should be: c (updates orders) -> b (declares orders) -> a (imports b which has orders)

        create_test_file(root, "a.preql", "import b;");
        create_test_file(
            root,
            "b.preql",
            r#"
            datasource orders (
                id: key
            )
            address db.orders;
        "#,
        );
        create_test_file(root, "c.preql", "persist orders;");

        // Start from a file that imports all others (or just test individual relationships)
        let a_path = root.join("a.preql");
        let mut resolver = ImportResolver::new();
        let graph = resolver.resolve(&a_path).unwrap();

        // Verify datasource is tracked
        assert!(graph.datasource_declarations.contains_key("orders"));
        
        // b should come before a (import dependency)
        let b_idx = graph.order.iter().position(|p| p.ends_with("b.preql")).unwrap();
        let a_idx = graph.order.iter().position(|p| p.ends_with("a.preql")).unwrap();
        assert!(b_idx < a_idx, "b should come before a due to import");
    }

    #[test]
    fn test_persist_before_declare() {
        let temp = TempDir::new().unwrap();
        let root = temp.path();

        // updater.preql persists to "orders"
        // declarer.preql declares datasource "orders" and imports updater
        // Order should be: updater (updates orders) -> declarer (declares orders)

        create_test_file(root, "updater.preql", "persist orders;");
        create_test_file(
            root,
            "declarer.preql",
            r#"
            import updater;
            datasource orders (
                id: key
            )
            address db.orders;
        "#,
        );

        let declarer_path = root.join("declarer.preql");
        let mut resolver = ImportResolver::new();
        let graph = resolver.resolve(&declarer_path).unwrap();

        let updater_idx = graph
            .order
            .iter()
            .position(|p| p.ends_with("updater.preql"))
            .unwrap();
        let declarer_idx = graph
            .order
            .iter()
            .position(|p| p.ends_with("declarer.preql"))
            .unwrap();

        assert!(
            updater_idx < declarer_idx,
            "updater (persist) should come before declarer (datasource)"
        );
    }

    #[test]
    fn test_full_dependency_chain() {
        let temp = TempDir::new().unwrap();
        let root = temp.path();

        // Setup:
        // - base.preql: declares datasource "orders"
        // - updater.preql: persists to "orders" (doesn't import base)
        // - consumer.preql: imports base (uses orders datasource)
        //
        // Expected order: updater -> base -> consumer
        // Because:
        // - updater updates orders, so must run before base (which declares it)
        // - base declares orders, so must run before consumer (which imports base and thus depends on orders)

        create_test_file(
            root,
            "base.preql",
            r#"
            datasource orders (
                id: key,
                amount: metric
            )
            address db.orders;
        "#,
        );
        create_test_file(
            root,
            "updater.preql",
            r#"
            persist orders where amount > 100;
        "#,
        );
        create_test_file(
            root,
            "consumer.preql",
            r#"
            import base;
            // uses orders datasource
        "#,
        );

        // Create an entry point that imports everything
        create_test_file(
            root,
            "main.preql",
            r#"
            import updater;
            import consumer;
        "#,
        );

        let main_path = root.join("main.preql");
        let mut resolver = ImportResolver::new();
        let graph = resolver.resolve(&main_path).unwrap();

        assert_eq!(graph.order.len(), 4);

        let updater_idx = graph
            .order
            .iter()
            .position(|p| p.ends_with("updater.preql"))
            .unwrap();
        let base_idx = graph
            .order
            .iter()
            .position(|p| p.ends_with("base.preql"))
            .unwrap();
        let consumer_idx = graph
            .order
            .iter()
            .position(|p| p.ends_with("consumer.preql"))
            .unwrap();
        let main_idx = graph
            .order
            .iter()
            .position(|p| p.ends_with("main.preql"))
            .unwrap();

        // updater must come before base (persist before declare)
        assert!(
            updater_idx < base_idx,
            "updater should come before base: updater={}, base={}",
            updater_idx,
            base_idx
        );

        // base must come before consumer (consumer imports base which has the datasource)
        assert!(
            base_idx < consumer_idx,
            "base should come before consumer: base={}, consumer={}",
            base_idx,
            consumer_idx
        );

        // main comes last (imports everything)
        assert!(
            updater_idx < main_idx && base_idx < main_idx && consumer_idx < main_idx,
            "main should come after all others"
        );
    }

    #[test]
    fn test_multiple_datasources() {
        let temp = TempDir::new().unwrap();
        let root = temp.path();

        create_test_file(
            root,
            "models.preql",
            r#"
            datasource customers (
                id: key
            )
            address db.customers;
            
            datasource orders (
                id: key,
                customer_id
            )
            address db.orders;
        "#,
        );

        let models_path = root.join("models.preql");
        let mut resolver = ImportResolver::new();
        let graph = resolver.resolve(&models_path).unwrap();

        assert_eq!(graph.datasource_declarations.len(), 2);
        assert!(graph.datasource_declarations.contains_key("customers"));
        assert!(graph.datasource_declarations.contains_key("orders"));
    }
}