use crate::parser::{parse_imports, ImportStatement, ParseError};
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

/// A node in the dependency graph
#[derive(Debug, Clone, Serialize)]
pub struct FileNode {
    /// Absolute path to the file
    pub path: PathBuf,
    /// Relative path from the root
    pub relative_path: PathBuf,
    /// List of imports in this file
    pub imports: Vec<ImportInfo>,
    /// List of resolved dependencies (paths to other files)
    pub dependencies: Vec<PathBuf>,
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

/// Result of dependency resolution
#[derive(Debug, Clone, Serialize)]
pub struct DependencyGraph {
    /// Root file that was analyzed
    pub root: PathBuf,
    /// All files in dependency order (dependencies come before dependents)
    pub order: Vec<PathBuf>,
    /// Detailed information about each file
    pub files: HashMap<PathBuf, FileNode>,
    /// Any errors encountered (non-fatal)
    pub warnings: Vec<String>,
}

/// Resolver for PreQL import dependencies
pub struct ImportResolver {
    /// Cache of parsed files
    parsed_cache: HashMap<PathBuf, Vec<ImportStatement>>,
    /// Track files currently being processed (for cycle detection)
    processing: HashSet<PathBuf>,
    /// Track fully processed files
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
        let mut order: Vec<PathBuf> = Vec::new();

        // BFS to collect all files first
        let mut queue: VecDeque<PathBuf> = VecDeque::new();
        let mut seen: HashSet<PathBuf> = HashSet::new();

        queue.push_back(root_path.clone());
        seen.insert(root_path.clone());

        while let Some(current_path) = queue.pop_front() {
            let imports = self.parse_file(&current_path)?;
            let file_dir = current_path.parent().unwrap_or(Path::new("."));

            let mut import_infos: Vec<ImportInfo> = Vec::new();
            let mut dependencies: Vec<PathBuf> = Vec::new();

            for import in &imports {
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
                        dependencies.push(canonical.clone());

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

            let relative_path = pathdiff::diff_paths(&current_path, root_dir)
                .unwrap_or_else(|| current_path.clone());

            files.insert(
                current_path.clone(),
                FileNode {
                    path: current_path,
                    relative_path,
                    imports: import_infos,
                    dependencies,
                },
            );
        }

        // Topological sort
        order = self.topological_sort(&files)?;

        Ok(DependencyGraph {
            root: root_path,
            order,
            files,
            warnings: self.warnings.clone(),
        })
    }

    fn parse_file(&mut self, path: &Path) -> Result<Vec<ImportStatement>, ResolveError> {
        if let Some(cached) = self.parsed_cache.get(path) {
            return Ok(cached.clone());
        }

        let content = fs::read_to_string(path).map_err(|e| ResolveError::IoError {
            path: path.to_path_buf(),
            source: e,
        })?;

        let imports = parse_imports(&content).map_err(|e| ResolveError::ParseError {
            path: path.to_path_buf(),
            source: e,
        })?;

        self.parsed_cache.insert(path.to_path_buf(), imports.clone());
        Ok(imports)
    }

    fn topological_sort(
        &self,
        files: &HashMap<PathBuf, FileNode>,
    ) -> Result<Vec<PathBuf>, ResolveError> {
        let mut in_degree: HashMap<&PathBuf, usize> = HashMap::new();
        let mut dependents: HashMap<&PathBuf, Vec<&PathBuf>> = HashMap::new();

        // Initialize
        for path in files.keys() {
            in_degree.insert(path, 0);
            dependents.insert(path, Vec::new());
        }

        // Calculate in-degrees and build reverse adjacency
        for (path, node) in files {
            for dep in &node.dependencies {
                if files.contains_key(dep) {
                    *in_degree.get_mut(path).unwrap() += 1;
                    dependents.get_mut(dep).unwrap().push(path);
                }
            }
        }

        // Kahn's algorithm
        let mut queue: VecDeque<&PathBuf> = VecDeque::new();
        let mut result: Vec<PathBuf> = Vec::new();

        // Start with nodes that have no dependencies
        for (path, &degree) in &in_degree {
            if degree == 0 {
                queue.push_back(path);
            }
        }

        while let Some(current) = queue.pop_front() {
            result.push(current.clone());

            for dependent in dependents.get(current).unwrap_or(&Vec::new()) {
                let degree = in_degree.get_mut(dependent).unwrap();
                *degree -= 1;
                if *degree == 0 {
                    queue.push_back(dependent);
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
        assert!(b_idx < a_idx);
    }
}
