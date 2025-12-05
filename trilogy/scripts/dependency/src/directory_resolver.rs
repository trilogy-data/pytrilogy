use crate::parser::parse_file;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: PathBuf,
    pub datasources: Vec<String>,
    pub persists: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DirectoryGraph {
    pub files: HashMap<PathBuf, FileInfo>,
    pub imports: HashMap<PathBuf, Vec<PathBuf>>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Edge {
    pub from: PathBuf,
    pub to: PathBuf,
    pub reason: EdgeReason,
}

#[derive(Debug, Clone)]
pub enum EdgeReason {
    Import,
    PersistBeforeDeclare { datasource: String },
}

/// Process files in a directory, discovering transitive imports
pub fn process_directory_with_imports(
    initial_files: Vec<PathBuf>,
) -> Result<DirectoryGraph, String> {
    let mut all_imports: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
    let mut files_info: HashMap<PathBuf, FileInfo> = HashMap::new();
    let mut files_to_process = initial_files;
    let mut processed_files: HashSet<PathBuf> = HashSet::new();
    let mut warnings = Vec::new();

    while let Some(file) = files_to_process.pop() {
        let canonical = match fs::canonicalize(&file) {
            Ok(c) => c,
            Err(e) => {
                warnings.push(format!("Failed to canonicalize {}: {}", file.display(), e));
                continue;
            }
        };

        if processed_files.contains(&canonical) {
            continue;
        }
        processed_files.insert(canonical.clone());

        let content = match fs::read_to_string(&file) {
            Ok(c) => c,
            Err(e) => {
                warnings.push(format!("Failed to read {}: {}", file.display(), e));
                continue;
            }
        };

        let parsed = match parse_file(&content) {
            Ok(p) => p,
            Err(e) => {
                warnings.push(format!("Failed to parse {}: {}", file.display(), e));
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
                        resolved_imports.push(resolved_canonical.clone());
                        if !processed_files.contains(&resolved_canonical) {
                            files_to_process.push(resolved_canonical);
                        }
                    }
                }
            }
        }

        let datasources: Vec<String> = parsed.datasources.iter().map(|d| d.name.clone()).collect();
        let persists: Vec<String> = parsed.persists.iter().map(|p| p.target_datasource.clone()).collect();

        all_imports.insert(canonical.clone(), resolved_imports);
        files_info.insert(
            canonical.clone(),
            FileInfo {
                path: canonical,
                datasources,
                persists,
            },
        );
    }

    Ok(DirectoryGraph {
        files: files_info,
        imports: all_imports,
        warnings,
    })
}

/// Build edges from a directory graph
pub fn build_edges(graph: &DirectoryGraph) -> Vec<Edge> {
    let mut edges = Vec::new();
    let known_files: HashSet<PathBuf> = graph.files.keys().cloned().collect();

    // Rule 1: Import dependencies (imported files run before importing files)
    for (file, imports) in &graph.imports {
        for resolved_path in imports {
            if !known_files.contains(resolved_path) {
                continue;
            }
            edges.push(Edge {
                from: resolved_path.clone(),
                to: file.clone(),
                reason: EdgeReason::Import,
            });
        }
    }

    // Rule 2: Persist-before-declare
    for (declarer_path, declarer_info) in &graph.files {
        for ds_name in &declarer_info.datasources {
            for (updater_path, updater_info) in &graph.files {
                if updater_path == declarer_path {
                    continue;
                }

                if updater_info.persists.contains(ds_name) {
                    let updater_imports_declarer = graph
                        .imports
                        .get(updater_path)
                        .map(|imports| imports.contains(declarer_path))
                        .unwrap_or(false);

                    if !updater_imports_declarer {
                        edges.push(Edge {
                            from: updater_path.clone(),
                            to: declarer_path.clone(),
                            reason: EdgeReason::PersistBeforeDeclare {
                                datasource: ds_name.clone(),
                            },
                        });
                    }
                }
            }
        }
    }

    edges
}
