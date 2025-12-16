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
    TransitivePersistOrder {
        upstream_datasource: String,
        downstream_datasource: String,
    },
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
    // Files that persist to a datasource must run BEFORE files that declare that datasource.
    // This takes precedence over import edges - if the updater imports the declarer,
    // we need to remove that import edge and add the persist-before-declare edge instead.
    for (declarer_path, declarer_info) in &graph.files {
        for ds_name in &declarer_info.datasources {
            for (updater_path, updater_info) in &graph.files {
                if updater_path == declarer_path {
                    continue;
                }

                if updater_info.persists.contains(ds_name) {
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

    // Rule 3: Remove import edges that conflict with persist-before-declare
    // If file A imports file B, but A also persists to a datasource declared by B,
    // then the import edge (B -> A) conflicts with persist-before-declare (A -> B).
    // In this case, persist-before-declare takes precedence.
    let persist_edges: HashSet<(PathBuf, PathBuf)> = edges
        .iter()
        .filter(|e| matches!(e.reason, EdgeReason::PersistBeforeDeclare { .. }))
        .map(|e| (e.from.clone(), e.to.clone()))
        .collect();

    edges.retain(|edge| {
        if matches!(edge.reason, EdgeReason::Import) {
            // Check if there's a conflicting persist-before-declare edge in the opposite direction
            let reverse = (edge.to.clone(), edge.from.clone());
            !persist_edges.contains(&reverse)
        } else {
            true
        }
    });

    // Rule 4: Transitive persist ordering between updaters
    // If X1 persists to datasource A (declared in declarer_A), and
    // X2 persists to datasource B (declared in declarer_B), and
    // declarer_B imports declarer_A (directly or transitively),
    // then X1 must run before X2.
    //
    // This ensures that when B's datasource depends on A's data (through imports),
    // any updates to A complete before updates to B.

    // First, compute transitive imports for each file
    let transitive_imports = compute_transitive_imports(&graph.imports);

    // Build a map from datasource name to the file that declares it
    let mut datasource_to_declarer: HashMap<String, PathBuf> = HashMap::new();
    for (path, info) in &graph.files {
        for ds_name in &info.datasources {
            datasource_to_declarer.insert(ds_name.clone(), path.clone());
        }
    }

    // Build a map from datasource name to files that persist to it
    let mut datasource_to_updaters: HashMap<String, Vec<PathBuf>> = HashMap::new();
    for (path, info) in &graph.files {
        for persist_target in &info.persists {
            datasource_to_updaters
                .entry(persist_target.clone())
                .or_default()
                .push(path.clone());
        }
    }

    // For each pair of datasources where one's declarer imports the other's declarer,
    // add edges between their updaters
    for (ds_a, declarer_a) in &datasource_to_declarer {
        for (ds_b, declarer_b) in &datasource_to_declarer {
            if ds_a == ds_b || declarer_a == declarer_b {
                continue;
            }

            // Check if declarer_B transitively imports declarer_A
            if let Some(b_imports) = transitive_imports.get(declarer_b) {
                if b_imports.contains(declarer_a) {
                    // declarer_B imports declarer_A, so all updaters of A must run before updaters of B
                    if let (Some(updaters_a), Some(updaters_b)) = (
                        datasource_to_updaters.get(ds_a),
                        datasource_to_updaters.get(ds_b),
                    ) {
                        for updater_a in updaters_a {
                            for updater_b in updaters_b {
                                if updater_a != updater_b && known_files.contains(updater_a) && known_files.contains(updater_b) {
                                    edges.push(Edge {
                                        from: updater_a.clone(),
                                        to: updater_b.clone(),
                                        reason: EdgeReason::TransitivePersistOrder {
                                            upstream_datasource: ds_a.clone(),
                                            downstream_datasource: ds_b.clone(),
                                        },
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    edges
}

/// Compute transitive imports for each file
fn compute_transitive_imports(
    imports: &HashMap<PathBuf, Vec<PathBuf>>,
) -> HashMap<PathBuf, HashSet<PathBuf>> {
    let mut result: HashMap<PathBuf, HashSet<PathBuf>> = HashMap::new();

    for file in imports.keys() {
        let mut visited: HashSet<PathBuf> = HashSet::new();
        let mut stack: Vec<PathBuf> = imports.get(file).cloned().unwrap_or_default();

        while let Some(current) = stack.pop() {
            if visited.contains(&current) {
                continue;
            }
            visited.insert(current.clone());

            if let Some(current_imports) = imports.get(&current) {
                for imp in current_imports {
                    if !visited.contains(imp) {
                        stack.push(imp.clone());
                    }
                }
            }
        }

        result.insert(file.clone(), visited);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn create_test_file(dir: &std::path::Path, name: &str, content: &str) -> PathBuf {
        let path = dir.join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&path, content).unwrap();
        path
    }

    #[test]
    fn test_transitive_persist_order() {
        // Setup:
        // - order_product_items.preql: declares datasource "order_product_items"
        // - sales_reporting.preql: declares datasource "sales_reporting" AND imports order_product_items
        // - incremental_opi.preql: persists to "order_product_items"
        // - incremental_sales.preql: persists to "sales_reporting"
        //
        // Expected: incremental_opi -> incremental_sales (transitive persist order)
        // Because sales_reporting imports order_product_items, so updates to order_product_items
        // must complete before updates to sales_reporting.

        let temp = TempDir::new().unwrap();
        let root = temp.path();

        create_test_file(
            root,
            "order_product_items.preql",
            r#"
            datasource order_product_items (
                id: key
            )
            address db.opi;
            "#,
        );

        create_test_file(
            root,
            "sales_reporting.preql",
            r#"
            import order_product_items;

            datasource sales_reporting (
                id: key
            )
            address db.sales;
            "#,
        );

        create_test_file(
            root,
            "incremental_opi.preql",
            r#"
            import order_product_items;
            persist order_product_items where date = @load_date;
            "#,
        );

        create_test_file(
            root,
            "incremental_sales.preql",
            r#"
            import sales_reporting;
            persist sales_reporting where date = @load_date;
            "#,
        );

        // Collect all files
        let files: Vec<PathBuf> = fs::read_dir(root)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |ext| ext == "preql"))
            .collect();

        let graph = process_directory_with_imports(files).unwrap();
        let edges = build_edges(&graph);

        // Find the transitive persist order edge
        let transitive_edge = edges.iter().find(|e| {
            matches!(e.reason, EdgeReason::TransitivePersistOrder { .. })
        });

        assert!(
            transitive_edge.is_some(),
            "Expected a transitive persist order edge. Edges: {:?}",
            edges.iter().map(|e| (e.from.file_name(), e.to.file_name(), &e.reason)).collect::<Vec<_>>()
        );

        let edge = transitive_edge.unwrap();
        assert!(
            edge.from.ends_with("incremental_opi.preql"),
            "Expected from to be incremental_opi.preql, got {:?}",
            edge.from
        );
        assert!(
            edge.to.ends_with("incremental_sales.preql"),
            "Expected to to be incremental_sales.preql, got {:?}",
            edge.to
        );
    }
}
