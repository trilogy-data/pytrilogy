use clap::{Parser, Subcommand, ValueEnum};
use preql_import_resolver::{parse_file, ImportResolver, ParsedFile};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(name = "preql-import-resolver")]
#[command(author, version, about = "Parse PreQL files and resolve import/datasource dependencies")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    /// Output format
    #[arg(long, short, default_value = "json", global = true)]
    format: OutputFormat,
}

#[derive(Subcommand)]
enum Commands {
    /// Parse a file or directory and list imports, datasources, and persist statements
    Parse {
        /// Path to a PreQL file or directory containing PreQL files
        path: PathBuf,
        /// Recursively search directories
        #[arg(long, short)]
        recursive: bool,
        /// Only show direct imports (don't resolve transitive dependencies)
        #[arg(long)]
        direct_only: bool,
    },
    /// Resolve all dependencies from a root file or directory with datasource-aware ordering
    Resolve {
        /// Path to a PreQL file or directory containing PreQL files
        path: PathBuf,
        /// Only output the dependency order (list of paths)
        #[arg(long)]
        order_only: bool,
        /// Recursively search directories
        #[arg(long, short)]
        recursive: bool,
    },
    /// Analyze datasources in a file or directory
    Datasources {
        /// Path to a PreQL file or directory
        path: PathBuf,
        /// Recursively search directories
        #[arg(long, short)]
        recursive: bool,
    },
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum)]
enum OutputFormat {
    Json,
    Pretty,
}

#[derive(Serialize)]
struct ParseOutput {
    file: PathBuf,
    imports: Vec<ImportOutput>,
    datasources: Vec<DatasourceOutput>,
    persists: Vec<PersistOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resolved_dependencies: Option<Vec<PathBuf>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<String>,
}

#[derive(Serialize)]
struct DirectoryParseOutput {
    files: BTreeMap<PathBuf, FileParseResult>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum FileParseResult {
    Success {
        imports: Vec<ImportOutput>,
        datasources: Vec<DatasourceOutput>,
        persists: Vec<PersistOutput>,
        #[serde(skip_serializing_if = "Option::is_none")]
        resolved_dependencies: Option<Vec<PathBuf>>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        warnings: Vec<String>,
    },
    Error {
        error: String,
    },
}

#[derive(Serialize)]
struct ImportOutput {
    raw_path: String,
    alias: Option<String>,
    is_stdlib: bool,
    parent_dirs: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    resolved_path: Option<PathBuf>,
}

#[derive(Serialize)]
struct DatasourceOutput {
    name: String,
}

#[derive(Serialize)]
struct PersistOutput {
    mode: String,
    target_datasource: String,
}

#[derive(Serialize)]
struct DatasourceAnalysis {
    /// All datasources found with their declaring files
    declarations: BTreeMap<String, PathBuf>,
    /// Datasources and the files that update them
    updaters: BTreeMap<String, Vec<PathBuf>>,
    /// Files that depend on each datasource (through imports)
    dependents: BTreeMap<String, Vec<PathBuf>>,
}

#[derive(Serialize)]
struct ErrorOutput {
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    file: Option<PathBuf>,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let result = match &cli.command {
        Commands::Parse {
            path,
            recursive,
            direct_only,
        } => handle_parse(path, *recursive, *direct_only, cli.format),
        Commands::Resolve { path, order_only, recursive } => handle_resolve(path, *order_only, *recursive, cli.format),
        Commands::Datasources { path, recursive } => {
            handle_datasources(path, *recursive, cli.format)
        }
    };

    match result {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            let error_output = ErrorOutput {
                error: e.to_string(),
                file: None,
            };
            match cli.format {
                OutputFormat::Json => {
                    eprintln!("{}", serde_json::to_string(&error_output).unwrap());
                }
                OutputFormat::Pretty => {
                    eprintln!("{}", serde_json::to_string_pretty(&error_output).unwrap());
                }
            }
            ExitCode::FAILURE
        }
    }
}

fn handle_parse(
    path: &PathBuf,
    recursive: bool,
    direct_only: bool,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    if path.is_file() {
        handle_parse_file(path, direct_only, format)
    } else if path.is_dir() {
        handle_parse_directory(path, recursive, direct_only, format)
    } else {
        Err(format!("Path does not exist or is not accessible: {}", path.display()).into())
    }
}

fn handle_parse_file(
    file: &PathBuf,
    direct_only: bool,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let content = std::fs::read_to_string(file)?;
    let parsed = parse_file(&content)?;

    let (resolved_dependencies, warnings) = if direct_only {
        (None, Vec::new())
    } else {
        let mut resolver = ImportResolver::new();
        match resolver.resolve(file) {
            Ok(graph) => {
                let deps: Vec<PathBuf> = graph
                    .order
                    .into_iter()
                    .filter(|p| p != &graph.root)
                    .collect();
                (
                    if deps.is_empty() { None } else { Some(deps) },
                    graph.warnings,
                )
            }
            Err(e) => (None, vec![format!("Dependency resolution failed: {}", e)]),
        }
    };

    let import_outputs: Vec<ImportOutput> = if direct_only {
        parsed
            .imports
            .into_iter()
            .map(|i| ImportOutput {
                raw_path: i.raw_path,
                alias: i.alias,
                is_stdlib: i.is_stdlib,
                parent_dirs: i.parent_dirs,
                resolved_path: None,
            })
            .collect()
    } else {
        let file_dir = file.parent().unwrap_or(std::path::Path::new("."));
        parsed
            .imports
            .into_iter()
            .map(|i| {
                let resolved_path = if i.is_stdlib {
                    None
                } else {
                    i.resolve(file_dir).and_then(|p| {
                        if p.exists() {
                            std::fs::canonicalize(&p).ok()
                        } else {
                            None
                        }
                    })
                };
                ImportOutput {
                    raw_path: i.raw_path,
                    alias: i.alias,
                    is_stdlib: i.is_stdlib,
                    parent_dirs: i.parent_dirs,
                    resolved_path,
                }
            })
            .collect()
    };

    let datasource_outputs: Vec<DatasourceOutput> = parsed
        .datasources
        .into_iter()
        .map(|d| DatasourceOutput { name: d.name })
        .collect();

    let persist_outputs: Vec<PersistOutput> = parsed
        .persists
        .into_iter()
        .map(|p| PersistOutput {
            mode: p.mode.to_string(),
            target_datasource: p.target_datasource,
        })
        .collect();

    let output = ParseOutput {
        file: file.clone(),
        imports: import_outputs,
        datasources: datasource_outputs,
        persists: persist_outputs,
        resolved_dependencies,
        warnings,
    };

    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string(&output)?),
        OutputFormat::Pretty => println!("{}", serde_json::to_string_pretty(&output)?),
    }

    Ok(())
}

fn handle_parse_directory(
    dir: &PathBuf,
    recursive: bool,
    direct_only: bool,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let files = collect_preql_files(dir, recursive)?;
    let mut results: BTreeMap<PathBuf, FileParseResult> = BTreeMap::new();

    for file in files {
        let result = match std::fs::read_to_string(&file) {
            Ok(content) => match parse_file(&content) {
                Ok(parsed) => {
                    let (resolved_dependencies, warnings) = if direct_only {
                        (None, Vec::new())
                    } else {
                        let mut resolver = ImportResolver::new();
                        match resolver.resolve(&file) {
                            Ok(graph) => {
                                let deps: Vec<PathBuf> = graph
                                    .order
                                    .into_iter()
                                    .filter(|p| p != &graph.root)
                                    .collect();
                                (
                                    if deps.is_empty() { None } else { Some(deps) },
                                    graph.warnings,
                                )
                            }
                            Err(e) => (None, vec![format!("Dependency resolution failed: {}", e)]),
                        }
                    };

                    let import_outputs: Vec<ImportOutput> = if direct_only {
                        parsed
                            .imports
                            .into_iter()
                            .map(|i| ImportOutput {
                                raw_path: i.raw_path,
                                alias: i.alias,
                                is_stdlib: i.is_stdlib,
                                parent_dirs: i.parent_dirs,
                                resolved_path: None,
                            })
                            .collect()
                    } else {
                        let file_dir = file.parent().unwrap_or(std::path::Path::new("."));
                        parsed
                            .imports
                            .into_iter()
                            .map(|i| {
                                let resolved_path = if i.is_stdlib {
                                    None
                                } else {
                                    i.resolve(file_dir).and_then(|p| {
                                        if p.exists() {
                                            std::fs::canonicalize(&p).ok()
                                        } else {
                                            None
                                        }
                                    })
                                };
                                ImportOutput {
                                    raw_path: i.raw_path,
                                    alias: i.alias,
                                    is_stdlib: i.is_stdlib,
                                    parent_dirs: i.parent_dirs,
                                    resolved_path,
                                }
                            })
                            .collect()
                    };

                    let datasource_outputs: Vec<DatasourceOutput> = parsed
                        .datasources
                        .into_iter()
                        .map(|d| DatasourceOutput { name: d.name })
                        .collect();

                    let persist_outputs: Vec<PersistOutput> = parsed
                        .persists
                        .into_iter()
                        .map(|p| PersistOutput {
                            mode: p.mode.to_string(),
                            target_datasource: p.target_datasource,
                        })
                        .collect();

                    FileParseResult::Success {
                        imports: import_outputs,
                        datasources: datasource_outputs,
                        persists: persist_outputs,
                        resolved_dependencies,
                        warnings,
                    }
                }
                Err(e) => FileParseResult::Error {
                    error: e.to_string(),
                },
            },
            Err(e) => FileParseResult::Error {
                error: e.to_string(),
            },
        };

        let relative_path = file
            .strip_prefix(dir)
            .map(|p| p.to_path_buf())
            .unwrap_or(file);
        results.insert(relative_path, result);
    }

    let output = DirectoryParseOutput { files: results };

    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string(&output)?),
        OutputFormat::Pretty => println!("{}", serde_json::to_string_pretty(&output)?),
    }

    Ok(())
}

fn handle_datasources(
    path: &PathBuf,
    recursive: bool,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let files = if path.is_file() {
        vec![path.clone()]
    } else if path.is_dir() {
        collect_preql_files(path, recursive)?
    } else {
        return Err(format!("Path does not exist: {}", path.display()).into());
    };

    let mut declarations: BTreeMap<String, PathBuf> = BTreeMap::new();
    let mut updaters: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();
    let mut all_files_parsed: Vec<(PathBuf, ParsedFile)> = Vec::new();

    // First pass: collect all declarations and updaters
    for file in &files {
        let content = std::fs::read_to_string(file)?;
        if let Ok(parsed) = parse_file(&content) {
            for ds in &parsed.datasources {
                declarations.insert(ds.name.clone(), file.clone());
            }
            for persist in &parsed.persists {
                updaters
                    .entry(persist.target_datasource.clone())
                    .or_default()
                    .push(file.clone());
            }
            all_files_parsed.push((file.clone(), parsed));
        }
    }

    // Second pass: find dependents (files that import files containing datasources)
    let mut dependents: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();

    for (file, parsed) in &all_files_parsed {
        let file_dir = file.parent().unwrap_or(std::path::Path::new("."));

        for import in &parsed.imports {
            if import.is_stdlib {
                continue;
            }

            if let Some(resolved) = import.resolve(file_dir) {
                if resolved.exists() {
                    if let Ok(canonical) = std::fs::canonicalize(&resolved) {
                        // Check what datasources are declared in the imported file
                        for (ds_name, declaring_file) in &declarations {
                            if let Ok(decl_canonical) = std::fs::canonicalize(declaring_file) {
                                if canonical == decl_canonical {
                                    dependents
                                        .entry(ds_name.clone())
                                        .or_default()
                                        .push(file.clone());
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let analysis = DatasourceAnalysis {
        declarations,
        updaters,
        dependents,
    };

    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string(&analysis)?),
        OutputFormat::Pretty => println!("{}", serde_json::to_string_pretty(&analysis)?),
    }

    Ok(())
}

fn collect_preql_files(dir: &PathBuf, recursive: bool) -> Result<Vec<PathBuf>, std::io::Error> {
    let mut files = Vec::new();

    if recursive {
        collect_preql_files_recursive(dir, &mut files)?;
    } else {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() && is_preql_file(&path) {
                files.push(path);
            }
        }
    }

    files.sort();
    Ok(files)
}

fn collect_preql_files_recursive(
    dir: &PathBuf,
    files: &mut Vec<PathBuf>,
) -> Result<(), std::io::Error> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            collect_preql_files_recursive(&path, files)?;
        } else if path.is_file() && is_preql_file(&path) {
            files.push(path);
        }
    }

    Ok(())
}

fn is_preql_file(path: &PathBuf) -> bool {
    path.extension().is_some_and(|ext| ext == "preql")
}

fn handle_resolve(
    path: &PathBuf,
    order_only: bool,
    recursive: bool,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    if path.is_file() {
        handle_resolve_file(path, order_only, format)
    } else if path.is_dir() {
        handle_resolve_directory(path, order_only, recursive, format)
    } else {
        Err(format!("Path does not exist or is not accessible: {}", path.display()).into())
    }
}

fn handle_resolve_file(
    file: &PathBuf,
    order_only: bool,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut resolver = ImportResolver::new();
    let graph = resolver.resolve(file)?;

    if order_only {
        let paths: Vec<&PathBuf> = graph.order.iter().collect();
        match format {
            OutputFormat::Json => println!("{}", serde_json::to_string(&paths)?),
            OutputFormat::Pretty => println!("{}", serde_json::to_string_pretty(&paths)?),
        }
    } else {
        match format {
            OutputFormat::Json => println!("{}", serde_json::to_string(&graph)?),
            OutputFormat::Pretty => println!("{}", serde_json::to_string_pretty(&graph)?),
        }
    }

    Ok(())
}

fn handle_resolve_directory(
    dir: &PathBuf,
    order_only: bool,
    recursive: bool,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::collections::HashSet;

    let files = collect_preql_files(dir, recursive)?;
    
    if files.is_empty() {
        return Err(format!("No .preql files found in {}", dir.display()).into());
    }

    #[derive(Serialize, Clone)]
    struct FileInfo {
        path: PathBuf,
        imports: Vec<String>,
        datasources: Vec<String>,
        persists: Vec<PersistInfo>,
    }

    #[derive(Serialize, Clone)]
    struct PersistInfo {
        mode: String,
        target: String,
    }

    #[derive(Serialize, Clone)]
    struct Edge {
        from: PathBuf,
        to: PathBuf,
        reason: EdgeReason,
    }

    #[derive(Serialize, Clone)]
    #[serde(tag = "type")]
    enum EdgeReason {
        #[serde(rename = "declare_before_use")]
        DeclareBeforeUse { datasource: String },
        #[serde(rename = "persist_before_declare")]
        PersistBeforeDeclare { datasource: String },
    }

    #[derive(Serialize)]
    struct GraphOutput {
        directory: PathBuf,
        files: Vec<PathBuf>,
        edges: Vec<Edge>,
        warnings: Vec<String>,
    }

    let mut files_info: HashMap<PathBuf, FileInfo> = HashMap::new();
    let mut all_imports: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
    let mut warnings: Vec<String> = Vec::new();
    let mut edges: Vec<Edge> = Vec::new();

    // First pass: parse all files and collect info
    for file in &files {
        let canonical = match std::fs::canonicalize(file) {
            Ok(c) => c,
            Err(e) => {
                warnings.push(format!("Failed to canonicalize {}: {}", file.display(), e));
                continue;
            }
        };

        let content = std::fs::read_to_string(file)?;
        match parse_file(&content) {
            Ok(parsed) => {
                let mut resolved_imports: Vec<PathBuf> = Vec::new();

                // Resolve imports
                let file_dir = file.parent().unwrap_or(std::path::Path::new("."));
                for import in &parsed.imports {
                    if import.is_stdlib {
                        continue;
                    }
                    if let Some(resolved) = import.resolve(file_dir) {
                        if resolved.exists() {
                            if let Ok(resolved_canonical) = std::fs::canonicalize(&resolved) {
                                resolved_imports.push(resolved_canonical);
                            }
                        } else {
                            warnings.push(format!(
                                "Import '{}' in {} resolved to non-existent file: {}",
                                import.raw_path,
                                file.display(),
                                resolved.display()
                            ));
                        }
                    }
                }

                // Collect datasource names
                let datasource_names: Vec<String> = parsed
                    .datasources
                    .iter()
                    .map(|d| d.name.clone())
                    .collect();

                // Collect persist info
                let persist_infos: Vec<PersistInfo> = parsed
                    .persists
                    .iter()
                    .map(|p| PersistInfo {
                        mode: p.mode.to_string(),
                        target: p.target_datasource.clone(),
                    })
                    .collect();

                let import_names: Vec<String> = parsed
                    .imports
                    .iter()
                    .map(|i| i.raw_path.clone())
                    .collect();

                all_imports.insert(canonical.clone(), resolved_imports);
                files_info.insert(
                    canonical.clone(),
                    FileInfo {
                        path: canonical,
                        imports: import_names,
                        datasources: datasource_names,
                        persists: persist_infos,
                    },
                );
            }
            Err(e) => {
                warnings.push(format!("Failed to parse {}: {}", file.display(), e));
            }
        }
    }

    let known_files: HashSet<PathBuf> = files_info.keys().cloned().collect();

    // Build edges based on direct imports only
    for (file, imports) in &all_imports {
        // Get datasources that this file persists to
        let persisted_datasources: HashSet<String> = files_info
            .get(file)
            .map(|info| info.persists.iter().map(|p| p.target.clone()).collect())
            .unwrap_or_default();

        for resolved_path in imports {
            if !known_files.contains(resolved_path) {
                continue;
            }

            // Get datasources declared in the imported file
            let imported_datasources: Vec<String> = files_info
                .get(resolved_path)
                .map(|info| info.datasources.clone())
                .unwrap_or_default();

            for ds_name in imported_datasources {
                if persisted_datasources.contains(&ds_name) {
                    // Case 2: file imports then updates datasource
                    // -> file must run before imported file (to update before re-declare)
                    edges.push(Edge {
                        from: file.clone(),
                        to: resolved_path.clone(),
                        reason: EdgeReason::PersistBeforeDeclare {
                            datasource: ds_name,
                        },
                    });
                } else {
                    // Case 1: file imports datasource (read-only)
                    // -> imported file must run before this file
                    edges.push(Edge {
                        from: resolved_path.clone(),
                        to: file.clone(),
                        reason: EdgeReason::DeclareBeforeUse { datasource: ds_name },
                    });
                }
            }
        }
    }

    // Deduplicate edges (same from/to/reason type)
    edges.sort_by(|a, b| (&a.from, &a.to).cmp(&(&b.from, &b.to)));
    edges.dedup_by(|a, b| {
        a.from == b.from
            && a.to == b.to
            && std::mem::discriminant(&a.reason) == std::mem::discriminant(&b.reason)
    });

    if order_only {
        let file_list: Vec<&PathBuf> = files_info.keys().collect();
        match format {
            OutputFormat::Json => println!("{}", serde_json::to_string(&file_list)?),
            OutputFormat::Pretty => println!("{}", serde_json::to_string_pretty(&file_list)?),
        }
    } else {
        let output = GraphOutput {
            directory: dir.clone(),
            files: files_info.keys().cloned().collect(),
            edges,
            warnings,
        };

        match format {
            OutputFormat::Json => println!("{}", serde_json::to_string(&output)?),
            OutputFormat::Pretty => println!("{}", serde_json::to_string_pretty(&output)?),
        }
    }

    Ok(())
}