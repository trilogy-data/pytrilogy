use clap::{Parser, Subcommand, ValueEnum};
use preql_import_resolver::{parse_imports, ImportResolver};
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(name = "preql-import-resolver")]
#[command(author, version, about = "Parse PreQL files and resolve import dependencies")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    /// Output format
    #[arg(long, short, default_value = "json", global = true)]
    format: OutputFormat,
}

#[derive(Subcommand)]
enum Commands {
    /// Parse a file or directory and list imports with resolved dependencies
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
    /// Resolve all dependencies from a root file
    Resolve {
        /// Path to the root PreQL file
        file: PathBuf,
        /// Only output the dependency order (list of paths)
        #[arg(long)]
        order_only: bool,
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
    /// All resolved dependencies in order (transitive closure)
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
        /// All resolved dependencies in order (transitive closure)
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
        Commands::Resolve { file, order_only } => handle_resolve(file, *order_only, cli.format),
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
    let imports = parse_imports(&content)?;

    let (resolved_dependencies, warnings) = if direct_only {
        (None, Vec::new())
    } else {
        // Use the resolver to get full transitive dependencies
        let mut resolver = ImportResolver::new();
        match resolver.resolve(file) {
            Ok(graph) => {
                // Filter out the root file itself from dependencies
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
            Err(e) => {
                // If resolution fails, include it as a warning but still show direct imports
                (None, vec![format!("Dependency resolution failed: {}", e)])
            }
        }
    };

    let import_outputs: Vec<ImportOutput> = if direct_only {
        imports
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
        // Resolve each import path
        let file_dir = file.parent().unwrap_or(std::path::Path::new("."));
        imports
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

    let output = ParseOutput {
        file: file.clone(),
        imports: import_outputs,
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
            Ok(content) => match parse_imports(&content) {
                Ok(imports) => {
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
                        imports
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
                        imports
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

                    FileParseResult::Success {
                        imports: import_outputs,
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
    path.extension().map(|ext| ext == "preql").unwrap_or(false)
}

fn handle_resolve(
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