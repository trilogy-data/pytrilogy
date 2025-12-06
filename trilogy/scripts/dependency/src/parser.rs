use pest::Parser;
use pest_derive::Parser;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Parser)]
#[grammar = "preql.pest"]
pub struct PreqlParser;

/// Represents a parsed import statement
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ImportStatement {
    /// The raw path as written in the import (e.g., "models.customer")
    pub raw_path: String,
    /// Number of parent directory traversals (from leading dots)
    pub parent_dirs: usize,
    /// Optional alias for the import
    pub alias: Option<String>,
    /// Whether this is a stdlib import
    pub is_stdlib: bool,
}

impl ImportStatement {
    /// Resolve this import to an absolute file path
    pub fn resolve(&self, working_dir: &Path) -> Option<PathBuf> {
        if self.is_stdlib {
            return None; // Skip stdlib imports
        }

        let mut base = working_dir.to_path_buf();

        // Navigate up parent directories
        for _ in 0..self.parent_dirs {
            base = base.parent()?.to_path_buf();
        }

        // Convert dot-separated path to file path
        let parts: Vec<&str> = self.raw_path.split('.').collect();
        for part in &parts {
            base.push(part);
        }

        // Add .preql extension
        base.set_extension("preql");

        Some(base)
    }

    /// Get the effective alias (uses last path component if no explicit alias)
    pub fn effective_alias(&self) -> &str {
        self.alias
            .as_deref()
            .unwrap_or_else(|| self.raw_path.split('.').last().unwrap_or(&self.raw_path))
    }
}

/// Represents a datasource declaration
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatasourceDeclaration {
    /// The name/identifier of the datasource
    pub name: String,
}

/// Represents a persist statement that updates a datasource
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PersistStatement {
    /// The mode of persistence (append, overwrite, persist)
    pub mode: PersistMode,
    /// The target datasource being updated
    pub target_datasource: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PersistMode {
    Append,
    Overwrite,
    Persist,
}

impl std::fmt::Display for PersistMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PersistMode::Append => write!(f, "append"),
            PersistMode::Overwrite => write!(f, "overwrite"),
            PersistMode::Persist => write!(f, "persist"),
        }
    }
}

/// All parsed elements from a PreQL file relevant to dependency resolution
#[derive(Debug, Clone, Default)]
pub struct ParsedFile {
    pub imports: Vec<ImportStatement>,
    pub datasources: Vec<DatasourceDeclaration>,
    pub persists: Vec<PersistStatement>,
}

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Failed to parse file: {0}")]
    PestError(#[from] pest::error::Error<Rule>),

    #[error("Invalid import statement structure")]
    InvalidImportStructure,

    #[error("Invalid datasource statement structure")]
    InvalidDatasourceStructure,

    #[error("Invalid persist statement structure")]
    InvalidPersistStructure,
}

/// Parse a PreQL file and extract all dependency-relevant statements
pub fn parse_file(content: &str) -> Result<ParsedFile, ParseError> {
    let pairs = PreqlParser::parse(Rule::file, content)?;
    let mut result = ParsedFile::default();

    for pair in pairs {
        if pair.as_rule() == Rule::file {
            for inner in pair.into_inner() {
                if inner.as_rule() == Rule::statement {
                    for stmt in inner.into_inner() {
                        match stmt.as_rule() {
                            Rule::import_statement => {
                                if let Some(import) = parse_import_statement(stmt)? {
                                    result.imports.push(import);
                                }
                            }
                            Rule::datasource_statement => {
                                if let Some(ds) = parse_datasource_statement(stmt)? {
                                    result.datasources.push(ds);
                                }
                            }
                            Rule::persist_statement => {
                                if let Some(persist) = parse_persist_statement(stmt)? {
                                    result.persists.push(persist);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }

    Ok(result)
}

/// Legacy function for backward compatibility
pub fn parse_imports(content: &str) -> Result<Vec<ImportStatement>, ParseError> {
    Ok(parse_file(content)?.imports)
}

fn parse_import_statement(
    pair: pest::iterators::Pair<Rule>,
) -> Result<Option<ImportStatement>, ParseError> {
    let mut parent_dirs: usize = 0;
    let mut raw_path = String::new();
    let mut alias = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::relative_dots => {
                let dots = inner.as_str();
                // First dot is part of the syntax, each additional dot goes up one more dir
                parent_dirs = dots.len().saturating_sub(1);
            }
            Rule::import_path => {
                raw_path = inner.as_str().to_string();
            }
            Rule::import_alias => {
                for alias_inner in inner.into_inner() {
                    if alias_inner.as_rule() == Rule::identifier {
                        alias = Some(alias_inner.as_str().to_string());
                    }
                }
            }
            _ => {}
        }
    }

    if raw_path.is_empty() {
        return Err(ParseError::InvalidImportStructure);
    }

    // Check if it's a stdlib import
    let is_stdlib = raw_path.starts_with("std.");

    Ok(Some(ImportStatement {
        raw_path,
        parent_dirs,
        alias,
        is_stdlib,
    }))
}

fn parse_datasource_statement(
    pair: pest::iterators::Pair<Rule>,
) -> Result<Option<DatasourceDeclaration>, ParseError> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::identifier {
            let name = inner.as_str().to_string();
            return Ok(Some(DatasourceDeclaration { name }));
        }
    }
    Err(ParseError::InvalidDatasourceStructure)
}

fn parse_persist_statement(
    pair: pest::iterators::Pair<Rule>,
) -> Result<Option<PersistStatement>, ParseError> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::auto_persist => {
                return parse_auto_persist(inner);
            }
            Rule::full_persist => {
                return parse_full_persist(inner);
            }
            _ => {}
        }
    }
    Err(ParseError::InvalidPersistStructure)
}

fn parse_auto_persist(
    pair: pest::iterators::Pair<Rule>,
) -> Result<Option<PersistStatement>, ParseError> {
    let mut mode = None;
    let mut target = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::persist_mode => {
                mode = Some(parse_persist_mode(inner.as_str()));
            }
            Rule::identifier => {
                if target.is_none() {
                    target = Some(inner.as_str().to_string());
                }
            }
            _ => {}
        }
    }

    match (mode, target) {
        (Some(mode), Some(target_datasource)) => Ok(Some(PersistStatement {
            mode,
            target_datasource,
        })),
        _ => Err(ParseError::InvalidPersistStructure),
    }
}

fn parse_full_persist(
    pair: pest::iterators::Pair<Rule>,
) -> Result<Option<PersistStatement>, ParseError> {
    let mut mode = None;
    let mut target = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::persist_mode => {
                mode = Some(parse_persist_mode(inner.as_str()));
            }
            Rule::target_identifier => {
                // Get the identifier inside target_identifier
                for id in inner.into_inner() {
                    if id.as_rule() == Rule::identifier {
                        target = Some(id.as_str().to_string());
                    }
                }
            }
            _ => {}
        }
    }

    match (mode, target) {
        (Some(mode), Some(target_datasource)) => Ok(Some(PersistStatement {
            mode,
            target_datasource,
        })),
        _ => Err(ParseError::InvalidPersistStructure),
    }
}

fn parse_persist_mode(s: &str) -> PersistMode {
    match s.to_lowercase().as_str() {
        "append" => PersistMode::Append,
        "overwrite" => PersistMode::Overwrite,
        _ => PersistMode::Persist,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_import() {
        let content = "import models.customer;";
        let parsed = parse_file(content).unwrap();
        assert_eq!(parsed.imports.len(), 1);
        assert_eq!(parsed.imports[0].raw_path, "models.customer");
        assert_eq!(parsed.imports[0].parent_dirs, 0);
        assert!(parsed.imports[0].alias.is_none());
    }

    #[test]
    fn test_import_with_alias() {
        let content = "import models.customer as cust;";
        let parsed = parse_file(content).unwrap();
        assert_eq!(parsed.imports.len(), 1);
        assert_eq!(parsed.imports[0].raw_path, "models.customer");
        assert_eq!(parsed.imports[0].alias, Some("cust".to_string()));
    }

    #[test]
    fn test_relative_import() {
        let content = "import ..models.customer;";
        let parsed = parse_file(content).unwrap();
        assert_eq!(parsed.imports.len(), 1);
        assert_eq!(parsed.imports[0].raw_path, "models.customer");
        assert_eq!(parsed.imports[0].parent_dirs, 1);
    }

    #[test]
    fn test_stdlib_import() {
        let content = "import std.aggregates;";
        let parsed = parse_file(content).unwrap();
        assert_eq!(parsed.imports.len(), 1);
        assert!(parsed.imports[0].is_stdlib);
    }

    #[test]
    fn test_datasource_simple() {
        let content = r#"
            datasource orders (
                order_id: key,
                customer_id,
                amount: metric
            )
            address my_database.orders;
        "#;
        let parsed = parse_file(content).unwrap();
        assert_eq!(parsed.datasources.len(), 1);
        assert_eq!(parsed.datasources[0].name, "orders");
    }

    #[test]
    fn test_datasource_with_grain() {
        let content = r#"
            datasource customers (
                id: customer_id,
                name: customer_name
            )
            grain (customer_id)
            address `my_db.customers`;
        "#;
        let parsed = parse_file(content).unwrap();
        assert_eq!(parsed.datasources.len(), 1);
        assert_eq!(parsed.datasources[0].name, "customers");
    }

    #[test]
    fn test_auto_persist() {
        let content = "persist orders;";
        let parsed = parse_file(content).unwrap();
        assert_eq!(parsed.persists.len(), 1);
        assert_eq!(parsed.persists[0].target_datasource, "orders");
        assert_eq!(parsed.persists[0].mode, PersistMode::Persist);
    }

    #[test]
    fn test_auto_persist_with_where() {
        let content = "append orders where status = 'active';";
        let parsed = parse_file(content).unwrap();
        assert_eq!(parsed.persists.len(), 1);
        assert_eq!(parsed.persists[0].target_datasource, "orders");
        assert_eq!(parsed.persists[0].mode, PersistMode::Append);
    }

    #[test]
    fn test_full_persist() {
        let content = "overwrite into target_orders from select order_id, amount;";
        let parsed = parse_file(content).unwrap();
        assert_eq!(parsed.persists.len(), 1);
        assert_eq!(parsed.persists[0].target_datasource, "target_orders");
        assert_eq!(parsed.persists[0].mode, PersistMode::Overwrite);
    }

    #[test]
    fn test_full_persist_with_source() {
        let content = "persist staging into final_orders by customer_id from select *;";
        let parsed = parse_file(content).unwrap();
        assert_eq!(parsed.persists.len(), 1);
        assert_eq!(parsed.persists[0].target_datasource, "final_orders");
    }

    #[test]
    fn test_mixed_file() {
        let content = r#"
            import models.customer;
            import models.orders as ord;
            
            datasource local_orders (
                order_id: key,
                amount: metric
            )
            address local.orders;
            
            persist local_orders where date > '2024-01-01';
            
            overwrite into aggregated_orders from
                select customer_id, sum(amount) -> total_amount;
        "#;
        let parsed = parse_file(content).unwrap();
        assert_eq!(parsed.imports.len(), 2);
        assert_eq!(parsed.datasources.len(), 1);
        assert_eq!(parsed.persists.len(), 2);
    }

    #[test]
    fn test_multiple_imports() {
        let content = r#"
            import models.customer;
            import models.orders as ord;
            // comment
            import ..shared.utils;
        "#;
        let parsed = parse_file(content).unwrap();
        assert_eq!(parsed.imports.len(), 3);
    }
}