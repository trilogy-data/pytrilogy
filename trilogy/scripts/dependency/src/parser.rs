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
        self.alias.as_deref().unwrap_or_else(|| {
            self.raw_path.split('.').last().unwrap_or(&self.raw_path)
        })
    }
}

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Failed to parse file: {0}")]
    PestError(#[from] pest::error::Error<Rule>),
    
    #[error("Invalid import statement structure")]
    InvalidStructure,
}

/// Parse a PreQL file and extract all import statements
pub fn parse_imports(content: &str) -> Result<Vec<ImportStatement>, ParseError> {
    let pairs = PreqlParser::parse(Rule::file, content)?;
    let mut imports = Vec::new();

    for pair in pairs {
        if pair.as_rule() == Rule::file {
            for inner in pair.into_inner() {
                if inner.as_rule() == Rule::statement {
                    for stmt in inner.into_inner() {
                        if stmt.as_rule() == Rule::import_statement {
                            if let Some(import) = parse_import_statement(stmt)? {
                                imports.push(import);
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(imports)
}

fn parse_import_statement(pair: pest::iterators::Pair<Rule>) -> Result<Option<ImportStatement>, ParseError> {
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
        return Err(ParseError::InvalidStructure);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_import() {
        let content = "import models.customer;";
        let imports = parse_imports(content).unwrap();
        assert_eq!(imports.len(), 1);
        assert_eq!(imports[0].raw_path, "models.customer");
        assert_eq!(imports[0].parent_dirs, 0);
        assert!(imports[0].alias.is_none());
    }

    #[test]
    fn test_import_with_alias() {
        let content = "import models.customer as cust;";
        let imports = parse_imports(content).unwrap();
        assert_eq!(imports.len(), 1);
        assert_eq!(imports[0].raw_path, "models.customer");
        assert_eq!(imports[0].alias, Some("cust".to_string()));
    }

    #[test]
    fn test_relative_import() {
        let content = "import ..models.customer;";
        let imports = parse_imports(content).unwrap();
        assert_eq!(imports.len(), 1);
        assert_eq!(imports[0].raw_path, "models.customer");
        assert_eq!(imports[0].parent_dirs, 1);
    }

    #[test]
    fn test_stdlib_import() {
        let content = "import std.aggregates;";
        let imports = parse_imports(content).unwrap();
        assert_eq!(imports.len(), 1);
        assert!(imports[0].is_stdlib);
    }

    #[test]
    fn test_multiple_imports() {
        let content = r#"
            import models.customer;
            import models.orders as ord;
            // comment
            import ..shared.utils;
        "#;
        let imports = parse_imports(content).unwrap();
        assert_eq!(imports.len(), 3);
    }
}
