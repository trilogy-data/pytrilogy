// Dependency-resolution view over the full Trilogy grammar. Walks the
// `trilogy.pest` parse tree produced by `TrilogyParser` and extracts only the
// three statement kinds that matter for dependency ordering: imports,
// datasources, and persists. Everything else is ignored.
//
// Historically this module had its own permissive grammar (`preql.pest`) that
// could parse partial / malformed files. The strict grammar refuses those, so
// directory_resolver callers now surface a warning for files that can't parse
// cleanly (the lark/pest pipelines would reject them at compile time anyway).

use crate::trilogy_parser::{Rule, TrilogyParser};
use pest::iterators::Pair;
use pest::Parser;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ImportStatement {
    pub raw_path: String,
    pub parent_dirs: usize,
    pub alias: Option<String>,
    pub is_stdlib: bool,
}

impl ImportStatement {
    pub fn resolve(&self, working_dir: &Path) -> Option<PathBuf> {
        if self.is_stdlib {
            return None;
        }

        let mut base = working_dir.to_path_buf();
        for _ in 0..self.parent_dirs {
            base = base.parent()?.to_path_buf();
        }
        for part in self.raw_path.split('.') {
            base.push(part);
        }
        base.set_extension("preql");
        Some(base)
    }

    pub fn effective_alias(&self) -> &str {
        self.alias
            .as_deref()
            .unwrap_or_else(|| self.raw_path.split('.').last().unwrap_or(&self.raw_path))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatasourceDeclaration {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PersistStatement {
    pub mode: PersistMode,
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

pub fn parse_file(content: &str) -> Result<ParsedFile, ParseError> {
    let mut pairs = TrilogyParser::parse(Rule::start, content)?;
    let start = pairs
        .next()
        .ok_or(ParseError::InvalidImportStructure)?;

    let mut result = ParsedFile::default();
    for top in start.into_inner() {
        if top.as_rule() != Rule::block {
            continue;
        }
        // block = { statement ~ _TERMINATOR }; `statement` is silent, so its
        // inner rule (import_statement, datasource, persist_statement, ...)
        // appears as a direct child of block.
        for stmt in top.into_inner() {
            match stmt.as_rule() {
                Rule::import_statement => {
                    result.imports.push(extract_import(stmt)?);
                }
                Rule::datasource => {
                    result.datasources.push(extract_datasource(stmt)?);
                }
                Rule::persist_statement => {
                    result.persists.push(extract_persist(stmt)?);
                }
                _ => {}
            }
        }
    }

    Ok(result)
}

pub fn parse_imports(content: &str) -> Result<Vec<ImportStatement>, ParseError> {
    Ok(parse_file(content)?.imports)
}

// import_statement = { ^"import" ~ IMPORT_DOT* ~ dotted_identifier_tail ~ (^"as" ~ IDENTIFIER)? }
// `dotted_identifier_tail` is silent, so children are the IMPORT_DOT tokens
// followed by IDENTIFIER tokens for every path component and the optional alias.
fn extract_import(pair: Pair<Rule>) -> Result<ImportStatement, ParseError> {
    let full_text = pair.as_str();
    let mut n_dots = 0usize;
    let mut idents: Vec<String> = Vec::new();
    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::IMPORT_DOT => n_dots += 1,
            Rule::IDENTIFIER => idents.push(child.as_str().to_string()),
            _ => {}
        }
    }
    if idents.is_empty() {
        return Err(ParseError::InvalidImportStructure);
    }

    // Whether the final identifier is an alias. `as` is a reserved keyword, so
    // a bare `as` token inside the statement text is unambiguous.
    let has_alias = full_text
        .split_ascii_whitespace()
        .any(|tok| tok.eq_ignore_ascii_case("as"));
    let alias = if has_alias && idents.len() >= 2 {
        Some(idents.pop().unwrap())
    } else {
        None
    };

    let raw_path = idents.join(".");
    let is_stdlib = raw_path == "std" || raw_path.starts_with("std.");
    // Historical convention: leading dot prefix `..` means "one level up", so
    // the first dot is part of the relative-import syntax and each extra dot
    // adds one parent traversal.
    let parent_dirs = n_dots.saturating_sub(1);

    Ok(ImportStatement {
        raw_path,
        parent_dirs,
        alias,
        is_stdlib,
    })
}

// datasource = { DATASOURCE_ROOT? ~ (DATASOURCE_PARTIAL | SHORTHAND_MODIFIER)? ~ "datasource" ~ IDENTIFIER ~ "(" ~ ... }
// The first direct IDENTIFIER child is always the datasource name.
fn extract_datasource(pair: Pair<Rule>) -> Result<DatasourceDeclaration, ParseError> {
    for child in pair.into_inner() {
        if child.as_rule() == Rule::IDENTIFIER {
            return Ok(DatasourceDeclaration {
                name: child.as_str().to_string(),
            });
        }
    }
    Err(ParseError::InvalidDatasourceStructure)
}

fn extract_persist(pair: Pair<Rule>) -> Result<PersistStatement, ParseError> {
    // persist_statement = { full_persist | auto_persist }
    let inner = pair
        .into_inner()
        .next()
        .ok_or(ParseError::InvalidPersistStructure)?;
    match inner.as_rule() {
        Rule::auto_persist => extract_auto_persist(inner),
        Rule::full_persist => extract_full_persist(inner),
        _ => Err(ParseError::InvalidPersistStructure),
    }
}

// auto_persist = { PERSIST_MODE ~ IDENTIFIER ~ where? }
fn extract_auto_persist(pair: Pair<Rule>) -> Result<PersistStatement, ParseError> {
    let mut mode: Option<PersistMode> = None;
    let mut target: Option<String> = None;
    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::PERSIST_MODE => mode = Some(parse_persist_mode(child.as_str())),
            Rule::IDENTIFIER if target.is_none() => {
                target = Some(child.as_str().to_string());
            }
            _ => {}
        }
    }
    match (mode, target) {
        (Some(mode), Some(target_datasource)) => Ok(PersistStatement {
            mode,
            target_datasource,
        }),
        _ => Err(ParseError::InvalidPersistStructure),
    }
}

// full_persist = { PERSIST_MODE ~ (!"into" ~ IDENTIFIER)? ~ "into" ~ IDENTIFIER ~ persist_partition_clause? ~ "from" ~ select_statement }
// Literals (`into`, `from`) are not emitted as children, so we see PERSIST_MODE,
// optionally a source IDENTIFIER, then the target IDENTIFIER, then the select
// subtree. Taking the LAST direct IDENTIFIER yields the post-`into` target.
fn extract_full_persist(pair: Pair<Rule>) -> Result<PersistStatement, ParseError> {
    let mut mode: Option<PersistMode> = None;
    let mut last_ident: Option<String> = None;
    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::PERSIST_MODE => mode = Some(parse_persist_mode(child.as_str())),
            Rule::IDENTIFIER => last_ident = Some(child.as_str().to_string()),
            _ => {}
        }
    }
    match (mode, last_ident) {
        (Some(mode), Some(target_datasource)) => Ok(PersistStatement {
            mode,
            target_datasource,
        }),
        _ => Err(ParseError::InvalidPersistStructure),
    }
}

fn parse_persist_mode(s: &str) -> PersistMode {
    match s.to_ascii_lowercase().as_str() {
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
        let parsed = parse_file("import models.customer;").unwrap();
        assert_eq!(parsed.imports.len(), 1);
        assert_eq!(parsed.imports[0].raw_path, "models.customer");
        assert_eq!(parsed.imports[0].parent_dirs, 0);
        assert!(parsed.imports[0].alias.is_none());
    }

    #[test]
    fn test_import_with_alias() {
        let parsed = parse_file("import models.customer as cust;").unwrap();
        assert_eq!(parsed.imports.len(), 1);
        assert_eq!(parsed.imports[0].raw_path, "models.customer");
        assert_eq!(parsed.imports[0].alias, Some("cust".to_string()));
    }

    #[test]
    fn test_relative_import() {
        let parsed = parse_file("import ..models.customer;").unwrap();
        assert_eq!(parsed.imports.len(), 1);
        assert_eq!(parsed.imports[0].raw_path, "models.customer");
        assert_eq!(parsed.imports[0].parent_dirs, 1);
    }

    #[test]
    fn test_sibling_relative_import() {
        let parsed = parse_file("import .customer;").unwrap();
        assert_eq!(parsed.imports[0].raw_path, "customer");
        assert_eq!(parsed.imports[0].parent_dirs, 0);
    }

    #[test]
    fn test_stdlib_import() {
        let parsed = parse_file("import std.aggregates;").unwrap();
        assert!(parsed.imports[0].is_stdlib);
    }

    #[test]
    fn test_datasource_simple() {
        let content = r#"
            key order_id int;
            datasource orders (
                order_id: order_id,
                amount: amount
            )
            grain (order_id)
            address my_database.orders;
        "#;
        let parsed = parse_file(content).unwrap();
        assert_eq!(parsed.datasources.len(), 1);
        assert_eq!(parsed.datasources[0].name, "orders");
    }

    #[test]
    fn test_datasource_with_quoted_address() {
        let content = r#"
            key customer_id int;
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
        let parsed = parse_file("persist orders;").unwrap();
        assert_eq!(parsed.persists.len(), 1);
        assert_eq!(parsed.persists[0].target_datasource, "orders");
        assert_eq!(parsed.persists[0].mode, PersistMode::Persist);
    }

    #[test]
    fn test_append_auto_persist() {
        let parsed = parse_file("append orders;").unwrap();
        assert_eq!(parsed.persists.len(), 1);
        assert_eq!(parsed.persists[0].target_datasource, "orders");
        assert_eq!(parsed.persists[0].mode, PersistMode::Append);
    }

    #[test]
    fn test_full_persist() {
        let content = r#"
            key order_id int;
            overwrite into target_orders from select order_id;
        "#;
        let parsed = parse_file(content).unwrap();
        assert_eq!(parsed.persists.len(), 1);
        assert_eq!(parsed.persists[0].target_datasource, "target_orders");
        assert_eq!(parsed.persists[0].mode, PersistMode::Overwrite);
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
        assert_eq!(parsed.imports[1].alias, Some("ord".to_string()));
        assert_eq!(parsed.imports[2].parent_dirs, 1);
    }

    #[test]
    fn test_mixed_file() {
        let content = r#"
            import models.customer;

            key order_id int;
            datasource local_orders (
                order_id: order_id
            )
            grain (order_id)
            address local.orders;

            persist local_orders;
        "#;
        let parsed = parse_file(content).unwrap();
        assert_eq!(parsed.imports.len(), 1);
        assert_eq!(parsed.datasources.len(), 1);
        assert_eq!(parsed.persists.len(), 1);
        assert_eq!(parsed.datasources[0].name, "local_orders");
        assert_eq!(parsed.persists[0].target_datasource, "local_orders");
    }
}
