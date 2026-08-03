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

/// How a datasource is backed. Only `Literal` yields a physical address that
/// can be joined against externally-observed state; `Templated` addresses
/// resolve at run time and are surfaced raw rather than silently dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AddressKind {
    /// `address x.y` or a backtick-quoted literal — a physical warehouse table.
    Literal,
    /// `address f`...`` — an f-string; `address` holds the raw template.
    Templated,
    /// `query ...` — a view over other assets; no physical table.
    Query,
    /// `file ...` — a local file source; `address` holds the raw spec.
    File,
}

impl AddressKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            AddressKind::Literal => "literal",
            AddressKind::Templated => "templated",
            AddressKind::Query => "query",
            AddressKind::File => "file",
        }
    }
}

impl std::fmt::Display for AddressKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatasourceDeclaration {
    pub name: String,
    /// Physical address for `Literal` (quoting stripped), raw template for
    /// `Templated`, raw path spec for `File`, `None` for `Query`.
    pub address: Option<String>,
    pub address_kind: AddressKind,
    /// `root datasource` — a source the script reads, not a managed asset it writes.
    pub is_root: bool,
    /// `partial datasource` — covers only a subset of its grain, so it cannot
    /// answer a query on its own.
    pub is_partial: bool,
    /// Declares a `partition by` clause.
    pub is_partitioned: bool,
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
                // `from x.y import a, b` is the same file edge as `import x.y`;
                // the concept list rides in its own `import_concepts` child, so
                // the shared extractor sees only the path (and alias) tokens.
                // `self import as X` re-imports the current file under a
                // namespace and creates no cross-file edge, so it is ignored.
                Rule::import_statement | Rule::selective_import_statement => {
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
// The first direct IDENTIFIER child is always the datasource name; the
// backing (address | query | file) and the partition clause are direct
// children as well.
fn extract_datasource(pair: Pair<Rule>) -> Result<DatasourceDeclaration, ParseError> {
    let mut name: Option<String> = None;
    let mut address: Option<String> = None;
    let mut address_kind: Option<AddressKind> = None;
    let mut is_root = false;
    let mut is_partial = false;
    let mut is_partitioned = false;

    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::DATASOURCE_ROOT => is_root = true,
            Rule::DATASOURCE_PARTIAL => is_partial = true,
            Rule::IDENTIFIER if name.is_none() => name = Some(child.as_str().to_string()),
            // address = { "address" ~ (F_QUOTED_ADDRESS | QUOTED_ADDRESS | ADDRESS) }
            Rule::address => {
                let tok = child
                    .into_inner()
                    .next()
                    .ok_or(ParseError::InvalidDatasourceStructure)?;
                match tok.as_rule() {
                    Rule::F_QUOTED_ADDRESS => {
                        // f`...` — keep the raw template body; it cannot be
                        // resolved statically and must not be dropped.
                        address_kind = Some(AddressKind::Templated);
                        address = Some(
                            tok.as_str()
                                .trim_start_matches(['f', 'F'])
                                .trim_matches('`')
                                .to_string(),
                        );
                    }
                    Rule::QUOTED_ADDRESS => {
                        // `...` with an optional inner '...' layer.
                        address_kind = Some(AddressKind::Literal);
                        address = Some(
                            tok.as_str().trim_matches('`').trim_matches('\'').to_string(),
                        );
                    }
                    _ => {
                        address_kind = Some(AddressKind::Literal);
                        address = Some(tok.as_str().to_string());
                    }
                }
            }
            Rule::query => address_kind = Some(AddressKind::Query),
            Rule::file => {
                address_kind = Some(AddressKind::File);
                // Raw spec after the (always 4-byte) `file` keyword.
                address = Some(child.as_str()[4..].trim().to_string());
            }
            Rule::datasource_partition_clause => is_partitioned = true,
            _ => {}
        }
    }

    match (name, address_kind) {
        (Some(name), Some(address_kind)) => Ok(DatasourceDeclaration {
            name,
            address,
            address_kind,
            is_root,
            is_partial,
            is_partitioned,
        }),
        _ => Err(ParseError::InvalidDatasourceStructure),
    }
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
        let ds = &parsed.datasources[0];
        assert_eq!(ds.name, "orders");
        assert_eq!(ds.address.as_deref(), Some("my_database.orders"));
        assert_eq!(ds.address_kind, AddressKind::Literal);
        assert!(!ds.is_root);
        assert!(!ds.is_partitioned);
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
        let ds = &parsed.datasources[0];
        assert_eq!(ds.name, "customers");
        assert_eq!(ds.address.as_deref(), Some("my_db.customers"));
        assert_eq!(ds.address_kind, AddressKind::Literal);
    }

    #[test]
    fn test_root_partitioned_datasource() {
        let content = r#"
            key event_id int;
            root datasource events (
                event_id: event_id
            )
            grain (event_id)
            address analytics.events
            partition by event_id;
        "#;
        let parsed = parse_file(content).unwrap();
        let ds = &parsed.datasources[0];
        assert!(ds.is_root);
        assert!(ds.is_partitioned);
        assert_eq!(ds.address.as_deref(), Some("analytics.events"));
    }

    #[test]
    fn test_templated_address_datasource() {
        let content = r#"
            key order_id int;
            datasource orders (
                order_id: order_id
            )
            grain (order_id)
            address f`{{env}}.orders`;
        "#;
        let parsed = parse_file(content).unwrap();
        let ds = &parsed.datasources[0];
        assert_eq!(ds.address_kind, AddressKind::Templated);
        // Raw template body is kept — it cannot be resolved statically.
        assert_eq!(ds.address.as_deref(), Some("{{env}}.orders"));
    }

    #[test]
    fn test_query_datasource_has_no_address() {
        let content = r#"
            key order_id int;
            datasource order_view (
                order_id: order_id
            )
            grain (order_id)
            query '''select 1 as order_id''';
        "#;
        let parsed = parse_file(content).unwrap();
        let ds = &parsed.datasources[0];
        assert_eq!(ds.address_kind, AddressKind::Query);
        assert!(ds.address.is_none());
    }

    #[test]
    fn test_file_datasource() {
        let content = r#"
            key launch_id int;
            datasource launches (
                launch_id: launch_id
            )
            grain (launch_id)
            file `gcs://bucket/launch_report/launch.parquet`;
        "#;
        let parsed = parse_file(content).unwrap();
        let ds = &parsed.datasources[0];
        assert_eq!(ds.address_kind, AddressKind::File);
        // Raw spec: file paths may themselves contain `:` (`gcs://`), so the
        // backticked form is kept verbatim rather than unquoted ambiguously.
        assert_eq!(
            ds.address.as_deref(),
            Some("`gcs://bucket/launch_report/launch.parquet`")
        );
    }

    #[test]
    fn test_file_datasource_read_write_pair() {
        let content = r#"
            key launch_id int;
            datasource launches (
                launch_id: launch_id
            )
            grain (launch_id)
            file `https://host/launch.parquet`:`gcs://bucket/launch.parquet`;
        "#;
        let parsed = parse_file(content).unwrap();
        let ds = &parsed.datasources[0];
        assert_eq!(ds.address_kind, AddressKind::File);
        assert_eq!(
            ds.address.as_deref(),
            Some("`https://host/launch.parquet`:`gcs://bucket/launch.parquet`")
        );
    }

    #[test]
    fn test_root_partial_file_datasource() {
        let content = r#"
            key a_id int;
            root partial datasource a_raw (
                a_id: a_id
            )
            grain (a_id)
            file `./a_raw_source.py`;
        "#;
        let parsed = parse_file(content).unwrap();
        let ds = &parsed.datasources[0];
        assert_eq!(ds.name, "a_raw");
        assert!(ds.is_root);
        assert!(ds.is_partial);
        assert!(!ds.is_partitioned);
        assert_eq!(ds.address_kind, AddressKind::File);
    }

    #[test]
    fn test_partial_datasource_name_not_shadowed_by_modifier() {
        let content = r#"
            key customer_id int;
            partial datasource customer_revenue (
                customer_id: customer_id
            )
            grain (customer_id)
            address db.customer_revenue;
        "#;
        let parsed = parse_file(content).unwrap();
        let ds = &parsed.datasources[0];
        assert_eq!(ds.name, "customer_revenue");
        assert!(ds.is_partial);
        assert!(!ds.is_root);
    }

    #[test]
    fn test_multiple_datasources_keep_independent_flags() {
        let content = r#"
            key order_id int;
            root datasource raw_orders (
                order_id: order_id
            )
            grain (order_id)
            address raw.orders;

            datasource orders (
                order_id: order_id
            )
            grain (order_id)
            address db.orders;
        "#;
        let parsed = parse_file(content).unwrap();
        assert_eq!(parsed.datasources.len(), 2);
        assert!(parsed.datasources[0].is_root);
        assert_eq!(parsed.datasources[0].address.as_deref(), Some("raw.orders"));
        assert!(!parsed.datasources[1].is_root);
        assert_eq!(parsed.datasources[1].address.as_deref(), Some("db.orders"));
    }

    #[test]
    fn test_quoted_address_strips_inner_quote_layer() {
        let content = r#"
            key order_id int;
            datasource orders (
                order_id: order_id
            )
            grain (order_id)
            address `'my project.my dataset.orders'`;
        "#;
        let parsed = parse_file(content).unwrap();
        let ds = &parsed.datasources[0];
        assert_eq!(ds.address_kind, AddressKind::Literal);
        assert_eq!(
            ds.address.as_deref(),
            Some("my project.my dataset.orders")
        );
    }

    #[test]
    fn test_address_kind_strings_are_stable() {
        // These are the serialized `address_kind` values in CLI JSON output.
        assert_eq!(AddressKind::Literal.to_string(), "literal");
        assert_eq!(AddressKind::Templated.to_string(), "templated");
        assert_eq!(AddressKind::Query.to_string(), "query");
        assert_eq!(AddressKind::File.to_string(), "file");
    }

    #[test]
    fn test_selective_import_creates_edge() {
        let parsed = parse_file("from models.customer import customer_id;").unwrap();
        assert_eq!(parsed.imports.len(), 1);
        assert_eq!(parsed.imports[0].raw_path, "models.customer");
        assert!(parsed.imports[0].alias.is_none());
    }

    #[test]
    fn test_selective_import_with_alias() {
        let parsed = parse_file("from ..models.customer as cust import customer_id, name;").unwrap();
        assert_eq!(parsed.imports.len(), 1);
        assert_eq!(parsed.imports[0].raw_path, "models.customer");
        assert_eq!(parsed.imports[0].alias, Some("cust".to_string()));
        assert_eq!(parsed.imports[0].parent_dirs, 1);
    }

    #[test]
    fn test_self_import_is_not_an_edge() {
        let parsed = parse_file("self import as me;").unwrap();
        assert!(parsed.imports.is_empty());
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
