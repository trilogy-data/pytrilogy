//! Exercises the crate the way an external Rust consumer does — only through
//! `trilogy_parser::*`, with default features (no pyo3). Anything this file
//! cannot reach is not actually published API, whatever the internal comments
//! claim.

use pest::Parser;
use trilogy_parser::{
    parse_file, parse_imports, AddressKind, ParseError, PersistMode, Rule, TrilogyParser,
};

#[test]
fn full_grammar_parser_is_reachable() {
    let pairs = TrilogyParser::parse(Rule::start, "const x <- 5;\nselect x + 1 -> y;")
        .expect("full trilogy syntax should parse");
    assert!(pairs.count() > 0);
}

#[test]
fn parse_error_rule_is_nameable() {
    // `ParseError::PestError` carries `pest::error::Error<Rule>`; a consumer must
    // be able to name `Rule` to destructure it.
    let err = parse_file("this is not trilogy at all @@@").unwrap_err();
    match err {
        ParseError::PestError(inner) => {
            let _: &pest::error::Error<Rule> = &inner;
        }
        other => panic!("expected a pest error, got {other:?}"),
    }
}

#[test]
fn datasource_metadata_is_public() {
    let parsed = parse_file(
        r#"
        key order_id int;
        root partial datasource orders (
            order_id: order_id
        )
        grain (order_id)
        address warehouse.orders
        partition by order_id;
        "#,
    )
    .unwrap();

    let ds = &parsed.datasources[0];
    assert_eq!(ds.name, "orders");
    assert_eq!(ds.address.as_deref(), Some("warehouse.orders"));
    assert_eq!(ds.address_kind, AddressKind::Literal);
    assert_eq!(ds.address_kind.as_str(), "literal");
    assert!(ds.is_root);
    assert!(ds.is_partial);
    assert!(ds.is_partitioned);
}

#[test]
fn import_and_persist_surfaces_are_public() {
    let imports = parse_imports("import models.orders as ord;\nfrom .models.customer import cid;")
        .unwrap();
    assert_eq!(imports.len(), 2);
    assert_eq!(imports[0].raw_path, "models.orders");
    assert_eq!(imports[0].alias.as_deref(), Some("ord"));
    assert_eq!(imports[1].raw_path, "models.customer");

    let parsed = parse_file("persist orders;").unwrap();
    assert_eq!(parsed.persists[0].mode, PersistMode::Persist);
    assert_eq!(parsed.persists[0].target_datasource, "orders");
}
