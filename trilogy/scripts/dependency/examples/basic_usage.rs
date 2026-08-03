//! The README's "Rust Library Usage" snippet, kept here so `cargo test` compiles
//! it and the crates.io landing page cannot rot.

use pest::Parser;
use std::path::Path;
use trilogy_parser::{parse_file, AddressKind, ImportResolver, Rule, TrilogyParser};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Dependency-relevant extraction: imports, datasources, persists.
    let parsed = parse_file("import models.orders; datasource o (id: key) address db.o;")?;
    for ds in &parsed.datasources {
        if ds.address_kind == AddressKind::Literal {
            println!("{} -> {}", ds.name, ds.address.as_deref().unwrap_or(""));
        }
    }

    // Transitive resolution across files, with ETL-aware ordering.
    let mut resolver = ImportResolver::new();
    let graph = resolver.resolve(Path::new("main.preql"))?;
    println!("{:?}", graph.order);

    // Or drive the full-language pest grammar directly.
    let tree = TrilogyParser::parse(Rule::start, "const x <- 5; select x + 1 -> y;")?;
    println!("{}", tree.count());
    Ok(())
}
