# Trilogy Parser (`trilogy-parser`)

A Rust-based CLI tool and Python library for parsing PreQL (Trilogy) files and resolving import dependencies with ETL-aware dependency ordering.

## Features

- Parse PreQL files to extract imports, datasource declarations, and persist statements
- Resolve import dependencies transitively
- Build dependency graphs with ETL-aware ordering:
  - Files that persist (write) to a datasource run before files that declare it, even if they import it.
  - Standard import dependencies (imported files run before importing files)

Exit codes:
- `0`: Success
- `1`: Error (parse error, file not found, circular dependency, etc.)

## Installation

```bash
cargo add trilogy-parser          # library
cargo install trilogy-parser      # CLI (installs `trilogy-parser-cli`)
```

Crate versions track [pytrilogy](https://pypi.org/project/pytrilogy/) releases: a
published `trilogy-parser` version always carries the grammar of the pytrilogy
release with the same number.

### Feature flags

| Feature | Default | Effect |
| --- | --- | --- |
| *(none)* | ✅ | Pure Rust. No Python toolchain required. |
| `python` | | Builds the PyO3 bindings (`python_bindings`, `parse_trilogy_syntax*`). Used only by the wheel build. |

Rust consumers should stay on default features; enabling `python` pulls in pyo3
and requires a Python interpreter at build time.

## Rust Library Usage

```rust
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
```

`DatasourceDeclaration` / `DatasourceInfo` carry the backing of each datasource:
`address` plus an `address_kind` of `literal`, `templated` (an `f`-string address
resolved at run time), `query` (a view, no physical table), or `file`. Flags
`is_root`, `is_partial`, and `is_partitioned` mirror the corresponding modifiers.

## CLI Usage

### Parse a single file

```bash
trilogy-parser-cli parse path/to/file.preql --format pretty
```

### Parse a directory

```bash
trilogy-parser-cli parse path/to/directory --recursive --format json
```

### Resolve dependencies

```bash
trilogy-parser-cli resolve path/to/file.preql --format pretty
```

### Analyze datasources

```bash
trilogy-parser-cli datasources path/to/directory --recursive
```

## Python Integration

The Rust resolver is integrated into the Python package via PyO3 and maturin.

### Building the Python Extension

```bash
cd trilogy/scripts/dependency
maturin develop  # For development
# or
maturin build --release  # For production
pip install target/wheels/*.whl
```

### Using in Python

```python
from trilogy.scripts.dependency import DependencyResolver, ETLDependencyStrategy, create_script_nodes
from pathlib import Path

# Create script nodes from files
files = [Path("model1.preql"), Path("model2.preql")]
nodes = create_script_nodes(files)

# Use the ETL dependency strategy (backed by Rust)
resolver = DependencyResolver(strategy=ETLDependencyStrategy())
graph = resolver.build_graph(nodes)

# Get execution order (graphs use the Rust-backed graph facade;
# nodes are script-path strings)
from trilogy.core import graph as nx
execution_order = list(nx.topological_sort(graph))
```

The `ETLDependencyStrategy` uses the Rust-based resolver under the hood for fast, accurate dependency analysis based on:
- Import statements
- Datasource declarations
- Persist statements (append/overwrite/persist)

## Development

### Running Rust Tests

```bash
cd trilogy/scripts/dependency
cargo test                                      # default (pure-Rust) features
cargo check --features python --all-targets     # type-check the PyO3 layer
```

Test layout:
- Unit tests for the parser — imports, datasource backings/modifiers, persist statements
- Unit tests for the resolver — dependency resolution and ordering
- `tests/cli_integration.rs` — every CLI command, including the JSON output contract
- `tests/public_api.rs` — the crate's exported surface, reached only through
  `trilogy_parser::*`, so a regression in what is actually published fails here

### Building the CLI

```bash
cargo build --release
```

The binary will be at `target/release/trilogy-parser-cli` (or `.exe` on Windows).

### Building for Python

Run maturin from the base of the pytrilogy repo. [not this directory.]

```bash
# Development mode (installs in current Python environment)
maturin develop

# Production build
maturin build --release

# The wheel will be in target/wheels/
```

## Dependency Ordering Rules

The resolver implements three key dependency rules:

1. **Import Dependencies** : Imported files should run before importing files
2. **Persist-Before-Declare**: Files that persist to a datasource must run before files that declare it, even if they import that file. 

### Edge Cases

- Case 1: file A imports from file B → B must run before A for all datasources in B
- Case 2: file A imports from file B, then updates datasource from file B → update takes precedence, so A runs before B.

## Scope

`src/trilogy.pest` is the full Trilogy grammar — the same one that backs
pytrilogy's pest parser — so `TrilogyParser::parse(Rule::start, ..)` accepts any
valid Trilogy source.

The higher-level helpers (`parse_file`, `parse_imports`, `ImportResolver`) walk
only the dependency-relevant constructs of that tree: imports, datasource
declarations, and persist statements. Everything else in a file is parsed and
skipped. For anything beyond dependency analysis, drive the grammar directly.