# PreQL Import Resolver

A Rust-based CLI tool and Python library for parsing PreQL files and resolving import dependencies with ETL-aware dependency ordering.

## Features

- Parse PreQL files to extract imports, datasource declarations, and persist statements
- Resolve import dependencies transitively
- Build dependency graphs with ETL-aware ordering:
  - Files that persist (write) to a datasource run before files that declare it
  - Files that declare a datasource run before files that import it
  - Standard import dependencies (imported files run before importing files)
- Comprehensive test coverage for both CLI and library functionality

Exit codes:
- `0`: Success
- `1`: Error (parse error, file not found, circular dependency, etc.)

## CLI Usage

### Parse a single file

```bash
preql-import-resolver parse path/to/file.preql --format pretty
```

### Parse a directory

```bash
preql-import-resolver parse path/to/directory --recursive --format json
```

### Resolve dependencies

```bash
preql-import-resolver resolve path/to/file.preql --format pretty
```

### Analyze datasources

```bash
preql-import-resolver datasources path/to/directory --recursive
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

# Get execution order
import networkx as nx
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
cargo test
```

All tests include:
- Unit tests for parser (17 tests covering imports, datasources, persist statements)
- Unit tests for resolver (5 tests covering dependency resolution logic)
- Integration tests for CLI (12 tests covering all CLI commands)

### Building the CLI

```bash
cargo build --release
```

The binary will be at `target/release/preql-import-resolver` (or `.exe` on Windows).

### Building for Python

The project uses maturin to build Python wheels:

```bash
# Development mode (installs in current Python environment)
maturin develop

# Production build
maturin build --release

# The wheel will be in target/wheels/
```

## Dependency Ordering Rules

The resolver implements three key dependency rules:

1. **Import Dependencies** (highest priority): Imported files must run before importing files
2. **Persist-Before-Declare**: Files that persist to a datasource must run before files that declare it
   - Exception: If a file imports another file and also persists to a datasource declared in that file, the import dependency takes precedence
3. **Declare-Before-Use**: Files that declare a datasource must run before files that depend on it through imports

### Edge Cases

- Case 1: file a imports from file b → b must run before a for all datasources in b
- Case 2: file a imports from file b, then updates a datasource from file b → import takes precedence, so b runs before a

## CI Integration

The GitHub Actions workflow automatically:
1. Sets up Rust toolchain
2. Installs maturin
3. Builds the Rust extension
4. Installs the wheel into the Python environment
5. Runs all Python tests with the extension available

## Grammar Limitations

The grammar is currently focused on dependency-relevant constructs (imports, datasources, persist statements). It does not parse the full PreQL syntax, which allows for faster parsing when only dependency information is needed. We may extend it to full compatibility in the future. 