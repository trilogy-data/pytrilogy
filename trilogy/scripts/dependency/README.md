# PreQL Import Resolver

A Rust CLI tool that parses PreQL files and resolves import dependencies, producing a topologically sorted dependency order.

## Features

- **Fast Parsing**: Uses pest parser generator for efficient parsing
- **Dependency Resolution**: Recursively resolves all imports and produces dependency order
- **Cycle Detection**: Detects and reports circular dependencies
- **Structured Output**: JSON output for easy integration with Python or other tools
- **Extensible Grammar**: Parser grammar designed to be extended for full PreQL parsing

## Installation

### Build from source

```bash
# Clone and build
cd preql-import-resolver
cargo build --release

# Binary will be at ./target/release/preql-import-resolver
```

### Install globally

```bash
cargo install --path .
```

## Usage

### Parse a single file

Extract imports from a single PreQL file:

```bash
# JSON output (default)
preql-import-resolver parse path/to/file.preql

# Pretty-printed JSON
preql-import-resolver parse path/to/file.preql --format pretty
```

Example output:
```json
{
  "file": "models/customer.preql",
  "imports": [
    {
      "raw_path": "shared.utils",
      "alias": null,
      "is_stdlib": false,
      "parent_dirs": 0
    },
    {
      "raw_path": "std.aggregates",
      "alias": null,
      "is_stdlib": true,
      "parent_dirs": 0
    }
  ]
}
```

### Resolve dependencies

Resolve all dependencies starting from a root file:

```bash
# Full dependency graph
preql-import-resolver resolve path/to/main.preql

# Only the dependency order (list of paths)
preql-import-resolver resolve path/to/main.preql --order-only
```

Example output (order-only):
```json
[
  "/absolute/path/to/shared/utils.preql",
  "/absolute/path/to/models/base.preql",
  "/absolute/path/to/models/customer.preql",
  "/absolute/path/to/main.preql"
]
```

Files are ordered so that dependencies come before dependents. This is the order you would need to process files to ensure all imports are available.

### Full dependency graph output

The full output includes detailed information:

```json
{
  "root": "/path/to/main.preql",
  "order": ["/path/to/dep.preql", "/path/to/main.preql"],
  "files": {
    "/path/to/main.preql": {
      "path": "/path/to/main.preql",
      "relative_path": "main.preql",
      "imports": [...],
      "dependencies": ["/path/to/dep.preql"]
    }
  },
  "warnings": []
}
```

## Import Syntax Supported

```
# Simple import
import models.customer;

# Import with alias
import models.customer as cust;

# Relative import (one directory up)
import ..shared.utils;

# Relative import (two directories up)
import ...common.types;

# Standard library imports (ignored in resolution)
import std.aggregates;
```

## Python Integration

See `python_example.py` for a complete example of calling this tool from Python:

```python
import subprocess
import json

def resolve_dependencies(root_file: str) -> list[str]:
    """Get dependency order for a PreQL file."""
    result = subprocess.run(
        ["preql-import-resolver", "resolve", root_file, "--order-only"],
        capture_output=True,
        text=True,
        check=True
    )
    return json.loads(result.stdout)

# Returns list of file paths in dependency order
order = resolve_dependencies("main.preql")
```

## Extending the Parser

The pest grammar in `src/preql.pest` is designed to be extended. To add support for more PreQL constructs:

1. Add grammar rules to `src/preql.pest`
2. Add corresponding parsing logic in `src/parser.rs`
3. Export new types through `src/lib.rs`

## Error Handling

The tool returns structured JSON errors:

```json
{
  "error": "Circular dependency detected: a.preql -> b.preql -> a.preql",
  "file": null
}
```

Exit codes:
- `0`: Success
- `1`: Error (parse error, file not found, circular dependency, etc.)

## License

MIT
