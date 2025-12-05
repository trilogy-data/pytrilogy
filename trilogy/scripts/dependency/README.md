# Trilogy Fast Parser

A Rust toolit for rapid partial processing of .preql files to establish dependency order and other information.


Exit codes:
- `0`: Success
- `1`: Error (parse error, file not found, circular dependency, etc.)

## Edge Cases

Case 1: file a imports from file b : b-> a edge for all datasources in b. 
Case 2: file a imports from file b, then updates a datasource from file b. a->b edge for all datasources in b.

## Extensions

The grammar is limited at the moment; we may eventually promote it up to full compatibility. 