## Context

Python project for semantic ETL and reporting.

Provides a SQL-like language with optimizations. 

Primarily python, with some rust in performance critical paths. 

## Styles

Prefer minimal, concise code and small functions. Keep comments concise and targeted at otherwise non-intuitive code.

## Development
Always use the local .venv

## Build Backend

Project uses a build_backend defined in ./build_backend.py, which mostly uses maturin. The custom wrapper exists to be able to pull in requirements from requirements.txt + version dynamically.

