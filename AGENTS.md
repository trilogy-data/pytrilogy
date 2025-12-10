## Context

Python project for semantic ETL and reporting.

Provides a SQL-like language with optimizations. 

Primarily python, with some rust in performance critical paths. 

## Styles

Prefer minimal, concise code and small functions. Keep comments concise and targeted at otherwise non-intuitive code.

Avoid comments in tests unless they substantially add code.

Avoid defining functions inside functions where possible to make testing easier.

## Development

- Always use a local venv for python. It should be in the base of the project.
- Always type-hint (we use mypy)

After all changes are done, confirm we're good by running all of these checks:

```bash
ruff check . --fix
mypy trilogy
black .
```
## Wheel Building

Project uses a build_backend defined in .scripts/build_backend.py, which mostly uses maturin. The custom wrapper exists to be able to pull in requirements from requirements.txt + version dynamically.

## CLI

The trilogy/scripts/trilogy.py file is the CLI entrypoint. The CLI is used for management, interacitons and other tasks. 