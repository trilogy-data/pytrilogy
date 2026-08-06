# Rust script datasource agent benchmark

This directory reserves the Rust mirror of `script_agent_python`. Rust script
datasources are not implemented in Trilogy yet, so `run_eval.py` exits with a
clear unsupported message. True-positive Arrow programs will be added when the
datasource address type and execution contract exist.

The case catalog should be sourced from
`../script_agent_python/query_prompts.json` so Python and Rust measure the same
work. Keep language-specific prompts and reference implementations here; share
runner/scoring behavior through `evals/common` and `evals/script_agent`.
