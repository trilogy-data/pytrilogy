"""Shared, multi-suite agent-trajectory viewer.

Entry point: ``evals/trajectory_viewer.py``. Suites are discovered from
``evals/*/spec.py`` (see :mod:`viewer.suites`); everything benchmark-specific
flows through the suite's ``BenchmarkSpec``.
"""
