"""Unit tests for the argv-rewriting helpers used by the multi-category driver.

The driver re-invokes ``run_eval.py`` as a subprocess once per category leg and
has to (a) strip ``--both-modes``/``--categories``/``--category`` and (b)
replace any per-leg-overridden flags (``--output-dir``, ``--monitor``) so the
legs don't share an output dir. ``--enriched-model-dir`` is preserved so the
enriched leg can still find its model. These helpers are pure-text so they test
in isolation."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "evals"))

from common.main import (
    _argv_has_flag,
    _filter_both_modes_argv,
)


def test_filter_strips_driver_and_overridden_flags():
    got = _filter_both_modes_argv(
        [
            "--categories",
            "sql_bare,ingest",
            "--num-queries",
            "20",
            "--monitor",
            "quiet",
            "--output-dir",
            "/x",
            "--concurrency",
            "4",
        ]
    )
    assert got == ["--num-queries", "20", "--concurrency", "4"]


def test_filter_preserves_enriched_model_dir():
    """The enriched leg needs the dir, and non-enriched legs ignore it, so it
    rides along on every leg rather than being stripped."""
    got = _filter_both_modes_argv(
        ["--both-modes", "--enriched-model-dir", "/some/path", "--num-queries", "5"]
    )
    assert got == ["--enriched-model-dir", "/some/path", "--num-queries", "5"]


def test_filter_handles_equals_form():
    got = _filter_both_modes_argv(
        ["--both-modes", "--category=ingest", "--num-queries", "5"]
    )
    assert got == ["--num-queries", "5"]


def test_filter_preserves_unrelated_flags():
    got = _filter_both_modes_argv(
        ["--model", "deepseek/deepseek-v4-flash", "--provider", "openrouter"]
    )
    assert got == ["--model", "deepseek/deepseek-v4-flash", "--provider", "openrouter"]


def test_argv_has_flag_space_form():
    assert _argv_has_flag(["--concurrency", "4"], "--concurrency") is True


def test_argv_has_flag_equals_form():
    assert _argv_has_flag(["--concurrency=4"], "--concurrency") is True


def test_argv_has_flag_absent():
    assert _argv_has_flag(["--num-queries", "20"], "--concurrency") is False


def test_argv_has_flag_does_not_match_prefix():
    """--concurrency should not match a hypothetical --concurrency-overflow."""
    assert _argv_has_flag(["--concurrency-extra", "x"], "--concurrency") is False
