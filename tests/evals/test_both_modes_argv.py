"""Unit tests for the argv-rewriting helpers used by ``--both-modes``.

The driver re-invokes ``run_eval.py`` as a subprocess once per leg (base +
enriched) and has to (a) strip ``--both-modes`` itself and (b) replace any
per-leg-overridden flags so the legs don't end up sharing output dirs or
both trying to load the enriched model. These helpers are pure-text so they
test in isolation."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "evals"))

from common.main import (  # noqa: E402
    _argv_has_flag,
    _extract_enriched_dir_from_argv,
    _filter_both_modes_argv,
)


def test_filter_strips_both_modes_and_overridden_flags():
    got = _filter_both_modes_argv(
        [
            "--both-modes",
            "--enriched-model-dir",
            "/some/path",
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


def test_filter_handles_equals_form():
    got = _filter_both_modes_argv(
        ["--both-modes", "--enriched-model-dir=/x", "--num-queries", "5"]
    )
    assert got == ["--num-queries", "5"]


def test_filter_preserves_unrelated_flags():
    got = _filter_both_modes_argv(
        ["--model", "deepseek/deepseek-v4-flash", "--provider", "openrouter"]
    )
    assert got == ["--model", "deepseek/deepseek-v4-flash", "--provider", "openrouter"]


def test_extract_enriched_dir_space_form():
    assert _extract_enriched_dir_from_argv(["--enriched-model-dir", "/x"]) == "/x"


def test_extract_enriched_dir_equals_form():
    assert _extract_enriched_dir_from_argv(["--enriched-model-dir=/y"]) == "/y"


def test_extract_enriched_dir_missing():
    assert _extract_enriched_dir_from_argv(["--other", "a"]) is None


def test_argv_has_flag_space_form():
    assert _argv_has_flag(["--concurrency", "4"], "--concurrency") is True


def test_argv_has_flag_equals_form():
    assert _argv_has_flag(["--concurrency=4"], "--concurrency") is True


def test_argv_has_flag_absent():
    assert _argv_has_flag(["--num-queries", "20"], "--concurrency") is False


def test_argv_has_flag_does_not_match_prefix():
    """--concurrency should not match a hypothetical --concurrency-overflow."""
    assert _argv_has_flag(["--concurrency-extra", "x"], "--concurrency") is False
