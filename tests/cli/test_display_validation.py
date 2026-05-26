"""Coverage for display helpers used by the unit/integration validation CLI."""

from __future__ import annotations

import pytest

from trilogy.scripts import display_core, display_validation


@pytest.fixture
def rich_disabled(monkeypatch):
    monkeypatch.setattr(display_core, "RICH_AVAILABLE", False)
    monkeypatch.setattr(display_core, "console", None)
    monkeypatch.setattr(display_core, "error_console", None)


def test_show_validation_targets_no_rich_emits_text(rich_disabled, capsys):
    display_validation.show_validation_targets(["a", "b"], 17, mock=True)
    captured = capsys.readouterr().out
    assert "Validating unit (mocked)" in captured
    assert "2 datasource(s)" in captured
    assert "17 concept(s)" in captured
    assert "  a" in captured
    assert "  b" in captured


def test_show_validation_targets_integration_label(rich_disabled, capsys):
    display_validation.show_validation_targets(["only"], 1, mock=False)
    assert "integration" in capsys.readouterr().out


def test_show_validation_failures_empty_returns_silently(rich_disabled, capsys):
    display_validation.show_validation_failures([])
    assert capsys.readouterr().out == ""


def test_show_validation_failures_no_rich_groups_by_target(rich_disabled, capsys):
    failures = [
        display_validation.ValidationFailure(
            kind="datasource", target="orders", message="missing column id"
        ),
        display_validation.ValidationFailure(
            kind="datasource", target="orders", message="bad type for total"
        ),
        display_validation.ValidationFailure(
            kind="concept", target="local.total", message="negative count"
        ),
    ]
    display_validation.show_validation_failures(failures, script_label="report.preql")
    out = capsys.readouterr().out
    assert "Validation Failures (report.preql)" in out
    assert "[datasource] orders" in out
    assert "missing column id" in out
    assert "bad type for total" in out
    assert "[concept] local.total" in out


def test_show_validation_success_unit_mode(rich_disabled, capsys):
    display_validation.show_validation_success(
        mock=True, datasource_count=3, duration_seconds=1.5
    )
    out = capsys.readouterr().out
    assert "Unit validation passed" in out
    assert "3 datasource(s)" in out
    assert "in 1.50s" in out


def test_show_validation_success_integration_no_duration(rich_disabled, capsys):
    display_validation.show_validation_success(mock=False, datasource_count=0)
    out = capsys.readouterr().out
    assert "Integration validation passed" in out
    assert "0 datasource(s)" in out
    assert "in " not in out


def test_validation_progress_factory_returns_probe_context():
    from trilogy.scripts.display_refresh import _ProbeProgressContext

    ctx = display_validation.validation_progress(5)
    assert isinstance(ctx, _ProbeProgressContext)


def test_validation_progress_context_advance_set_label_register(rich_disabled):
    # All operations should be no-ops without rich, never raising.
    ctx = display_validation.ValidationProgressContext(total=2)
    with ctx:
        ctx.set_label("one")
        ctx.advance()
        ctx.set_label("two")
        ctx.advance()
        ctx.register_capture_context(lambda: "ctx-id")


def test_validation_failure_dataclass_is_hashable():
    a = display_validation.ValidationFailure(kind="concept", target="x", message="m")
    b = display_validation.ValidationFailure(kind="concept", target="x", message="m")
    assert hash(a) == hash(b)
    assert a == b
