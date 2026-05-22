import pytest
from click.testing import CliRunner

from trilogy.scripts.trilogy import cli


def test_render_cli_html(tmp_path):
    pytest.importorskip("markdown")
    src = tmp_path / "report.md"
    src.write_text("# Title\n\nhello world\n", encoding="utf-8")
    out = tmp_path / "out.html"
    runner = CliRunner()
    result = runner.invoke(cli, ["render", str(src), "--to", "html", "-o", str(out)])
    if result.exception and not isinstance(result.exception, SystemExit):
        raise result.exception
    assert result.exit_code == 0, result.output
    assert "Rendered report" in result.output
    assert "hello world" in out.read_text(encoding="utf-8")


def test_render_cli_default_output(tmp_path):
    pytest.importorskip("markdown")
    src = tmp_path / "report.md"
    src.write_text("# Title\n", encoding="utf-8")
    result = CliRunner().invoke(cli, ["render", str(src), "--to", "html"])
    assert result.exit_code == 0, result.output
    assert (tmp_path / "report.html").exists()


def test_render_cli_surfaces_error(tmp_path, monkeypatch):
    src = tmp_path / "report.md"
    src.write_text("# Title\n", encoding="utf-8")

    def boom(*args, **kwargs):
        raise RuntimeError("kaboom")

    monkeypatch.setattr("trilogy.scripts.render.render_report", boom)
    result = CliRunner().invoke(cli, ["render", str(src), "--to", "html"])
    assert result.exit_code == 1
    assert "kaboom" in result.output
