"""End-to-end test that parses README.md and runs the Quick Start block.

Hits the live trilogy-public-models host and raw.githubusercontent.com — the
point is to catch drift between the README, the CLI, and the remote model
assets.
"""

import os
import re
import shlex
from pathlib import Path

from click.testing import CliRunner

from trilogy.scripts.trilogy import cli

README = Path(__file__).resolve().parents[2] / "README.md"


def _extract_quickstart_commands() -> list[str]:
    text = README.read_text(encoding="utf-8")
    match = re.search(
        r"### Quick Start\b.*?```bash\n(.*?)\n```",
        text,
        re.DOTALL,
    )
    assert match, "Could not locate Quick Start bash block in README.md"
    lines: list[str] = []
    for raw in match.group(1).splitlines():
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        lines.append(stripped)
    return lines


def test_readme_quickstart_runs(tmp_path: Path) -> None:
    commands = _extract_quickstart_commands()
    assert any(c.startswith("trilogy public fetch") for c in commands)
    assert any(c.startswith("trilogy refresh") for c in commands)

    runner = CliRunner()
    original_cwd = Path.cwd()
    cwd = tmp_path
    try:
        for cmd in commands:
            tokens = shlex.split(cmd)
            if tokens[0] == "cd":
                target = (cwd / tokens[1]).resolve()
                assert target.is_dir(), f"cd target missing: {target}"
                cwd = target
                continue
            assert tokens[0] == "trilogy", f"unexpected command in block: {cmd}"
            sub = tokens[1:]
            if sub and sub[0] == "serve":
                continue

            os.chdir(cwd)
            result = runner.invoke(cli, sub, catch_exceptions=False)

            # refresh exits 2 when everything is already up to date
            ok_codes = {0, 2} if sub and sub[0] == "refresh" else {0}
            assert result.exit_code in ok_codes, (
                f"trilogy {' '.join(sub)}\n"
                f"exit_code={result.exit_code}\n"
                f"output:\n{result.output}"
            )
    finally:
        os.chdir(original_cwd)
