"""Public command for Trilogy CLI - fetch models from trilogy-public-models."""

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import click

from trilogy.scripts.display import (
    print_error,
    print_info,
    print_success,
    print_warning,
)

RAW_BASE = "https://raw.githubusercontent.com/trilogy-data/trilogy-public-models/main"
PAGES_PREFIX = "https://trilogy-data.github.io/trilogy-public-models/"
INDEX_URL = f"{RAW_BASE}/studio/index.json"
STUDIO_BASE = f"{RAW_BASE}/studio"
EXAMPLE_MARKER = "/examples/"


@dataclass(frozen=True)
class ModelEntry:
    name: str
    filename: str
    engine: str
    description: str
    tags: tuple[str, ...]


def _normalize_url(url: str) -> str:
    """Rewrite github.io asset URLs to raw.githubusercontent.com for reliable fetches."""
    if url.startswith(PAGES_PREFIX):
        return RAW_BASE + "/" + url[len(PAGES_PREFIX) :]
    return url


def _http_get(url: str, timeout: float = 15.0) -> bytes:
    req = Request(_normalize_url(url), headers={"User-Agent": "trilogy-cli"})
    with urlopen(req, timeout=timeout) as resp:  # noqa: S310 - known HTTPS endpoint
        return resp.read()


def _fetch_index() -> list[ModelEntry]:
    raw = _http_get(INDEX_URL)
    data = json.loads(raw.decode("utf-8"))
    entries: list[ModelEntry] = []
    for item in data.get("files", []):
        entries.append(
            ModelEntry(
                name=item["name"],
                filename=item["filename"],
                engine=item.get("engine", ""),
                description=(item.get("description", "") or "").strip(),
                tags=tuple(item.get("tags", [])),
            )
        )
    entries.sort(key=lambda e: e.name)
    return entries


def _fetch_model_manifest(filename: str) -> dict[str, Any]:
    url = f"{STUDIO_BASE}/{filename}"
    return json.loads(_http_get(url).decode("utf-8"))


def _short_description(text: str, width: int = 60) -> str:
    for line in text.splitlines():
        cleaned = line.lstrip("# ").strip()
        if cleaned:
            return cleaned if len(cleaned) <= width else cleaned[: width - 1] + "…"
    return ""


def _component_target(component: dict[str, Any], root: Path) -> Path:
    url = component["url"]
    parsed = urlparse(url)
    filename = Path(parsed.path).name
    if EXAMPLE_MARKER in parsed.path:
        return root / "examples" / filename
    return root / filename


def _is_example(component: dict[str, Any]) -> bool:
    return EXAMPLE_MARKER in urlparse(component["url"]).path


@click.group()
def public() -> None:
    """Work with trilogy-public-models hosted at trilogy-data/trilogy-public-models."""


@public.command("list")
@click.option("--engine", "-e", type=str, default=None, help="Filter by engine.")
@click.option("--tag", "-t", type=str, default=None, help="Filter by tag.")
def list_cmd(engine: str | None, tag: str | None) -> None:
    """List available public models from the studio index."""
    try:
        entries = _fetch_index()
    except (HTTPError, URLError, TimeoutError) as exc:
        print_error(f"Failed to fetch index: {exc}")
        raise click.exceptions.Exit(1) from exc

    if engine:
        entries = [e for e in entries if e.engine == engine]
    if tag:
        entries = [e for e in entries if tag in e.tags]

    if not entries:
        print_warning("No models matched the filters.")
        return

    name_width = max(len(e.name) for e in entries)
    engine_width = max(len(e.engine) for e in entries)
    print_info(f"Found {len(entries)} model(s):")
    for e in entries:
        click.echo(
            f"  {e.name.ljust(name_width)}  "
            f"{e.engine.ljust(engine_width)}  "
            f"{_short_description(e.description)}"
        )
    click.echo()
    click.echo("Fetch a model with: trilogy public fetch <name> [--path <dir>]")


_SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]")


@public.command("fetch")
@click.argument("model", type=str)
@click.option(
    "--path",
    "-p",
    type=click.Path(file_okay=False, resolve_path=True),
    default=None,
    help="Target directory. Defaults to ./<model>.",
)
@click.option(
    "--examples/--no-examples",
    default=True,
    help="Also download example scripts and dashboards (default: include).",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Overwrite existing files if the target directory is not empty.",
)
def fetch_cmd(model: str, path: str | None, examples: bool, force: bool) -> None:
    """Fetch a public model by name, writing its files locally.

    Creates a directory containing all source files, setup scripts, and
    (optionally) example assets. Afterwards you can `cd` into it and run
    `trilogy refresh` / `trilogy serve` against the model.
    """
    if _SAFE_NAME.search(model):
        print_error(f"Invalid model name: {model!r}")
        raise click.exceptions.Exit(1)

    try:
        entries = _fetch_index()
    except (HTTPError, URLError, TimeoutError) as exc:
        print_error(f"Failed to fetch index: {exc}")
        raise click.exceptions.Exit(1) from exc

    entry = next((e for e in entries if e.name == model), None)
    if entry is None:
        print_error(f"No public model named {model!r}.")
        click.echo("Available models: " + ", ".join(e.name for e in entries))
        raise click.exceptions.Exit(1)

    try:
        manifest = _fetch_model_manifest(entry.filename)
    except (HTTPError, URLError, TimeoutError) as exc:
        print_error(f"Failed to fetch manifest for {model}: {exc}")
        raise click.exceptions.Exit(1) from exc

    target_root = Path(path) if path else Path.cwd() / model
    if target_root.exists() and any(target_root.iterdir()) and not force:
        print_error(
            f"Target directory {target_root} is not empty. "
            "Pass --force to overwrite, or choose another path."
        )
        raise click.exceptions.Exit(1)
    target_root.mkdir(parents=True, exist_ok=True)

    components = [
        c
        for c in manifest.get("components", [])
        if c.get("type") in {"trilogy", "sql"}
        or (examples and c.get("type") == "dashboard")
    ]
    if not examples:
        components = [c for c in components if not _is_example(c)]

    print_info(f"Fetching {entry.name} ({entry.engine}) into {target_root}")
    written: list[Path] = []
    for component in components:
        dest = _component_target(component, target_root)
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            payload = _http_get(component["url"])
        except (HTTPError, URLError, TimeoutError) as exc:
            print_warning(f"  skipped {component.get('name')}: {exc}")
            continue
        dest.write_bytes(payload)
        written.append(dest)
        rel = dest.relative_to(target_root)
        click.echo(f"  wrote {rel}")

    readme_path = target_root / "README.md"
    if not readme_path.exists() and manifest.get("description"):
        readme_path.write_text(manifest["description"] + "\n", encoding="utf-8")
        written.append(readme_path)

    toml_path = target_root / "trilogy.toml"
    if not toml_path.exists():
        dialect = "duck_db" if entry.engine == "duckdb" else entry.engine
        setup_sql = [
            str(p.relative_to(target_root).as_posix())
            for p in written
            if p.suffix == ".sql"
        ]
        lines = ["[engine]", f'dialect = "{dialect}"', ""]
        if setup_sql:
            lines += [
                "[setup]",
                "sql = [" + ", ".join(f'"{s}"' for s in setup_sql) + "]",
                "",
            ]
        toml_path.write_text("\n".join(lines), encoding="utf-8")
        written.append(toml_path)
        click.echo("  wrote trilogy.toml")

    print_success(f"Fetched {len(written)} file(s) for {entry.name}.")
    click.echo("\nNext steps:")
    click.echo(f"  cd {target_root}")
    click.echo("  trilogy refresh .        # build any managed assets")
    click.echo("  trilogy serve .          # open the studio UI")
