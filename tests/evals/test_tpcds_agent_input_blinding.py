from __future__ import annotations

import re
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "evals"))

from common import agent_runner, prompts, schema_md
from common.categories import categories_for
from common.replay import _database_path
from tpcds_agent.spec import SPEC

BENCHMARK_NAME = re.compile(r"\btpc[-_ ]?ds\b", re.IGNORECASE)


def assert_blinded(label: str, text: str) -> None:
    match = BENCHMARK_NAME.search(text)
    assert match is None, f"{label} exposes benchmark name as {match.group(0)!r}"


def test_generated_schema_has_generic_heading(tmp_path: Path) -> None:
    db_path = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        connection.execute("create table orders (order_id bigint)")

    rendered = schema_md.generate_schema_md(db_path)

    assert rendered.startswith("# Database schema\n")
    assert_blinded("generated schema", rendered)


def test_agent_tasks_do_not_name_benchmark() -> None:
    entry = prompts.active_prompts(SPEC)[0]
    tasks = {
        category.key: category.build_task(SPEC, entry)
        for category in categories_for(SPEC).values()
    }
    tasks["combined_ingest"] = prompts.build_task(SPEC, 1)

    for label, task in tasks.items():
        assert_blinded(f"{label} task", task)


def test_agent_config_does_not_name_benchmark(tmp_path: Path) -> None:
    agent_runner.write_trilogy_toml(
        tmp_path,
        SPEC,
        provider="deepseek",
        model="deepseek-chat",
        max_iterations=1,
    )

    assert_blinded(
        "trilogy.toml",
        (tmp_path / "trilogy.toml").read_text(encoding="utf-8"),
    )


def test_replay_accepts_the_previous_database_filename(tmp_path: Path) -> None:
    legacy = tmp_path / "tpcds.duckdb"
    legacy.touch()

    assert _database_path(tmp_path, SPEC.db_filename) == legacy


def test_business_questions_do_not_name_benchmark() -> None:
    for entry in prompts.active_prompts(SPEC):
        assert_blinded(f"question {entry['id']}", entry["prompt"])


def test_installed_enriched_model_does_not_name_benchmark(tmp_path: Path) -> None:
    result = agent_runner.install_enriched_model(
        tmp_path,
        SPEC.default_enriched_dir,
        SPEC.enriched_skip_prefixes,
    )
    assert result["exit_code"] == 0

    for model in (tmp_path / "raw").glob("*.preql"):
        assert_blinded(model.name, model.read_text(encoding="utf-8"))
