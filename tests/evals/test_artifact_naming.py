from __future__ import annotations

from evals.common.main import (
    chart_artifact_slug,
    model_namespace,
    run_artifact_slug,
)


def test_model_namespace_includes_provider_model_and_effort() -> None:
    assert (
        model_namespace("openai", "vendor/gpt:luna", "max")
        == "openai_vendor-gpt-luna_effort-max"
    )


def test_run_artifact_slug_includes_category() -> None:
    assert (
        run_artifact_slug(
            "20260814-132003",
            "sql schema",
            "openai",
            "gpt-5.6-luna",
            "max",
        )
        == "20260814-132003_sql-schema_openai_gpt-5.6-luna_effort-max"
    )


def test_chart_slug_drops_the_timestamp() -> None:
    assert (
        chart_artifact_slug("20260814-132003_sql-schema_openai_gpt-5.6-luna_effort-max")
        == "sql-schema_openai_gpt-5.6-luna_effort-max"
    )


def test_chart_slug_keeps_an_ab_label_from_output_dir() -> None:
    assert (
        chart_artifact_slug("20260817-163552_ingest_deepseek_deepseek-v4-flash_docsctl")
        == "ingest_deepseek_deepseek-v4-flash_docsctl"
    )


def test_chart_slug_passes_through_a_name_without_a_timestamp() -> None:
    assert chart_artifact_slug("scratch_run") == "scratch_run"
