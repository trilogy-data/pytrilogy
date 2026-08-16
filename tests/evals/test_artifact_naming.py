from __future__ import annotations

from evals.common.main import model_namespace, run_artifact_slug


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
