from preql.core.models import CTE, Grain, QueryDatasource


def test_cte_merge(test_environment, test_environment_graph):
    datasource = list(test_environment.datasources.values())[0]
    outputs = [c.concept for c in datasource.columns]
    output_map = {c: datasource.name for c in outputs}
    a = CTE(
        name="test",
        output_columns=[outputs[0]],
        related_columns=[],
        grain=Grain(),
        source=QueryDatasource(
            input_concepts=[outputs[0]],
            output_concepts=[outputs[0]],
            datasources=datasource,
            grain=Grain(),
            joins=[],
            source_map=output_map,
        ),
        source_map=output_map,
    )
    b = CTE(
        name="testb",
        output_columns=outputs,
        related_columns=[],
        grain=Grain(),
        source=QueryDatasource(
            input_concepts=outputs,
            output_concepts=outputs,
            datasources=datasource,
            grain=Grain(),
            joins=[],
            source_map=output_map,
        ),
        source_map=output_map,
    )

    merged = a + b
    assert merged.output_columns == outputs
