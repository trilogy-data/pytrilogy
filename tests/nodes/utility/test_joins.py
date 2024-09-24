from trilogy.dialect.common import render_join_concept
from trilogy.dialect.base import BaseDialect
from trilogy.core.models import CTE, Grain, QueryDatasource
from trilogy import parse


def test_render_join_concept():
    env, _ = parse(
        """key x int;
        key y int;
    
datasource x_source (
    y:y,
    y+1:x)
    grain(x)
address x_source;

        
        """
    )
    x = BaseDialect()
    concept = env.concepts["x"]
    y_concept = env.concepts["y"]
    ds = env.datasources["x_source"]
    rendered = render_join_concept(
        name="test",
        cte=CTE(
            name="test",
            output_columns=[concept, y_concept],
            grain=Grain(),
            source=QueryDatasource(
                input_concepts=[concept, y_concept],
                output_concepts=[concept, y_concept],
                datasources=[ds],
                grain=Grain(),
                joins=[],
                source_map={concept.address: {ds}, y_concept.address: {ds}},
            ),
            source_map={
                concept.address: [ds.identifier],
                y_concept.address: [ds.identifier],
            },
        ),
        render_expr=x.render_expr,
        quote_character=x.QUOTE_CHARACTER,
        inlined_ctes=["test"],
        concept=concept,
    )

    assert rendered == "x_source.`y` + 1"

    env, _ = parse(
        """key x int;
        key y int;
    
datasource x_source (
    y:y,
    raw('''y + 1'''):x)
    grain(x)
address x_source;

        
        """
    )
    x = BaseDialect()
    concept = env.concepts["x"]
    y_concept = env.concepts["y"]
    ds = env.datasources["x_source"]
    rendered = render_join_concept(
        name="test",
        cte=CTE(
            name="test",
            output_columns=[concept, y_concept],
            grain=Grain(),
            source=QueryDatasource(
                input_concepts=[concept, y_concept],
                output_concepts=[concept, y_concept],
                datasources=[ds],
                grain=Grain(),
                joins=[],
                source_map={concept.address: {ds}, y_concept.address: {ds}},
            ),
            source_map={
                concept.address: [ds.identifier],
                y_concept.address: [ds.identifier],
            },
        ),
        render_expr=x.render_expr,
        quote_character=x.QUOTE_CHARACTER,
        inlined_ctes=["test"],
        concept=concept,
    )

    assert rendered == "y + 1"
