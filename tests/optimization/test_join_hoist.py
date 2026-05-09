from trilogy import Dialects


def _hoist_setup(executor):
    """Tiny per-day fact table with two channels — enough to set up two
    parallel cumulative windows whose comparison gets considered for hoist."""
    executor.execute_text("""
        key id int;
        property id.channel string;
        property id.day int;
        property id.amount int;

        datasource sales (
            id: id,
            channel: channel,
            day: day,
            amount: amount,
        )
        grain (id)
        query '''
        SELECT 1 AS id, 'WEB' AS channel, 1 AS day, 10 AS amount UNION ALL
        SELECT 2, 'STORE', 1, 5 UNION ALL
        SELECT 3, 'WEB', 2, 20 UNION ALL
        SELECT 4, 'STORE', 2, 30
        ''';
    """)


def test_hoist_preserves_concepts_referenced_via_output_lineage():
    """Regression: ``JoinHoist`` would strip a join the outer SELECT still
    needs through an alias's lineage. ``store_c <- alias(store_cume)`` makes
    ``store_cume`` reachable only via the joined CTE; without walking output
    lineage, the optimizer hoisted the join + cume comparison and the renderer
    fell back to ``INVALID_REFERENCE_BUG_<...>`` markers in the emitted SQL."""
    executor = Dialects.DUCK_DB.default_executor()
    _hoist_setup(executor)

    queries = executor.parse_text("""
        auto web_daily <- sum(amount ? channel = 'WEB') by day;
        auto store_daily <- sum(amount ? channel = 'STORE') by day;
        property day.web_cume <- sum web_daily over day order by day asc;
        property day.store_cume <- sum store_daily over day order by day asc;
        auto web_visible <- case when web_daily is not null then web_cume else null end;
        auto store_visible <- case when store_daily is not null then store_cume else null end;

        WHERE channel in ('WEB', 'STORE') and web_cume > store_cume
        SELECT
            day,
            web_visible as web_v,
            store_visible as store_v,
            web_cume as web_c,
            store_cume as store_c
        ORDER BY day asc;
    """)
    sql = executor.generate_sql(queries[-1])[0]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    rows = executor.execute_raw_sql(sql).fetchall()
    # day=1 has web_cume=10 < store_cume=5? actually 10 > 5 → row survives
    # day=2 has web_cume=30 vs store_cume=35 → fails predicate → only day=1 row
    assert len(rows) == 1
    assert rows[0].day == 1
    assert rows[0].web_c == 10
    assert rows[0].store_c == 5
