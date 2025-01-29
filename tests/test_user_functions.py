from trilogy import Dialects


def test_user_function_def():
    x = Dialects.DUCK_DB.default_executor()

    results = x.execute_query(
        """
def percent_ratio(a, b, digits=3) -> round(a::float / b * 100, digits);

                 
select @percent_ratio(10, 100) as ratio;
                 
                 """
    )

    assert results.fetchall()[0].ratio == 10.0
