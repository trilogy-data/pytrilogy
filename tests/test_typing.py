from trilogy import Dialects


def test_typing():
    env = Dialects.DUCK_DB.default_executor()
    env.environment.parse(
        """

type email string;

key customer_id int;
property customer_id.email string::email;

def is_valid_email(email) -> contains(email, '@');

datasource customers (
    id:customer_id,
    email: email
)
grain (customer_id)
query '''
select 1 as id, 'bright@gmail.com' as email
union all
select 2 as id, 'funky@hotmail.com' as email
''';


"""
    )

    results = env.execute_query(
        """SELECT
customer_id,
@is_valid_email(email)->valid;"""
    )

    for row in results.fetchall():
        assert row.valid is True

    assert "email" in env.environment.data_types

    assert env.environment.concepts["email"].datatype.traits == ["email"]
