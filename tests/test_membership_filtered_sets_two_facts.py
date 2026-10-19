from trilogy import Dialects
from trilogy.core.models.environment import Environment

MODEL = """
key customer_sk int;
property customer_sk.last_name string;
property customer_sk.state string;
key ss_id int;
key ss_customer_sk int;
property ss_id.ss_year int;
key ws_id int;
key ws_customer_sk int;
property ws_id.ws_year int;

datasource customers (sk: customer_sk, ln: last_name, st: state)
grain (customer_sk)
query '''select * from (values (1,'A','TX'),(2,'B','TX'),(3,'C','GA'),(4,'D','TX')) as t(sk,ln,st)''';

datasource store_sales (id: ss_id, cust: ss_customer_sk, yr: ss_year)
grain (ss_id)
query '''select * from (values (1,1,2002),(2,2,2002),(3,3,2001),(4,4,2002)) as t(id,cust,yr)''';

datasource web_sales (id: ws_id, cust: ws_customer_sk, yr: ws_year)
grain (ws_id)
query '''select * from (values (1,1,2002),(2,3,2002),(3,2,2001),(4,4,2002)) as t(id,cust,yr)''';

auto a <- ss_customer_sk ? ss_year = 2002;
auto b <- ws_customer_sk ? ws_year = 2002;
"""


def _rows(body: str):
    e = Dialects.DUCK_DB.default_executor(environment=Environment())
    e.parse_text(MODEL)
    return [tuple(r) for r in e.execute_query(body).fetchall()]


def test_filtered_memberships_from_two_facts():
    rows = _rows(
        "select last_name where customer_sk in a and customer_sk in b "
        "order by last_name asc;"
    )
    assert rows == [("A",), ("D",)]


def test_filtered_memberships_from_two_facts_grouped():
    rows = _rows(
        "select state, count(customer_sk) as c "
        "where customer_sk in a and customer_sk in b order by state asc;"
    )
    assert rows == [("TX", 2)]


def test_filtered_membership_and_exclusion_from_two_facts():
    rows = _rows(
        "select last_name where customer_sk in a and customer_sk not in b "
        "order by last_name asc;"
    )
    assert rows == [("B",)]
