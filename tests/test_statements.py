
from preql.parser import parse
# from preql.compiler import compile
from os.path import dirname, join
from preql.parsing.exceptions import ParseError


def test_declarations():
    declarations = '''concept user_id int:key metadata(description="the description");
concept display_name string:property metadata(description="The display name ");
concept about_me string:property metadata(description="User provided description");
concept post_id int:key;
    '''
    parsed = parse(declarations)


def test_duplicate_declarations():
    declarations = '''concept user_id int:key metadata(description="the description");
concept display_name string:property metadata(description="The display name ");
concept about_me string:property metadata(description="User provided description");
concept post_id int:key;
concept post_id int:key;
    '''
    try:
        parsed = parse(declarations)
    except Exception as e:
        assert 'line 5' in str(e)


def test_datasource():
    text = '''concept user_id int:key metadata(description="the description");
concept display_name string:property metadata(description="The display name ");
concept about_me string:property metadata(description="User provided description");
concept post_id int:key;


datasource posts (
    user_id: user_id,
    id: post_id
    )
    grain (id)
    address bigquery-public-data.stackoverflow.post_history
;
'''
    parsed = parse(text)
    print(parsed)
