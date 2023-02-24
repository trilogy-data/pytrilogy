from preql.dialect.base import safe_quote


def test_safe_quote():
    for input, expected_output in [
        ["abc.123.def", '"abc"."123"."def"'],
        ["def", '"def"'],
    ]:
        assert expected_output == safe_quote(input, '"')
