from preql.core.models import Select, Grain
from preql.parser import parse

def test_declarations():
    declarations = """key namespace.user_id int metadata(description="the description");
    metric namespace.count <- count(namespace.user_id);
    """
    env, _ = parse(declarations)

    assert env.concepts['namespace.user_id'].namespace == 'namespace'
    assert env.concepts['namespace.count'].namespace == 'namespace'
