from trilogy.parser import parse


def test_declarations():
    declarations = """key namespace.user_id int metadata(description="the description");
    metric namespace.count <- count(namespace.user_id);
    metric namespace.distinct_count <- count_distinct(namespace.user_id); #the distinct count of user ids
    """
    env, _ = parse(declarations)

    assert env.concepts["namespace.user_id"].namespace == "namespace"
    assert env.concepts["namespace.count"].namespace == "namespace"
    assert (
        env.concepts["namespace.distinct_count"].metadata.description
        == "the distinct count of user ids"
    )

    assert env.concepts["namespace.user_id"].metadata.description == "the description"
