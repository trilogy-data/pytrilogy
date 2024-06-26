from trilogy import parse


def test_metadata():
    env, _ = parse(
        """key user_id int metadata(description="the description");
property user_id.display_name string metadata(description="The display name");"""
    )

    assert env.concepts["user_id"].metadata.description == "the description"
    assert env.concepts["display_name"].metadata.description == "The display name"
