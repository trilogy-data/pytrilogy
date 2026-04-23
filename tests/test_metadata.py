from trilogy import parse


def test_metadata():
    env, _ = parse("""key user_id int metadata(description="the description");
property user_id.display_name string metadata(description="The display name");""")

    assert env.concepts["user_id"].metadata.description == "the description"
    assert env.concepts["display_name"].metadata.description == "The display name"


def test_properties_block_comment_description():
    env, _ = parse("""key user_id int;
properties user_id (
    last_name string, # Customer last name
    first_name string, # Customer first name
);""")

    assert env.concepts["last_name"].metadata.description == " Customer last name"
    assert env.concepts["first_name"].metadata.description == " Customer first name"


def test_concept_block_comment_description_mixed():
    env, _ = parse("""key hash_comment int; # hashed note
key slash_comment int; // slashed note
""")

    assert env.concepts["hash_comment"].metadata.description == " hashed note"
    assert env.concepts["slash_comment"].metadata.description == " slashed note"


# def test_import_metadata():
#     env = Environment(working_path=Path(__file__).parent)
#     env, _ = parse(
#         """import test_env as env; # Dragon metrics
#         import test_env as env2;""",
#         environment=env,
#     )

#     assert "Dragon metrics" in env.concepts["env.id"].metadata.description

#     env2 = env.concepts["env2.id"]
#     assert env2.namespace == "env2"
#     assert env.concepts["env2.id"].metadata.description is None
