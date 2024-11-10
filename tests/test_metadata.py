from trilogy import parse, Environment
from pathlib import Path


def test_metadata():
    env, _ = parse(
        """key user_id int metadata(description="the description");
property user_id.display_name string metadata(description="The display name");"""
    )

    assert env.concepts["user_id"].metadata.description == "the description"
    assert env.concepts["display_name"].metadata.description == "The display name"


def test_import_metadata():
    env = Environment(working_path=Path(__file__).parent)
    env, _ = parse(
        """import test_env as env; # Dragon metrics
        import test_env as env2;""",
        environment=env,
    )

    assert "Dragon metrics" in env.concepts["env.id"].metadata.description

    env2 = env.concepts["env2.id"]
    assert env2.namespace == "env2"
    assert env.concepts["env2.id"].metadata.description is None
