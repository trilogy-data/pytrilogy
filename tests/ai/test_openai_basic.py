import os
from pathlib import Path

import pytest

from trilogy import Environment
from trilogy.ai import Conversation, OpenAIProvider


def load_secret(key: str) -> str | None:
    """Load a secret from .env.secrets file in the current working directory."""
    secrets_path = Path.cwd() / ".env.secrets"

    if secrets_path.exists():
        with open(secrets_path, "r") as f:
            for line in f:
                line = line.strip()
                if line.startswith(f"{key}="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")

    # Fallback to environment variable
    return os.getenv(key)


env_path = Path(__file__).parent.parent / "modeling" / "faa"


def test_basic_openai_completion():
    # Load API key from .env.secrets file
    api_key = load_secret("OPENAI_KEY")

    if not api_key:
        pytest.skip("OPENAI_KEY not found in .env.secrets or environment variables")

    environment, _ = Environment(working_path=env_path).parse("""import flight;""")
    provider = OpenAIProvider(
        name="test_openai",
        api_key=api_key,
        model="gpt-5-chat-latest",
    )

    conversation = Conversation.create(
        id="test_convo_1",
        provider=provider,
    )

    response = conversation.generate_query(
        "number of flights by month in 2020", environment
    )

    assert "where dep_time.year = 2020" in response, response
    assert "count(id2) as flights" in response, response
