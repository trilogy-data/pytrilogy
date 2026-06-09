"""Run the basic Google completion flow and dump the full LLM conversation
trace to local_scripts/google_trace.txt for manual sanity-checking."""

from pathlib import Path

from tests.ai.test_providers_basic import (
    GOOGLE_LATEST_MODEL,
    _format_conversation,
    env_path,
    validate_response,
)
from tests.conftest import load_secret
from trilogy import Environment
from trilogy.ai import Provider
from trilogy.ai.conversation import Conversation
from trilogy.ai.execute import build_provider

OUT = Path(__file__).parent / "google_trace.txt"


def main() -> None:
    api_key = load_secret("GOOGLE_API_KEY")
    if not api_key:
        raise SystemExit("GOOGLE_API_KEY not found")

    environment, _ = Environment(working_path=env_path).parse("""import flight;""")
    conversation = Conversation.create(
        provider=build_provider(Provider.GOOGLE, GOOGLE_LATEST_MODEL, api_key)
    )
    response = conversation.generate_query(
        user_input="number of flights by month in 2020", environment=environment
    )
    transcript = _format_conversation(conversation)
    OUT.write_text(transcript, encoding="utf-8")

    _, parsed = environment.parse(response)
    validate_response(response, parsed[-1], environment, transcript)
    print(f"final query: {response}")
    print(f"transcript written to {OUT}")


if __name__ == "__main__":
    main()
