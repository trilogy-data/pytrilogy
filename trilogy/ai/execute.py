from os import environ

from trilogy import Environment
from trilogy.ai.conversation import Conversation
from trilogy.ai.enums import Provider
from trilogy.ai.providers.base import LLMProvider


def text_to_query(
    environment: Environment,
    user_input: str,
    provider: Provider,
    model: str,
    secret: str | None = None,
) -> str:
    llm_provider: LLMProvider

    if provider == Provider.OPENAI:
        from trilogy.ai.providers.openai import OpenAIProvider

        secret = secret or environ.get("OPENAI_API_KEY")
        if not secret:
            raise ValueError("OpenAI API key is required")
        llm_provider = OpenAIProvider(
            name="openai",
            api_key=secret,
            model=model,
        )
    elif provider == Provider.ANTHROPIC:
        from trilogy.ai.providers.anthropic import AnthropicProvider

        secret = secret or environ.get("ANTHROPIC_API_KEY")
        if not secret:
            raise ValueError("Anthropic API key is required")
        llm_provider = AnthropicProvider(
            name="anthropic",
            api_key=secret,
            model=model,
        )
    elif provider == Provider.GOOGLE:
        from trilogy.ai.providers.google import GoogleProvider

        secret = secret or environ.get("GOOGLE_API_KEY")
        if not secret:
            raise ValueError("Google API key is required")
        llm_provider = GoogleProvider(
            name="google",
            api_key=secret,
            model=model,
        )
    else:
        raise ValueError(f"Unsupported provider: {provider}")
    conversation = Conversation.create(
        id="test_convo_1",
        provider=llm_provider,
    )

    response = conversation.generate_query(
        user_input=user_input, environment=environment
    )

    return response
