from pathlib import Path

import pytest

from trilogy import Environment
from trilogy.ai.conversation import Conversation
from trilogy.ai.enums import Provider
from trilogy.ai.models import (
    LLMMessage,
    LLMRequestOptions,
    LLMResponse,
    LLMToolCall,
    UsageDict,
)
from trilogy.ai.prompts import TRILOGY_CREATE_QUERY_TOOL, TRILOGY_QUERY_TOOL
from trilogy.ai.providers.base import LLMProvider

env_path = Path(__file__).parent.parent / "modeling" / "faa"
VALID_QUERY = "select dep_time.month, count(id2) as total_flights where dep_time.year = 2020 order by dep_time.month asc"
INVALID_QUERY = "select missing.field where dep_time.year = 2020"


class ScriptedProvider(LLMProvider):
    def __init__(self, responses: list[LLMResponse]):
        super().__init__("scripted", "test-key", "test-model", Provider.OPENAI)
        self.responses = responses
        self.call_count = 0

    def generate_completion(
        self, options: LLMRequestOptions, history: list[LLMMessage]
    ) -> LLMResponse:
        self.call_count += 1
        return self.responses.pop(0)


def make_response(*tool_calls: LLMToolCall, text: str = "") -> LLMResponse:
    return LLMResponse(
        text=text,
        usage=UsageDict(prompt_tokens=0, completion_tokens=0, total_tokens=0),
        tool_calls=list(tool_calls),
    )


def make_tool_call(name: str, query: str | None = None) -> LLMToolCall:
    arguments = {} if query is None else {"query": query}
    return LLMToolCall(name=name, arguments=arguments)


def user_messages(conversation: Conversation) -> list[str]:
    return [
        message.content for message in conversation.messages if message.role == "user"
    ]


def test_generate_query_requires_submit_query_after_create_query():
    environment, _ = Environment(working_path=env_path).parse("""import flight;""")
    provider = ScriptedProvider(
        responses=[
            make_response(make_tool_call(TRILOGY_CREATE_QUERY_TOOL.name, VALID_QUERY)),
            make_response(make_tool_call(TRILOGY_QUERY_TOOL.name, VALID_QUERY)),
        ]
    )

    conversation = Conversation.create(provider=provider)
    response = conversation.generate_query(
        user_input="number of flights by month in 2020",
        environment=environment,
    )

    assert provider.call_count == 2
    assert response == f"{VALID_QUERY};"
    assert any(
        TRILOGY_CREATE_QUERY_TOOL.name in message
        and "process_query returned" in message
        for message in user_messages(conversation)
    )


def test_generate_query_accepts_create_and_submit_in_one_response():
    environment, _ = Environment(working_path=env_path).parse("""import flight;""")
    provider = ScriptedProvider(
        responses=[
            make_response(
                make_tool_call(TRILOGY_CREATE_QUERY_TOOL.name, VALID_QUERY),
                make_tool_call(TRILOGY_QUERY_TOOL.name, VALID_QUERY),
            )
        ]
    )

    conversation = Conversation.create(provider=provider)
    response = conversation.generate_query(
        user_input="number of flights by month in 2020",
        environment=environment,
    )

    assert provider.call_count == 1
    assert response == f"{VALID_QUERY};"


def test_generate_query_reprompts_after_invalid_submit_query():
    environment, _ = Environment(working_path=env_path).parse("""import flight;""")
    provider = ScriptedProvider(
        responses=[
            make_response(make_tool_call(TRILOGY_QUERY_TOOL.name, INVALID_QUERY)),
            make_response(make_tool_call(TRILOGY_QUERY_TOOL.name, VALID_QUERY)),
        ]
    )

    conversation = Conversation.create(provider=provider)
    response = conversation.generate_query(
        user_input="number of flights by month in 2020",
        environment=environment,
    )

    assert provider.call_count == 2
    assert response == f"{VALID_QUERY};"
    assert any(
        TRILOGY_QUERY_TOOL.name in message and "failed" in message
        for message in user_messages(conversation)
    )


def test_generate_query_reprompts_after_invalid_create_query():
    environment, _ = Environment(working_path=env_path).parse("""import flight;""")
    provider = ScriptedProvider(
        responses=[
            make_response(
                make_tool_call(TRILOGY_CREATE_QUERY_TOOL.name, INVALID_QUERY)
            ),
            make_response(make_tool_call(TRILOGY_QUERY_TOOL.name, VALID_QUERY)),
        ]
    )

    conversation = Conversation.create(provider=provider)
    response = conversation.generate_query(
        user_input="number of flights by month in 2020",
        environment=environment,
    )

    assert provider.call_count == 2
    assert response == f"{VALID_QUERY};"
    assert any(
        TRILOGY_CREATE_QUERY_TOOL.name in message and "failed" in message
        for message in user_messages(conversation)
    )


def test_generate_query_reprompts_when_no_tool_call_is_returned():
    environment, _ = Environment(working_path=env_path).parse("""import flight;""")
    provider = ScriptedProvider(
        responses=[
            make_response(text="plain text is not allowed here"),
            make_response(make_tool_call(TRILOGY_QUERY_TOOL.name, VALID_QUERY)),
        ]
    )

    conversation = Conversation.create(provider=provider)
    response = conversation.generate_query(
        user_input="number of flights by month in 2020",
        environment=environment,
    )

    assert provider.call_count == 2
    assert response == f"{VALID_QUERY};"
    assert any(
        "You must call either" in message for message in user_messages(conversation)
    )


def test_generate_query_reprompts_after_unknown_tool_call():
    environment, _ = Environment(working_path=env_path).parse("""import flight;""")
    provider = ScriptedProvider(
        responses=[
            make_response(make_tool_call("not_a_real_tool", VALID_QUERY)),
            make_response(make_tool_call(TRILOGY_QUERY_TOOL.name, VALID_QUERY)),
        ]
    )

    conversation = Conversation.create(provider=provider)
    response = conversation.generate_query(
        user_input="number of flights by month in 2020",
        environment=environment,
    )

    assert provider.call_count == 2
    assert response == f"{VALID_QUERY};"
    assert any(
        "Unknown tool call" in message for message in user_messages(conversation)
    )


def test_generate_query_reprompts_after_missing_query_argument():
    environment, _ = Environment(working_path=env_path).parse("""import flight;""")
    provider = ScriptedProvider(
        responses=[
            make_response(make_tool_call(TRILOGY_QUERY_TOOL.name)),
            make_response(make_tool_call(TRILOGY_QUERY_TOOL.name, VALID_QUERY)),
        ]
    )

    conversation = Conversation.create(provider=provider)
    response = conversation.generate_query(
        user_input="number of flights by month in 2020",
        environment=environment,
    )

    assert provider.call_count == 2
    assert response == f"{VALID_QUERY};"
    assert any(
        "without a non-empty query" in message
        for message in user_messages(conversation)
    )

    def test_extract_response_markdown_fallback():
        provider = ScriptedProvider(responses=[])
        conversation = Conversation.create(provider=provider)

        response = conversation.extract_response(
            'Reasoning: test """```trilogy\nselect 1 as one;\n```"""'
        )

        assert response == "select 1 as one;"

    def test_extract_response_prefers_submit_tool_call():
        provider = ScriptedProvider(responses=[])
        conversation = Conversation.create(provider=provider)

        response = conversation.extract_response(
            make_response(make_tool_call(TRILOGY_QUERY_TOOL.name, "select 4 as four"))
        )

        assert response == "select 4 as four"

    def test_extract_response_returns_response_text_without_tool_call():
        provider = ScriptedProvider(responses=[])
        conversation = Conversation.create(provider=provider)

        response = conversation.extract_response(
            make_response(text='prefix """select 5 as five;""" suffix')
        )

        assert response == "select 5 as five;"

    def test_generate_query_raises_after_attempt_limit():
        environment, _ = Environment(working_path=env_path).parse("""import flight;""")
        provider = ScriptedProvider(
            responses=[make_response(text="no tool call") for _ in range(2)]
        )
        conversation = Conversation.create(provider=provider)

        with pytest.raises(Exception, match="Failed to generate a valid query"):
            conversation.generate_query(
                user_input="number of flights by month in 2020",
                environment=environment,
                attempts=2,
            )

        assert provider.call_count == 2
