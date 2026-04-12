from pathlib import Path

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


def test_generate_query_requires_submit_query_after_create_query():
    environment, _ = Environment(working_path=env_path).parse("""import flight;""")
    provider = ScriptedProvider(
        responses=[
            LLMResponse(
                text="",
                usage=UsageDict(prompt_tokens=0, completion_tokens=0, total_tokens=0),
                tool_calls=[
                    LLMToolCall(
                        name=TRILOGY_CREATE_QUERY_TOOL.name,
                        arguments={"query": VALID_QUERY},
                    )
                ],
            ),
            LLMResponse(
                text="",
                usage=UsageDict(prompt_tokens=0, completion_tokens=0, total_tokens=0),
                tool_calls=[
                    LLMToolCall(
                        name=TRILOGY_QUERY_TOOL.name,
                        arguments={"query": VALID_QUERY},
                    )
                ],
            ),
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
        TRILOGY_CREATE_QUERY_TOOL.name in message.content
        and "process_query returned" in message.content
        for message in conversation.messages
        if message.role == "user"
    )


def test_generate_query_reprompts_after_invalid_submit_query():
    environment, _ = Environment(working_path=env_path).parse("""import flight;""")
    provider = ScriptedProvider(
        responses=[
            LLMResponse(
                text="",
                usage=UsageDict(prompt_tokens=0, completion_tokens=0, total_tokens=0),
                tool_calls=[
                    LLMToolCall(
                        name=TRILOGY_QUERY_TOOL.name,
                        arguments={"query": INVALID_QUERY},
                    )
                ],
            ),
            LLMResponse(
                text="",
                usage=UsageDict(prompt_tokens=0, completion_tokens=0, total_tokens=0),
                tool_calls=[
                    LLMToolCall(
                        name=TRILOGY_QUERY_TOOL.name,
                        arguments={"query": VALID_QUERY},
                    )
                ],
            ),
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
        TRILOGY_QUERY_TOOL.name in message.content and "failed" in message.content
        for message in conversation.messages
        if message.role == "user"
    )
