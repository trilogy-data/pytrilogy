from dataclasses import dataclass
from typing import Literal, Union

from trilogy import Environment
from trilogy.ai.models import LLMMessage, LLMRequestOptions
from trilogy.ai.prompts import TRILOGY_LEAD_IN, create_query_prompt
from trilogy.ai.providers.base import LLMProvider


@dataclass
class Conversation:
    id: str
    messages: list[LLMMessage]
    provider: LLMProvider

    @classmethod
    def create(
        cls, id: str, provider: LLMProvider, model_prompt: str = TRILOGY_LEAD_IN
    ) -> "Conversation":
        system_message = LLMMessage(role="system", content=model_prompt)
        messages = [system_message]
        return cls(id=id, messages=messages, provider=provider)

    def add_message(
        self,
        message: Union[LLMMessage, str],
        role: Literal["user", "assistant"] = "user",
    ) -> None:
        """
        Add a message to the conversation.

        Args:
            message: Either an LLMMessage object or a string content
            role: The role for the message if a string is provided (default: 'user')
        """
        if isinstance(message, str):
            message = LLMMessage(role=role, content=message)
        self.messages.append(message)

    def get_response(self) -> LLMMessage:
        options = LLMRequestOptions()
        response = self.provider.generate_completion(options, history=self.messages)
        response_message = LLMMessage(role="assistant", content=response.text)
        self.add_message(response_message)
        return response_message

    def extract_response(self, content: str) -> str:
        # get contents in triple backticks
        content = content.replace('"""', "```")
        if "```" in content:
            parts = content.split("```")
            if len(parts) >= 3:
                return parts[1].strip()
        return content

    def generate_query(self, user_input: str, environment: Environment) -> str:
        self.add_message(create_query_prompt(user_input, environment), role="user")
        response_message = self.get_response()
        return self.extract_response(response_message.content)
