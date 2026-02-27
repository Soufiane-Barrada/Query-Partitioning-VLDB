import os
import requests
from typing import Any, Dict, Optional, List
from pydantic import Field
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.messages import (
    AIMessage, AIMessageChunk, BaseMessage, ChatMessage,
    HumanMessage, SystemMessage, ToolMessage,
)

# Helper Converters

def _convert_message_to_dict(message: BaseMessage) -> dict:
    message_dict = {"role": "", "content": message.content}
    if isinstance(message, HumanMessage):
        message_dict["role"] = "user"
    elif isinstance(message, AIMessage):
        message_dict["role"] = "assistant"
    elif isinstance(message, SystemMessage):
        message_dict["role"] = "system"
    elif isinstance(message, ToolMessage):
        message_dict["role"] = "tool"
        message_dict["tool_call_id"] = message.tool_call_id
    else:
        raise ValueError(f"Unknown message type: {type(message)}")
    if message.name:
        message_dict["name"] = message.name
    return message_dict


def _convert_dict_to_message(response_dict: Dict[str, Any]) -> BaseMessage:
    role = response_dict["role"]
    content = response_dict.get("content", "")
    if role == "user":
        return HumanMessage(content=content)
    elif role == "assistant":
        additional_kwargs = {}
        if tool_calls := response_dict.get("tool_calls"):
            additional_kwargs["tool_calls"] = tool_calls
        return AIMessageChunk(content=content, additional_kwargs=additional_kwargs)
    elif role == "system":
        return SystemMessage(content=content)
    elif role == "tool":
        return ToolMessage(
            content=content,
            tool_call_id=response_dict["tool_call_id"],
            name=response_dict.get("name"),
        )
    else:
        return ChatMessage(content=content, role=role)


# LLM Wrapper

class RITSChat(BaseChatModel):
    endpoint_url: str
    model_name: str
    max_tokens: int = 4096
    temperature: float = 0.7

    best_of: int = 1
    frequency_penalty: float = 0
    logit_bias: Optional[Dict[str, float]] = Field(default_factory=dict)
    min_tokens: int = 0
    n: int = 1
    presence_penalty: float = 0
    seed: Optional[int] = None
    top_p: float = 0.8
    top_k: int = 20
    streaming: bool = False
    repetition_penalty: float = 1.05
    length_penalty: float = 1
    ignore_eos: bool = False
    stop: Optional[List[str]] = None
    
    @property
    def _default_params(self) -> Dict[str, Any]:
        return {
            "best_of": self.best_of, "frequency_penalty": self.frequency_penalty,
            "logit_bias": self.logit_bias, "max_tokens": self.max_tokens,
            "min_tokens": self.min_tokens, "n": self.n,
            "presence_penalty": self.presence_penalty, "seed": self.seed,
            "temperature": self.temperature, "top_p": self.top_p, "top_k": self.top_k,
            "repetition_penalty": self.repetition_penalty, "length_penalty": self.length_penalty,
            "ignore_eos": self.ignore_eos,
        }

    @staticmethod
    def _convert_messages_to_dicts(messages: list[BaseMessage]) -> list[dict]:
        return [_convert_message_to_dict(message) for message in messages]

    def _create_chat_result(self, response: Dict) -> ChatResult:
        generations = []
        for choice in response["choices"]:
            message = _convert_dict_to_message(choice["message"])
            gen = ChatGeneration(
                message=message,
                generation_info=dict(finish_reason=choice.get("finish_reason")),
            )
            generations.append(gen)
        token_usage = response.get("usage", {})
        llm_output = {
            "token_usage": token_usage, "model_name": self.model_name,
            "system_fingerprint": response.get("system_fingerprint", ""),
        }
        return ChatResult(generations=generations, llm_output=llm_output)

    def _generate(
        self, messages: list[BaseMessage], stop: Optional[list[str]] = None, **kwargs: Any
    ) -> ChatResult:
        params = {**self._default_params, **kwargs}
        extra_body = params.pop("extra_body", {})

        payload = {
            "messages": self._convert_messages_to_dicts(messages),
            "stop": stop or [],
            "model": self.model_name,
            **params,
            **extra_body,
        }
        response = requests.post(
            url=self.endpoint_url,
            headers={"RITS_API_KEY": os.environ.get("RITS_API_KEY")},
            json=payload,
        )
        if response.status_code != 200:
            raise ValueError(
                f"Failed to call RITS: {response.text} with status code {response.status_code}"
            )
        return self._create_chat_result(response.json())

    @property
    def _llm_type(self) -> str:
        return "RITS-model"
