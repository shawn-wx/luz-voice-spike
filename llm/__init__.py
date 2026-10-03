"""Pluggable LLM providers for the Luz voice spike.

Each provider implements LLMProvider.chat_stream() yielding text deltas.
Select via LLM_PROVIDER env var: groq | deepseek | stub (default: groq,
falls back to stub if no API key is configured).

Adding a new vendor: subclass LLMProvider in llm/<vendor>.py and register
it in get_provider() below.
"""
import os

from .base import LLMProvider  # noqa: F401  (re-exported for convenience)


def get_provider(name: str = None) -> "LLMProvider":
    name = (name or os.environ.get("LLM_PROVIDER", "groq")).lower()
    if name == "groq":
        from .groq import GroqProvider
        p = GroqProvider()
        if p.has_key():
            return p
        print("[llm] GROQ_API_KEY not set, falling back to stub")
        name = "stub"
    if name == "deepseek":
        from .deepseek import DeepSeekProvider
        p = DeepSeekProvider()
        if p.has_key():
            return p
        print("[llm] DEEPSEEK_API_KEY not set, falling back to stub")
        name = "stub"
    if name == "dashscope":
        from .dashscope import DashScopeProvider
        p = DashScopeProvider()
        if p.has_key():
            return p
        print("[llm] DASHSCOPE_API_KEY not set, falling back to stub")
        name = "stub"
    if name == "stub":
        from .stub import StubProvider
        return StubProvider()
    # future vendors: gemini, mistral, qwen, glm, ...
    raise ValueError(f"Unknown LLM provider: {name}")
