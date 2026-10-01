"""Pluggable LLM providers for the Luz voice spike.

Each provider implements LLMProvider.chat_stream() yielding text deltas.
Select via LLM_PROVIDER env var: deepseek | stub (default: deepseek,
falls back to stub if no API key is configured).

Adding a new vendor: subclass LLMProvider in llm/<vendor>.py and register
it in get_provider() below.
"""
import os

from .base import LLMProvider  # noqa: F401  (re-exported for convenience)


def get_provider(name: str = None) -> "LLMProvider":
    name = (name or os.environ.get("LLM_PROVIDER", "deepseek")).lower()
    if name == "deepseek":
        from .deepseek import DeepSeekProvider
        p = DeepSeekProvider()
        if p.has_key():
            return p
        print("[llm] DEEPSEEK_API_KEY not set, falling back to stub")
        name = "stub"
    if name == "stub":
        from .stub import StubProvider
        return StubProvider()
    # future vendors: openai, anthropic, gemini, ...
    raise ValueError(f"Unknown LLM provider: {name}")
