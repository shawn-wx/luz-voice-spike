"""Stub provider: fixed warm reply, for offline tests and fallback."""
import asyncio

from .base import LLMProvider

STUB_REPLY = "¡Hola! Qué gusto escucharte. Cuéntame, ¿cómo va tu día?"


class StubProvider(LLMProvider):
    name = "stub"

    async def chat_stream(self, messages, system,
                          max_tokens=150, temperature=0.7):
        # Yield in two chunks to exercise the streaming path.
        half = len(STUB_REPLY) // 2
        yield STUB_REPLY[:half]
        await asyncio.sleep(0.05)
        yield STUB_REPLY[half:]
