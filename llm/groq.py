"""Groq provider via its OpenAI-compatible chat API (streaming SSE).

Groq runs on LPU silicon: lowest TTFT class for short voice prompts.
Free tier needs no credit card. API: https://api.groq.com/openai/v1
"""
import json
import os

import httpx

from .base import LLMProvider

API_URL = os.environ.get("GROQ_API_URL",
                         "https://api.groq.com/openai/v1/chat/completions")
# NOTE (2026-10-01): llama-3.1-8b-instant / llama-3.3-70b-versatile became
# enterprise-only on 2026-08-16; gpt-oss-20b is the fastest self-serve model.
MODEL = os.environ.get("GROQ_MODEL", "openai/gpt-oss-20b")


class GroqProvider(LLMProvider):
    name = "groq"

    def __init__(self, api_key: str = None,
                 api_url: str = API_URL, model: str = MODEL):
        self.api_key = api_key or os.environ.get("GROQ_API_KEY", "")
        self.api_url = api_url
        self.model = model

    def has_key(self) -> bool:
        return bool(self.api_key)

    async def chat_stream(self, messages, system,
                          max_tokens=150, temperature=0.7):
        if not self.api_key:
            raise RuntimeError("GROQ_API_KEY not configured")
        payload = {
            "model": self.model,
            "messages": [{"role": "system", "content": system}] + messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "stream": True,
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
            async with client.stream("POST", self.api_url,
                                     json=payload, headers=headers) as resp:
                if resp.status_code != 200:
                    body = (await resp.aread())[:300].decode(
                        "utf-8", "replace")
                    raise RuntimeError(
                        f"Groq HTTP {resp.status_code}: {body}")
                async for line in resp.aiter_lines():
                    if not line.startswith("data:"):
                        continue
                    data = line[5:].strip()
                    if data == "[DONE]":
                        break
                    try:
                        evt = json.loads(data)
                    except ValueError:
                        continue
                    try:
                        delta = evt["choices"][0]["delta"].get("content", "")
                    except (KeyError, IndexError):
                        continue
                    if delta:
                        yield delta
