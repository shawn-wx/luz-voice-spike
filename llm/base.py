"""Abstract interface every LLM vendor must implement."""
from abc import ABC, abstractmethod
from typing import AsyncIterator, Dict, List


class LLMProvider(ABC):
    #: short id used in LLM_PROVIDER and in latency reports
    name: str = "base"

    @abstractmethod
    def chat_stream(
        self,
        messages: List[Dict[str, str]],
        system: str,
        max_tokens: int = 150,
        temperature: float = 0.7,
    ) -> AsyncIterator[str]:
        """Yield text deltas as they arrive from the model.

        Must be async-generator; callers cancel it on barge-in, so it must
        react promptly to cancellation (close the HTTP stream).
        """
        raise NotImplementedError
        yield  # make it an async generator function

    def has_key(self) -> bool:
        """True if this provider is configured with credentials."""
        return True
