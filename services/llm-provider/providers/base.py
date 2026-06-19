from abc import ABC, abstractmethod

class BaseLLMProvider(ABC):
    @abstractmethod
    async def generate(self, prompt: str, context: list[str]) -> str:
        pass
